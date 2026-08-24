//! Process-isolated concurrent publication contract for one Range Engine.

use crate::{AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord, RangeServingState};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use okv_consensus::{
    sign_tagged_log_statement, tagged_log_public_key, CellLogSetMember, CellLogSetPolicy,
    CellMutation, CellTaggedLogCertificate, CellTaggedLogStatement, RequestIdentity,
};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_sim::{CommitEnvelope, CommitEnvelopeParts};
use okv_slate::{AuthorityManifestReference, SlateEngine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::config::Settings;
use slatedb::Db;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

const DATABASE_PATH: &str = "range-serving-concurrency";
const CELL_ID: [u8; 16] = [0x41; 16];
const TENANT_ID: [u8; 16] = [0x52; 16];
const GENERATION: u64 = 11;
const LOG_SET_ID: u16 = 17;
const BASE_VERSION: u64 = 2;
const FIRST_TARGET: u64 = 3;
const LAST_TARGET: u64 = 9;
const READER_TASKS: usize = 8;

pub const RANGE_SERVING_FIXTURE_CELL_ID: [u8; 16] = CELL_ID;
pub const RANGE_SERVING_FIXTURE_TENANT_ID: [u8; 16] = TENANT_ID;

/// Unsafe subject selected by the concurrent-publication eval lane.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeServingConcurrencyMode {
    Correct,
    AcceptStaleRollback,
    SkipReaderOverlap,
    AcceptMixedResult,
    SkipStaleProbe,
}

impl RangeServingConcurrencyMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AcceptStaleRollback => "accept_stale_rollback",
            Self::SkipReaderOverlap => "skip_reader_overlap",
            Self::AcceptMixedResult => "accept_mixed_result",
            Self::SkipStaleProbe => "skip_stale_probe",
        }
    }
}

/// Child-process inputs for one deterministic publication history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingConcurrencyConfig {
    pub seed: u64,
    pub mode: RangeServingConcurrencyMode,
}

/// Stable semantic and timing receipt from one child process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingConcurrencyReceipt {
    pub seed: u64,
    pub mode: RangeServingConcurrencyMode,
    pub reader_tasks: u64,
    pub publications: u64,
    pub retained_old_reads: u64,
    pub current_new_reads: u64,
    pub mixed_results: u64,
    pub stale_probe_attempted: bool,
    pub stale_probe_refused: bool,
    pub final_target: u64,
    pub publication_nanos: Vec<u64>,
    pub checks: BTreeMap<String, bool>,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

struct ReaderResult {
    old_exact: bool,
    new_exact: bool,
}

enum ReaderCommand {
    Retain {
        reply: oneshot::Sender<()>,
    },
    Verify {
        old_target: u64,
        new_target: u64,
        reply: oneshot::Sender<ReaderResult>,
    },
}

/// Run one concurrency history in a fresh process.
///
/// # Errors
///
/// Returns an error when the child cannot start or return a valid receipt.
pub fn run_range_serving_concurrency_contract(
    seed: u64,
    mode: RangeServingConcurrencyMode,
    executable: &Path,
) -> Result<RangeServingConcurrencyReceipt, String> {
    let config = serde_json::to_string(&RangeServingConcurrencyConfig { seed, mode })
        .map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("range-serving-concurrency-node")
        .arg("--config-json")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "concurrent Range Engine worker failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

/// Execute one deterministic same-base tail-publication history.
///
/// # Errors
///
/// Returns an error when real `SlateDB`, certificate, task, or read work fails.
#[allow(clippy::too_many_lines)]
pub async fn run_range_serving_concurrency_worker(
    config: RangeServingConcurrencyConfig,
) -> Result<RangeServingConcurrencyReceipt, String> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mutations = fixture_mutations();
    let envelopes = fixture_envelopes(&mutations);
    let engine = build_engine(Arc::clone(&store), config.seed).await?;
    for sequence in 1..=BASE_VERSION {
        engine
            .apply(model_batch(sequence, &mutations))
            .await
            .map_err(|error| error.to_string())?;
    }
    engine.flush().await.map_err(|error| error.to_string())?;
    let manifest = latest_manifest_reference(Arc::clone(&store)).await?;
    engine.close().await.map_err(|error| error.to_string())?;

    let (policy, signing_seeds) = log_policy()?;
    let policies = BTreeMap::from([(LOG_SET_ID, policy.clone())]);
    let records = envelopes
        .iter()
        .skip(usize::try_from(BASE_VERSION).map_err(|error| error.to_string())?)
        .map(|envelope| certified_record(envelope, &policy, &signing_seeds))
        .collect::<Result<Vec<_>, _>>()?;
    let root = AuthorityRangeRoot {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        manifest,
        covered_through: BASE_VERSION,
        minimum_readable_version: 1,
        log_chain_sha256: Sha256::digest(envelopes[1].encode()).into(),
    };

    let initial = open_view(
        Arc::clone(&store),
        &root,
        FIRST_TARGET,
        &records,
        &policies,
        config.seed ^ FIRST_TARGET,
    )
    .await?;
    let initial_token = initial.publication_token();
    let stale_candidate = open_view(
        Arc::clone(&store),
        &root,
        FIRST_TARGET,
        &records,
        &policies,
        config.seed ^ 0x5a5a,
    )
    .await?;
    let state = Arc::new(RangeServingState::new(initial));
    let mut readers = Vec::with_capacity(READER_TASKS);
    let mut senders = Vec::with_capacity(READER_TASKS);
    for _ in 0..READER_TASKS {
        let (sender, mut receiver) = mpsc::channel::<ReaderCommand>(2);
        senders.push(sender);
        let state = Arc::clone(&state);
        let mutations = mutations.clone();
        readers.push(tokio::spawn(async move {
            let mut retained = None;
            while let Some(command) = receiver.recv().await {
                match command {
                    ReaderCommand::Retain { reply } => {
                        retained = Some(state.current().map_err(|error| error.to_string())?);
                        let _ = reply.send(());
                    }
                    ReaderCommand::Verify {
                        old_target,
                        new_target,
                        reply,
                    } => {
                        let old_rows = match retained.take() {
                            Some(old) => old
                                .scan_at(&[], &[0xff], old_target, usize::MAX)
                                .await
                                .map_err(|error| error.to_string())?,
                            None => Vec::new(),
                        };
                        let current = state.current().map_err(|error| error.to_string())?;
                        let new_rows = current
                            .scan_at(&[], &[0xff], new_target, usize::MAX)
                            .await
                            .map_err(|error| error.to_string())?;
                        let _ = reply.send(ReaderResult {
                            old_exact: old_rows == expected_rows(&mutations, old_target),
                            new_exact: new_rows == expected_rows(&mutations, new_target),
                        });
                    }
                }
            }
            Ok::<(), String>(())
        }));
    }

    let mut retained_old_reads = 0_u64;
    let mut current_new_reads = 0_u64;
    let mut mixed_results = 0_u64;
    let mut publication_nanos = Vec::new();
    let mut expected_token = initial_token.clone();
    for target in (FIRST_TARGET + 1)..=LAST_TARGET {
        if config.mode != RangeServingConcurrencyMode::SkipReaderOverlap {
            let mut replies = Vec::with_capacity(READER_TASKS);
            for sender in &senders {
                let (reply, receive) = oneshot::channel();
                sender
                    .send(ReaderCommand::Retain { reply })
                    .await
                    .map_err(|error| error.to_string())?;
                replies.push(receive);
            }
            for reply in replies {
                reply.await.map_err(|error| error.to_string())?;
            }
        }

        let replacement = open_view(
            Arc::clone(&store),
            &root,
            target,
            &records,
            &policies,
            config.seed ^ target,
        )
        .await?;
        let replacement_token = replacement.publication_token();
        let started = Instant::now();
        state
            .install_if_current(&expected_token, replacement)
            .map_err(|error| error.to_string())?;
        publication_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));

        let mut replies = Vec::with_capacity(READER_TASKS);
        for sender in &senders {
            let (reply, receive) = oneshot::channel();
            sender
                .send(ReaderCommand::Verify {
                    old_target: target - 1,
                    new_target: target,
                    reply,
                })
                .await
                .map_err(|error| error.to_string())?;
            replies.push(receive);
        }
        for reply in replies {
            let result = reply.await.map_err(|error| error.to_string())?;
            retained_old_reads = retained_old_reads.saturating_add(u64::from(result.old_exact));
            current_new_reads = current_new_reads.saturating_add(u64::from(result.new_exact));
            mixed_results =
                mixed_results.saturating_add(u64::from(!result.old_exact || !result.new_exact));
        }
        expected_token = replacement_token;
    }

    drop(senders);
    for reader in readers {
        reader
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
    }

    if config.mode == RangeServingConcurrencyMode::AcceptMixedResult {
        mixed_results = mixed_results.saturating_add(1);
    }
    let stale_probe_attempted = config.mode != RangeServingConcurrencyMode::SkipStaleProbe;
    let stale_probe_refused = if !stale_probe_attempted {
        false
    } else if config.mode == RangeServingConcurrencyMode::AcceptStaleRollback {
        state
            .install_if_current(&expected_token, stale_candidate)
            .is_err()
    } else {
        state
            .install_if_current(&initial_token, stale_candidate)
            .is_err()
    };
    let final_target = state
        .current()
        .map_err(|error| error.to_string())?
        .target_version();
    let publications = LAST_TARGET.saturating_sub(FIRST_TARGET);
    let expected_reads =
        publications.saturating_mul(u64::try_from(READER_TASKS).unwrap_or(u64::MAX));
    let checks = BTreeMap::from([
        (
            "reader_overlap_exercised".to_owned(),
            retained_old_reads == expected_reads,
        ),
        (
            "retained_old_views_exact".to_owned(),
            retained_old_reads == expected_reads,
        ),
        (
            "current_new_views_exact".to_owned(),
            current_new_reads == expected_reads,
        ),
        ("no_mixed_results".to_owned(), mixed_results == 0),
        ("stale_probe_exercised".to_owned(), stale_probe_attempted),
        (
            "stale_same_base_rollback_refused".to_owned(),
            stale_probe_refused,
        ),
        ("final_target_exact".to_owned(), final_target == LAST_TARGET),
    ]);
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let semantic = (
        config.seed,
        config.mode,
        publications,
        retained_old_reads,
        current_new_reads,
        mixed_results,
        stale_probe_attempted,
        stale_probe_refused,
        final_target,
        &checks,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(RangeServingConcurrencyReceipt {
        seed: config.seed,
        mode: config.mode,
        reader_tasks: u64::try_from(READER_TASKS).unwrap_or(u64::MAX),
        publications,
        retained_old_reads,
        current_new_reads,
        mixed_results,
        stale_probe_attempted,
        stale_probe_refused,
        final_target,
        publication_nanos,
        checks,
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

async fn build_engine(store: Arc<dyn ObjectStore>, seed: u64) -> Result<SlateEngine, String> {
    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };
    Db::builder(DATABASE_PATH, store)
        .with_settings(settings)
        .with_seed(seed)
        .build()
        .await
        .map(SlateEngine::new)
        .map_err(|error| error.to_string())
}

async fn open_view(
    store: Arc<dyn ObjectStore>,
    root: &AuthorityRangeRoot,
    target: u64,
    records: &[CertifiedTxLogRecord],
    policies: &BTreeMap<u16, CellLogSetPolicy>,
    seed: u64,
) -> Result<AuthorityBoundRangeView, String> {
    let count =
        usize::try_from(target.saturating_sub(BASE_VERSION)).map_err(|error| error.to_string())?;
    AuthorityBoundRangeView::open(
        DATABASE_PATH,
        store,
        root.clone(),
        target,
        records[..count].to_vec(),
        policies,
        seed,
    )
    .await
    .map_err(|error| error.to_string())
}

fn fixture_mutations() -> BTreeMap<u64, Vec<CellMutation>> {
    let mut fixture = BTreeMap::from([
        (
            1,
            vec![
                CellMutation::Set {
                    key: b"a".to_vec(),
                    value: b"a1".to_vec(),
                },
                CellMutation::Set {
                    key: b"b".to_vec(),
                    value: b"b1".to_vec(),
                },
            ],
        ),
        (
            2,
            vec![
                CellMutation::Set {
                    key: b"a".to_vec(),
                    value: b"a2".to_vec(),
                },
                CellMutation::Set {
                    key: b"c".to_vec(),
                    value: b"c2".to_vec(),
                },
            ],
        ),
    ]);
    for sequence in FIRST_TARGET..=LAST_TARGET {
        let mut mutations = vec![CellMutation::Set {
            key: b"a".to_vec(),
            value: format!("a{sequence}").into_bytes(),
        }];
        if sequence == 6 {
            mutations.push(CellMutation::Clear { key: b"c".to_vec() });
        } else {
            mutations.push(CellMutation::Set {
                key: format!("k{sequence}").into_bytes(),
                value: format!("v{sequence}").into_bytes(),
            });
        }
        fixture.insert(sequence, mutations);
    }
    fixture
}

fn fixture_envelopes(mutations: &BTreeMap<u64, Vec<CellMutation>>) -> Vec<CommitEnvelope> {
    fixture_envelopes_through(mutations, LAST_TARGET).expect("fixture envelopes encode")
}

fn fixture_envelopes_through(
    mutations: &BTreeMap<u64, Vec<CellMutation>>,
    target: u64,
) -> Result<Vec<CommitEnvelope>, String> {
    let mut previous_log_chain = [0_u8; 32];
    let mut envelopes = Vec::new();
    for sequence in 1..=target {
        let mut client_id = [0_u8; 16];
        client_id[8..].copy_from_slice(&sequence.to_be_bytes());
        let sequence_mutations = mutations
            .get(&sequence)
            .ok_or_else(|| format!("fixture has no mutation batch at version {sequence}"))?;
        let envelope = CommitEnvelope::from_parts(CommitEnvelopeParts {
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            generation: GENERATION,
            version: Version::from_parts(GENERATION, sequence),
            log_index: sequence,
            client_id,
            request_id: sequence,
            resolver_set_id: [0x63; 16],
            read_conflicts: Vec::new(),
            write_conflicts: Vec::new(),
            canonical_mutations: serde_json::to_vec(sequence_mutations)
                .map_err(|error| error.to_string())?,
            required_resolvers: vec![1],
            required_log_tags: vec![LOG_SET_ID],
            previous_log_chain,
        });
        previous_log_chain = Sha256::digest(envelope.encode()).into();
        envelopes.push(envelope);
    }
    Ok(envelopes)
}

fn model_batch(sequence: u64, mutations: &BTreeMap<u64, Vec<CellMutation>>) -> CommitBatch {
    CommitBatch {
        version: Version::new(sequence),
        identity: CommitIdentity::for_test(sequence),
        mutations: mutations[&sequence]
            .iter()
            .map(|mutation| match mutation {
                CellMutation::Clear { key } => Mutation::Clear { key: key.clone() },
                CellMutation::Set { key, value } => Mutation::Set {
                    key: key.clone(),
                    value: value.clone(),
                },
            })
            .collect(),
    }
}

fn expected_rows(
    mutations: &BTreeMap<u64, Vec<CellMutation>>,
    target: u64,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = BTreeMap::new();
    for sequence in 1..=target {
        for mutation in &mutations[&sequence] {
            match mutation {
                CellMutation::Clear { key } => {
                    rows.remove(key);
                }
                CellMutation::Set { key, value } => {
                    rows.insert(key.clone(), value.clone());
                }
            }
        }
    }
    rows.into_iter().collect()
}

async fn latest_manifest_reference(
    store: Arc<dyn ObjectStore>,
) -> Result<AuthorityManifestReference, String> {
    use futures_util::TryStreamExt;
    use object_store::path::Path as ObjectPath;

    let prefix = ObjectPath::from(format!("{DATABASE_PATH}/manifest"));
    let manifests = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| error.to_string())?;
    let latest = manifests
        .into_iter()
        .max_by(|left, right| left.location.cmp(&right.location))
        .ok_or_else(|| "SlateDB fixture emitted no manifest".to_owned())?;
    let bytes = store
        .get(&latest.location)
        .await
        .map_err(|error| error.to_string())?
        .bytes()
        .await
        .map_err(|error| error.to_string())?;
    Ok(AuthorityManifestReference {
        key: latest.location.to_string(),
        length: u64::try_from(bytes.len()).map_err(|error| error.to_string())?,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    })
}

fn log_policy() -> Result<(CellLogSetPolicy, BTreeMap<u64, Vec<u8>>), String> {
    let seeds = BTreeMap::from([
        (101, vec![0x11; 32]),
        (102, vec![0x22; 32]),
        (103, vec![0x33; 32]),
    ]);
    let members = seeds
        .iter()
        .map(|(node_id, seed)| {
            tagged_log_public_key(seed)
                .map(|public_key| CellLogSetMember {
                    node_id: *node_id,
                    public_key,
                })
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        CellLogSetPolicy {
            format_version: 1,
            generation: GENERATION,
            policy_epoch: 1,
            log_set_id: LOG_SET_ID,
            quorum_size: 2,
            ratekeeper_soft_limit_bytes: 4096,
            members,
        },
        seeds,
    ))
}

fn certified_record(
    envelope: &CommitEnvelope,
    policy: &CellLogSetPolicy,
    seeds: &BTreeMap<u64, Vec<u8>>,
) -> Result<CertifiedTxLogRecord, String> {
    let encoded = envelope.encode();
    let (encoded_client_id, request_id) = envelope.client_identity();
    let mut client_id = [0_u8; 8];
    client_id.copy_from_slice(&encoded_client_id[8..]);
    let statement = CellTaggedLogStatement {
        format_version: 1,
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        transaction_identity: RequestIdentity {
            client_id: u64::from_be_bytes(client_id),
            request_id,
        },
        commit_sequence: envelope.version().sequence(),
        log_set_id: LOG_SET_ID,
        policy_epoch: policy.policy_epoch,
        envelope_sha256: Sha256::digest(&encoded).into(),
        durable_position: envelope.version().sequence(),
    };
    let attestations = seeds
        .iter()
        .take(2)
        .map(|(node_id, seed)| {
            sign_tagged_log_statement(*node_id, seed, &statement).map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CertifiedTxLogRecord {
        envelope: encoded,
        certificates: vec![CellTaggedLogCertificate {
            statement,
            attestations,
        }],
    })
}

pub(crate) async fn build_final_range_serving_state(
    seed: u64,
) -> Result<Arc<RangeServingState>, String> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mutations = fixture_mutations();
    let envelopes = fixture_envelopes(&mutations);
    let engine = build_engine(Arc::clone(&store), seed).await?;
    for sequence in 1..=BASE_VERSION {
        engine
            .apply(model_batch(sequence, &mutations))
            .await
            .map_err(|error| error.to_string())?;
    }
    engine.flush().await.map_err(|error| error.to_string())?;
    let manifest = latest_manifest_reference(Arc::clone(&store)).await?;
    engine.close().await.map_err(|error| error.to_string())?;
    let (policy, signing_seeds) = log_policy()?;
    let policies = BTreeMap::from([(LOG_SET_ID, policy.clone())]);
    let records = envelopes
        .iter()
        .skip(usize::try_from(BASE_VERSION).map_err(|error| error.to_string())?)
        .map(|envelope| certified_record(envelope, &policy, &signing_seeds))
        .collect::<Result<Vec<_>, _>>()?;
    let root = AuthorityRangeRoot {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        manifest,
        covered_through: BASE_VERSION,
        minimum_readable_version: 1,
        log_chain_sha256: Sha256::digest(envelopes[1].encode()).into(),
    };
    let view = open_view(
        store,
        &root,
        LAST_TARGET,
        &records,
        &policies,
        seed ^ LAST_TARGET,
    )
    .await?;
    Ok(Arc::new(RangeServingState::new(view)))
}

/// Build one real authority-bound in-memory Range Engine fixture from complete
/// versioned mutation batches.
///
/// This helper exists for independent adapter process gates. It uses the same
/// pinned `SlateDB` base, signed txLog records, and immutable serving view as
/// the normal range-serving contracts.
///
/// # Errors
///
/// Returns an error for an invalid frontier, a missing version, storage
/// construction, or certificate verification failure.
pub async fn build_fixture_range_serving_state(
    seed: u64,
    base_version: u64,
    target_version: u64,
    mutations: &BTreeMap<u64, Vec<CellMutation>>,
) -> Result<Arc<RangeServingState>, String> {
    if base_version == 0 || target_version < base_version {
        return Err("fixture requires 0 < base version <= target version".to_owned());
    }
    for sequence in 1..=target_version {
        if !mutations.contains_key(&sequence) {
            return Err(format!(
                "fixture has no mutation batch at version {sequence}"
            ));
        }
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let envelopes = fixture_envelopes_through(mutations, target_version)?;
    let engine = build_engine(Arc::clone(&store), seed).await?;
    for sequence in 1..=base_version {
        engine
            .apply(model_batch(sequence, mutations))
            .await
            .map_err(|error| error.to_string())?;
    }
    engine.flush().await.map_err(|error| error.to_string())?;
    let manifest = latest_manifest_reference(Arc::clone(&store)).await?;
    engine.close().await.map_err(|error| error.to_string())?;
    let (policy, signing_seeds) = log_policy()?;
    let policies = BTreeMap::from([(LOG_SET_ID, policy.clone())]);
    let records = envelopes
        .iter()
        .skip(usize::try_from(base_version).map_err(|error| error.to_string())?)
        .map(|envelope| certified_record(envelope, &policy, &signing_seeds))
        .collect::<Result<Vec<_>, _>>()?;
    let base_index =
        usize::try_from(base_version.saturating_sub(1)).map_err(|error| error.to_string())?;
    let root = AuthorityRangeRoot {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        generation: GENERATION,
        manifest,
        covered_through: base_version,
        minimum_readable_version: 1,
        log_chain_sha256: Sha256::digest(envelopes[base_index].encode()).into(),
    };
    let view = AuthorityBoundRangeView::open(
        DATABASE_PATH,
        store,
        root,
        target_version,
        records,
        &policies,
        seed ^ target_version,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(Arc::new(RangeServingState::new(view)))
}

pub(crate) fn final_range_serving_rows() -> Vec<(Vec<u8>, Vec<u8>)> {
    range_serving_rows_at(LAST_TARGET)
}

pub(crate) fn range_serving_rows_at(target: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    expected_rows(&fixture_mutations(), target)
}
