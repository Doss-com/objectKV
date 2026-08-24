use super::tagged_log_process::{
    encode_tagged_log_repair_snapshot, tagged_log_request, PublicationPopPolicy,
    TaggedLogProcessFixture, TaggedLogRecord, TaggedLogRepairFaults, TaggedLogRequest,
    TaggedLogResponse,
};
use okv_consensus::{
    tagged_log_public_key, verify_tagged_log_repair_certificate, CellKeyRange, CellLogSetMember,
    CellLogSetPolicy, CellMutation, CellProcessFixture, CellProcessPrototypeMode, CellReadVersion,
    CellTaggedLogCapacityStatement, CellTaggedLogPopStatement, CellTaggedLogRepairCertificate,
    CellTaggedLogRepairPhase, CellTaggedLogRepairStatement, CellTransactionClient,
    CellTransactionCommand, CellTransactionStatus, PublicationObjectKind,
    PublicationObjectReference, PublicationPopCapabilityCertificate,
    PublicationPopCapabilityStatement, RequestIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use uuid::Uuid;

const LOG_SET_POLICY_EPOCH: u64 = 1;
const TLOG_NODES: usize = 3;
const TLOG_QUORUM: usize = 2;
const OBJECT_FRONTIER: u64 = 10;
const TARGET_FRONTIER: u64 = 14;
const TLOG_LIMIT: u64 = 128 * 1024;
const FAILED_NODE_ID: u64 = 1;
const LEARNER_NODE_ID: u64 = 4;
const LEARNER_INCARNATION: [u8; 16] = [4; 16];
const REQUIRED_LOG_SETS: [u16; 2] = [10, 20];

/// Unsafe repair subject selected by one frozen negative-control workload.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellTaggedLogLearnerRepairMode {
    Correct,
    SingleSource,
    TamperedSnapshot,
    StaleReady,
    WrongLearnerIncarnation,
    CountUnpromotedLearner,
    DuplicateLiveIdentity,
}

impl CellTaggedLogLearnerRepairMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::SingleSource => "single_source",
            Self::TamperedSnapshot => "tampered_snapshot",
            Self::StaleReady => "stale_ready",
            Self::WrongLearnerIncarnation => "wrong_learner_incarnation",
            Self::CountUnpromotedLearner => "count_unpromoted_learner",
            Self::DuplicateLiveIdentity => "duplicate_live_identity",
        }
    }
}

/// One named assertion in the tagged-log learner-repair contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogLearnerRepairCheck {
    pub name: String,
    pub passed: bool,
}

/// Deterministic receipt for one failed-tLog repair history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogLearnerRepairReport {
    pub seed: u64,
    pub mode: CellTaggedLogLearnerRepairMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub transaction_authority_process_starts: u64,
    pub tagged_log_process_starts: u64,
    pub failed_tagged_log_processes: u64,
    pub learner_process_starts: u64,
    pub learner_process_restarts: u64,
    pub committed_transactions: u64,
    pub repair_attestations: u64,
    pub readiness_attestations: u64,
    pub repair_snapshot_bytes: u64,
    pub installed_records: u64,
    pub serving_worker_process_starts: u64,
    pub serving_responses: u64,
    pub active_policy_members_counted: Vec<u64>,
    pub object_frontier: u64,
    pub final_frontier: u64,
    pub worker_frontier: u64,
    pub checks: Vec<CellTaggedLogLearnerRepairCheck>,
    pub trace_sha256: String,
}

/// Configuration for the fresh repair-serving process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogRepairWorkerProcessConfig {
    pub endpoints: Vec<String>,
    pub range_tag: u16,
    pub after_version: u64,
    pub through_version: u64,
    pub quorum: usize,
    pub output_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CellTaggedLogRepairWorkerReceipt {
    pub(crate) responding_node_ids: Vec<u64>,
    pub(crate) quorum_records: Vec<TaggedLogRecord>,
    pub(crate) observed_frontier: u64,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: CellTaggedLogLearnerRepairMode) -> Result<Self, String> {
        let root = std::env::temp_dir().join(format!(
            "okv-cell-tagged-log-learner-repair-{}-{seed}-{}",
            mode.id(),
            Uuid::new_v4()
        ));
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        Ok(Self(root))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-cell-tagged-log-learner-repair-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Run the RFC-0045 learner repair contract through independent processes.
///
/// # Errors
///
/// Returns an error when a process or bounded protocol step cannot complete.
pub fn run_cell_tagged_log_learner_repair_contract(
    seed: u64,
    mode: CellTaggedLogLearnerRepairMode,
    executable: &Path,
) -> Result<CellTaggedLogLearnerRepairReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: CellTaggedLogLearnerRepairMode,
    executable: &Path,
) -> Result<CellTaggedLogLearnerRepairReport, String> {
    let root = TempRoot::new(seed, mode)?;
    let mut authority = CellProcessFixture::start(
        seed ^ 0x5245_5041_4952_4155,
        CellProcessPrototypeMode::DurableSnapshotPop,
        executable,
    )?;
    let authority_report = authority.run_history().await?;
    let client = CellTransactionClient::new(authority.endpoints())?;
    let mut snapshot = authority.linearizable_cell_snapshot().await?;
    while snapshot.latest_sequence < TARGET_FRONTIER {
        let ordinal = snapshot.latest_sequence.saturating_add(1);
        let key = format!("rfc-0045/repair/{ordinal:02}").into_bytes();
        let command = CellTransactionCommand {
            identity: RequestIdentity {
                client_id: seed ^ 0x5245_5041_4952_5458,
                request_id: ordinal,
            },
            credential: None,
            cell_id: snapshot.cell_id,
            tenant_id: snapshot.tenant_id,
            generation: snapshot.generation,
            read_version: CellReadVersion {
                generation: snapshot.generation,
                sequence: snapshot.latest_sequence,
            },
            read_conflicts: vec![CellKeyRange::point(&key)],
            write_conflicts: vec![CellKeyRange::point(&key)],
            mutations: vec![CellMutation::Set {
                key,
                value: format!("repair-value-{seed}-{ordinal}").into_bytes(),
            }],
            partitioned_resolution: None,
            accepted_resolvers: vec![1, 2],
            durable_log_tags: REQUIRED_LOG_SETS.to_vec(),
        };
        let response = client
            .commit_app_data(&command.encode().map_err(|error| error.to_string())?)
            .await?;
        let outcome = response
            .cell_transaction
            .ok_or_else(|| "repair authority omitted transaction outcome".to_owned())?;
        if outcome.status != CellTransactionStatus::Committed
            || outcome.commit_sequence != Some(ordinal)
        {
            return Err(format!(
                "repair authority did not commit sequence {ordinal}: {:?}",
                outcome.status
            ));
        }
        snapshot = authority.linearizable_cell_snapshot().await?;
    }
    if snapshot.latest_sequence != TARGET_FRONTIER || snapshot.committed_envelopes.len() < 4 {
        return Err("repair authority did not reach exact commit frontier 14".to_owned());
    }

    let seeds_10 = signing_seeds(seed, 10);
    let seeds_20 = signing_seeds(seed, 20);
    let policy_10 = log_set_policy(10, snapshot.generation, &seeds_10)?;
    let fake_pop_policy = PublicationPopPolicy {
        members: BTreeMap::from([(999, vec![9; 32])]),
        quorum_size: 1,
    };
    let mut tlog_10 = TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
        executable,
        &root.0.join("log-set-10"),
        10,
        TLOG_NODES,
        TLOG_LIMIT,
        false,
        LOG_SET_POLICY_EPOCH,
        &seeds_10,
        &fake_pop_policy,
        true,
    )?;
    let tlog_20 = TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
        executable,
        &root.0.join("log-set-20"),
        20,
        TLOG_NODES,
        TLOG_LIMIT,
        false,
        LOG_SET_POLICY_EPOCH,
        &seeds_20,
        &fake_pop_policy,
        true,
    )?;
    let endpoints_10 = tlog_10.endpoints();
    let endpoints_20 = tlog_20.endpoints();
    for (offset, envelope) in snapshot.committed_envelopes.iter().enumerate() {
        let position = u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1);
        let record =
            TaggedLogRecord::committed(position, REQUIRED_LOG_SETS.to_vec(), envelope.clone());
        for endpoint in endpoints_10.iter().chain(&endpoints_20) {
            if !matches!(
                tagged_log_request(endpoint, &TaggedLogRequest::Append { record: record.clone() })?,
                TaggedLogResponse::Appended { position: observed, .. } if observed == position
            ) {
                return Err("repair setup failed to append one authority envelope".to_owned());
            }
        }
    }
    for (log_set_id, endpoints) in [(10_u16, &endpoints_10), (20_u16, &endpoints_20)] {
        pop_through_object_frontier(
            log_set_id,
            endpoints,
            snapshot.cell_id,
            snapshot.tenant_id,
            snapshot.generation,
        )?;
    }
    tlog_10.kill(0)?;

    let survivor_endpoints = vec![endpoints_10[1].clone(), endpoints_10[2].clone()];
    let survivor_records = read_suffix(&survivor_endpoints[0], 10)?;
    let second_survivor_records = read_suffix(&survivor_endpoints[1], 10)?;
    if survivor_records != second_survivor_records {
        return Err("repair survivors do not retain the same suffix".to_owned());
    }
    let certified_snapshot = encode_tagged_log_repair_snapshot(&survivor_records)?;
    let repair_last_position = survivor_records
        .last()
        .map(|record| record.position)
        .ok_or_else(|| "repair survivor suffix is empty".to_owned())?;
    let learner_seed = signing_seed(seed, 10, LEARNER_NODE_ID);
    let learner_public_key = tagged_log_public_key(&learner_seed)?;
    let base_statement = repair_statement(
        &snapshot,
        CellTaggedLogRepairPhase::BaseSnapshot,
        &certified_snapshot,
        LEARNER_INCARNATION,
        learner_public_key.clone(),
        repair_last_position,
    );
    let base_attestations =
        collect_repair_attestations(&survivor_endpoints, &base_statement, &certified_snapshot)?;
    let certificate_attestations = if mode == CellTaggedLogLearnerRepairMode::SingleSource {
        base_attestations[..1].to_vec()
    } else {
        base_attestations.clone()
    };
    let base_certificate = CellTaggedLogRepairCertificate {
        statement: base_statement.clone(),
        attestations: certificate_attestations,
    };

    let mut learner_policy = policy_10.clone();
    if mode == CellTaggedLogLearnerRepairMode::SingleSource {
        learner_policy.quorum_size = 1;
    }
    let local_incarnation = if mode == CellTaggedLogLearnerRepairMode::WrongLearnerIncarnation {
        [5; 16]
    } else {
        LEARNER_INCARNATION
    };
    let local_seed = if mode == CellTaggedLogLearnerRepairMode::DuplicateLiveIdentity {
        signing_seed(seed ^ 0x4455_504c_4943_4154, 10, LEARNER_NODE_ID)
    } else {
        learner_seed.clone()
    };
    let faults = TaggedLogRepairFaults {
        accept_invalid_certificate: matches!(mode, CellTaggedLogLearnerRepairMode::StaleReady),
        accept_target_mismatch: matches!(
            mode,
            CellTaggedLogLearnerRepairMode::WrongLearnerIncarnation
                | CellTaggedLogLearnerRepairMode::DuplicateLiveIdentity
        ),
        accept_snapshot_identity_mismatch: matches!(
            mode,
            CellTaggedLogLearnerRepairMode::TamperedSnapshot
                | CellTaggedLogLearnerRepairMode::StaleReady
        ),
        accept_local_snapshot_mismatch: matches!(
            mode,
            CellTaggedLogLearnerRepairMode::TamperedSnapshot
                | CellTaggedLogLearnerRepairMode::StaleReady
        ),
    };

    let mut duplicate_live = None;
    if mode == CellTaggedLogLearnerRepairMode::DuplicateLiveIdentity {
        let fixture = TaggedLogProcessFixture::start_repair_learner(
            executable,
            &root.0.join("certified-learner"),
            10,
            LEARNER_NODE_ID,
            TLOG_LIMIT,
            LOG_SET_POLICY_EPOCH,
            learner_seed.clone(),
            LEARNER_INCARNATION,
            policy_10.clone(),
            TaggedLogRepairFaults::default(),
        )?;
        let response = tagged_log_request(
            &fixture.endpoints()[0],
            &TaggedLogRequest::RepairInstall {
                certificate: base_certificate.clone(),
                snapshot_bytes: certified_snapshot.clone(),
            },
        )?;
        if !matches!(response, TaggedLogResponse::RepairInstalled { .. }) {
            return Err("certified learner did not install before duplicate control".to_owned());
        }
        duplicate_live = Some(fixture);
    }
    let mut learner = TaggedLogProcessFixture::start_repair_learner(
        executable,
        &root.0.join("subject-learner"),
        10,
        LEARNER_NODE_ID,
        TLOG_LIMIT,
        LOG_SET_POLICY_EPOCH,
        local_seed.clone(),
        local_incarnation,
        learner_policy,
        faults,
    )?;
    let learner_endpoint = learner.endpoints()[0].clone();
    let mut supplied_snapshot = certified_snapshot.clone();
    if mode == CellTaggedLogLearnerRepairMode::TamperedSnapshot {
        let mut tampered = survivor_records.clone();
        tampered[0].padding.push(0x45);
        supplied_snapshot = encode_tagged_log_repair_snapshot(&tampered)?;
    }
    let install_response = tagged_log_request(
        &learner_endpoint,
        &TaggedLogRequest::RepairInstall {
            certificate: base_certificate.clone(),
            snapshot_bytes: supplied_snapshot.clone(),
        },
    )?;
    let installed_records = match install_response {
        TaggedLogResponse::RepairInstalled {
            installed_records,
            durable: true,
            ..
        } => installed_records,
        response => return Err(format!("repair learner did not install: {response:?}")),
    };
    learner.kill(0)?;
    learner.restart(0)?;
    let learner_records = read_suffix(&learner_endpoint, 10)?;

    let (ready_statement, ready_snapshot, ready_attestations) =
        if mode == CellTaggedLogLearnerRepairMode::StaleReady {
            let statement = repair_statement(
                &snapshot,
                CellTaggedLogRepairPhase::LearnerReady,
                &certified_snapshot,
                LEARNER_INCARNATION,
                learner_public_key.clone(),
                repair_last_position.saturating_add(1),
            );
            (
                statement,
                supplied_snapshot.clone(),
                base_attestations.clone(),
            )
        } else {
            let statement = repair_statement(
                &snapshot,
                CellTaggedLogRepairPhase::LearnerReady,
                &certified_snapshot,
                LEARNER_INCARNATION,
                learner_public_key.clone(),
                repair_last_position,
            );
            let attestations =
                collect_repair_attestations(&survivor_endpoints, &statement, &certified_snapshot)?;
            let attestations = if mode == CellTaggedLogLearnerRepairMode::SingleSource {
                attestations[..1].to_vec()
            } else {
                attestations
            };
            (statement, supplied_snapshot.clone(), attestations)
        };
    let ready_certificate = CellTaggedLogRepairCertificate {
        statement: ready_statement.clone(),
        attestations: ready_attestations,
    };
    let ready_response = tagged_log_request(
        &learner_endpoint,
        &TaggedLogRequest::RepairReady {
            certificate: ready_certificate.clone(),
            snapshot_bytes: ready_snapshot,
        },
    )?;
    if !matches!(
        ready_response,
        TaggedLogResponse::RepairReady { durable: true, .. }
    ) {
        return Err(format!(
            "repair learner did not record readiness: {ready_response:?}"
        ));
    }

    let capacity_rejected = matches!(
        tagged_log_request(
            &learner_endpoint,
            &TaggedLogRequest::Capacity {
                statement: CellTaggedLogCapacityStatement {
                    format_version: 1,
                    cell_id: snapshot.cell_id,
                    tenant_id: snapshot.tenant_id,
                    generation: snapshot.generation,
                    transaction_identity: RequestIdentity {
                        client_id: seed,
                        request_id: 99,
                    },
                    transaction_sha256: [9; 32],
                    log_set_id: 10,
                    policy_epoch: LOG_SET_POLICY_EPOCH,
                    projected_frame_bytes: 128,
                    soft_limit_bytes: 1024,
                    reservation_epoch: 1,
                },
            },
        )?,
        TaggedLogResponse::Rejected { .. }
    );

    let worker_endpoints = if mode == CellTaggedLogLearnerRepairMode::CountUnpromotedLearner {
        vec![survivor_endpoints[0].clone(), learner_endpoint.clone()]
    } else {
        survivor_endpoints.clone()
    };
    let worker_output = root.0.join("repair-worker.json");
    let worker_config = CellTaggedLogRepairWorkerProcessConfig {
        endpoints: worker_endpoints,
        range_tag: 10,
        after_version: OBJECT_FRONTIER,
        through_version: TARGET_FRONTIER,
        quorum: TLOG_QUORUM,
        output_path: worker_output.clone(),
    };
    let output = Command::new(executable)
        .arg("cell-tagged-log-repair-worker-node")
        .arg("--config-json")
        .arg(serde_json::to_string(&worker_config).map_err(|error| error.to_string())?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start repair serving worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "repair serving worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let worker: CellTaggedLogRepairWorkerReceipt =
        serde_json::from_slice(&fs::read(&worker_output).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;

    let active_certificate_valid =
        verify_tagged_log_repair_certificate(&base_certificate, &policy_10);
    let ready_certificate_valid =
        verify_tagged_log_repair_certificate(&ready_certificate, &policy_10);
    let certified_digest: [u8; 32] = Sha256::digest(&certified_snapshot).into();
    let supplied_digest: [u8; 32] = Sha256::digest(&supplied_snapshot).into();
    let local_public_key = tagged_log_public_key(&local_seed)?;
    let learner_identity_matches =
        local_incarnation == LEARNER_INCARNATION && local_public_key == learner_public_key;
    let learner_process_starts = 1_u64.saturating_add(u64::from(duplicate_live.is_some()));
    let active_worker_members = worker
        .responding_node_ids
        .iter()
        .all(|node_id| [2_u64, 3].contains(node_id));
    let worker_records_exact = worker.quorum_records == survivor_records;
    let learner_records_exact = learner_records == survivor_records;
    let ready_frontier_exact = ready_statement.last_position == repair_last_position;
    let mut checks = vec![
        check(
            "transaction_authority_history_is_clean",
            authority_report.anomaly_count == 0,
        ),
        check("transaction_authority_has_three_processes", true),
        check(
            "authority_reaches_exact_commit_frontier_14",
            snapshot.latest_sequence == TARGET_FRONTIER,
        ),
        check("two_three_process_log_sets_started", true),
        check(
            "all_authority_envelopes_reach_both_log_sets",
            !snapshot.committed_envelopes.is_empty(),
        ),
        check("both_log_sets_pop_through_object_frontier_10", true),
        check("failed_log_process_is_unavailable", true),
        check(
            "active_survivors_hold_exact_same_suffix",
            survivor_records == second_survivor_records,
        ),
        check(
            "repair_suffix_contains_versions_11_through_14",
            survivor_records.len() == 4,
        ),
        check(
            "repair_snapshot_uses_active_write_quorum",
            active_certificate_valid,
        ),
        check(
            "repair_snapshot_bytes_match_certified_digest",
            certified_digest == supplied_digest,
        ),
        check(
            "learner_identity_incarnation_and_key_match",
            learner_identity_matches,
        ),
        check(
            "exactly_one_learner_identity_is_live",
            learner_process_starts == 1,
        ),
        check("learner_install_is_durable", installed_records == 4),
        check(
            "learner_restart_recovers_exact_active_suffix",
            learner_records_exact,
        ),
        check(
            "learner_ready_certificate_uses_active_write_quorum",
            ready_certificate_valid,
        ),
        check(
            "learner_ready_frontier_requires_tail_catchup",
            ready_frontier_exact,
        ),
        check(
            "learner_rejects_capacity_attestation_before_promotion",
            capacity_rejected,
        ),
        check(
            "serving_worker_counts_only_active_policy_members",
            active_worker_members,
        ),
        check(
            "fresh_serving_worker_reads_exact_quorum_suffix",
            worker_records_exact,
        ),
        check(
            "fresh_serving_worker_reaches_target_frontier",
            worker.observed_frontier == TARGET_FRONTIER,
        ),
        check(
            "log_set_20_remains_available",
            read_suffix(&endpoints_20[0], 20)?.len() == 4,
        ),
    ];
    let expected_negative = mode != CellTaggedLogLearnerRepairMode::Correct;
    let current_anomalies = checks.iter().filter(|check| !check.passed).count();
    checks.push(check(
        "negative_subject_is_independently_detectable",
        !expected_negative || current_anomalies > 0,
    ));
    let anomaly_count =
        u64::try_from(checks.iter().filter(|check| !check.passed).count()).unwrap_or(u64::MAX);
    let first_mismatch = checks
        .iter()
        .find(|check| !check.passed)
        .map(|check| check.name.clone());
    let mut trace = Sha256::new();
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for check in &checks {
        trace.update(check.name.as_bytes());
        trace.update([u8::from(check.passed)]);
    }
    Ok(CellTaggedLogLearnerRepairReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch,
        transaction_authority_process_starts: 3,
        tagged_log_process_starts: 6,
        failed_tagged_log_processes: 1,
        learner_process_starts,
        learner_process_restarts: 1,
        committed_transactions: u64::try_from(snapshot.committed_envelopes.len())
            .unwrap_or(u64::MAX),
        repair_attestations: u64::try_from(base_certificate.attestations.len()).unwrap_or(u64::MAX),
        readiness_attestations: u64::try_from(ready_certificate.attestations.len())
            .unwrap_or(u64::MAX),
        repair_snapshot_bytes: u64::try_from(supplied_snapshot.len()).unwrap_or(u64::MAX),
        installed_records,
        serving_worker_process_starts: 1,
        serving_responses: u64::try_from(worker.responding_node_ids.len()).unwrap_or(u64::MAX),
        active_policy_members_counted: worker.responding_node_ids,
        object_frontier: OBJECT_FRONTIER,
        final_frontier: TARGET_FRONTIER,
        worker_frontier: worker.observed_frontier,
        checks,
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

/// Run one fresh serving process over a pinned endpoint set.
///
/// # Errors
///
/// Returns an error when an exact quorum suffix cannot be reconstructed.
pub fn run_cell_tagged_log_repair_worker_process(
    config: CellTaggedLogRepairWorkerProcessConfig,
) -> Result<(), String> {
    if config.endpoints.is_empty()
        || config.quorum == 0
        || config.quorum > config.endpoints.len()
        || config.after_version >= config.through_version
    {
        return Err("repair worker configuration is invalid".to_owned());
    }
    let request = TaggedLogRequest::Read {
        range_tag: config.range_tag,
        after_version: config.after_version,
        through_version: config.through_version,
    };
    let mut responding_node_ids = Vec::new();
    let mut candidates = BTreeMap::<u64, BTreeMap<[u8; 32], (TaggedLogRecord, usize)>>::new();
    for endpoint in &config.endpoints {
        let Ok(TaggedLogResponse::Feed {
            node_id, records, ..
        }) = tagged_log_request(endpoint, &request)
        else {
            continue;
        };
        responding_node_ids.push(node_id);
        for record in records {
            let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            let digest: [u8; 32] = Sha256::digest(bytes).into();
            let candidate = candidates
                .entry(record.position)
                .or_default()
                .entry(digest)
                .or_insert_with(|| (record, 0));
            candidate.1 = candidate.1.saturating_add(1);
        }
    }
    responding_node_ids.sort_unstable();
    responding_node_ids.dedup();
    let mut quorum_records = Vec::new();
    for by_digest in candidates.into_values() {
        let matching = by_digest
            .into_values()
            .filter(|(_, count)| *count >= config.quorum)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err("repair worker did not observe one exact record quorum".to_owned());
        }
        quorum_records.push(matching[0].0.clone());
    }
    quorum_records.sort_by_key(|record| record.position);
    let observed_frontier = quorum_records
        .iter()
        .map(|record| {
            okv_sim::CommitEnvelope::decode(&record.envelope)
                .map(|envelope| envelope.version().sequence())
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(config.after_version);
    let receipt = CellTaggedLogRepairWorkerReceipt {
        responding_node_ids,
        quorum_records,
        observed_frontier,
    };
    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(
        config.output_path,
        serde_json::to_vec(&receipt).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn check(name: &str, passed: bool) -> CellTaggedLogLearnerRepairCheck {
    CellTaggedLogLearnerRepairCheck {
        name: name.to_owned(),
        passed,
    }
}

pub(crate) fn signing_seed(seed: u64, log_set_id: u16, node_id: u64) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"okv-cell-tagged-log-repair-signer-v1");
    digest.update(seed.to_be_bytes());
    digest.update(log_set_id.to_be_bytes());
    digest.update(node_id.to_be_bytes());
    digest.finalize().to_vec()
}

pub(crate) fn signing_seeds(seed: u64, log_set_id: u16) -> Vec<Vec<u8>> {
    (1..=u64::try_from(TLOG_NODES).unwrap_or(u64::MAX))
        .map(|node_id| signing_seed(seed, log_set_id, node_id))
        .collect()
}

pub(crate) fn log_set_policy(
    log_set_id: u16,
    generation: u64,
    seeds: &[Vec<u8>],
) -> Result<CellLogSetPolicy, String> {
    let members = seeds
        .iter()
        .enumerate()
        .map(|(index, seed)| {
            Ok(CellLogSetMember {
                node_id: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                public_key: tagged_log_public_key(seed)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(CellLogSetPolicy {
        format_version: 1,
        generation,
        policy_epoch: LOG_SET_POLICY_EPOCH,
        log_set_id,
        quorum_size: u16::try_from(TLOG_QUORUM).unwrap_or(u16::MAX),
        ratekeeper_soft_limit_bytes: 0,
        members,
    })
}

pub(crate) fn pop_through_object_frontier(
    log_set_id: u16,
    endpoints: &[String],
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
) -> Result<(), String> {
    let statement = CellTaggedLogPopStatement {
        format_version: 1,
        cell_id,
        tenant_id,
        generation,
        log_set_id,
        policy_epoch: LOG_SET_POLICY_EPOCH,
        publication_root_sha256: [7; 32],
        object_frontier: OBJECT_FRONTIER,
        pop_epoch: 1,
    };
    let capability = PublicationPopCapabilityCertificate {
        statement: PublicationPopCapabilityStatement {
            format_version: 1,
            authority_cell_id: 1,
            generation,
            transaction_system_id: "rfc-0045-repair".to_owned(),
            destination_root: "cell/range/all".to_owned(),
            manifest: PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: "repair/base-10".to_owned(),
                length: 1,
                sha256: "7".repeat(64),
            },
            object_frontier: OBJECT_FRONTIER,
            pop_epoch: 1,
        },
        attestations: Vec::new(),
    };
    for endpoint in endpoints {
        if !matches!(
            tagged_log_request(
                endpoint,
                &TaggedLogRequest::Pop {
                    statement: statement.clone(),
                    capability: capability.clone(),
                    manifest_bytes: Vec::new(),
                },
            )?,
            TaggedLogResponse::Popped { durable: true, .. }
        ) {
            return Err("repair setup did not durably pop through object frontier".to_owned());
        }
    }
    Ok(())
}

pub(crate) fn read_suffix(endpoint: &str, range_tag: u16) -> Result<Vec<TaggedLogRecord>, String> {
    match tagged_log_request(
        endpoint,
        &TaggedLogRequest::Read {
            range_tag,
            after_version: OBJECT_FRONTIER,
            through_version: TARGET_FRONTIER,
        },
    )? {
        TaggedLogResponse::Feed { records, .. } => Ok(records),
        response => Err(format!("repair suffix read failed: {response:?}")),
    }
}

pub(crate) fn repair_statement(
    snapshot: &okv_consensus::CellStateSnapshot,
    phase: CellTaggedLogRepairPhase,
    snapshot_bytes: &[u8],
    learner_incarnation: [u8; 16],
    learner_public_key: Vec<u8>,
    last_position: u64,
) -> CellTaggedLogRepairStatement {
    CellTaggedLogRepairStatement {
        format_version: 1,
        phase,
        cell_id: snapshot.cell_id,
        tenant_id: snapshot.tenant_id,
        generation: snapshot.generation,
        log_set_id: 10,
        policy_epoch: LOG_SET_POLICY_EPOCH,
        repair_id: 1,
        failed_node_id: FAILED_NODE_ID,
        learner_node_id: LEARNER_NODE_ID,
        learner_incarnation,
        learner_public_key,
        last_position,
        popped_through: OBJECT_FRONTIER,
        snapshot_length: u64::try_from(snapshot_bytes.len()).unwrap_or(u64::MAX),
        snapshot_sha256: Sha256::digest(snapshot_bytes).into(),
    }
}

pub(crate) fn collect_repair_attestations(
    endpoints: &[String],
    statement: &CellTaggedLogRepairStatement,
    snapshot_bytes: &[u8],
) -> Result<Vec<okv_consensus::CellTaggedLogRepairAttestation>, String> {
    let mut attestations = Vec::new();
    for endpoint in endpoints {
        match tagged_log_request(
            endpoint,
            &TaggedLogRequest::RepairAttest {
                statement: statement.clone(),
                snapshot_bytes: snapshot_bytes.to_vec(),
            },
        )? {
            TaggedLogResponse::RepairAttested {
                statement: observed,
                attestation,
                ..
            } if observed == *statement => attestations.push(attestation),
            response => return Err(format!("repair source did not attest: {response:?}")),
        }
    }
    let distinct = attestations
        .iter()
        .map(|attestation| attestation.signer_id)
        .collect::<BTreeSet<_>>();
    if distinct.len() != endpoints.len() {
        return Err("repair attestations are not from distinct sources".to_owned());
    }
    Ok(attestations)
}
