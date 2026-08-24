//! Process-composed authority-rooted `SlateDB` base plus certified txLog handoff.

use crate::range_serving_view::{
    AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord,
};
use crate::tagged_log_process::{
    tagged_log_request, TaggedLogProcessFixture, TaggedLogRecord, TaggedLogRequest,
    TaggedLogResponse,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use okv_consensus::{
    CellLogSetMember, CellLogSetPolicy, CellMutation, CellProcessFixture, CellProcessPrototypeMode,
    CellTaggedLogCertificate, CellTaggedLogStatement, GenerationCredential, PublicationAction,
    PublicationAuthorityProcessFixture, PublicationAuthorityState, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectIdentity, PublicationObjectKind,
    PublicationObjectReference, PublicationOutcome, PublicationRevisionToken, RequestIdentity,
    SnapshotClosure, SnapshotLeaseToken,
};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_sim::CommitEnvelope;
use okv_slate::{
    inspect_latest_physical_manifest, AuthorityManifestReference, MvccGcPhysicalManifestReceipt,
    SlateEngine,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::{CompactionWorkerOptions, CompactorOptions, Settings};
use slatedb::Db;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const FORMAT_VERSION: u16 = 1;
const PUBLICATION_CELL_ID: u64 = 17;
const TRANSACTION_SYSTEM_ID: &str = "cell-process-g1";
const DESTINATION_ROOT: &str = "cell-17/ranges/all/range-serving-handoff";
const DATABASE_PATH: &str = "range-serving";
const TLOG_NODES: usize = 3;
const TLOG_QUORUM: usize = 2;
const TLOG_RETAINED_BYTES_LIMIT: u64 = 65_536;
const POLICY_EPOCH: u64 = 1;
const RANGE_CACHE_BYTES: usize = 16 * 1024 * 1024;
const RANGE_CACHE_PART_BYTES: usize = 64 * 1024;
const AUTHORITY_READ_TIMEOUT_MILLIS: u64 = 5_000;
const UNAVAILABLE_PROBE_TIMEOUT_MILLIS: u64 = 50;

/// One unsafe subject for the frozen base-tail process gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeServingHandoffMode {
    Correct,
    PublishReplacementBeforeOldWorker,
    OmitIntermediateTail,
    TamperCertificate,
    StalePolicyEpoch,
    WrongExpectedPriorRoot,
    SkipAuthorityFailover,
    IgnorePinnedOldRoot,
    ReuseStaleMarkEpoch,
    RetirePermitBeforeDelete,
    ReuseStaleAuthoritySnapshot,
    FallbackToStaleAuthorityWhenUnavailable,
}

impl RangeServingHandoffMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::PublishReplacementBeforeOldWorker => "publish_replacement_before_old_worker",
            Self::OmitIntermediateTail => "omit_intermediate_tail",
            Self::TamperCertificate => "tamper_certificate",
            Self::StalePolicyEpoch => "stale_policy_epoch",
            Self::WrongExpectedPriorRoot => "wrong_expected_prior_root",
            Self::SkipAuthorityFailover => "skip_authority_failover",
            Self::IgnorePinnedOldRoot => "ignore_pinned_old_root",
            Self::ReuseStaleMarkEpoch => "reuse_stale_mark_epoch",
            Self::RetirePermitBeforeDelete => "retire_permit_before_delete",
            Self::ReuseStaleAuthoritySnapshot => "reuse_stale_authority_snapshot",
            Self::FallbackToStaleAuthorityWhenUnavailable => {
                "fallback_to_stale_authority_when_unavailable"
            }
        }
    }
}

/// How one disposable worker obtains publication state before opening storage.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeServingWorkerAuthorityMode {
    /// Read the replicated authority and refuse if it is unavailable.
    #[default]
    Live,
    /// Unsafe control that skips the live read and injects older state.
    InjectedStale,
    /// Unsafe control that falls back to older state after a failed live read.
    LiveThenStaleFallback,
}

/// Configuration for one disposable Range Engine worker process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingHandoffWorkerConfig {
    pub object_root: PathBuf,
    pub publication_endpoints: Vec<String>,
    pub destination_root: String,
    pub expected_root: PublicationObjectReference,
    pub cache_root: Option<PathBuf>,
    pub historical_lease: Option<SnapshotLeaseToken>,
    pub authority_mode: RangeServingWorkerAuthorityMode,
    pub authority_read_timeout_millis: u64,
    pub injected_authority_snapshot: Option<PublicationAuthorityState>,
    pub target_version: u64,
    pub log_sets: Vec<RangeServingHandoffLogSet>,
    pub fault: RangeServingHandoffWorkerFault,
    pub seed: u64,
    pub output_path: PathBuf,
}

/// One signed txLog set available to a disposable worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingHandoffLogSet {
    pub policy: CellLogSetPolicy,
    pub endpoints: Vec<String>,
}

/// Worker-local unsafe behavior used by one controller mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingHandoffWorkerFault {
    pub omit_intermediate_tail: bool,
    pub tamper_certificate: bool,
    pub stale_policy_epoch: bool,
}

/// Stable receipt from one disposable Range Engine worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingHandoffWorkerReceipt {
    pub authority_snapshot_source: String,
    pub historical_lease_validated: bool,
    pub resolved_expected_root: bool,
    pub root_manifest_key: String,
    pub base_frontier: u64,
    pub target_version: u64,
    pub observed_frontier: u64,
    pub physical_objects_verified: u64,
    pub txlog_survivor_responses: u64,
    pub txlog_certificates: u64,
    pub authenticated_tail_records: u64,
    pub view_opened: bool,
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub error: Option<String>,
}

/// Stable receipt for one M0 to M1 process-composed serving handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingHandoffReport {
    pub seed: u64,
    pub mode: RangeServingHandoffMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub transaction_process_starts: u64,
    pub publication_process_starts: u64,
    pub txlog_process_starts: u64,
    pub worker_process_starts: u64,
    pub authority_process_kills: u64,
    pub txlog_process_kills: u64,
    pub authority_failovers: u64,
    pub base_m0_frontier: u64,
    pub base_m1_frontier: u64,
    pub target_version: u64,
    pub m0_tail_records: u64,
    pub m1_tail_records: u64,
    pub post_gc_tail_records: u64,
    pub txlog_certificates: u64,
    pub reclamation_candidates: u64,
    pub lease_protected_rejections: u64,
    pub delete_permits: u64,
    pub reclaimed_objects: u64,
    pub cache_resurrection_attempts: u64,
    pub cache_resurrection_opened: u64,
    pub authority_unavailable_attempts: u64,
    pub authority_unavailable_opened: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PublishedRangeRoot {
    format_version: u16,
    range: AuthorityRangeRoot,
    physical_closure: Vec<PublicationObjectReference>,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: RangeServingHandoffMode) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!(
            "okv-range-serving-handoff-{}-{seed}-{}",
            mode.id(),
            Uuid::new_v4()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-range-serving-handoff-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Execute the first process-composed M0 to M1 serving-root handoff.
///
/// # Errors
///
/// Returns an error when a required authority, txLog, worker, or physical
/// object-store fixture cannot execute. Semantic disagreements are returned in
/// the report.
pub fn run_range_serving_handoff_contract(
    seed: u64,
    mode: RangeServingHandoffMode,
    executable: &Path,
) -> Result<RangeServingHandoffReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_controller(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_controller(
    seed: u64,
    mode: RangeServingHandoffMode,
    executable: &Path,
) -> Result<RangeServingHandoffReport, String> {
    let root = TempRoot::new(seed, mode)?;
    let object_root = root.0.join("object-store");
    fs::create_dir_all(&object_root).map_err(|error| error.to_string())?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&object_root).map_err(|error| error.to_string())?,
    );

    let mut transaction = CellProcessFixture::start(
        seed,
        CellProcessPrototypeMode::DurableSnapshotPop,
        executable,
    )?;
    let transaction_report = transaction.run_history().await?;
    let final_cell = transaction_report
        .final_cell
        .clone()
        .ok_or_else(|| "transaction fixture omitted final cell state".to_owned())?;
    let envelopes = final_cell
        .committed_envelopes
        .iter()
        .map(|encoded| {
            CommitEnvelope::decode(encoded)
                .map(|envelope| (encoded.clone(), envelope))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if envelopes.len() < 3 {
        return Err("range handoff requires at least three committed envelopes".to_owned());
    }
    let m0_index = envelopes.len() - 3;
    let m1_index = envelopes.len() - 2;
    let target_index = envelopes.len() - 1;
    let base_m0_frontier = envelopes[m0_index].1.version().sequence();
    let base_m1_frontier = envelopes[m1_index].1.version().sequence();
    let target_version = envelopes[target_index].1.version().sequence();

    let engine = build_slate_engine(Arc::clone(&store), seed).await?;
    for (_, envelope) in envelopes.iter().take(m0_index + 1) {
        engine
            .apply(slate_batch(envelope)?)
            .await
            .map_err(|error| error.to_string())?;
    }
    engine.flush().await.map_err(|error| error.to_string())?;
    let physical_m0 =
        inspect_latest_physical_manifest(Arc::clone(&store), DATABASE_PATH, seed ^ 0x0a00).await?;
    engine
        .apply(slate_batch(&envelopes[m1_index].1)?)
        .await
        .map_err(|error| error.to_string())?;
    engine.flush().await.map_err(|error| error.to_string())?;
    let m0_data_keys = physical_m0
        .live_ssts
        .iter()
        .map(|object| object.key.as_str())
        .collect::<BTreeSet<_>>();
    let compaction_started = Instant::now();
    let physical_m1 = loop {
        let candidate =
            inspect_latest_physical_manifest(Arc::clone(&store), DATABASE_PATH, seed ^ 0x0b00)
                .await?;
        if candidate
            .live_ssts
            .iter()
            .all(|object| !m0_data_keys.contains(object.key.as_str()))
        {
            break candidate;
        }
        if compaction_started.elapsed() >= Duration::from_secs(5) {
            return Err(
                "Range Engine handoff compaction did not replace the M0 data closure".to_owned(),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    engine.close().await.map_err(|error| error.to_string())?;

    let published_m0 = published_root(
        &physical_m0,
        &envelopes[m0_index].1,
        final_cell.cell_id,
        final_cell.tenant_id,
        final_cell.generation,
    );
    let published_m1 = published_root(
        &physical_m1,
        &envelopes[m1_index].1,
        final_cell.cell_id,
        final_cell.tenant_id,
        final_cell.generation,
    );
    let (root_m0_ref, root_m0_keys) =
        write_published_root(Arc::clone(&store), &published_m0).await?;
    let (root_m1_ref, root_m1_keys) =
        write_published_root(Arc::clone(&store), &published_m1).await?;
    let m0_objects = published_object_map(&root_m0_ref, &published_m0);
    let m1_objects = published_object_map(&root_m1_ref, &published_m1);
    let reclamation_objects = m0_objects
        .iter()
        .filter(|(key, _)| !m1_objects.contains_key(*key))
        .map(|(key, reference)| (key.clone(), reference.clone()))
        .collect::<BTreeMap<_, _>>();
    if reclamation_objects.is_empty() {
        return Err("M0 has no objects outside the M1 closure".to_owned());
    }

    let mut authority = PublicationAuthorityProcessFixture::start_for_generation(
        executable,
        seed ^ 0x4155_5448_524f_4f54,
        PUBLICATION_CELL_ID,
        final_cell.generation,
        TRANSACTION_SYSTEM_ID,
    )
    .await?;
    let authority_client = authority.client_starting_with(101)?;
    publish_root(
        &authority_client,
        seed,
        100,
        final_cell.generation,
        "range-m0",
        &root_m0_ref,
        root_m0_keys.clone(),
        None,
    )
    .await?;
    let old_root_lease = acquire_old_root_lease(
        &authority_client,
        seed,
        final_cell.generation,
        target_version,
        &root_m0_ref,
        root_m0_keys.clone(),
    )
    .await?;
    let m0_authority_snapshot = authority_client.read().await?;
    let m0_cache_root = root.0.join("range-cache-m0");

    let required_log_sets = envelopes[m1_index..]
        .iter()
        .flat_map(|(_, envelope)| envelope.required_log_tags().iter().copied())
        .collect::<BTreeSet<_>>();
    let mut log_fixtures = Vec::new();
    let mut worker_log_sets = Vec::new();
    for log_set_id in required_log_sets {
        let seeds = signing_seeds(seed, log_set_id);
        let fixture = TaggedLogProcessFixture::start_signed(
            executable,
            &root.0.join(format!("txlog-{log_set_id}")),
            log_set_id,
            TLOG_NODES,
            TLOG_RETAINED_BYTES_LIMIT,
            false,
            POLICY_EPOCH,
            &seeds,
        )?;
        let endpoints = fixture.endpoints();
        for (offset, (encoded, envelope)) in envelopes[m1_index..].iter().enumerate() {
            let position = u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1);
            let record = TaggedLogRecord::committed(
                position,
                envelope.required_log_tags().to_vec(),
                encoded.clone(),
            );
            let append_acks = endpoints
                .iter()
                .filter(|endpoint| {
                    matches!(
                        tagged_log_request(
                            endpoint,
                            &TaggedLogRequest::Append {
                                record: record.clone(),
                            },
                        ),
                        Ok(TaggedLogResponse::Appended { .. })
                    )
                })
                .count();
            if append_acks < TLOG_QUORUM {
                return Err(format!(
                    "txLog set {log_set_id} did not durably append position {position}"
                ));
            }
        }
        worker_log_sets.push(RangeServingHandoffLogSet {
            policy: log_policy(final_cell.generation, log_set_id, &seeds)?,
            endpoints,
        });
        log_fixtures.push(fixture);
    }
    let killed_index = usize::try_from(seed % TLOG_NODES as u64).unwrap_or(0);
    for fixture in &mut log_fixtures {
        fixture.kill(killed_index)?;
    }

    let replacement_published_early =
        mode == RangeServingHandoffMode::PublishReplacementBeforeOldWorker;
    let wrong_expected_prior = mode == RangeServingHandoffMode::WrongExpectedPriorRoot;
    let mut replacement_status = None;
    if replacement_published_early {
        replacement_status = Some(
            publish_root(
                &authority_client,
                seed,
                200,
                final_cell.generation,
                "range-m1",
                &root_m1_ref,
                root_m1_keys.clone(),
                Some(root_m0_ref.clone()),
            )
            .await?,
        );
    }

    let m0_fault = RangeServingHandoffWorkerFault {
        omit_intermediate_tail: mode == RangeServingHandoffMode::OmitIntermediateTail,
        tamper_certificate: mode == RangeServingHandoffMode::TamperCertificate,
        stale_policy_epoch: mode == RangeServingHandoffMode::StalePolicyEpoch,
    };
    let m0_worker = run_worker(
        executable,
        &root.0,
        "m0",
        RangeServingHandoffWorkerConfig {
            object_root: object_root.clone(),
            publication_endpoints: authority.endpoints(),
            destination_root: DESTINATION_ROOT.to_owned(),
            expected_root: root_m0_ref.clone(),
            cache_root: Some(m0_cache_root.clone()),
            historical_lease: Some(old_root_lease.clone()),
            authority_mode: RangeServingWorkerAuthorityMode::Live,
            authority_read_timeout_millis: AUTHORITY_READ_TIMEOUT_MILLIS,
            injected_authority_snapshot: None,
            target_version,
            log_sets: worker_log_sets.clone(),
            fault: m0_fault,
            seed: seed ^ 0x0c00,
            output_path: root.0.join("worker-m0.json"),
        },
    )?;

    let authority_failover_exercised = mode != RangeServingHandoffMode::SkipAuthorityFailover;
    let replacement_client = if authority_failover_exercised {
        authority.kill_leader_and_elect_successor(101, 102).await?;
        authority.client_starting_with(102)?
    } else {
        authority_client.clone()
    };
    if !replacement_published_early {
        let expected = if wrong_expected_prior {
            None
        } else {
            Some(root_m0_ref.clone())
        };
        replacement_status = Some(
            publish_root(
                &replacement_client,
                seed,
                200,
                final_cell.generation,
                "range-m1",
                &root_m1_ref,
                root_m1_keys,
                expected,
            )
            .await?,
        );
    }

    let m1_worker = run_worker(
        executable,
        &root.0,
        "m1",
        RangeServingHandoffWorkerConfig {
            object_root: object_root.clone(),
            publication_endpoints: authority.endpoints(),
            destination_root: DESTINATION_ROOT.to_owned(),
            expected_root: root_m1_ref.clone(),
            cache_root: None,
            historical_lease: None,
            authority_mode: RangeServingWorkerAuthorityMode::Live,
            authority_read_timeout_millis: AUTHORITY_READ_TIMEOUT_MILLIS,
            injected_authority_snapshot: None,
            target_version,
            log_sets: worker_log_sets.clone(),
            fault: RangeServingHandoffWorkerFault::default(),
            seed: seed ^ 0x0d00,
            output_path: root.0.join("worker-m1.json"),
        },
    )?;
    let pinned_state = replacement_client.read().await?;
    let pinned_mark_epoch = pinned_state.root_intent_epoch;
    let bypassed_pinned_key = if mode == RangeServingHandoffMode::IgnorePinnedOldRoot {
        let key = reclamation_objects
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| "M0 reclamation candidate disappeared".to_owned())?;
        store
            .delete(&ObjectPath::from(key.as_str()))
            .await
            .map_err(|error| error.to_string())?;
        Some(key)
    } else {
        None
    };
    let mut lease_protected_rejections = 0_u64;
    for (index, (key, reference)) in reclamation_objects.iter().enumerate() {
        let response = replacement_client
            .commit(&publication_command(
                seed,
                300_u64.saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
                final_cell.generation,
                PublicationAction::ReserveDelete {
                    plan_id: format!("pinned-m0-{index}"),
                    mark_epoch: pinned_mark_epoch,
                    key: key.clone(),
                    identity: object_identity(reference),
                },
            ))
            .await?;
        if response.status == PublicationCommandStatus::ObjectNamedByIntent {
            lease_protected_rejections = lease_protected_rejections.saturating_add(1);
        }
    }
    let pinned_old_root_objects_remain_exact =
        exact_objects_exist(Arc::clone(&store), reclamation_objects.values()).await;
    let released = replacement_client
        .commit(&publication_command(
            seed,
            400,
            final_cell.generation,
            PublicationAction::ReleaseLease {
                lease_id: old_root_lease.lease_id.clone(),
                expected_lease_epoch: old_root_lease.lease_epoch,
            },
        ))
        .await?;
    let after_release = replacement_client.read().await?;
    let release_advanced_epoch = released.status == PublicationCommandStatus::Accepted
        && after_release.root_intent_epoch > pinned_mark_epoch;
    let sweep_mark_epoch = if mode == RangeServingHandoffMode::ReuseStaleMarkEpoch {
        pinned_mark_epoch
    } else {
        after_release.root_intent_epoch
    };
    let mut delete_permits = 0_u64;
    let mut delete_retirements = 0_u64;
    let cache_resurrection_worker = run_worker(
        executable,
        &root.0,
        "m0-cache-resurrection",
        RangeServingHandoffWorkerConfig {
            object_root: object_root.clone(),
            publication_endpoints: authority.endpoints(),
            destination_root: DESTINATION_ROOT.to_owned(),
            expected_root: root_m0_ref.clone(),
            cache_root: Some(m0_cache_root.clone()),
            historical_lease: Some(old_root_lease.clone()),
            authority_mode: if mode == RangeServingHandoffMode::ReuseStaleAuthoritySnapshot {
                RangeServingWorkerAuthorityMode::InjectedStale
            } else {
                RangeServingWorkerAuthorityMode::Live
            },
            authority_read_timeout_millis: AUTHORITY_READ_TIMEOUT_MILLIS,
            injected_authority_snapshot: (mode
                == RangeServingHandoffMode::ReuseStaleAuthoritySnapshot)
                .then_some(m0_authority_snapshot.clone()),
            target_version,
            log_sets: worker_log_sets.clone(),
            fault: RangeServingHandoffWorkerFault::default(),
            seed: seed ^ 0x0f00,
            output_path: root.0.join("worker-m0-cache-resurrection.json"),
        },
    )?;
    let authority_unavailable_worker = run_worker(
        executable,
        &root.0,
        "m0-authority-unavailable",
        RangeServingHandoffWorkerConfig {
            object_root: object_root.clone(),
            publication_endpoints: vec!["127.0.0.1:1".to_owned()],
            destination_root: DESTINATION_ROOT.to_owned(),
            expected_root: root_m0_ref.clone(),
            cache_root: Some(m0_cache_root),
            historical_lease: Some(old_root_lease.clone()),
            authority_mode: if mode
                == RangeServingHandoffMode::FallbackToStaleAuthorityWhenUnavailable
            {
                RangeServingWorkerAuthorityMode::LiveThenStaleFallback
            } else {
                RangeServingWorkerAuthorityMode::Live
            },
            authority_read_timeout_millis: UNAVAILABLE_PROBE_TIMEOUT_MILLIS,
            injected_authority_snapshot: Some(m0_authority_snapshot),
            target_version,
            log_sets: worker_log_sets.clone(),
            fault: RangeServingHandoffWorkerFault::default(),
            seed: seed ^ 0x1000,
            output_path: root.0.join("worker-m0-authority-unavailable.json"),
        },
    )?;
    for (index, (key, reference)) in reclamation_objects.iter().enumerate() {
        let request_base =
            500_u64.saturating_add(u64::try_from(index).unwrap_or(u64::MAX).saturating_mul(2));
        let reserved = replacement_client
            .commit(&publication_command(
                seed,
                request_base,
                final_cell.generation,
                PublicationAction::ReserveDelete {
                    plan_id: format!("released-m0-{index}"),
                    mark_epoch: sweep_mark_epoch,
                    key: key.clone(),
                    identity: object_identity(reference),
                },
            ))
            .await?;
        let Some(PublicationOutcome::DeleteReserved { permit }) = reserved.outcome else {
            continue;
        };
        delete_permits = delete_permits.saturating_add(1);
        if mode == RangeServingHandoffMode::RetirePermitBeforeDelete {
            let retired = replacement_client
                .commit(&publication_command(
                    seed,
                    request_base.saturating_add(1),
                    final_cell.generation,
                    PublicationAction::RetireDelete { permit },
                ))
                .await?;
            if retired.status == PublicationCommandStatus::Accepted {
                delete_retirements = delete_retirements.saturating_add(1);
            }
            continue;
        }
        if bypassed_pinned_key.as_ref() != Some(key) {
            store
                .delete(&ObjectPath::from(key.as_str()))
                .await
                .map_err(|error| error.to_string())?;
        }
        let retired = replacement_client
            .commit(&publication_command(
                seed,
                request_base.saturating_add(1),
                final_cell.generation,
                PublicationAction::RetireDelete { permit },
            ))
            .await?;
        if retired.status == PublicationCommandStatus::Accepted {
            delete_retirements = delete_retirements.saturating_add(1);
        }
    }
    let reclaimed_objects =
        count_missing_objects(Arc::clone(&store), reclamation_objects.values()).await;
    let post_gc_worker = run_worker(
        executable,
        &root.0,
        "m1-post-gc",
        RangeServingHandoffWorkerConfig {
            object_root,
            publication_endpoints: authority.endpoints(),
            destination_root: DESTINATION_ROOT.to_owned(),
            expected_root: root_m1_ref.clone(),
            cache_root: None,
            historical_lease: None,
            authority_mode: RangeServingWorkerAuthorityMode::Live,
            authority_read_timeout_millis: AUTHORITY_READ_TIMEOUT_MILLIS,
            injected_authority_snapshot: None,
            target_version,
            log_sets: worker_log_sets,
            fault: RangeServingHandoffWorkerFault::default(),
            seed: seed ^ 0x0e00,
            output_path: root.0.join("worker-m1-post-gc.json"),
        },
    )?;
    let final_authority = replacement_client.read().await?;
    let reclamation_candidate_count = u64::try_from(reclamation_objects.len()).unwrap_or(u64::MAX);
    let checks = BTreeMap::from([
        (
            "source_transaction_clean".to_owned(),
            transaction_report.anomaly_count == 0,
        ),
        (
            "base_versions_are_ordered_and_sparse_safe".to_owned(),
            base_m0_frontier < base_m1_frontier && base_m1_frontier < target_version,
        ),
        (
            "m0_worker_resolved_exact_authority_root".to_owned(),
            m0_worker.resolved_expected_root,
        ),
        (
            "m0_plus_certified_tail_reaches_target".to_owned(),
            m0_worker.view_opened
                && m0_worker.base_frontier == base_m0_frontier
                && m0_worker.observed_frontier == target_version
                && m0_worker.authenticated_tail_records == 2,
        ),
        (
            "replacement_publication_accepted".to_owned(),
            replacement_status == Some(PublicationCommandStatus::Accepted),
        ),
        (
            "authority_failover_exercised".to_owned(),
            authority_failover_exercised,
        ),
        (
            "final_authority_root_is_m1".to_owned(),
            final_authority.roots.get(DESTINATION_ROOT) == Some(&root_m1_ref),
        ),
        (
            "m1_worker_resolved_exact_authority_root".to_owned(),
            m1_worker.resolved_expected_root,
        ),
        (
            "m1_plus_post_base_tail_reaches_target".to_owned(),
            m1_worker.view_opened
                && m1_worker.base_frontier == base_m1_frontier
                && m1_worker.observed_frontier == target_version
                && m1_worker.authenticated_tail_records == 1,
        ),
        (
            "both_workers_verify_physical_closures".to_owned(),
            m0_worker.physical_objects_verified
                == u64::try_from(published_m0.physical_closure.len()).unwrap_or(u64::MAX)
                && m1_worker.physical_objects_verified
                    == u64::try_from(published_m1.physical_closure.len()).unwrap_or(u64::MAX),
        ),
        (
            "both_workers_use_real_txlog_quorums".to_owned(),
            m0_worker.txlog_survivor_responses >= 4
                && m1_worker.txlog_survivor_responses >= 4
                && m0_worker.txlog_certificates == 4
                && m1_worker.txlog_certificates == 2,
        ),
        (
            "both_workers_match_transaction_oracle".to_owned(),
            m0_worker.rows == final_cell.rows && m1_worker.rows == final_cell.rows,
        ),
        (
            "old_root_snapshot_lease_acquired".to_owned(),
            old_root_lease.snapshot_version == target_version
                && old_root_lease.closure.object_keys == root_m0_keys,
        ),
        (
            "pinned_old_root_delete_rejected".to_owned(),
            lease_protected_rejections == reclamation_candidate_count,
        ),
        (
            "pinned_old_root_objects_remain_exact".to_owned(),
            pinned_old_root_objects_remain_exact,
        ),
        (
            "lease_release_advances_root_epoch".to_owned(),
            release_advanced_epoch,
        ),
        (
            "released_old_root_reclaimed_exactly".to_owned(),
            reclaimed_objects == reclamation_candidate_count
                && delete_permits == reclamation_candidate_count
                && delete_retirements == reclamation_candidate_count,
        ),
        (
            "released_old_root_cache_reopen_rejected".to_owned(),
            !cache_resurrection_worker.view_opened
                && cache_resurrection_worker.authority_snapshot_source == "live",
        ),
        (
            "authority_unavailable_reopen_rejected".to_owned(),
            !authority_unavailable_worker.view_opened
                && authority_unavailable_worker.authority_snapshot_source == "live_unavailable",
        ),
        (
            "replacement_survives_old_root_reclamation".to_owned(),
            post_gc_worker.view_opened
                && post_gc_worker.rows == final_cell.rows
                && post_gc_worker.observed_frontier == target_version,
        ),
        (
            "delete_reservations_retired".to_owned(),
            final_authority.deletion_reservations.is_empty(),
        ),
    ]);
    build_report(
        seed,
        mode,
        checks,
        &transaction_report,
        &m0_worker,
        &m1_worker,
        &post_gc_worker,
        &cache_resurrection_worker,
        &authority_unavailable_worker,
        log_fixtures.len(),
        authority_failover_exercised,
        base_m0_frontier,
        base_m1_frontier,
        target_version,
        reclamation_candidate_count,
        lease_protected_rejections,
        delete_permits,
        reclaimed_objects,
    )
}

/// Run one disposable worker and persist a semantic receipt even when the
/// serving view fails closed.
///
/// # Errors
///
/// Returns an error only for process, authority, object, txLog transport, or
/// receipt I/O failure.
#[allow(clippy::too_many_lines)]
pub async fn run_range_serving_handoff_worker_process(
    config: RangeServingHandoffWorkerConfig,
) -> Result<(), String> {
    let publication = okv_consensus::PublicationClient::new(config.publication_endpoints.clone())?;
    let (publication_state, authority_snapshot_source) = match config.authority_mode {
        RangeServingWorkerAuthorityMode::InjectedStale => (
            config
                .injected_authority_snapshot
                .clone()
                .ok_or_else(|| "injected-stale worker omitted its authority snapshot".to_owned())?,
            "injected_stale",
        ),
        RangeServingWorkerAuthorityMode::Live
        | RangeServingWorkerAuthorityMode::LiveThenStaleFallback => {
            let authority_read = match tokio::time::timeout(
                Duration::from_millis(config.authority_read_timeout_millis.max(1)),
                publication.read(),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(format!(
                    "publication authority read exceeded {} ms",
                    config.authority_read_timeout_millis.max(1)
                )),
            };
            match authority_read {
                Ok(state) => (state, "live"),
                Err(_error)
                    if config.authority_mode
                        == RangeServingWorkerAuthorityMode::LiveThenStaleFallback =>
                {
                    (
                        config.injected_authority_snapshot.clone().ok_or_else(|| {
                            "authority-fallback worker omitted its stale snapshot".to_owned()
                        })?,
                        "unavailable_stale_fallback",
                    )
                }
                Err(error) => {
                    return persist_worker_receipt(
                        &config.output_path,
                        &RangeServingHandoffWorkerReceipt {
                            authority_snapshot_source: "live_unavailable".to_owned(),
                            historical_lease_validated: false,
                            resolved_expected_root: false,
                            root_manifest_key: String::new(),
                            base_frontier: 0,
                            target_version: config.target_version,
                            observed_frontier: 0,
                            physical_objects_verified: 0,
                            txlog_survivor_responses: 0,
                            txlog_certificates: 0,
                            authenticated_tail_records: 0,
                            view_opened: false,
                            rows: Vec::new(),
                            error: Some(format!("publication authority unavailable: {error}")),
                        },
                    );
                }
            }
        }
    };
    let resolved = publication_state
        .roots
        .get(&config.destination_root)
        .cloned()
        .ok_or_else(|| "Range Engine worker found no authority root".to_owned())?;
    let resolved_expected_root = resolved == config.expected_root;
    if !resolved_expected_root {
        return persist_worker_receipt(
            &config.output_path,
            &RangeServingHandoffWorkerReceipt {
                authority_snapshot_source: authority_snapshot_source.to_owned(),
                historical_lease_validated: false,
                resolved_expected_root: false,
                root_manifest_key: String::new(),
                base_frontier: 0,
                target_version: config.target_version,
                observed_frontier: 0,
                physical_objects_verified: 0,
                txlog_survivor_responses: 0,
                txlog_certificates: 0,
                authenticated_tail_records: 0,
                view_opened: false,
                rows: Vec::new(),
                error: Some("authority root differs from the expected serving root".to_owned()),
            },
        );
    }
    let historical_lease_validated = if let Some(lease) = &config.historical_lease {
        let valid = publication_state
            .validate_active_snapshot_lease(lease)
            .is_ok()
            && lease.snapshot_version == config.target_version
            && lease.closure.manifest == resolved
            && lease.closure.object_keys.contains(&resolved.key);
        if !valid {
            return persist_worker_receipt(
                &config.output_path,
                &RangeServingHandoffWorkerReceipt {
                    authority_snapshot_source: authority_snapshot_source.to_owned(),
                    historical_lease_validated: false,
                    resolved_expected_root: true,
                    root_manifest_key: resolved.key,
                    base_frontier: 0,
                    target_version: config.target_version,
                    observed_frontier: 0,
                    physical_objects_verified: 0,
                    txlog_survivor_responses: 0,
                    txlog_certificates: 0,
                    authenticated_tail_records: 0,
                    view_opened: false,
                    rows: Vec::new(),
                    error: Some(
                        "historical lease is not active for the resolved authority root".to_owned(),
                    ),
                },
            );
        }
        true
    } else {
        false
    };
    let backend: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&config.object_root).map_err(|error| error.to_string())?,
    );
    let store = if let Some(cache_root) = &config.cache_root {
        CachedObjectStore::builder(cache_root, backend)
            .with_max_cache_size_bytes(Some(RANGE_CACHE_BYTES))
            .with_part_size_bytes(RANGE_CACHE_PART_BYTES)
            .with_cache_on_flush(false)
            .with_scan_interval(None)
            .with_max_open_file_handles(32)
            .build()
            .await
            .map(|store| store as Arc<dyn ObjectStore>)
            .map_err(|error| format!("build Range Engine cache: {error}"))?
    } else {
        backend
    };
    let root_bytes = read_exact_object(Arc::clone(&store), &resolved).await?;
    let published: PublishedRangeRoot =
        serde_json::from_slice(&root_bytes).map_err(|error| error.to_string())?;
    if published.format_version != FORMAT_VERSION {
        return Err("published range root has an unsupported format".to_owned());
    }
    let mut physical_objects_verified = 0_u64;
    for reference in &published.physical_closure {
        read_exact_object(Arc::clone(&store), reference).await?;
        physical_objects_verified = physical_objects_verified.saturating_add(1);
    }
    let policies = config
        .log_sets
        .iter()
        .map(|set| (set.policy.log_set_id, set.policy.clone()))
        .collect::<BTreeMap<_, _>>();
    let (mut records, survivor_responses) = certified_tail(
        &config.log_sets,
        published.range.covered_through,
        config.target_version,
    )?;
    if config.fault.omit_intermediate_tail && records.len() > 1 {
        records.remove(0);
    }
    if config.fault.tamper_certificate {
        if let Some(signature) = records
            .first_mut()
            .and_then(|record| record.certificates.first_mut())
            .and_then(|certificate| certificate.attestations.first_mut())
            .and_then(|attestation| attestation.signature.first_mut())
        {
            *signature ^= 0xff;
        }
    }
    if config.fault.stale_policy_epoch {
        if let Some(certificate) = records
            .first_mut()
            .and_then(|record| record.certificates.first_mut())
        {
            certificate.statement.policy_epoch =
                certificate.statement.policy_epoch.saturating_add(1);
        }
    }
    let certificate_count = records.iter().fold(0_u64, |total, record| {
        total.saturating_add(u64::try_from(record.certificates.len()).unwrap_or(u64::MAX))
    });
    let view = if let Some(lease) = &config.historical_lease {
        AuthorityBoundRangeView::open_historical(
            DATABASE_PATH,
            Arc::clone(&store),
            &resolved,
            published.range.clone(),
            config.target_version,
            records,
            &policies,
            config.seed,
            &publication_state,
            lease,
        )
        .await
    } else {
        AuthorityBoundRangeView::open(
            DATABASE_PATH,
            Arc::clone(&store),
            published.range.clone(),
            config.target_version,
            records,
            &policies,
            config.seed,
        )
        .await
    };
    let receipt = match view {
        Ok(view) => {
            let rows = view
                .scan_at(&[], &[0xff], config.target_version, 10_000)
                .await
                .map_err(|error| error.to_string())?;
            RangeServingHandoffWorkerReceipt {
                authority_snapshot_source: authority_snapshot_source.to_owned(),
                historical_lease_validated,
                resolved_expected_root,
                root_manifest_key: view.manifest_key().to_owned(),
                base_frontier: view.base_frontier(),
                target_version: config.target_version,
                observed_frontier: view.target_version(),
                physical_objects_verified,
                txlog_survivor_responses: survivor_responses,
                txlog_certificates: certificate_count,
                authenticated_tail_records: view.authenticated_tail_records(),
                view_opened: true,
                rows,
                error: None,
            }
        }
        Err(error) => RangeServingHandoffWorkerReceipt {
            authority_snapshot_source: authority_snapshot_source.to_owned(),
            historical_lease_validated,
            resolved_expected_root,
            root_manifest_key: published.range.manifest.key,
            base_frontier: published.range.covered_through,
            target_version: config.target_version,
            observed_frontier: published.range.covered_through,
            physical_objects_verified,
            txlog_survivor_responses: survivor_responses,
            txlog_certificates: certificate_count,
            authenticated_tail_records: 0,
            view_opened: false,
            rows: Vec::new(),
            error: Some(error.to_string()),
        },
    };
    persist_worker_receipt(&config.output_path, &receipt)
}

async fn build_slate_engine(store: Arc<dyn ObjectStore>, seed: u64) -> Result<SlateEngine, String> {
    let scheduler_options = HashMap::from([
        ("min_compaction_sources".to_owned(), "2".to_owned()),
        ("max_compaction_sources".to_owned(), "2".to_owned()),
    ]);
    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: Some(CompactorOptions {
            poll_interval: Duration::from_millis(20),
            max_concurrent_compactions: 1,
            scheduler_options,
            worker: Some(CompactionWorkerOptions {
                max_concurrent_compactions: 1,
                compactions_poll_interval: Duration::from_millis(20),
                heartbeat_interval: Duration::from_millis(40),
                max_subcompactions: 1,
                min_filter_keys: 1,
                ..CompactionWorkerOptions::default()
            }),
            commit_compacted_interval: Duration::from_millis(20),
            worker_heartbeat_timeout: Duration::from_secs(2),
            ..CompactorOptions::default()
        }),
        garbage_collector_options: None,
        ..Settings::default()
    };
    Db::builder(DATABASE_PATH, store)
        .with_settings(settings)
        .with_seed(seed ^ 0x51a7_e000)
        .build()
        .await
        .map(SlateEngine::new)
        .map_err(|error| error.to_string())
}

fn slate_batch(envelope: &CommitEnvelope) -> Result<CommitBatch, String> {
    let (client_id, request_id) = envelope.client_identity();
    let mutations: Vec<CellMutation> = serde_json::from_slice(envelope.canonical_mutations())
        .map_err(|error| error.to_string())?;
    let mutations = mutations
        .into_iter()
        .map(|mutation| match mutation {
            CellMutation::Clear { key } => Mutation::Clear { key },
            CellMutation::Set { key, value } => Mutation::Set { key, value },
        })
        .collect();
    Ok(CommitBatch {
        version: Version::new(envelope.version().sequence()),
        identity: CommitIdentity::new(client_id, request_id),
        mutations,
    })
}

fn published_root(
    physical: &MvccGcPhysicalManifestReceipt,
    last_envelope: &CommitEnvelope,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
) -> PublishedRangeRoot {
    let manifest = AuthorityManifestReference {
        key: physical.manifest.key.clone(),
        length: physical.manifest.length,
        sha256: physical.manifest.sha256.clone(),
    };
    let physical_closure = std::iter::once(PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: physical.manifest.key.clone(),
        length: physical.manifest.length,
        sha256: physical.manifest.sha256.clone(),
    })
    .chain(
        physical
            .live_ssts
            .iter()
            .map(|object| PublicationObjectReference {
                kind: PublicationObjectKind::Data,
                key: object.key.clone(),
                length: object.length,
                sha256: object.sha256.clone(),
            }),
    )
    .collect();
    PublishedRangeRoot {
        format_version: FORMAT_VERSION,
        range: AuthorityRangeRoot {
            cell_id,
            tenant_id,
            generation,
            manifest,
            covered_through: last_envelope.version().sequence(),
            minimum_readable_version: 1,
            log_chain_sha256: Sha256::digest(last_envelope.encode()).into(),
        },
        physical_closure,
    }
}

fn published_object_map(
    root: &PublicationObjectReference,
    published: &PublishedRangeRoot,
) -> BTreeMap<String, PublicationObjectReference> {
    std::iter::once((root.key.clone(), root.clone()))
        .chain(
            published
                .physical_closure
                .iter()
                .map(|reference| (reference.key.clone(), reference.clone())),
        )
        .collect()
}

async fn acquire_old_root_lease(
    client: &okv_consensus::PublicationClient,
    seed: u64,
    generation: u64,
    target_version: u64,
    manifest: &PublicationObjectReference,
    object_keys: BTreeSet<String>,
) -> Result<SnapshotLeaseToken, String> {
    for (request_id, action) in [
        (
            110,
            PublicationAction::ObserveCommittedFrontier {
                committed_frontier: target_version,
            },
        ),
        (
            111,
            PublicationAction::SetRetentionWindow {
                expected_policy_epoch: 0,
                retention_window: target_version,
            },
        ),
    ] {
        let response = client
            .commit(&publication_command(seed, request_id, generation, action))
            .await?;
        if response.status != PublicationCommandStatus::Accepted {
            return Err(format!(
                "old-root lease prerequisite returned {:?}",
                response.status
            ));
        }
    }
    let acquired = client
        .commit(&publication_command(
            seed,
            112,
            generation,
            PublicationAction::AcquireLease {
                lease_id: "range-m0-reader".to_owned(),
                tenant_id: "tenant-range-serving-handoff".to_owned(),
                snapshot_version: target_version,
                owner: "range-engine-m0".to_owned(),
                purpose: "serve-pinned-old-root".to_owned(),
                deadline_tick: 100,
                closure: SnapshotClosure {
                    manifest: manifest.clone(),
                    object_keys,
                },
            },
        ))
        .await?;
    match acquired.outcome {
        Some(PublicationOutcome::LeaseAcquired { token })
            if acquired.status == PublicationCommandStatus::Accepted =>
        {
            Ok(token)
        }
        _ => Err(format!(
            "old-root lease acquisition returned {:?}",
            acquired.status
        )),
    }
}

fn publication_command(
    seed: u64,
    request_id: u64,
    generation: u64,
    action: PublicationAction,
) -> PublicationCommand {
    PublicationCommand {
        identity: request_identity(seed, request_id),
        credential: GenerationCredential {
            generation,
            transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
        },
        action,
    }
}

fn object_identity(reference: &PublicationObjectReference) -> PublicationObjectIdentity {
    PublicationObjectIdentity {
        revision: PublicationRevisionToken::default(),
        length: reference.length,
        sha256: reference.sha256.clone(),
    }
}

async fn exact_objects_exist<'a>(
    store: Arc<dyn ObjectStore>,
    objects: impl Iterator<Item = &'a PublicationObjectReference>,
) -> bool {
    for object in objects {
        if read_exact_object(Arc::clone(&store), object).await.is_err() {
            return false;
        }
    }
    true
}

async fn count_missing_objects<'a>(
    store: Arc<dyn ObjectStore>,
    objects: impl Iterator<Item = &'a PublicationObjectReference>,
) -> u64 {
    let mut missing = 0_u64;
    for object in objects {
        if matches!(
            store.get(&ObjectPath::from(object.key.as_str())).await,
            Err(object_store::Error::NotFound { .. })
        ) {
            missing = missing.saturating_add(1);
        }
    }
    missing
}

async fn write_published_root(
    store: Arc<dyn ObjectStore>,
    published: &PublishedRangeRoot,
) -> Result<(PublicationObjectReference, BTreeSet<String>), String> {
    let bytes = serde_json::to_vec(published).map_err(|error| error.to_string())?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let key = format!("range-roots/sha256/{sha256}.manifest");
    store
        .put(&ObjectPath::from(key.as_str()), bytes.clone().into())
        .await
        .map_err(|error| error.to_string())?;
    let reference = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: key.clone(),
        length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        sha256,
    };
    let keys = std::iter::once(key)
        .chain(
            published
                .physical_closure
                .iter()
                .map(|object| object.key.clone()),
        )
        .collect();
    Ok((reference, keys))
}

#[allow(clippy::too_many_arguments)]
async fn publish_root(
    client: &okv_consensus::PublicationClient,
    seed: u64,
    request_base: u64,
    generation: u64,
    publication_id: &str,
    manifest: &PublicationObjectReference,
    object_keys: BTreeSet<String>,
    expected_prior_root: Option<PublicationObjectReference>,
) -> Result<PublicationCommandStatus, String> {
    let credential = GenerationCredential {
        generation,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };
    let prepared = client
        .commit(&PublicationCommand {
            identity: request_identity(seed, request_base),
            credential: credential.clone(),
            action: PublicationAction::Prepare {
                publication_id: publication_id.to_owned(),
                intent: PublicationIntent {
                    object_keys,
                    manifest: manifest.clone(),
                    destination_root: DESTINATION_ROOT.to_owned(),
                    expected_prior_root: expected_prior_root.clone(),
                },
            },
        })
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Ok(prepared.status);
    }
    let published = client
        .commit(&PublicationCommand {
            identity: request_identity(seed, request_base.saturating_add(1)),
            credential,
            action: PublicationAction::Publish {
                publication_id: publication_id.to_owned(),
                destination_root: DESTINATION_ROOT.to_owned(),
                expected_prior_root,
                manifest: manifest.clone(),
            },
        })
        .await?;
    Ok(published.status)
}

fn signing_seeds(seed: u64, log_set_id: u16) -> Vec<Vec<u8>> {
    (0_u64..TLOG_NODES as u64)
        .map(|index| {
            let mut hasher = Sha256::new();
            hasher.update(b"okv-range-serving-txlog-key-v1");
            hasher.update(seed.to_be_bytes());
            hasher.update(log_set_id.to_be_bytes());
            hasher.update(index.to_be_bytes());
            hasher.finalize().to_vec()
        })
        .collect()
}

fn log_policy(
    generation: u64,
    log_set_id: u16,
    seeds: &[Vec<u8>],
) -> Result<CellLogSetPolicy, String> {
    let members = seeds
        .iter()
        .enumerate()
        .map(|(index, seed)| {
            Ok(CellLogSetMember {
                node_id: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                public_key: okv_consensus::tagged_log_public_key(seed)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(CellLogSetPolicy {
        format_version: FORMAT_VERSION,
        generation,
        policy_epoch: POLICY_EPOCH,
        log_set_id,
        quorum_size: u16::try_from(TLOG_QUORUM).unwrap_or(u16::MAX),
        ratekeeper_soft_limit_bytes: TLOG_RETAINED_BYTES_LIMIT,
        members,
    })
}

fn certified_tail(
    log_sets: &[RangeServingHandoffLogSet],
    after_version: u64,
    target_version: u64,
) -> Result<(Vec<CertifiedTxLogRecord>, u64), String> {
    let mut records = BTreeMap::<u64, CertifiedTxLogRecord>::new();
    let mut survivor_responses = 0_u64;
    for set in log_sets {
        let request = TaggedLogRequest::Read {
            range_tag: set.policy.log_set_id,
            after_version,
            through_version: target_version,
        };
        let mut candidates = BTreeMap::<u64, BTreeMap<String, (TaggedLogRecord, usize)>>::new();
        for endpoint in &set.endpoints {
            let Ok(TaggedLogResponse::Feed { records, .. }) =
                tagged_log_request(endpoint, &request)
            else {
                continue;
            };
            survivor_responses = survivor_responses.saturating_add(1);
            for record in records {
                let bytes = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
                let digest = format!("{:x}", Sha256::digest(bytes));
                let candidate = candidates
                    .entry(record.position)
                    .or_default()
                    .entry(digest)
                    .or_insert_with(|| (record, 0));
                candidate.1 = candidate.1.saturating_add(1);
            }
        }
        let mut quorum_records = Vec::new();
        for by_digest in candidates.into_values() {
            let matching = by_digest
                .into_values()
                .filter(|(_, count)| *count >= TLOG_QUORUM)
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err("Range Engine worker observed no unique txLog quorum".to_owned());
            }
            quorum_records.push(matching[0].0.clone());
        }
        quorum_records.sort_by_key(|record| record.position);
        for record in quorum_records {
            let envelope =
                CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
            let (encoded_client_id, request_id) = envelope.client_identity();
            let mut client_id = [0_u8; 8];
            client_id.copy_from_slice(&encoded_client_id[8..]);
            let statement = CellTaggedLogStatement {
                format_version: FORMAT_VERSION,
                cell_id: envelope.cell_id(),
                tenant_id: envelope.tenant_id(),
                generation: envelope.generation(),
                transaction_identity: RequestIdentity {
                    client_id: u64::from_be_bytes(client_id),
                    request_id,
                },
                commit_sequence: envelope.version().sequence(),
                log_set_id: set.policy.log_set_id,
                policy_epoch: set.policy.policy_epoch,
                envelope_sha256: Sha256::digest(&record.envelope).into(),
                durable_position: record.position,
            };
            let mut attestations = Vec::new();
            for endpoint in &set.endpoints {
                let Ok(TaggedLogResponse::Attested {
                    statement: observed,
                    attestation,
                    ..
                }) = tagged_log_request(
                    endpoint,
                    &TaggedLogRequest::Attest {
                        statement: statement.clone(),
                    },
                )
                else {
                    continue;
                };
                if observed == statement {
                    attestations.push(attestation);
                }
            }
            if attestations.len() < TLOG_QUORUM {
                return Err("Range Engine worker could not obtain txLog attestations".to_owned());
            }
            let certificate = CellTaggedLogCertificate {
                statement,
                attestations,
            };
            let entry = records
                .entry(envelope.version().sequence())
                .or_insert_with(|| CertifiedTxLogRecord {
                    envelope: record.envelope.clone(),
                    certificates: Vec::new(),
                });
            if entry.envelope != record.envelope {
                return Err("txLog sets returned different commit envelopes".to_owned());
            }
            entry.certificates.push(certificate);
        }
    }
    Ok((records.into_values().collect(), survivor_responses))
}

async fn read_exact_object(
    store: Arc<dyn ObjectStore>,
    reference: &PublicationObjectReference,
) -> Result<Vec<u8>, String> {
    let capacity = usize::try_from(reference.length)
        .map_err(|_| format!("object is too large to authenticate: {}", reference.key))?;
    let path = ObjectPath::from(reference.key.as_str());
    let mut bytes = Vec::with_capacity(capacity);
    let mut start = 0_u64;
    while start < reference.length {
        let end = start
            .saturating_add(u64::try_from(RANGE_CACHE_PART_BYTES).unwrap_or(u64::MAX))
            .min(reference.length);
        bytes.extend_from_slice(
            &store
                .get_range(&path, start..end)
                .await
                .map_err(|error| error.to_string())?,
        );
        start = end;
    }
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != reference.length
        || format!("{:x}", Sha256::digest(&bytes)) != reference.sha256
    {
        return Err(format!("object identity mismatch for {}", reference.key));
    }
    Ok(bytes)
}

fn run_worker(
    executable: &Path,
    root: &Path,
    label: &str,
    mut config: RangeServingHandoffWorkerConfig,
) -> Result<RangeServingHandoffWorkerReceipt, String> {
    config.output_path = root.join(format!("worker-{label}.json"));
    let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("range-serving-handoff-worker-node")
        .arg("--config-json")
        .arg(config_json)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "Range Engine worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&fs::read(&config.output_path).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn persist_worker_receipt(
    path: &Path,
    receipt: &RangeServingHandoffWorkerReceipt,
) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "worker receipt has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let bytes = serde_json::to_vec(receipt).map_err(|error| error.to_string())?;
    fs::write(path, bytes).map_err(|error| error.to_string())?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
fn build_report(
    seed: u64,
    mode: RangeServingHandoffMode,
    checks: BTreeMap<String, bool>,
    transaction: &okv_consensus::CellProcessPrototypeReport,
    m0: &RangeServingHandoffWorkerReceipt,
    m1: &RangeServingHandoffWorkerReceipt,
    post_gc: &RangeServingHandoffWorkerReceipt,
    cache_resurrection: &RangeServingHandoffWorkerReceipt,
    authority_unavailable: &RangeServingHandoffWorkerReceipt,
    log_set_count: usize,
    authority_failover: bool,
    base_m0_frontier: u64,
    base_m1_frontier: u64,
    target_version: u64,
    reclamation_candidates: u64,
    lease_protected_rejections: u64,
    delete_permits: u64,
    reclaimed_objects: u64,
) -> Result<RangeServingHandoffReport, String> {
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let semantic = (
        seed,
        mode,
        &checks,
        base_m0_frontier,
        base_m1_frontier,
        target_version,
        m0.authenticated_tail_records,
        m1.authenticated_tail_records,
        post_gc.authenticated_tail_records,
        cache_resurrection.view_opened,
        authority_unavailable.view_opened,
        m0.txlog_certificates
            .saturating_add(m1.txlog_certificates)
            .saturating_add(post_gc.txlog_certificates),
        reclamation_candidates,
        lease_protected_rejections,
        delete_permits,
        reclaimed_objects,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(RangeServingHandoffReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        transaction_process_starts: transaction.process_starts,
        publication_process_starts: 3,
        txlog_process_starts: u64::try_from(log_set_count.saturating_mul(TLOG_NODES))
            .unwrap_or(u64::MAX),
        worker_process_starts: 5,
        authority_process_kills: u64::from(authority_failover),
        txlog_process_kills: u64::try_from(log_set_count).unwrap_or(u64::MAX),
        authority_failovers: u64::from(authority_failover),
        base_m0_frontier,
        base_m1_frontier,
        target_version,
        m0_tail_records: m0.authenticated_tail_records,
        m1_tail_records: m1.authenticated_tail_records,
        post_gc_tail_records: post_gc.authenticated_tail_records,
        txlog_certificates: m0
            .txlog_certificates
            .saturating_add(m1.txlog_certificates)
            .saturating_add(post_gc.txlog_certificates),
        reclamation_candidates,
        lease_protected_rejections,
        delete_permits,
        reclaimed_objects,
        cache_resurrection_attempts: 1,
        cache_resurrection_opened: u64::from(cache_resurrection.view_opened),
        authority_unavailable_attempts: 1,
        authority_unavailable_opened: u64::from(authority_unavailable.view_opened),
        checks,
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

fn request_identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: seed.max(1),
        request_id,
    }
}
