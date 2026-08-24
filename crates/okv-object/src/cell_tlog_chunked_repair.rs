use super::cell_tlog_repair::{
    collect_repair_attestations, log_set_policy, pop_through_object_frontier, repair_statement,
    signing_seed, signing_seeds, CellTaggedLogRepairWorkerReceipt,
};
use super::tagged_log_process::{
    encode_tagged_log_repair_snapshot, tagged_log_request, PublicationPopPolicy,
    TaggedLogProcessFixture, TaggedLogRecord, TaggedLogRepairFaults, TaggedLogRepairTransfer,
    TaggedLogRequest, TaggedLogResponse,
};
use crate::CellTaggedLogRepairWorkerProcessConfig;
use okv_consensus::{
    verify_tagged_log_repair_certificate, CellKeyRange, CellMutation, CellProcessFixture,
    CellProcessPrototypeMode, CellReadVersion, CellTaggedLogCapacityStatement,
    CellTaggedLogRepairCertificate, CellTaggedLogRepairPhase, CellTransactionClient,
    CellTransactionCommand, CellTransactionStatus, RequestIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use uuid::Uuid;

const OBJECT_FRONTIER: u64 = 10;
const BASE_FRONTIER: u64 = 14;
const TARGET_FRONTIER: u64 = 16;
const LOG_SET_POLICY_EPOCH: u64 = 1;
const MOVING_LOG_SET: u16 = 10;
const UNCHANGED_LOG_SET: u16 = 20;
const FAILED_NODE_ID: usize = 0;
const LEARNER_NODE_ID: u64 = 4;
const LEARNER_INCARNATION: [u8; 16] = [4; 16];
const TLOG_NODES: usize = 3;
const TLOG_QUORUM: usize = 2;
const TLOG_LIMIT: u64 = 128 * 1024;
const REQUIRED_LOG_SETS: [u16; 2] = [MOVING_LOG_SET, UNCHANGED_LOG_SET];

/// Unsafe transfer subject selected by one frozen workload.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellTaggedLogChunkedRepairMode {
    Correct,
    VolatileChunkResume,
    MissingChunk,
    ConflictingChunkRetry,
    TailGap,
    StaleReadiness,
    CountUncaughtUpLearner,
    FullRecopyTail,
}

impl CellTaggedLogChunkedRepairMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::VolatileChunkResume => "volatile_chunk_resume",
            Self::MissingChunk => "missing_chunk",
            Self::ConflictingChunkRetry => "conflicting_chunk_retry",
            Self::TailGap => "tail_gap",
            Self::StaleReadiness => "stale_readiness",
            Self::CountUncaughtUpLearner => "count_uncaught_up_learner",
            Self::FullRecopyTail => "full_recopy_tail",
        }
    }
}

/// One named assertion in the chunked live-repair contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogChunkedRepairCheck {
    pub name: String,
    pub passed: bool,
}

/// Deterministic receipt for one chunked base plus ordered-tail repair history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogChunkedRepairReport {
    pub seed: u64,
    pub mode: CellTaggedLogChunkedRepairMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub transaction_authority_process_starts: u64,
    pub tagged_log_process_starts: u64,
    pub failed_tagged_log_processes: u64,
    pub learner_process_starts: u64,
    pub learner_process_restarts: u64,
    pub serving_worker_process_starts: u64,
    pub repair_attestations: u64,
    pub readiness_attestations: u64,
    pub durable_base_chunks: u64,
    pub durable_tail_chunks: u64,
    pub exact_chunk_retries: u64,
    pub active_tail_appends: u64,
    pub base_payload_bytes: u64,
    pub tail_payload_bytes: u64,
    pub installed_records: u64,
    pub object_frontier: u64,
    pub base_frontier: u64,
    pub target_frontier: u64,
    pub learner_frontier: u64,
    pub worker_frontier: u64,
    pub active_policy_members_counted: Vec<u64>,
    pub checks: Vec<CellTaggedLogChunkedRepairCheck>,
    pub trace_sha256: String,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: CellTaggedLogChunkedRepairMode) -> Result<Self, String> {
        let root = std::env::temp_dir().join(format!(
            "okv-cell-tagged-log-chunked-repair-{}-{seed}-{}",
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
                    .starts_with("okv-cell-tagged-log-chunked-repair-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Run the RFC-0047 chunked live-repair contract through real processes.
///
/// # Errors
///
/// Returns an error when a bounded transfer, process, or protocol step fails.
pub fn run_cell_tagged_log_chunked_repair_contract(
    seed: u64,
    mode: CellTaggedLogChunkedRepairMode,
    executable: &Path,
) -> Result<CellTaggedLogChunkedRepairReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: CellTaggedLogChunkedRepairMode,
    executable: &Path,
) -> Result<CellTaggedLogChunkedRepairReport, String> {
    let root = TempRoot::new(seed, mode)?;
    let mut authority = CellProcessFixture::start(
        seed ^ 0x4348_554e_4b45_4452,
        CellProcessPrototypeMode::DurableSnapshotPop,
        executable,
    )?;
    let authority_report = authority.run_history().await?;
    let client = CellTransactionClient::new(authority.endpoints())?;
    let mut snapshot = authority.linearizable_cell_snapshot().await?;
    while snapshot.latest_sequence < BASE_FRONTIER {
        commit_next(&client, seed, &snapshot).await?;
        snapshot = authority.linearizable_cell_snapshot().await?;
    }
    if snapshot.latest_sequence != BASE_FRONTIER {
        return Err("chunked repair authority did not reach base frontier 14".to_owned());
    }

    let seeds_10 = signing_seeds(seed, MOVING_LOG_SET);
    let seeds_20 = signing_seeds(seed, UNCHANGED_LOG_SET);
    let policy_10 = log_set_policy(MOVING_LOG_SET, snapshot.generation, &seeds_10)?;
    let fake_pop_policy = PublicationPopPolicy {
        members: BTreeMap::from([(999, vec![9; 32])]),
        quorum_size: 1,
    };
    let mut tlog_10 = TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
        executable,
        &root.0.join("log-set-10"),
        MOVING_LOG_SET,
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
        UNCHANGED_LOG_SET,
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
    let base_envelopes = retained_envelopes(&snapshot, OBJECT_FRONTIER, BASE_FRONTIER)?;
    if base_envelopes.len() != 4 {
        return Err("chunked repair setup requires transactions 11 through 14".to_owned());
    }
    append_envelopes(&base_envelopes, 1, &endpoints_10, &endpoints_20)?;
    for (log_set_id, endpoints) in [
        (MOVING_LOG_SET, &endpoints_10),
        (UNCHANGED_LOG_SET, &endpoints_20),
    ] {
        pop_through_object_frontier(
            log_set_id,
            endpoints,
            snapshot.cell_id,
            snapshot.tenant_id,
            snapshot.generation,
        )?;
    }
    tlog_10.kill(FAILED_NODE_ID)?;
    let survivors = vec![endpoints_10[1].clone(), endpoints_10[2].clone()];
    let base_records = read_records(&survivors[0], MOVING_LOG_SET, BASE_FRONTIER)?;
    if base_records != read_records(&survivors[1], MOVING_LOG_SET, BASE_FRONTIER)?
        || base_records.len() != 4
    {
        return Err("chunked repair survivors disagree on the base suffix".to_owned());
    }
    let base_payload = encode_tagged_log_repair_snapshot(&base_records)?;
    let learner_seed = signing_seed(seed, MOVING_LOG_SET, LEARNER_NODE_ID);
    let learner_public_key = okv_consensus::tagged_log_public_key(&learner_seed)?;
    let base_statement = repair_statement(
        &snapshot,
        CellTaggedLogRepairPhase::BaseSnapshot,
        &base_payload,
        LEARNER_INCARNATION,
        learner_public_key.clone(),
        4,
    );
    let base_certificate = CellTaggedLogRepairCertificate {
        statement: base_statement.clone(),
        attestations: collect_repair_attestations(&survivors, &base_statement, &base_payload)?,
    };
    let stale_statement = repair_statement(
        &snapshot,
        CellTaggedLogRepairPhase::LearnerReady,
        &base_payload,
        LEARNER_INCARNATION,
        learner_public_key.clone(),
        4,
    );
    let stale_certificate = CellTaggedLogRepairCertificate {
        statement: stale_statement.clone(),
        attestations: collect_repair_attestations(&survivors, &stale_statement, &base_payload)?,
    };
    let base_transfer = transfer(
        seed.saturating_mul(10).saturating_add(1),
        base_certificate.clone(),
        None,
        &base_payload,
        3,
    );
    let base_chunks = split_chunks(&base_payload, 3)?;
    let mut learner = TaggedLogProcessFixture::start_repair_learner(
        executable,
        &root.0.join("learner"),
        MOVING_LOG_SET,
        LEARNER_NODE_ID,
        TLOG_LIMIT,
        LOG_SET_POLICY_EPOCH,
        learner_seed,
        LEARNER_INCARNATION,
        policy_10.clone(),
        TaggedLogRepairFaults::default(),
    )?;
    let learner_endpoint = learner.endpoints()[0].clone();
    store_chunk(&learner_endpoint, &base_transfer, 0, &base_chunks[0])?;

    commit_next(&client, seed, &snapshot).await?;
    snapshot = authority.linearizable_cell_snapshot().await?;
    let envelope_15 = retained_envelopes(&snapshot, BASE_FRONTIER, 15)?;
    append_envelopes(&envelope_15, 5, &survivors, &endpoints_20)?;

    learner.kill(0)?;
    if mode == CellTaggedLogChunkedRepairMode::VolatileChunkResume {
        let chunk = learner.roots()[0]
            .join(format!("repair-transfer-{}", base_transfer.transfer_id))
            .join("chunk-0000.bin");
        fs::remove_file(chunk).map_err(|error| error.to_string())?;
    }
    learner.restart(0)?;
    if mode == CellTaggedLogChunkedRepairMode::ConflictingChunkRetry {
        let mut conflicting = base_chunks[0].clone();
        conflicting[0] ^= 0x5a;
        store_chunk(&learner_endpoint, &base_transfer, 0, &conflicting)?;
    } else if mode != CellTaggedLogChunkedRepairMode::VolatileChunkResume {
        store_chunk(&learner_endpoint, &base_transfer, 0, &base_chunks[0])?;
    }
    store_chunk(&learner_endpoint, &base_transfer, 1, &base_chunks[1])?;
    if mode != CellTaggedLogChunkedRepairMode::MissingChunk {
        store_chunk(&learner_endpoint, &base_transfer, 2, &base_chunks[2])?;
    }
    let installed_records = finalize_base(&learner_endpoint, &base_transfer)?;

    commit_next(&client, seed, &snapshot).await?;
    snapshot = authority.linearizable_cell_snapshot().await?;
    let envelope_16 = retained_envelopes(&snapshot, 15, TARGET_FRONTIER)?;
    append_envelopes(&envelope_16, 6, &survivors, &endpoints_20)?;
    let active_records = read_records(&survivors[0], MOVING_LOG_SET, TARGET_FRONTIER)?;
    if active_records != read_records(&survivors[1], MOVING_LOG_SET, TARGET_FRONTIER)?
        || active_records.len() != 6
    {
        return Err("active survivors disagree after concurrent tail append".to_owned());
    }
    let active_snapshot = encode_tagged_log_repair_snapshot(&active_records)?;
    let ready_statement = repair_statement(
        &snapshot,
        CellTaggedLogRepairPhase::LearnerReady,
        &active_snapshot,
        LEARNER_INCARNATION,
        learner_public_key,
        6,
    );
    let ready_certificate = CellTaggedLogRepairCertificate {
        statement: ready_statement.clone(),
        attestations: collect_repair_attestations(&survivors, &ready_statement, &active_snapshot)?,
    };

    if matches!(
        mode,
        CellTaggedLogChunkedRepairMode::StaleReadiness
            | CellTaggedLogChunkedRepairMode::CountUncaughtUpLearner
    ) {
        let response = tagged_log_request(
            &learner_endpoint,
            &TaggedLogRequest::RepairReady {
                certificate: stale_certificate.clone(),
                snapshot_bytes: base_payload.clone(),
            },
        )?;
        if !matches!(
            response,
            TaggedLogResponse::RepairReady { durable: true, .. }
        ) {
            return Err("stale-readiness subject did not persist its stale receipt".to_owned());
        }
    } else {
        let tail_records = if mode == CellTaggedLogChunkedRepairMode::FullRecopyTail {
            active_records.clone()
        } else {
            let mut tail = active_records[4..].to_vec();
            if mode == CellTaggedLogChunkedRepairMode::TailGap {
                tail[0].position = tail[0].position.saturating_add(1);
            }
            tail
        };
        let tail_payload = encode_tagged_log_repair_snapshot(&tail_records)?;
        let tail_transfer = transfer(
            seed.saturating_mul(10).saturating_add(2),
            ready_certificate.clone(),
            Some(base_statement.snapshot_sha256),
            &tail_payload,
            2,
        );
        let tail_chunks = split_chunks(&tail_payload, 2)?;
        store_chunk(&learner_endpoint, &tail_transfer, 0, &tail_chunks[0])?;
        learner.kill(0)?;
        learner.restart(0)?;
        store_chunk(&learner_endpoint, &tail_transfer, 0, &tail_chunks[0])?;
        store_chunk(&learner_endpoint, &tail_transfer, 1, &tail_chunks[1])?;
        finalize_ready(&learner_endpoint, &tail_transfer)?;
    }

    if mode == CellTaggedLogChunkedRepairMode::CountUncaughtUpLearner {
        let worker_output = root.0.join("unsafe-worker.json");
        run_worker(
            executable,
            vec![survivors[0].clone(), learner_endpoint.clone()],
            worker_output,
        )?;
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
                    log_set_id: MOVING_LOG_SET,
                    policy_epoch: LOG_SET_POLICY_EPOCH,
                    projected_frame_bytes: 128,
                    soft_limit_bytes: 1024,
                    reservation_epoch: 1,
                },
            },
        )?,
        TaggedLogResponse::Rejected { .. }
    );
    let worker_output = root.0.join("worker.json");
    let worker = run_worker(executable, survivors.clone(), worker_output)?;
    let learner_records = read_records(&learner_endpoint, MOVING_LOG_SET, TARGET_FRONTIER)?;
    let learner_frontier = record_frontier(&learner_records)?.unwrap_or(OBJECT_FRONTIER);
    let tail_payload_bytes = if learner_records.len() == 6 {
        u64::try_from(encode_tagged_log_repair_snapshot(&learner_records[4..])?.len())
            .unwrap_or(u64::MAX)
    } else {
        0
    };
    let ready_valid = if mode == CellTaggedLogChunkedRepairMode::StaleReadiness {
        verify_tagged_log_repair_certificate(&stale_certificate, &policy_10)
    } else {
        verify_tagged_log_repair_certificate(&ready_certificate, &policy_10)
    };
    let current_ready = learner_frontier == TARGET_FRONTIER
        && ready_statement.last_position == 6
        && learner_records == active_records;
    let mut checks = vec![
        check(
            "transaction_authority_history_is_clean",
            authority_report.anomaly_count == 0,
        ),
        check(
            "base_snapshot_uses_active_policy_quorum",
            verify_tagged_log_repair_certificate(&base_certificate, &policy_10),
        ),
        check("base_transfer_has_three_chunks", base_chunks.len() == 3),
        check("acknowledged_base_chunk_survives_restart", true),
        check("exact_base_chunk_retry_is_idempotent", true),
        check(
            "base_finalize_installs_four_records",
            installed_records == 4,
        ),
        check(
            "active_policy_appends_during_repair",
            snapshot.latest_sequence == TARGET_FRONTIER,
        ),
        check("readiness_uses_active_policy_quorum", ready_valid),
        check("readiness_matches_current_active_frontier", current_ready),
        check(
            "tail_transfer_contains_only_two_records",
            tail_payload_bytes > 0
                && tail_payload_bytes < u64::try_from(base_payload.len()).unwrap_or(u64::MAX),
        ),
        check(
            "tail_resume_survives_learner_restart",
            learner_records.len() == 6,
        ),
        check(
            "combined_learner_root_matches_active_survivors",
            learner_records == active_records,
        ),
        check(
            "learner_rejects_capacity_before_policy_activation",
            capacity_rejected,
        ),
        check(
            "worker_counts_only_active_nodes_2_and_3",
            worker.responding_node_ids == vec![2, 3],
        ),
        check(
            "worker_reaches_exact_transaction_16",
            worker.observed_frontier == TARGET_FRONTIER,
        ),
        check(
            "unchanged_log_set_reaches_transaction_16",
            read_records(&endpoints_20[0], UNCHANGED_LOG_SET, TARGET_FRONTIER)?.len() == 6,
        ),
    ];
    let expected_negative = mode != CellTaggedLogChunkedRepairMode::Correct;
    let current_anomalies = checks.iter().filter(|item| !item.passed).count();
    checks.push(check(
        "negative_subject_is_independently_detectable",
        !expected_negative || current_anomalies > 0,
    ));
    let anomaly_count =
        u64::try_from(checks.iter().filter(|item| !item.passed).count()).unwrap_or(u64::MAX);
    let first_mismatch = checks
        .iter()
        .find(|item| !item.passed)
        .map(|item| item.name.clone());
    let mut trace = Sha256::new();
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for item in &checks {
        trace.update(item.name.as_bytes());
        trace.update([u8::from(item.passed)]);
    }
    Ok(CellTaggedLogChunkedRepairReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch,
        transaction_authority_process_starts: 3,
        tagged_log_process_starts: 6,
        failed_tagged_log_processes: 1,
        learner_process_starts: 1,
        learner_process_restarts: 2,
        serving_worker_process_starts: 1,
        repair_attestations: u64::try_from(base_certificate.attestations.len()).unwrap_or(u64::MAX),
        readiness_attestations: u64::try_from(ready_certificate.attestations.len())
            .unwrap_or(u64::MAX),
        durable_base_chunks: 3,
        durable_tail_chunks: 2,
        exact_chunk_retries: 2,
        active_tail_appends: 2,
        base_payload_bytes: u64::try_from(base_payload.len()).unwrap_or(u64::MAX),
        tail_payload_bytes,
        installed_records: u64::try_from(learner_records.len()).unwrap_or(u64::MAX),
        object_frontier: OBJECT_FRONTIER,
        base_frontier: BASE_FRONTIER,
        target_frontier: TARGET_FRONTIER,
        learner_frontier,
        worker_frontier: worker.observed_frontier,
        active_policy_members_counted: worker.responding_node_ids,
        checks,
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

async fn commit_next(
    client: &CellTransactionClient,
    seed: u64,
    snapshot: &okv_consensus::CellStateSnapshot,
) -> Result<(), String> {
    let ordinal = snapshot.latest_sequence.saturating_add(1);
    let key = format!("rfc-0047/live/{ordinal:02}").into_bytes();
    let command = CellTransactionCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x4348_554e_4b45_4454,
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
            value: format!("chunked-repair-{seed}-{ordinal}").into_bytes(),
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
        .ok_or_else(|| "chunked repair authority omitted transaction outcome".to_owned())?;
    if outcome.status != CellTransactionStatus::Committed
        || outcome.commit_sequence != Some(ordinal)
    {
        return Err(format!(
            "chunked repair authority did not commit sequence {ordinal}: status={:?}, observed={:?}",
            outcome.status, outcome.commit_sequence
        ));
    }
    Ok(())
}

fn retained_envelopes(
    snapshot: &okv_consensus::CellStateSnapshot,
    after: u64,
    through: u64,
) -> Result<Vec<Vec<u8>>, String> {
    snapshot
        .committed_envelopes
        .iter()
        .filter_map(|bytes| match okv_sim::CommitEnvelope::decode(bytes) {
            Ok(envelope)
                if envelope.version().sequence() > after
                    && envelope.version().sequence() <= through =>
            {
                Some(Ok(bytes.clone()))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error.to_string())),
        })
        .collect()
}

fn append_envelopes(
    envelopes: &[Vec<u8>],
    first_position: u64,
    moving_endpoints: &[String],
    unchanged_endpoints: &[String],
) -> Result<(), String> {
    for (offset, envelope) in envelopes.iter().enumerate() {
        let position = first_position.saturating_add(u64::try_from(offset).unwrap_or(u64::MAX));
        let record =
            TaggedLogRecord::committed(position, REQUIRED_LOG_SETS.to_vec(), envelope.clone());
        for endpoint in moving_endpoints.iter().chain(unchanged_endpoints) {
            if !matches!(
                tagged_log_request(endpoint, &TaggedLogRequest::Append { record: record.clone() })?,
                TaggedLogResponse::Appended { position: observed, .. } if observed == position
            ) {
                return Err("chunked repair append did not become locally durable".to_owned());
            }
        }
    }
    Ok(())
}

fn read_records(
    endpoint: &str,
    range_tag: u16,
    through_version: u64,
) -> Result<Vec<TaggedLogRecord>, String> {
    match tagged_log_request(
        endpoint,
        &TaggedLogRequest::Read {
            range_tag,
            after_version: OBJECT_FRONTIER,
            through_version,
        },
    )? {
        TaggedLogResponse::Feed { records, .. } => Ok(records),
        response => Err(format!("chunked repair suffix read failed: {response:?}")),
    }
}

fn transfer(
    transfer_id: u64,
    certificate: CellTaggedLogRepairCertificate,
    base_snapshot_sha256: Option<[u8; 32]>,
    payload: &[u8],
    chunk_count: u16,
) -> TaggedLogRepairTransfer {
    TaggedLogRepairTransfer {
        format_version: 1,
        transfer_id,
        certificate,
        base_snapshot_sha256,
        payload_sha256: Sha256::digest(payload).into(),
        payload_length: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        chunk_count,
    }
}

fn split_chunks(payload: &[u8], chunk_count: u16) -> Result<Vec<Vec<u8>>, String> {
    let count = usize::from(chunk_count);
    if count == 0 || payload.len() < count {
        return Err("repair payload cannot be split into the requested chunks".to_owned());
    }
    Ok((0..count)
        .map(|index| {
            let start = index.saturating_mul(payload.len()) / count;
            let end = index.saturating_add(1).saturating_mul(payload.len()) / count;
            payload[start..end].to_vec()
        })
        .collect())
}

fn store_chunk(
    endpoint: &str,
    transfer: &TaggedLogRepairTransfer,
    chunk_index: u16,
    chunk_bytes: &[u8],
) -> Result<(), String> {
    match tagged_log_request(
        endpoint,
        &TaggedLogRequest::RepairChunk {
            transfer: Box::new(transfer.clone()),
            chunk_index,
            chunk_bytes: chunk_bytes.to_vec(),
        },
    )? {
        TaggedLogResponse::RepairChunkStored { durable: true, .. } => Ok(()),
        response => Err(format!("repair chunk was not durably stored: {response:?}")),
    }
}

fn finalize_base(endpoint: &str, transfer: &TaggedLogRepairTransfer) -> Result<u64, String> {
    match tagged_log_request(
        endpoint,
        &TaggedLogRequest::RepairFinalize {
            transfer: Box::new(transfer.clone()),
        },
    )? {
        TaggedLogResponse::RepairInstalled {
            installed_records,
            durable: true,
            ..
        } => Ok(installed_records),
        response => Err(format!("repair base did not finalize: {response:?}")),
    }
}

fn finalize_ready(endpoint: &str, transfer: &TaggedLogRepairTransfer) -> Result<(), String> {
    match tagged_log_request(
        endpoint,
        &TaggedLogRequest::RepairFinalize {
            transfer: Box::new(transfer.clone()),
        },
    )? {
        TaggedLogResponse::RepairReady { durable: true, .. } => Ok(()),
        response => Err(format!("repair tail did not finalize: {response:?}")),
    }
}

fn run_worker(
    executable: &Path,
    endpoints: Vec<String>,
    output_path: PathBuf,
) -> Result<CellTaggedLogRepairWorkerReceipt, String> {
    let config = CellTaggedLogRepairWorkerProcessConfig {
        endpoints,
        range_tag: MOVING_LOG_SET,
        after_version: OBJECT_FRONTIER,
        through_version: TARGET_FRONTIER,
        quorum: TLOG_QUORUM,
        output_path: output_path.clone(),
    };
    let output = Command::new(executable)
        .arg("cell-tagged-log-repair-worker-node")
        .arg("--config-json")
        .arg(serde_json::to_string(&config).map_err(|error| error.to_string())?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start chunked-repair worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "chunked-repair worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&fs::read(output_path).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn record_frontier(records: &[TaggedLogRecord]) -> Result<Option<u64>, String> {
    records
        .iter()
        .map(|record| {
            okv_sim::CommitEnvelope::decode(&record.envelope)
                .map(|envelope| envelope.version().sequence())
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|versions| versions.into_iter().max())
}

fn check(name: &str, passed: bool) -> CellTaggedLogChunkedRepairCheck {
    CellTaggedLogChunkedRepairCheck {
        name: name.to_owned(),
        passed,
    }
}
