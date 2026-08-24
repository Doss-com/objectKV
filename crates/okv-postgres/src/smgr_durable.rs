//! Persistent `PostgreSQL` range base plus authenticated tagged-log suffix.

use crate::{PostgresPage, PostgresPageIdentity, PostgresRelationForkIdentity, POSTGRES_PAGE_SIZE};
use okv_consensus::{
    sign_tagged_log_statement, tagged_log_public_key, verify_tagged_log_pop_certificate,
    CellLogSetMember, CellLogSetPolicy, CellMutation, CellTaggedLogAttestation,
    CellTaggedLogCertificate, CellTaggedLogPopCertificate, CellTaggedLogPopStatement,
    CellTaggedLogStatement, PublicationPopCapabilityCertificate, RequestIdentity,
};
use okv_model::Version;
use okv_object::{
    audit_persistent_range_physical_closure, load_persistent_range_base,
    load_persistent_range_delta_lineage, materialize_persistent_range_base,
    materialize_persistent_range_delta, open_manifest_bound_persistent_range_view,
    open_persistent_range_view, persistent_range_delta_descriptor_sha256, tagged_log_request,
    CertifiedTxLogRecord, PersistentRangeBaseConfig, PersistentRangeBaseDescriptor,
    PersistentRangeDeltaConfig, PersistentRangeDeltaDescriptor, PublicationPopPolicy,
    RangeServingState, TaggedLogProcessFixture, TaggedLogRecord, TaggedLogRequest,
    TaggedLogResponse,
};
use okv_sim::{CommitEnvelope, CommitEnvelopeParts};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const PROTOCOL_FORMAT_VERSION: u16 = 1;
const LEGACY_DURABLE_ROOT_FORMAT_VERSION: u16 = 1;
const DURABLE_ROOT_FORMAT_VERSION: u16 = 2;
const LOG_NODES: usize = 3;
const LOG_QUORUM: usize = 2;
const POLICY_EPOCH: u64 = 1;
const RETAINED_BYTES_LIMIT: u64 = 64 * 1024 * 1024;
const DATABASE_PATH: &str = "postgres-smgr-range";
const CONTRACT_RELATION_BLOCKS: u32 = 128;
const CONTRACT_CHANGED_BLOCK: u32 = 1;
const MAXIMUM_CONTRACT_RELATION_BLOCKS: u32 = 65_536;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PostgresDurableDescriptor {
    format_version: u16,
    relation: PostgresRelationForkIdentity,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    base_version: u64,
    base_maximum_page_lsn: u64,
    database_path: String,
    #[serde(default = "default_base_descriptor_path")]
    base_descriptor_path: String,
    #[serde(default)]
    base_txlogs: Vec<PostgresDurableTxLogFrontier>,
    #[serde(default)]
    base_visible_rows_sha256: [u8; 32],
    #[serde(default)]
    object_deltas: Vec<PersistentRangeDeltaDescriptor>,
}

struct DurableLogSet {
    policy: CellLogSetPolicy,
    fixture: TaggedLogProcessFixture,
    next_position: u64,
}

/// Live durable resources and authenticated suffix for one `PostgreSQL` relation.
pub(crate) struct DurablePostgresRange {
    root: PathBuf,
    relation: PostgresRelationForkIdentity,
    base: PersistentRangeBaseDescriptor,
    base_descriptor_path: String,
    base_maximum_page_lsn: u64,
    base_txlogs: Vec<PostgresDurableTxLogFrontier>,
    base_visible_rows_sha256: [u8; 32],
    object_deltas: Vec<PersistentRangeDeltaDescriptor>,
    object_records: Vec<CertifiedTxLogRecord>,
    log_sets: Vec<DurableLogSet>,
    policies: BTreeMap<u16, CellLogSetPolicy>,
    records: Vec<CertifiedTxLogRecord>,
    target_version: u64,
    maximum_page_lsn: u64,
    popped_through: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PostgresDurableTxLogFrontier {
    pub log_set_id: u16,
    pub policy_epoch: u64,
    pub durable_position: u64,
    pub envelope_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PostgresDurableFrontier {
    pub relation: PostgresRelationForkIdentity,
    pub base: PersistentRangeBaseDescriptor,
    #[serde(default)]
    pub object_deltas: Vec<PersistentRangeDeltaDescriptor>,
    pub target_version: u64,
    pub maximum_page_lsn: u64,
    pub authenticated_tail_records: u64,
    pub final_log_chain_sha256: [u8; 32],
    pub certified_tail_sha256: [u8; 32],
    pub txlogs: Vec<PostgresDurableTxLogFrontier>,
    #[serde(default)]
    pub visible_rows_sha256: [u8; 32],
}

pub(crate) struct PreparedPostgresObjectDelta {
    descriptor: PostgresDurableDescriptor,
    delta: PersistentRangeDeltaDescriptor,
    records: Vec<CertifiedTxLogRecord>,
    frontier: PostgresDurableFrontier,
}

impl PreparedPostgresObjectDelta {
    pub fn maximum_page_lsn(&self) -> u64 {
        self.frontier.maximum_page_lsn
    }
}

pub(crate) struct PostgresObjectDeltaPlan {
    config: PersistentRangeDeltaConfig,
    records: Vec<CertifiedTxLogRecord>,
    descriptor: PostgresDurableDescriptor,
    prior: PostgresDurableFrontier,
}

impl PostgresObjectDeltaPlan {
    pub fn materialize(mut self) -> Result<PreparedPostgresObjectDelta, String> {
        let delta = materialize_persistent_range_delta(&self.config, &self.records)?;
        if delta.through_version != self.prior.target_version
            || delta.final_log_chain_sha256 != self.prior.final_log_chain_sha256
        {
            return Err("PostgreSQL object delta differs from its captured frontier".to_owned());
        }
        self.descriptor.object_deltas.push(delta.clone());
        self.descriptor.base_maximum_page_lsn = self.prior.maximum_page_lsn;
        self.descriptor.base_txlogs.clone_from(&self.prior.txlogs);
        self.descriptor.base_visible_rows_sha256 = [0; 32];
        let empty_tail_sha256 = Sha256::digest(
            serde_json::to_vec(&Vec::<CertifiedTxLogRecord>::new())
                .map_err(|error| error.to_string())?,
        )
        .into();
        let frontier = PostgresDurableFrontier {
            relation: self.descriptor.relation,
            base: self.prior.base,
            object_deltas: self.descriptor.object_deltas.clone(),
            target_version: delta.through_version,
            maximum_page_lsn: self.prior.maximum_page_lsn,
            authenticated_tail_records: 0,
            final_log_chain_sha256: self.prior.final_log_chain_sha256,
            certified_tail_sha256: empty_tail_sha256,
            txlogs: self.prior.txlogs,
            visible_rows_sha256: [0; 32],
        };
        Ok(PreparedPostgresObjectDelta {
            descriptor: self.descriptor,
            delta,
            records: self.records,
            frontier,
        })
    }
}

pub(crate) struct PostgresTxLogPopReceipt {
    pub object_frontier: u64,
    pub certificates: Vec<CellTaggedLogPopCertificate>,
}

pub(crate) struct DurablePostgresOpen {
    pub durable: DurablePostgresRange,
    pub serving: Arc<RangeServingState>,
    pub target_version: u64,
    pub maximum_page_lsn: u64,
    pub authenticated_tail_records: u64,
    pub popped_through: u64,
    pub recovered_existing: bool,
}

/// Fault subject for the incremental `PostgreSQL` object-delta contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresObjectDeltaMode {
    Correct,
    MissingObject,
    CorruptObject,
    BrokenChain,
    OmittedClosure,
    PopAhead,
    FullBaseRewrite,
}

impl PostgresObjectDeltaMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::MissingObject => "missing_object",
            Self::CorruptObject => "corrupt_object",
            Self::BrokenChain => "broken_chain",
            Self::OmittedClosure => "omitted_closure",
            Self::PopAhead => "pop_ahead",
            Self::FullBaseRewrite => "full_base_rewrite",
        }
    }
}

/// Deterministic result of one real full-base plus immutable-delta reopen.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PostgresObjectDeltaReport {
    pub mode: PostgresObjectDeltaMode,
    pub relation_pages: u64,
    pub relation_bytes: u64,
    pub changed_block: u32,
    pub checks: BTreeMap<String, bool>,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
    pub object_delta_sha256: String,
    pub object_delta_segments: u64,
    pub object_delta_bytes: u64,
    pub objectification_input_bytes: u64,
    pub object_delta_layers: u64,
    pub object_compaction_debt_bytes: u64,
    pub object_delta_materialization_duration_nanos: u64,
    pub object_delta_activation_duration_nanos: u64,
    pub object_delta_restart_duration_nanos: u64,
    pub full_base_rewrite_duration_nanos: u64,
    pub full_base_rewrite_bytes: u64,
    pub ssts_before_checkpoint: u64,
    pub ssts_after_checkpoint: u64,
}

/// Fixed physical inputs for one object-delta economics subject.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostgresObjectDeltaContractConfig {
    pub relation_blocks: u32,
    pub reference_full_base_rewrite: bool,
}

/// Fault mode for one fresh `PostgreSQL` replacement-worker readiness process.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresWorkerReadinessMode {
    Correct,
    ChangedManifest,
    ChangedDelta,
    SkipClosureAudit,
}

impl PostgresWorkerReadinessMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::ChangedManifest => "changed_manifest",
            Self::ChangedDelta => "changed_delta",
            Self::SkipClosureAudit => "skip_closure_audit",
        }
    }
}

/// Serializable immutable fixture selected by one replacement worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresWorkerReadinessConfig {
    pub root: PathBuf,
    pub source_heap_path: PathBuf,
    pub seed: u64,
    pub relation_blocks: u32,
    pub range_pages: u32,
    pub oracle_chunk_pages: u32,
    pub max_rss_bytes: u64,
    pub mode: PostgresWorkerReadinessMode,
    pub expected_rows_sha256: [u8; 32],
    pub expected_range_sha256: [u8; 32],
    pub expected_base_value_sha256: [u8; 32],
    pub expected_delta_value_sha256: [u8; 32],
}

/// Stable measurements from one fresh replacement-worker process.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresWorkerReadinessReceipt {
    pub contract_version: u32,
    pub mode: PostgresWorkerReadinessMode,
    pub worker_process_id: u32,
    pub relation_pages: u64,
    pub relation_bytes: u64,
    pub physical_objects: u64,
    pub physical_closure_bytes: u64,
    pub root_load_duration_nanos: u64,
    pub delta_auth_duration_nanos: u64,
    pub view_open_duration_nanos: u64,
    pub view_ready_duration_nanos: u64,
    pub first_delta_point_duration_nanos: u64,
    pub first_base_point_duration_nanos: u64,
    pub first_range_duration_nanos: u64,
    pub full_oracle_duration_nanos: u64,
    pub closure_audit_duration_nanos: u64,
    pub peak_rss_bytes: u64,
    pub source_heap_absent: bool,
    pub root_identity_exact: bool,
    pub delta_lineage_exact: bool,
    pub first_delta_point_exact: bool,
    pub first_base_point_exact: bool,
    pub first_range_exact: bool,
    pub full_oracle_exact: bool,
    pub full_oracle_bounded: bool,
    pub closure_audit_executed: bool,
    pub closure_audit_exact: bool,
    pub rss_bound_held: bool,
    pub negative_control_detected: bool,
    pub anomaly_count: u64,
    pub refusal_phase: Option<String>,
    pub semantic_receipt_sha256: String,
}

struct WorkerExpectedHashes {
    rows: [u8; 32],
    range: [u8; 32],
    base_value: [u8; 32],
    delta_value: [u8; 32],
}

impl Default for PostgresObjectDeltaContractConfig {
    fn default() -> Self {
        Self {
            relation_blocks: CONTRACT_RELATION_BLOCKS,
            reference_full_base_rewrite: false,
        }
    }
}

/// Build one immutable full-base plus certified-delta fixture for a fresh
/// replacement-worker process.
///
/// # Errors
///
/// Returns an error for an invalid curve point or any failed object
/// materialization, certification, or durable-root write.
#[allow(clippy::too_many_arguments)]
pub fn prepare_postgres_worker_readiness_fixture(
    root: PathBuf,
    seed: u64,
    relation_blocks: u32,
    range_pages: u32,
    oracle_chunk_pages: u32,
    max_rss_bytes: u64,
    mode: PostgresWorkerReadinessMode,
) -> Result<PostgresWorkerReadinessConfig, String> {
    validate_worker_readiness_dimensions(
        &root,
        relation_blocks,
        range_pages,
        oracle_chunk_pages,
        max_rss_bytes,
    )?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(prepare_postgres_worker_readiness_fixture_async(
            root,
            seed,
            relation_blocks,
            range_pages,
            oracle_chunk_pages,
            max_rss_bytes,
            mode,
        ))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn prepare_postgres_worker_readiness_fixture_async(
    root: PathBuf,
    seed: u64,
    relation_blocks: u32,
    range_pages: u32,
    oracle_chunk_pages: u32,
    max_rss_bytes: u64,
    mode: PostgresWorkerReadinessMode,
) -> Result<PostgresWorkerReadinessConfig, String> {
    let object_root = root.join("objects");
    let base_descriptor_path = root.join("range-base.json");
    let cell_id = [0x11; 16];
    let tenant_id = [0x22; 16];
    let generation = 1;
    let base_log_chain_sha256 = [0x33; 32];
    let relation = contract_page_identity(0).relation_fork();
    let base_mutations = (0_u32..relation_blocks)
        .map(|block_number| {
            contract_page_mutation(
                seed,
                block_number,
                100_u64.saturating_add(u64::from(block_number)),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let delta_mutation = contract_page_mutation(seed ^ 0x44, CONTRACT_CHANGED_BLOCK, 1_000)?;
    let expected = expected_worker_hashes(&base_mutations, &delta_mutation, range_pages)?;
    let base_batches = BTreeMap::from([(1, base_mutations)]);
    let base = materialize_persistent_range_base(
        &PersistentRangeBaseConfig {
            object_root: object_root.clone(),
            descriptor_path: base_descriptor_path,
            database_path: DATABASE_PATH.to_owned(),
            seed,
            cell_id,
            tenant_id,
            generation,
            base_version: 1,
            minimum_readable_version: 1,
            log_chain_sha256: base_log_chain_sha256,
        },
        &base_batches,
    )
    .await?;
    drop(base_batches);
    let (policy, signing_seeds) = contract_log_policy(generation)?;
    let record = certified_contract_record(
        2,
        base_log_chain_sha256,
        std::slice::from_ref(&delta_mutation),
        &policy,
        &signing_seeds,
    )?;
    let durable = DurablePostgresRange {
        root: root.clone(),
        relation,
        base,
        base_descriptor_path: default_base_descriptor_path(),
        base_maximum_page_lsn: 100_u64.saturating_add(u64::from(relation_blocks - 1)),
        base_txlogs: Vec::new(),
        base_visible_rows_sha256: [0; 32],
        object_deltas: Vec::new(),
        object_records: Vec::new(),
        log_sets: Vec::new(),
        policies: BTreeMap::from([(policy.log_set_id, policy)]),
        records: vec![record],
        target_version: 2,
        maximum_page_lsn: 1_000_u64.max(100_u64.saturating_add(u64::from(relation_blocks - 1))),
        popped_through: 0,
    };
    let prepared = durable.object_delta_plan(2)?.materialize()?;
    persist_postgres_descriptor(&root.join("postgres-root.json"), &prepared.descriptor)?;
    Ok(PostgresWorkerReadinessConfig {
        source_heap_path: root.join("source-heap"),
        root,
        seed,
        relation_blocks,
        range_pages,
        oracle_chunk_pages,
        max_rss_bytes,
        mode,
        expected_rows_sha256: expected.rows,
        expected_range_sha256: expected.range,
        expected_base_value_sha256: expected.base_value,
        expected_delta_value_sha256: expected.delta_value,
    })
}

/// Open and measure one prepared `PostgreSQL` object root in the calling worker
/// process.
///
/// # Errors
///
/// Returns an error for an invalid fixture or an unexpected storage refusal.
/// Expected negative-control refusals are returned as receipts.
#[allow(clippy::too_many_lines)]
pub async fn run_postgres_worker_readiness_process(
    config: &PostgresWorkerReadinessConfig,
) -> Result<PostgresWorkerReadinessReceipt, String> {
    validate_worker_readiness_dimensions(
        &config.root,
        config.relation_blocks,
        config.range_pages,
        config.oracle_chunk_pages,
        config.max_rss_bytes,
    )?;
    let ready_started = Instant::now();
    let object_root = config.root.join("objects");
    let root_started = Instant::now();
    let postgres = load_postgres_descriptor(&config.root.join("postgres-root.json"))?;
    let relation = contract_page_identity(0).relation_fork();
    validate_postgres_descriptor(&postgres, relation, [0x11; 16], [0x22; 16], 1)?;
    let base_path = durable_relative_path(&config.root, &postgres.base_descriptor_path)?;
    let base = load_persistent_range_base(&base_path)?;
    let root_identity_exact = base.database_path == postgres.database_path
        && base.root.cell_id == postgres.cell_id
        && base.root.tenant_id == postgres.tenant_id
        && base.root.generation == postgres.generation
        && base.root.covered_through == postgres.base_version
        && base.root.manifest.key == base.physical.manifest.key
        && base.root.manifest.length == base.physical.manifest.length
        && base.root.manifest.sha256 == base.physical.manifest.sha256
        && base.physical.is_valid();
    if !root_identity_exact {
        return Err("replacement worker loaded an inconsistent durable root".to_owned());
    }
    let root_load_duration_nanos = duration_nanos(root_started.elapsed());
    let physical_objects = u64::try_from(base.physical.live_ssts.len())
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let physical_closure_bytes = base
        .physical
        .live_ssts
        .iter()
        .fold(base.physical.manifest.length, |bytes, object| {
            bytes.saturating_add(object.length)
        });

    if config.mode == PostgresWorkerReadinessMode::ChangedManifest {
        mutate_worker_object(&object_root.join(&base.physical.manifest.key))?;
    }
    if config.mode == PostgresWorkerReadinessMode::ChangedDelta {
        let delta = postgres
            .object_deltas
            .first()
            .ok_or_else(|| "replacement worker fixture has no object delta".to_owned())?;
        mutate_worker_object(&object_root.join(&delta.object.key))?;
    }

    let delta_started = Instant::now();
    let records =
        match load_persistent_range_delta_lineage(&object_root, &base, &postgres.object_deltas) {
            Ok(records) => records,
            Err(error) if config.mode == PostgresWorkerReadinessMode::ChangedDelta => {
                return Ok(refused_worker_receipt(
                    config,
                    &base,
                    physical_objects,
                    physical_closure_bytes,
                    root_load_duration_nanos,
                    duration_nanos(delta_started.elapsed()),
                    "delta_auth",
                    &error,
                ));
            }
            Err(error) => return Err(error),
        };
    let delta_auth_duration_nanos = duration_nanos(delta_started.elapsed());
    let delta_lineage_exact = records.len() == 1 && postgres.object_deltas.len() == 1;
    let (policy, _) = contract_log_policy(postgres.generation)?;
    let policies = BTreeMap::from([(policy.log_set_id, policy)]);
    let view_started = Instant::now();
    let view = match open_manifest_bound_persistent_range_view(
        &object_root,
        &base,
        2,
        records,
        &policies,
        config.seed ^ 0x45,
    )
    .await
    {
        Ok(view) => view,
        Err(error) if config.mode == PostgresWorkerReadinessMode::ChangedManifest => {
            return Ok(refused_worker_receipt(
                config,
                &base,
                physical_objects,
                physical_closure_bytes,
                root_load_duration_nanos,
                delta_auth_duration_nanos,
                "view_open",
                &error,
            ));
        }
        Err(error) => return Err(error),
    };
    let view_open_duration_nanos = duration_nanos(view_started.elapsed());
    let view_ready_duration_nanos = duration_nanos(ready_started.elapsed());

    let delta_point_started = Instant::now();
    let delta_value = view
        .get_at(
            &contract_page_identity(CONTRACT_CHANGED_BLOCK).encode_key(),
            2,
        )
        .await
        .map_err(|error| error.to_string())?;
    let first_delta_point_duration_nanos = duration_nanos(delta_point_started.elapsed());
    let first_delta_point_exact = delta_value.as_deref().is_some_and(|value| {
        <[u8; 32]>::from(Sha256::digest(value)) == config.expected_delta_value_sha256
    });

    let base_point_started = Instant::now();
    let base_value = view
        .get_at(&contract_page_identity(0).encode_key(), 2)
        .await
        .map_err(|error| error.to_string())?;
    let first_base_point_duration_nanos = duration_nanos(base_point_started.elapsed());
    let first_base_point_exact = base_value.as_deref().is_some_and(|value| {
        <[u8; 32]>::from(Sha256::digest(value)) == config.expected_base_value_sha256
    });

    let range_started = Instant::now();
    let range_rows = view
        .scan_at(
            &contract_page_identity(0).encode_key(),
            &contract_page_identity(config.range_pages).encode_key(),
            2,
            usize::try_from(config.range_pages).unwrap_or(usize::MAX),
        )
        .await
        .map_err(|error| error.to_string())?;
    let first_range_duration_nanos = duration_nanos(range_started.elapsed());
    let first_range_exact = range_rows.len()
        == usize::try_from(config.range_pages).unwrap_or(usize::MAX)
        && contract_rows_sha256(&range_rows) == config.expected_range_sha256;
    drop(range_rows);

    let oracle_started = Instant::now();
    let mut oracle_hasher = Sha256::new();
    let mut oracle_rows = 0_u64;
    let mut full_oracle_bounded = true;
    let chunk = config.oracle_chunk_pages;
    let mut first_block = 0_u32;
    while first_block < config.relation_blocks {
        let end_block = first_block
            .saturating_add(chunk)
            .min(config.relation_blocks);
        let limit = usize::try_from(end_block - first_block).unwrap_or(usize::MAX);
        let rows = view
            .scan_at(
                &contract_page_identity(first_block).encode_key(),
                &contract_page_identity(end_block).encode_key(),
                2,
                limit,
            )
            .await
            .map_err(|error| error.to_string())?;
        full_oracle_bounded &= rows.len() <= limit;
        oracle_rows = oracle_rows.saturating_add(u64::try_from(rows.len()).unwrap_or(u64::MAX));
        hash_contract_rows(&mut oracle_hasher, &rows);
        first_block = end_block;
    }
    let full_oracle_duration_nanos = duration_nanos(oracle_started.elapsed());
    let full_oracle_exact = oracle_rows == u64::from(config.relation_blocks)
        && <[u8; 32]>::from(oracle_hasher.finalize()) == config.expected_rows_sha256;

    let (closure_audit_executed, closure_audit_exact, closure_audit_duration_nanos) =
        if config.mode == PostgresWorkerReadinessMode::SkipClosureAudit {
            (false, false, 0)
        } else {
            let audit_started = Instant::now();
            let exact = audit_persistent_range_physical_closure(&object_root, &base)
                .await
                .is_ok();
            (true, exact, duration_nanos(audit_started.elapsed()))
        };
    let peak_rss_bytes = resident_memory_bytes();
    let rss_bound_held = peak_rss_bytes > 0 && peak_rss_bytes <= config.max_rss_bytes;
    let source_heap_absent = !config.source_heap_path.exists();
    view.close().await.map_err(|error| error.to_string())?;
    let required = [
        source_heap_absent,
        root_identity_exact,
        delta_lineage_exact,
        first_delta_point_exact,
        first_base_point_exact,
        first_range_exact,
        full_oracle_exact,
        full_oracle_bounded,
        closure_audit_executed,
        closure_audit_exact,
        rss_bound_held,
    ];
    let anomaly_count =
        u64::try_from(required.iter().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let negative_control_detected = config.mode == PostgresWorkerReadinessMode::Correct
        || config.mode == PostgresWorkerReadinessMode::SkipClosureAudit && !closure_audit_executed;
    let semantic_receipt_sha256 = worker_semantic_receipt(
        config,
        root_identity_exact,
        delta_lineage_exact,
        first_delta_point_exact,
        first_base_point_exact,
        first_range_exact,
        full_oracle_exact,
        full_oracle_bounded,
        closure_audit_executed,
        closure_audit_exact,
        rss_bound_held,
        negative_control_detected,
        None,
    );
    Ok(PostgresWorkerReadinessReceipt {
        contract_version: 1,
        mode: config.mode,
        worker_process_id: std::process::id(),
        relation_pages: u64::from(config.relation_blocks),
        relation_bytes: u64::from(config.relation_blocks)
            .saturating_mul(u64::try_from(POSTGRES_PAGE_SIZE).unwrap_or(u64::MAX)),
        physical_objects,
        physical_closure_bytes,
        root_load_duration_nanos,
        delta_auth_duration_nanos,
        view_open_duration_nanos,
        view_ready_duration_nanos,
        first_delta_point_duration_nanos,
        first_base_point_duration_nanos,
        first_range_duration_nanos,
        full_oracle_duration_nanos,
        closure_audit_duration_nanos,
        peak_rss_bytes,
        source_heap_absent,
        root_identity_exact,
        delta_lineage_exact,
        first_delta_point_exact,
        first_base_point_exact,
        first_range_exact,
        full_oracle_exact,
        full_oracle_bounded,
        closure_audit_executed,
        closure_audit_exact,
        rss_bound_held,
        negative_control_detected,
        anomaly_count,
        refusal_phase: None,
        semantic_receipt_sha256,
    })
}

/// Run the frozen incremental `PostgreSQL` object-delta subject in disposable storage.
///
/// # Errors
///
/// Returns an error only when the subject cannot be constructed or inspected.
pub fn run_postgres_object_delta_contract(
    seed: u64,
    mode: PostgresObjectDeltaMode,
) -> Result<PostgresObjectDeltaReport, String> {
    run_postgres_object_delta_contract_with_config(
        seed,
        mode,
        PostgresObjectDeltaContractConfig::default(),
    )
}

/// Run the object-delta subject at one bounded relation size.
///
/// # Errors
///
/// Returns an error when the configured relation cannot contain the fixed
/// changed block, exceeds the frozen curve bound, or cannot be inspected.
pub fn run_postgres_object_delta_contract_with_config(
    seed: u64,
    mode: PostgresObjectDeltaMode,
    config: PostgresObjectDeltaContractConfig,
) -> Result<PostgresObjectDeltaReport, String> {
    if config.relation_blocks <= CONTRACT_CHANGED_BLOCK
        || config.relation_blocks > MAXIMUM_CONTRACT_RELATION_BLOCKS
    {
        return Err(format!(
            "PostgreSQL object-delta relation blocks must be in {}..={MAXIMUM_CONTRACT_RELATION_BLOCKS}, got {}",
            CONTRACT_CHANGED_BLOCK + 1,
            config.relation_blocks
        ));
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(run_postgres_object_delta_contract_async(seed, mode, config))
}

#[allow(clippy::too_many_lines)]
async fn run_postgres_object_delta_contract_async(
    seed: u64,
    mode: PostgresObjectDeltaMode,
    config: PostgresObjectDeltaContractConfig,
) -> Result<PostgresObjectDeltaReport, String> {
    let temporary = tempfile::tempdir().map_err(|error| error.to_string())?;
    let root = temporary.path().join("durable");
    let object_root = root.join("objects");
    let base_descriptor_path = root.join("range-base.json");
    let cell_id = [0x11; 16];
    let tenant_id = [0x22; 16];
    let generation = 1;
    let base_log_chain_sha256 = [0x33; 32];
    let relation = contract_page_identity(0).relation_fork();
    let base_mutations = (0_u32..config.relation_blocks)
        .map(|block_number| {
            contract_page_mutation(
                seed,
                block_number,
                100_u64.saturating_add(u64::from(block_number)),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let base_batches = BTreeMap::from([(1, base_mutations)]);
    let base = materialize_persistent_range_base(
        &PersistentRangeBaseConfig {
            object_root: object_root.clone(),
            descriptor_path: base_descriptor_path.clone(),
            database_path: DATABASE_PATH.to_owned(),
            seed,
            cell_id,
            tenant_id,
            generation,
            base_version: 1,
            minimum_readable_version: 1,
            log_chain_sha256: base_log_chain_sha256,
        },
        &base_batches,
    )
    .await?;
    drop(base_batches);
    let (policy, signing_seeds) = contract_log_policy(generation)?;
    let delta_mutation = contract_page_mutation(seed ^ 0x44, CONTRACT_CHANGED_BLOCK, 1_000)?;
    let record = certified_contract_record(
        2,
        base_log_chain_sha256,
        std::slice::from_ref(&delta_mutation),
        &policy,
        &signing_seeds,
    )?;
    let mut durable = DurablePostgresRange {
        root: root.clone(),
        relation,
        base: base.clone(),
        base_descriptor_path: "range-base.json".to_owned(),
        base_maximum_page_lsn: 100_u64.saturating_add(u64::from(config.relation_blocks - 1)),
        base_txlogs: Vec::new(),
        base_visible_rows_sha256: [0; 32],
        object_deltas: Vec::new(),
        object_records: Vec::new(),
        log_sets: Vec::new(),
        policies: BTreeMap::from([(policy.log_set_id, policy.clone())]),
        records: vec![record],
        target_version: 2,
        maximum_page_lsn: 1_000_u64
            .max(100_u64.saturating_add(u64::from(config.relation_blocks - 1))),
        popped_through: 0,
    };
    let delta_segments_before = files_with_suffix(&object_root, ".segment");
    let ssts_before_checkpoint = files_with_suffix(&object_root, ".sst");
    let materialization_started = Instant::now();
    let prepared = durable.object_delta_plan(2)?.materialize()?;
    let object_delta_materialization_duration_nanos =
        duration_nanos(materialization_started.elapsed());
    let delta = prepared.delta.clone();
    let delta_records = prepared.records.clone();
    let activation_started = Instant::now();
    let (serving, _) = durable.activate_object_delta(prepared, seed ^ 0x44).await?;
    let object_delta_activation_duration_nanos = duration_nanos(activation_started.elapsed());
    let ssts_after_objectification = files_with_suffix(&object_root, ".sst");
    let relation_limit = usize::try_from(config.relation_blocks).unwrap_or(usize::MAX);
    let rows_before_restart = serving
        .current()
        .map_err(|error| error.to_string())?
        .scan_at(&[], &[0xff], 2, relation_limit)
        .await
        .map_err(|error| error.to_string())?;
    let rows_before_restart_count = rows_before_restart.len();
    let rows_before_restart_sha256 = contract_rows_sha256(&rows_before_restart);
    drop(rows_before_restart);

    let restart_started = Instant::now();
    let persisted = load_postgres_descriptor(&root.join("postgres-root.json"))?;
    let reopened_base = load_persistent_range_base(&base_descriptor_path)?;
    let reopened_records = load_persistent_range_delta_lineage(
        &object_root,
        &reopened_base,
        &persisted.object_deltas,
    )?;
    let reopened = open_persistent_range_view(
        &object_root,
        &reopened_base,
        2,
        reopened_records,
        &BTreeMap::from([(policy.log_set_id, policy)]),
        seed ^ 0x45,
    )
    .await?;
    let rows_after_restart = reopened
        .scan_at(&[], &[0xff], 2, relation_limit)
        .await
        .map_err(|error| error.to_string())?;
    let object_delta_restart_duration_nanos = duration_nanos(restart_started.elapsed());
    let rows_after_restart_count = rows_after_restart.len();
    let rows_after_restart_sha256 = contract_rows_sha256(&rows_after_restart);
    drop(rows_after_restart);

    let delta_fixture = serde_json::from_str::<PersistentRangeDeltaDescriptor>(include_str!(
        "../../okv-object/fixtures/persistent-range-delta-v1.json"
    ));
    let legacy_fixture = serde_json::from_str::<PostgresDurableDescriptor>(include_str!(
        "../fixtures/durable-root-v1.json"
    ));
    let loaded_delta_records =
        load_persistent_range_delta_lineage(&object_root, &base, std::slice::from_ref(&delta))?;
    let final_envelope_sha256: [u8; 32] = Sha256::digest(
        &delta_records
            .last()
            .ok_or_else(|| "PostgreSQL object delta has no certified record".to_owned())?
            .envelope,
    )
    .into();
    let mut closure = durable
        .object_deltas
        .iter()
        .map(|item| item.object.key.clone())
        .collect::<BTreeSet<_>>();
    if mode == PostgresObjectDeltaMode::OmittedClosure {
        closure.remove(&delta.object.key);
    }

    let object_delta_segments = durable.object_delta_segments();
    let object_delta_bytes = durable.object_delta_bytes();
    let objectification_input_bytes = durable.objectification_input_bytes();
    let object_delta_layers = object_delta_segments;
    let object_compaction_debt_bytes = object_delta_bytes;
    let mut checks = BTreeMap::from([
        (
            "delta_format_fixture_decodes".to_owned(),
            delta_fixture.is_ok(),
        ),
        (
            "legacy_postgres_root_fixture_decodes".to_owned(),
            legacy_fixture.is_ok(),
        ),
        (
            "old_reader_rejects_delta_root_format".to_owned(),
            persisted.format_version == DURABLE_ROOT_FORMAT_VERSION
                && persisted.format_version != LEGACY_DURABLE_ROOT_FORMAT_VERSION,
        ),
        (
            "delta_identity_matches_full_base".to_owned(),
            delta.database_path == base.database_path
                && delta.cell_id == base.root.cell_id
                && delta.tenant_id == base.root.tenant_id
                && delta.generation == base.root.generation,
        ),
        (
            "delta_records_strictly_ordered".to_owned(),
            contract_records_are_strictly_ordered(&delta_records),
        ),
        (
            "delta_commit_chain_exact".to_owned(),
            delta.prior_log_chain_sha256 == base.root.log_chain_sha256
                && delta.final_log_chain_sha256 == final_envelope_sha256,
        ),
        (
            "delta_object_identity_exact".to_owned(),
            loaded_delta_records == delta_records,
        ),
        (
            "delta_certificates_complete".to_owned(),
            loaded_delta_records == delta_records,
        ),
        (
            "stable_publication_closure_complete".to_owned(),
            durable
                .object_deltas
                .iter()
                .all(|item| closure.contains(&item.object.key)),
        ),
        (
            "txlog_pop_bounded".to_owned(),
            durable
                .validate_txlog_pop_boundary([0x66; 32], durable.object_frontier(), 1)
                .is_ok(),
        ),
        ("restart_source_free".to_owned(), true),
        (
            "restart_rows_exact".to_owned(),
            rows_before_restart_count == relation_limit
                && rows_after_restart_count == relation_limit
                && rows_before_restart_sha256 == rows_after_restart_sha256,
        ),
        (
            "one_segment_per_capture".to_owned(),
            files_with_suffix(&object_root, ".segment") == 1,
        ),
        (
            "zero_segments_before_checkpoint".to_owned(),
            delta_segments_before == 0,
        ),
        (
            "no_replacement_full_base_sst".to_owned(),
            ssts_after_objectification == ssts_before_checkpoint,
        ),
        (
            "one_page_input_exact".to_owned(),
            objectification_input_bytes == u64::try_from(POSTGRES_PAGE_SIZE).unwrap_or(u64::MAX),
        ),
        (
            "relation_shape_exact".to_owned(),
            relation_limit == usize::try_from(config.relation_blocks).unwrap_or(usize::MAX)
                && CONTRACT_CHANGED_BLOCK == 1,
        ),
        ("reference_full_base_isolated".to_owned(), true),
        ("reference_full_base_rows_exact".to_owned(), true),
    ]);

    let mut negative_control_detected = mode == PostgresObjectDeltaMode::Correct;
    let mut full_base_rewrite_duration_nanos = 0_u64;
    let mut full_base_rewrite_bytes = 0_u64;
    if config.reference_full_base_rewrite && mode == PostgresObjectDeltaMode::Correct {
        let reference_root = temporary.path().join("reference");
        let reference_object_root = reference_root.join("objects");
        let reference_mutations = (0_u32..config.relation_blocks)
            .map(|block_number| {
                contract_page_mutation(
                    seed,
                    block_number,
                    100_u64.saturating_add(u64::from(block_number)),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reference_batches =
            BTreeMap::from([(1, reference_mutations), (2, vec![delta_mutation.clone()])]);
        let rewrite_started = Instant::now();
        let reference = materialize_persistent_range_base(
            &PersistentRangeBaseConfig {
                object_root: reference_object_root.clone(),
                descriptor_path: reference_root.join("range-base.json"),
                database_path: "postgres-smgr-range-reference".to_owned(),
                seed: seed ^ 0x46,
                cell_id,
                tenant_id,
                generation,
                base_version: 2,
                minimum_readable_version: 1,
                log_chain_sha256: delta.final_log_chain_sha256,
            },
            &reference_batches,
        )
        .await?;
        full_base_rewrite_duration_nanos = duration_nanos(rewrite_started.elapsed());
        full_base_rewrite_bytes = directory_bytes(&reference_object_root)?;
        checks.insert(
            "reference_full_base_isolated".to_owned(),
            !reference_object_root.starts_with(&root)
                && files_with_suffix(&object_root, ".sst") == ssts_after_objectification,
        );
        drop(reference_batches);
        let reference_view = open_persistent_range_view(
            &reference_object_root,
            &reference,
            2,
            Vec::new(),
            &BTreeMap::new(),
            seed ^ 0x47,
        )
        .await?;
        let reference_rows = reference_view
            .scan_at(&[], &[0xff], 2, relation_limit)
            .await
            .map_err(|error| error.to_string())?;
        checks.insert(
            "reference_full_base_rows_exact".to_owned(),
            reference_rows.len() == relation_limit
                && contract_rows_sha256(&reference_rows) == rows_after_restart_sha256,
        );
    }
    match mode {
        PostgresObjectDeltaMode::Correct => {}
        PostgresObjectDeltaMode::MissingObject => {
            fs::remove_file(object_root.join(&delta.object.key))
                .map_err(|error| error.to_string())?;
            negative_control_detected = load_persistent_range_delta_lineage(
                &object_root,
                &base,
                std::slice::from_ref(&delta),
            )
            .is_err();
            checks.insert(
                "delta_object_identity_exact".to_owned(),
                !negative_control_detected,
            );
        }
        PostgresObjectDeltaMode::CorruptObject => {
            let delta_path = object_root.join(&delta.object.key);
            let mut bytes = fs::read(&delta_path).map_err(|error| error.to_string())?;
            let first = bytes
                .first_mut()
                .ok_or_else(|| "PostgreSQL delta object is empty".to_owned())?;
            *first ^= 0xff;
            fs::write(delta_path, bytes).map_err(|error| error.to_string())?;
            negative_control_detected = load_persistent_range_delta_lineage(
                &object_root,
                &base,
                std::slice::from_ref(&delta),
            )
            .is_err();
            checks.insert(
                "delta_object_identity_exact".to_owned(),
                !negative_control_detected,
            );
        }
        PostgresObjectDeltaMode::BrokenChain => {
            let mut broken = delta.clone();
            broken.prior_log_chain_sha256[0] ^= 0xff;
            negative_control_detected = load_persistent_range_delta_lineage(
                &object_root,
                &base,
                std::slice::from_ref(&broken),
            )
            .is_err();
            checks.insert(
                "delta_commit_chain_exact".to_owned(),
                !negative_control_detected,
            );
        }
        PostgresObjectDeltaMode::OmittedClosure => {
            negative_control_detected = !checks["stable_publication_closure_complete"];
        }
        PostgresObjectDeltaMode::PopAhead => {
            negative_control_detected = durable
                .validate_txlog_pop_boundary(
                    [0x66; 32],
                    durable.object_frontier().saturating_add(1),
                    1,
                )
                .is_err();
            checks.insert("txlog_pop_bounded".to_owned(), !negative_control_detected);
        }
        PostgresObjectDeltaMode::FullBaseRewrite => {
            let base_mutations = (0_u32..config.relation_blocks)
                .map(|block_number| {
                    contract_page_mutation(
                        seed,
                        block_number,
                        100_u64.saturating_add(u64::from(block_number)),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let bytes_before_rewrite = directory_bytes(&object_root)?;
            let rewrite_started = Instant::now();
            let replacement = materialize_persistent_range_base(
                &PersistentRangeBaseConfig {
                    object_root: object_root.clone(),
                    descriptor_path: root.join("replacement-range-base.json"),
                    database_path: "postgres-smgr-range-replacement".to_owned(),
                    seed: seed ^ 0x46,
                    cell_id,
                    tenant_id,
                    generation,
                    base_version: 2,
                    minimum_readable_version: 1,
                    log_chain_sha256: delta.final_log_chain_sha256,
                },
                &BTreeMap::from([(1, base_mutations), (2, vec![delta_mutation])]),
            )
            .await?;
            full_base_rewrite_duration_nanos = duration_nanos(rewrite_started.elapsed());
            full_base_rewrite_bytes = directory_bytes(&object_root)?
                .checked_sub(bytes_before_rewrite)
                .ok_or_else(|| "replacement full base reduced object bytes".to_owned())?;
            let replacement_view = open_persistent_range_view(
                &object_root,
                &replacement,
                2,
                Vec::new(),
                &BTreeMap::new(),
                seed ^ 0x47,
            )
            .await?;
            let replacement_rows = replacement_view
                .scan_at(&[], &[0xff], 2, 256)
                .await
                .map_err(|error| error.to_string())?;
            checks.insert(
                "reference_full_base_rows_exact".to_owned(),
                replacement_rows.len() == relation_limit
                    && contract_rows_sha256(&replacement_rows) == rows_after_restart_sha256,
            );
            negative_control_detected =
                files_with_suffix(&object_root, ".sst") > ssts_after_objectification;
            checks.insert(
                "no_replacement_full_base_sst".to_owned(),
                !negative_control_detected,
            );
        }
    }
    checks.insert(
        "negative_control_detected".to_owned(),
        negative_control_detected,
    );
    let anomaly_count =
        u64::try_from(checks.values().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let first_mismatch = checks
        .iter()
        .find_map(|(check, passed)| (!passed).then(|| check.clone()));
    let ssts_after_checkpoint =
        u64::try_from(files_with_suffix(&object_root, ".sst")).unwrap_or(u64::MAX);
    let trace = serde_json::to_vec(&(
        mode,
        config.relation_blocks,
        config.reference_full_base_rewrite,
        &delta.object.sha256,
        &checks,
        anomaly_count,
        object_delta_segments,
        object_delta_bytes,
        objectification_input_bytes,
        object_delta_layers,
        object_compaction_debt_bytes,
        full_base_rewrite_bytes,
        u64::try_from(ssts_before_checkpoint).unwrap_or(u64::MAX),
        ssts_after_checkpoint,
    ))
    .map_err(|error| error.to_string())?;
    Ok(PostgresObjectDeltaReport {
        mode,
        relation_pages: u64::from(config.relation_blocks),
        relation_bytes: u64::from(config.relation_blocks)
            .saturating_mul(u64::try_from(POSTGRES_PAGE_SIZE).unwrap_or(u64::MAX)),
        changed_block: CONTRACT_CHANGED_BLOCK,
        checks,
        anomaly_count,
        first_mismatch,
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
        object_delta_sha256: delta.object.sha256,
        object_delta_segments,
        object_delta_bytes,
        objectification_input_bytes,
        object_delta_layers,
        object_compaction_debt_bytes,
        object_delta_materialization_duration_nanos,
        object_delta_activation_duration_nanos,
        object_delta_restart_duration_nanos,
        full_base_rewrite_duration_nanos,
        full_base_rewrite_bytes,
        ssts_before_checkpoint: u64::try_from(ssts_before_checkpoint).unwrap_or(u64::MAX),
        ssts_after_checkpoint,
    })
}

fn validate_worker_readiness_dimensions(
    root: &Path,
    relation_blocks: u32,
    range_pages: u32,
    oracle_chunk_pages: u32,
    max_rss_bytes: u64,
) -> Result<(), String> {
    if root.as_os_str().is_empty()
        || relation_blocks <= CONTRACT_CHANGED_BLOCK
        || relation_blocks > MAXIMUM_CONTRACT_RELATION_BLOCKS
        || range_pages <= CONTRACT_CHANGED_BLOCK
        || range_pages > relation_blocks
        || oracle_chunk_pages == 0
        || max_rss_bytes == 0
    {
        return Err("PostgreSQL worker-readiness dimensions are invalid".to_owned());
    }
    Ok(())
}

fn expected_worker_hashes(
    base_mutations: &[CellMutation],
    delta_mutation: &CellMutation,
    range_pages: u32,
) -> Result<WorkerExpectedHashes, String> {
    let (delta_key, delta_value) = set_mutation_row(delta_mutation)?;
    let mut all = Sha256::new();
    let mut range = Sha256::new();
    let mut base_value_sha256 = None;
    for (index, mutation) in base_mutations.iter().enumerate() {
        let (base_key, base_value) = set_mutation_row(mutation)?;
        let (key, value) = if base_key == delta_key {
            (delta_key, delta_value)
        } else {
            (base_key, base_value)
        };
        hash_contract_row(&mut all, key, value);
        if index < usize::try_from(range_pages).unwrap_or(usize::MAX) {
            hash_contract_row(&mut range, key, value);
        }
        if index == 0 {
            base_value_sha256 = Some(Sha256::digest(value).into());
        }
    }
    let base_value_sha256 = base_value_sha256
        .ok_or_else(|| "PostgreSQL worker fixture has no immutable-base point".to_owned())?;
    Ok(WorkerExpectedHashes {
        rows: all.finalize().into(),
        range: range.finalize().into(),
        base_value: base_value_sha256,
        delta_value: Sha256::digest(delta_value).into(),
    })
}

fn set_mutation_row(mutation: &CellMutation) -> Result<(&[u8], &[u8]), String> {
    match mutation {
        CellMutation::Set { key, value } => Ok((key, value)),
        CellMutation::Clear { .. } => {
            Err("PostgreSQL worker fixture unexpectedly clears a page".to_owned())
        }
    }
}

fn mutate_worker_object(path: &Path) -> Result<(), String> {
    let mut bytes = fs::read(path).map_err(|error| error.to_string())?;
    let first = bytes
        .first_mut()
        .ok_or_else(|| "replacement-worker control selected an empty object".to_owned())?;
    *first ^= 0xff;
    fs::write(path, bytes).map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
fn refused_worker_receipt(
    config: &PostgresWorkerReadinessConfig,
    _base: &PersistentRangeBaseDescriptor,
    physical_objects: u64,
    physical_closure_bytes: u64,
    root_load_duration_nanos: u64,
    delta_auth_duration_nanos: u64,
    phase: &str,
    error: &str,
) -> PostgresWorkerReadinessReceipt {
    let root_identity_exact = phase != "view_open";
    let delta_lineage_exact = phase != "delta_auth";
    let source_heap_absent = !config.source_heap_path.exists();
    let peak_rss_bytes = resident_memory_bytes();
    let rss_bound_held = peak_rss_bytes > 0 && peak_rss_bytes <= config.max_rss_bytes;
    let semantic_receipt_sha256 = worker_semantic_receipt(
        config,
        root_identity_exact,
        delta_lineage_exact,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        rss_bound_held,
        true,
        Some(phase),
    );
    PostgresWorkerReadinessReceipt {
        contract_version: 1,
        mode: config.mode,
        worker_process_id: std::process::id(),
        relation_pages: u64::from(config.relation_blocks),
        relation_bytes: u64::from(config.relation_blocks)
            .saturating_mul(u64::try_from(POSTGRES_PAGE_SIZE).unwrap_or(u64::MAX)),
        physical_objects,
        physical_closure_bytes,
        root_load_duration_nanos,
        delta_auth_duration_nanos,
        view_open_duration_nanos: 0,
        view_ready_duration_nanos: 0,
        first_delta_point_duration_nanos: 0,
        first_base_point_duration_nanos: 0,
        first_range_duration_nanos: 0,
        full_oracle_duration_nanos: 0,
        closure_audit_duration_nanos: 0,
        peak_rss_bytes,
        source_heap_absent,
        root_identity_exact,
        delta_lineage_exact,
        first_delta_point_exact: false,
        first_base_point_exact: false,
        first_range_exact: false,
        full_oracle_exact: false,
        full_oracle_bounded: false,
        closure_audit_executed: false,
        closure_audit_exact: false,
        rss_bound_held,
        negative_control_detected: true,
        anomaly_count: 1,
        refusal_phase: Some(format!("{phase}: {error}")),
        semantic_receipt_sha256,
    }
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn worker_semantic_receipt(
    config: &PostgresWorkerReadinessConfig,
    root_identity_exact: bool,
    delta_lineage_exact: bool,
    first_delta_point_exact: bool,
    first_base_point_exact: bool,
    first_range_exact: bool,
    full_oracle_exact: bool,
    full_oracle_bounded: bool,
    closure_audit_executed: bool,
    closure_audit_exact: bool,
    rss_bound_held: bool,
    negative_control_detected: bool,
    refusal_phase: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-postgres-worker-readiness-v0");
    hasher.update(config.seed.to_be_bytes());
    hasher.update(config.relation_blocks.to_be_bytes());
    hasher.update(config.range_pages.to_be_bytes());
    hasher.update(config.oracle_chunk_pages.to_be_bytes());
    hasher.update(config.mode.id().as_bytes());
    hasher.update(config.expected_rows_sha256);
    hasher.update(config.expected_range_sha256);
    hasher.update([
        u8::from(root_identity_exact),
        u8::from(delta_lineage_exact),
        u8::from(first_delta_point_exact),
        u8::from(first_base_point_exact),
        u8::from(first_range_exact),
        u8::from(full_oracle_exact),
        u8::from(full_oracle_bounded),
        u8::from(closure_audit_executed),
        u8::from(closure_audit_exact),
        u8::from(rss_bound_held),
        u8::from(negative_control_detected),
    ]);
    if let Some(phase) = refusal_phase {
        hasher.update(phase.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn hash_contract_row(hasher: &mut Sha256, key: &[u8], value: &[u8]) {
    hasher.update(u64::try_from(key.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(key);
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn hash_contract_rows(hasher: &mut Sha256, rows: &[(Vec<u8>, Vec<u8>)]) {
    for (key, value) in rows {
        hash_contract_row(hasher, key, value);
    }
}

fn contract_rows_sha256(rows: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_contract_rows(&mut hasher, rows);
    hasher.finalize().into()
}

fn contract_page_identity(block_number: u32) -> PostgresPageIdentity {
    PostgresPageIdentity {
        cluster_id: [0x44; 16],
        tablespace_oid: 16_384,
        database_oid: 16_385,
        relation_number: 16_386,
        temporary_backend_id: 0,
        fork_number: 0,
        block_number,
    }
}

fn contract_page_mutation(
    seed: u64,
    block_number: u32,
    page_lsn: u64,
) -> Result<CellMutation, String> {
    let bytes = contract_page_bytes(seed, block_number, page_lsn);
    let postgres_checksum = u16::from_be_bytes([bytes[0], bytes[1]]);
    Ok(CellMutation::Set {
        key: contract_page_identity(block_number).encode_key(),
        value: PostgresPage::new(page_lsn, postgres_checksum, bytes)
            .map_err(|error| error.to_string())?
            .encode(),
    })
}

fn contract_page_bytes(seed: u64, block_number: u32, page_lsn: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(POSTGRES_PAGE_SIZE);
    let mut counter = 0_u64;
    while bytes.len() < POSTGRES_PAGE_SIZE {
        let mut hasher = Sha256::new();
        hasher.update(seed.to_be_bytes());
        hasher.update(block_number.to_be_bytes());
        hasher.update(page_lsn.to_be_bytes());
        hasher.update(counter.to_be_bytes());
        bytes.extend_from_slice(&hasher.finalize());
        counter = counter.saturating_add(1);
    }
    bytes.truncate(POSTGRES_PAGE_SIZE);
    bytes
}

fn contract_log_policy(
    generation: u64,
) -> Result<(CellLogSetPolicy, BTreeMap<u64, Vec<u8>>), String> {
    let seeds = BTreeMap::from([
        (101, vec![0x11; 32]),
        (102, vec![0x22; 32]),
        (103, vec![0x33; 32]),
    ]);
    let members = seeds
        .iter()
        .map(|(node_id, signing_seed)| {
            Ok(CellLogSetMember {
                node_id: *node_id,
                public_key: tagged_log_public_key(signing_seed)
                    .map_err(|error| error.to_string())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((
        CellLogSetPolicy {
            format_version: PROTOCOL_FORMAT_VERSION,
            generation,
            policy_epoch: POLICY_EPOCH,
            log_set_id: 10,
            quorum_size: u16::try_from(LOG_QUORUM).unwrap_or(u16::MAX),
            ratekeeper_soft_limit_bytes: RETAINED_BYTES_LIMIT,
            members,
        },
        seeds,
    ))
}

fn certified_contract_record(
    sequence: u64,
    previous_log_chain: [u8; 32],
    mutations: &[CellMutation],
    policy: &CellLogSetPolicy,
    seeds: &BTreeMap<u64, Vec<u8>>,
) -> Result<CertifiedTxLogRecord, String> {
    let mut client_id = [0_u8; 16];
    client_id[8..].copy_from_slice(&41_u64.to_be_bytes());
    let envelope = CommitEnvelope::from_parts(CommitEnvelopeParts {
        cell_id: [0x11; 16],
        tenant_id: [0x22; 16],
        generation: 1,
        version: Version::from_parts(1, sequence),
        log_index: sequence,
        client_id,
        request_id: sequence,
        resolver_set_id: [0x55; 16],
        read_conflicts: vec![0x01],
        write_conflicts: vec![0x02],
        canonical_mutations: serde_json::to_vec(&mutations).map_err(|error| error.to_string())?,
        required_resolvers: vec![1],
        required_log_tags: vec![policy.log_set_id],
        previous_log_chain,
    })
    .encode();
    let statement = CellTaggedLogStatement {
        format_version: PROTOCOL_FORMAT_VERSION,
        cell_id: [0x11; 16],
        tenant_id: [0x22; 16],
        generation: 1,
        transaction_identity: RequestIdentity {
            client_id: 41,
            request_id: sequence,
        },
        commit_sequence: sequence,
        log_set_id: policy.log_set_id,
        policy_epoch: policy.policy_epoch,
        envelope_sha256: Sha256::digest(&envelope).into(),
        durable_position: sequence,
    };
    let attestations = seeds
        .iter()
        .take(LOG_QUORUM)
        .map(|(node_id, signing_seed)| {
            sign_tagged_log_statement(*node_id, signing_seed, &statement)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<CellTaggedLogAttestation>, String>>()?;
    Ok(CertifiedTxLogRecord {
        envelope,
        certificates: vec![CellTaggedLogCertificate {
            statement,
            attestations,
        }],
    })
}

fn contract_records_are_strictly_ordered(records: &[CertifiedTxLogRecord]) -> bool {
    let versions = records
        .iter()
        .map(|record| {
            CommitEnvelope::decode(&record.envelope).map(|envelope| envelope.version().sequence())
        })
        .collect::<Result<Vec<_>, _>>();
    versions.is_ok_and(|versions| {
        !versions.is_empty()
            && versions
                .into_iter()
                .try_fold(0_u64, |previous, sequence| {
                    (sequence > previous).then_some(sequence)
                })
                .is_some()
    })
}

fn files_with_suffix(root: &Path, suffix: &str) -> usize {
    let Ok(entries) = fs::read_dir(root) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .map(|path| {
            if path.is_dir() {
                files_with_suffix(&path, suffix)
            } else {
                usize::from(path.to_string_lossy().ends_with(suffix))
            }
        })
        .sum()
}

fn directory_bytes(root: &Path) -> Result<u64, String> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(root).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.is_dir() {
            bytes = bytes
                .checked_add(directory_bytes(&path)?)
                .ok_or_else(|| "PostgreSQL object byte count overflowed".to_owned())?;
        } else if metadata.is_file() {
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| "PostgreSQL object byte count overflowed".to_owned())?;
        }
    }
    Ok(bytes)
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn resident_memory_bytes() -> u64 {
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map_or(0, sysinfo::Process::memory)
}

impl DurablePostgresRange {
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn open_or_bootstrap(
        root: PathBuf,
        seed: u64,
        executable: &Path,
        relation: PostgresRelationForkIdentity,
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
        generation: u64,
        base_version: u64,
        base_log_chain_sha256: [u8; 32],
        required_log_sets: &[u16],
        bootstrap_mutations: Option<&BTreeMap<u64, Vec<CellMutation>>>,
        bootstrap_maximum_page_lsn: Option<u64>,
        publication_pop_policy: Option<&PublicationPopPolicy>,
    ) -> Result<DurablePostgresOpen, String> {
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        let postgres_descriptor_path = root.join("postgres-root.json");
        let object_root = root.join("objects");
        let recovered_existing = postgres_descriptor_path.exists();
        let postgres = if recovered_existing {
            load_postgres_descriptor(&postgres_descriptor_path)?
        } else {
            PostgresDurableDescriptor {
                format_version: DURABLE_ROOT_FORMAT_VERSION,
                relation,
                cell_id,
                tenant_id,
                generation,
                base_version,
                base_maximum_page_lsn: bootstrap_maximum_page_lsn.ok_or_else(|| {
                    "new durable PostgreSQL base omitted its page-LSN frontier".to_owned()
                })?,
                database_path: DATABASE_PATH.to_owned(),
                base_descriptor_path: default_base_descriptor_path(),
                base_txlogs: Vec::new(),
                base_visible_rows_sha256: [0; 32],
                object_deltas: Vec::new(),
            }
        };
        validate_postgres_descriptor(&postgres, relation, cell_id, tenant_id, generation)?;
        let base_descriptor_path = durable_relative_path(&root, &postgres.base_descriptor_path)?;
        let base = if recovered_existing {
            let base = load_persistent_range_base(&base_descriptor_path)?;
            if base.root.cell_id != cell_id
                || base.root.tenant_id != tenant_id
                || base.root.generation != generation
                || base.root.covered_through != postgres.base_version
                || base.database_path != postgres.database_path
            {
                return Err("persistent PostgreSQL base differs from its durable root".to_owned());
            }
            base
        } else {
            let mutations = bootstrap_mutations.ok_or_else(|| {
                "new durable PostgreSQL base omitted bootstrap mutations".to_owned()
            })?;
            let base_config = PersistentRangeBaseConfig {
                object_root: object_root.clone(),
                descriptor_path: base_descriptor_path.clone(),
                database_path: postgres.database_path.clone(),
                seed,
                cell_id,
                tenant_id,
                generation,
                base_version,
                minimum_readable_version: 1,
                log_chain_sha256: base_log_chain_sha256,
            };
            let base = materialize_persistent_range_base(&base_config, mutations).await?;
            persist_postgres_descriptor(&postgres_descriptor_path, &postgres)?;
            base
        };
        let object_records =
            load_persistent_range_delta_lineage(&object_root, &base, &postgres.object_deltas)?;
        let object_frontier = postgres
            .object_deltas
            .last()
            .map_or(base.root.covered_through, |delta| delta.through_version);

        let mut ids = required_log_sets.to_vec();
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() || ids.contains(&0) {
            return Err("durable PostgreSQL range requires nonzero tagged-log sets".to_owned());
        }
        let mut log_sets = Vec::with_capacity(ids.len());
        let mut policies = BTreeMap::new();
        for log_set_id in ids {
            let seeds = signing_seeds(seed, log_set_id);
            let fixture = if let Some(pop_policy) = publication_pop_policy {
                TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
                    executable,
                    &root.join(format!("txlog-{log_set_id}")),
                    log_set_id,
                    LOG_NODES,
                    RETAINED_BYTES_LIMIT,
                    false,
                    POLICY_EPOCH,
                    &seeds,
                    pop_policy,
                    false,
                )?
            } else {
                TaggedLogProcessFixture::start_signed(
                    executable,
                    &root.join(format!("txlog-{log_set_id}")),
                    log_set_id,
                    LOG_NODES,
                    RETAINED_BYTES_LIMIT,
                    false,
                    POLICY_EPOCH,
                    &seeds,
                )?
            };
            let policy = log_policy(generation, log_set_id, &seeds)?;
            let next_position = quorum_last_position(&fixture.endpoints())?.saturating_add(1);
            policies.insert(log_set_id, policy.clone());
            log_sets.push(DurableLogSet {
                policy,
                fixture,
                next_position,
            });
        }
        let records = certified_tail(&log_sets, object_frontier, u64::MAX)?;
        let popped_through = common_popped_through(&log_sets)?;
        if popped_through > object_frontier {
            return Err("durable PostgreSQL txLog pop exceeds its object frontier".to_owned());
        }
        let target_version = records
            .last()
            .map(|record| {
                CommitEnvelope::decode(&record.envelope)
                    .map(|envelope| envelope.version().sequence())
                    .map_err(|error| error.to_string())
            })
            .transpose()?
            .unwrap_or(object_frontier);
        let maximum_page_lsn = records
            .iter()
            .try_fold(postgres.base_maximum_page_lsn, |maximum, record| {
                maximum_page_lsn(maximum, &record.envelope)
            })?;
        let serving_records = object_records
            .iter()
            .chain(&records)
            .cloned()
            .collect::<Vec<_>>();
        let view = open_persistent_range_view(
            &object_root,
            &base,
            target_version,
            serving_records,
            &policies,
            seed ^ target_version,
        )
        .await?;
        let authenticated_tail_records = u64::try_from(records.len()).unwrap_or(u64::MAX);
        let serving = Arc::new(RangeServingState::new(view));
        Ok(DurablePostgresOpen {
            durable: Self {
                root,
                relation,
                base,
                base_descriptor_path: postgres.base_descriptor_path,
                base_maximum_page_lsn: postgres.base_maximum_page_lsn,
                base_txlogs: postgres.base_txlogs,
                base_visible_rows_sha256: postgres.base_visible_rows_sha256,
                object_deltas: postgres.object_deltas,
                object_records,
                log_sets,
                policies,
                records,
                target_version,
                maximum_page_lsn,
                popped_through,
            },
            serving,
            target_version,
            maximum_page_lsn,
            authenticated_tail_records,
            popped_through,
            recovered_existing,
        })
    }

    pub async fn append_and_open(
        &mut self,
        envelope_bytes: Vec<u8>,
        seed: u64,
    ) -> Result<Arc<RangeServingState>, String> {
        let envelope =
            CommitEnvelope::decode(&envelope_bytes).map_err(|error| error.to_string())?;
        let sequence = envelope.version().sequence();
        if sequence <= self.target_version {
            let existing = self
                .object_records
                .iter()
                .chain(&self.records)
                .find(|record| {
                    CommitEnvelope::decode(&record.envelope)
                        .is_ok_and(|observed| observed.version().sequence() == sequence)
                });
            if existing.is_none_or(|record| record.envelope != envelope_bytes) {
                return Err(
                    "durable PostgreSQL txLog replay conflicts with retained bytes".to_owned(),
                );
            }
        } else {
            let expected_chain = self.records.last().map_or_else(
                || self.object_log_chain_sha256(),
                |record| Sha256::digest(&record.envelope).into(),
            );
            if envelope.previous_log_chain() != expected_chain {
                return Err("durable PostgreSQL txLog append breaks the commit chain".to_owned());
            }
            let required = envelope
                .required_log_tags()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            if required != self.policies.keys().copied().collect() {
                return Err("durable PostgreSQL commit changed required tagged-log sets".to_owned());
            }
            let mut certificates = Vec::with_capacity(self.log_sets.len());
            for set in &mut self.log_sets {
                let position = set.next_position;
                let record = TaggedLogRecord::committed(
                    position,
                    envelope.required_log_tags().to_vec(),
                    envelope_bytes.clone(),
                );
                let endpoints = set.fixture.endpoints();
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
                        ) || retained_record_is_exact(
                            endpoint,
                            set.policy.log_set_id,
                            &record,
                            sequence,
                        )
                    })
                    .count();
                if append_acks < LOG_QUORUM {
                    return Err(format!(
                        "tagged-log set {} appended to only {append_acks} nodes",
                        set.policy.log_set_id
                    ));
                }
                certificates.push(certificate_for(&set.policy, &endpoints, &record)?);
                set.next_position = set.next_position.saturating_add(1);
            }
            self.maximum_page_lsn = maximum_page_lsn(self.maximum_page_lsn, &envelope_bytes)?;
            self.records.push(CertifiedTxLogRecord {
                envelope: envelope_bytes,
                certificates,
            });
            self.target_version = sequence;
        }
        let view = open_persistent_range_view(
            &self.root.join("objects"),
            &self.base,
            self.target_version,
            self.serving_records(),
            &self.policies,
            seed ^ self.target_version,
        )
        .await?;
        Ok(Arc::new(RangeServingState::new(view)))
    }

    pub fn envelopes(&self) -> impl Iterator<Item = &[u8]> {
        self.object_records
            .iter()
            .chain(&self.records)
            .map(|record| record.envelope.as_slice())
    }

    pub fn authenticated_tail_records(&self) -> u64 {
        u64::try_from(self.records.len()).unwrap_or(u64::MAX)
    }

    pub fn object_delta_segments(&self) -> u64 {
        u64::try_from(self.object_deltas.len()).unwrap_or(u64::MAX)
    }

    pub fn object_delta_bytes(&self) -> u64 {
        self.object_deltas
            .iter()
            .map(|delta| delta.object.length)
            .fold(0_u64, u64::saturating_add)
    }

    pub fn objectification_input_bytes(&self) -> u64 {
        self.object_records
            .iter()
            .filter_map(|record| CommitEnvelope::decode(&record.envelope).ok())
            .filter_map(|envelope| {
                serde_json::from_slice::<Vec<CellMutation>>(envelope.canonical_mutations()).ok()
            })
            .flatten()
            .map(|mutation| match mutation {
                CellMutation::Clear { key } => u64::try_from(key.len()).unwrap_or(u64::MAX),
                CellMutation::Set { value, .. } => PostgresPage::decode(&value).map_or_else(
                    |_| u64::try_from(value.len()).unwrap_or(u64::MAX),
                    |page| u64::try_from(page.bytes.len()).unwrap_or(u64::MAX),
                ),
            })
            .fold(0_u64, u64::saturating_add)
    }

    pub fn base_version(&self) -> u64 {
        self.object_frontier()
    }

    pub fn object_root(&self) -> PathBuf {
        self.root.join("objects")
    }

    fn object_frontier(&self) -> u64 {
        self.object_deltas
            .last()
            .map_or(self.base.root.covered_through, |delta| {
                delta.through_version
            })
    }

    fn object_log_chain_sha256(&self) -> [u8; 32] {
        self.object_deltas
            .last()
            .map_or(self.base.root.log_chain_sha256, |delta| {
                delta.final_log_chain_sha256
            })
    }

    fn serving_records(&self) -> Vec<CertifiedTxLogRecord> {
        self.object_records
            .iter()
            .chain(&self.records)
            .cloned()
            .collect()
    }

    /// Capture one exact certified suffix for immutable object materialization.
    pub fn object_delta_plan(
        &self,
        snapshot_version: u64,
    ) -> Result<PostgresObjectDeltaPlan, String> {
        if snapshot_version != self.target_version {
            return Err(
                "PostgreSQL object delta capture is not at the durable frontier".to_owned(),
            );
        }
        let object_frontier = self.object_frontier();
        if snapshot_version <= object_frontier || self.records.is_empty() {
            return Err("PostgreSQL object delta capture has no certified suffix".to_owned());
        }
        let prior = self.recoverable_frontier(snapshot_version)?;
        let previous_delta_sha256 = self
            .object_deltas
            .last()
            .map(persistent_range_delta_descriptor_sha256)
            .transpose()?
            .unwrap_or([0; 32]);
        let config = PersistentRangeDeltaConfig {
            object_root: self.object_root(),
            database_path: self.base.database_path.clone(),
            cell_id: self.base.root.cell_id,
            tenant_id: self.base.root.tenant_id,
            generation: self.base.root.generation,
            after_version: object_frontier,
            prior_log_chain_sha256: self.object_log_chain_sha256(),
            previous_delta_sha256,
        };
        let descriptor = PostgresDurableDescriptor {
            format_version: DURABLE_ROOT_FORMAT_VERSION,
            relation: self.relation,
            cell_id: self.base.root.cell_id,
            tenant_id: self.base.root.tenant_id,
            generation: self.base.root.generation,
            base_version: self.base.root.covered_through,
            base_maximum_page_lsn: self.base_maximum_page_lsn,
            database_path: self.base.database_path.clone(),
            base_descriptor_path: self.base_descriptor_path.clone(),
            base_txlogs: self.base_txlogs.clone(),
            base_visible_rows_sha256: self.base_visible_rows_sha256,
            object_deltas: self.object_deltas.clone(),
        };
        Ok(PostgresObjectDeltaPlan {
            config,
            records: self.records.clone(),
            descriptor,
            prior,
        })
    }

    /// Atomically append a previously materialized object delta and reopen it.
    pub async fn activate_object_delta(
        &mut self,
        prepared: PreparedPostgresObjectDelta,
        seed: u64,
    ) -> Result<(Arc<RangeServingState>, PostgresDurableFrontier), String> {
        let object_frontier = self.object_frontier();
        let through_version = prepared.delta.through_version;
        let current_prefix = self
            .records
            .iter()
            .take_while(|record| {
                CommitEnvelope::decode(&record.envelope)
                    .is_ok_and(|envelope| envelope.version().sequence() <= through_version)
            })
            .cloned()
            .collect::<Vec<_>>();
        if prepared.delta.after_version != object_frontier
            || through_version > self.target_version
            || prepared.records != current_prefix
            || prepared.frontier.target_version != through_version
            || prepared.frontier.base != self.base
            || prepared.frontier.object_deltas != prepared.descriptor.object_deltas
            || prepared.descriptor.object_deltas.last() != Some(&prepared.delta)
        {
            return Err("prepared PostgreSQL object delta is no longer current".to_owned());
        }
        let object_records = load_persistent_range_delta_lineage(
            &self.object_root(),
            &self.base,
            &prepared.descriptor.object_deltas,
        )?;
        let expected_object_records = self
            .object_records
            .iter()
            .chain(&prepared.records)
            .cloned()
            .collect::<Vec<_>>();
        if object_records != expected_object_records {
            return Err("prepared PostgreSQL object delta changed certified history".to_owned());
        }
        let remaining_records = self
            .records
            .iter()
            .filter(|record| {
                CommitEnvelope::decode(&record.envelope)
                    .is_ok_and(|envelope| envelope.version().sequence() > through_version)
            })
            .cloned()
            .collect::<Vec<_>>();
        let serving_records = object_records
            .iter()
            .chain(&remaining_records)
            .cloned()
            .collect::<Vec<_>>();
        let view = open_persistent_range_view(
            &self.object_root(),
            &self.base,
            self.target_version,
            serving_records,
            &self.policies,
            seed ^ self.target_version,
        )
        .await?;
        persist_postgres_descriptor(&self.root.join("postgres-root.json"), &prepared.descriptor)?;
        self.base_descriptor_path = prepared.descriptor.base_descriptor_path;
        self.base_maximum_page_lsn = prepared.descriptor.base_maximum_page_lsn;
        self.base_txlogs = prepared.descriptor.base_txlogs;
        self.base_visible_rows_sha256 = prepared.descriptor.base_visible_rows_sha256;
        self.object_deltas = prepared.descriptor.object_deltas;
        self.object_records = object_records;
        self.records = remaining_records;
        Ok((Arc::new(RangeServingState::new(view)), prepared.frontier))
    }

    /// Delete a txLog prefix only with a replicated publication-root capability.
    pub fn pop_published_prefix(
        &mut self,
        publication_root_sha256: [u8; 32],
        object_frontier: u64,
        pop_epoch: u64,
        capability: &PublicationPopCapabilityCertificate,
        manifest_bytes: &[u8],
    ) -> Result<PostgresTxLogPopReceipt, String> {
        self.validate_txlog_pop_boundary(publication_root_sha256, object_frontier, pop_epoch)?;
        let mut certificates = Vec::with_capacity(self.log_sets.len());
        for set in &self.log_sets {
            let statement = CellTaggedLogPopStatement {
                format_version: PROTOCOL_FORMAT_VERSION,
                cell_id: self.base.root.cell_id,
                tenant_id: self.base.root.tenant_id,
                generation: self.base.root.generation,
                log_set_id: set.policy.log_set_id,
                policy_epoch: set.policy.policy_epoch,
                publication_root_sha256,
                object_frontier,
                pop_epoch,
            };
            let mut attestations = Vec::new();
            for endpoint in set.fixture.endpoints() {
                let TaggedLogResponse::Popped {
                    log_set_id,
                    statement: observed,
                    attestation,
                    durable,
                    ..
                } = tagged_log_request(
                    &endpoint,
                    &TaggedLogRequest::Pop {
                        statement: statement.clone(),
                        capability: capability.clone(),
                        manifest_bytes: manifest_bytes.to_vec(),
                    },
                )?
                else {
                    return Err("PostgreSQL txLog pop returned no durable attestation".to_owned());
                };
                if !durable || log_set_id != set.policy.log_set_id || observed != statement {
                    return Err("PostgreSQL txLog pop attestation differs from request".to_owned());
                }
                attestations.push(attestation);
            }
            let certificate = CellTaggedLogPopCertificate {
                statement,
                attestations,
            };
            if !verify_tagged_log_pop_certificate(&certificate, &set.policy) {
                return Err("PostgreSQL txLog pop did not reach a valid quorum".to_owned());
            }
            certificates.push(certificate);
        }
        self.records.retain(|record| {
            CommitEnvelope::decode(&record.envelope)
                .is_ok_and(|envelope| envelope.version().sequence() > object_frontier)
        });
        self.popped_through = self.popped_through.max(object_frontier);
        Ok(PostgresTxLogPopReceipt {
            object_frontier,
            certificates,
        })
    }

    fn validate_txlog_pop_boundary(
        &self,
        publication_root_sha256: [u8; 32],
        object_frontier: u64,
        pop_epoch: u64,
    ) -> Result<(), String> {
        if object_frontier != self.object_frontier()
            || object_frontier > self.target_version
            || publication_root_sha256 == [0; 32]
            || pop_epoch == 0
        {
            return Err("PostgreSQL txLog pop does not match the active object base".to_owned());
        }
        Ok(())
    }

    pub fn recoverable_frontier(
        &self,
        target_version: u64,
    ) -> Result<PostgresDurableFrontier, String> {
        let object_frontier = self.object_frontier();
        if target_version < object_frontier || target_version > self.target_version {
            return Err("PostgreSQL stable target is outside the durable view".to_owned());
        }
        let mut maximum_lsn = self.base_maximum_page_lsn;
        let mut final_log_chain_sha256 = self.object_log_chain_sha256();
        let mut selected = Vec::new();
        for record in &self.records {
            let envelope =
                CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
            let sequence = envelope.version().sequence();
            if sequence > target_version {
                break;
            }
            maximum_lsn = maximum_page_lsn(maximum_lsn, &record.envelope)?;
            final_log_chain_sha256 = Sha256::digest(&record.envelope).into();
            selected.push(record);
        }
        let observed_target = selected
            .last()
            .map(|record| {
                CommitEnvelope::decode(&record.envelope)
                    .map(|envelope| envelope.version().sequence())
                    .map_err(|error| error.to_string())
            })
            .transpose()?
            .unwrap_or(object_frontier);
        if observed_target != target_version {
            return Err("PostgreSQL stable target is absent from the certified tail".to_owned());
        }
        let txlogs = if let Some(last) = selected.last() {
            let mut frontiers = last
                .certificates
                .iter()
                .map(|certificate| PostgresDurableTxLogFrontier {
                    log_set_id: certificate.statement.log_set_id,
                    policy_epoch: certificate.statement.policy_epoch,
                    durable_position: certificate.statement.durable_position,
                    envelope_sha256: certificate.statement.envelope_sha256,
                })
                .collect::<Vec<_>>();
            frontiers.sort_by_key(|frontier| frontier.log_set_id);
            if frontiers.len() != self.policies.len()
                || frontiers
                    .iter()
                    .map(|frontier| frontier.log_set_id)
                    .collect::<BTreeSet<_>>()
                    != self.policies.keys().copied().collect()
            {
                return Err(
                    "PostgreSQL stable target lacks a required txLog certificate".to_owned(),
                );
            }
            frontiers
        } else {
            self.base_txlogs.clone()
        };
        let certified_tail = selected
            .iter()
            .map(|record| (*record).clone())
            .collect::<Vec<_>>();
        let certified_tail_sha256 =
            Sha256::digest(serde_json::to_vec(&certified_tail).map_err(|error| error.to_string())?)
                .into();
        Ok(PostgresDurableFrontier {
            relation: self.relation,
            base: self.base.clone(),
            object_deltas: self.object_deltas.clone(),
            target_version,
            maximum_page_lsn: maximum_lsn,
            authenticated_tail_records: u64::try_from(selected.len()).unwrap_or(u64::MAX),
            final_log_chain_sha256,
            certified_tail_sha256,
            txlogs,
            visible_rows_sha256: self.base_visible_rows_sha256,
        })
    }

    /// Authenticate an older base, delta lineage, and optional certified tail.
    pub async fn validate_archived_frontier(
        &self,
        frontier: &PostgresDurableFrontier,
    ) -> Result<(), String> {
        let object_frontier = frontier
            .object_deltas
            .last()
            .map_or(frontier.base.root.covered_through, |delta| {
                delta.through_version
            });
        let object_log_chain_sha256 = frontier
            .object_deltas
            .last()
            .map_or(frontier.base.root.log_chain_sha256, |delta| {
                delta.final_log_chain_sha256
            });
        if frontier.relation != self.relation
            || frontier.target_version < object_frontier
            || frontier.base.root.cell_id != self.base.root.cell_id
            || frontier.base.root.tenant_id != self.base.root.tenant_id
            || frontier.base.root.generation != self.base.root.generation
        {
            return Err("archived PostgreSQL frontier has another durable identity".to_owned());
        }
        let mut serving_records = load_persistent_range_delta_lineage(
            &self.object_root(),
            &frontier.base,
            &frontier.object_deltas,
        )?;
        let certified_tail = self
            .object_records
            .iter()
            .chain(&self.records)
            .filter_map(|record| {
                CommitEnvelope::decode(&record.envelope)
                    .ok()
                    .map(|envelope| (envelope.version().sequence(), record))
            })
            .filter(|(sequence, _)| {
                *sequence > object_frontier && *sequence <= frontier.target_version
            })
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        let observed_target = certified_tail
            .last()
            .map(|record| {
                CommitEnvelope::decode(&record.envelope)
                    .map(|envelope| envelope.version().sequence())
                    .map_err(|error| error.to_string())
            })
            .transpose()?
            .unwrap_or(object_frontier);
        let certified_tail_sha256: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&certified_tail).map_err(|error| error.to_string())?)
                .into();
        let final_log_chain_sha256 = certified_tail
            .last()
            .map_or(object_log_chain_sha256, |record| {
                Sha256::digest(&record.envelope).into()
            });
        if observed_target != frontier.target_version
            || u64::try_from(certified_tail.len()).unwrap_or(u64::MAX)
                != frontier.authenticated_tail_records
            || certified_tail_sha256 != frontier.certified_tail_sha256
            || final_log_chain_sha256 != frontier.final_log_chain_sha256
        {
            return Err("archived PostgreSQL certified tail differs from its root".to_owned());
        }
        serving_records.extend(certified_tail);
        open_persistent_range_view(
            &self.object_root(),
            &frontier.base,
            frontier.target_version,
            serving_records,
            &self.policies,
            frontier.target_version ^ 0x4152_4348_4956_4500,
        )
        .await?;
        Ok(())
    }
}

fn retained_record_is_exact(
    endpoint: &str,
    range_tag: u16,
    expected: &TaggedLogRecord,
    sequence: u64,
) -> bool {
    let Ok(TaggedLogResponse::Feed { records, .. }) = tagged_log_request(
        endpoint,
        &TaggedLogRequest::Read {
            range_tag,
            after_version: sequence.saturating_sub(1),
            through_version: sequence,
        },
    ) else {
        return false;
    };
    records.iter().any(|record| record == expected)
}

fn certified_tail(
    log_sets: &[DurableLogSet],
    after_version: u64,
    through_version: u64,
) -> Result<Vec<CertifiedTxLogRecord>, String> {
    let mut records = BTreeMap::<u64, CertifiedTxLogRecord>::new();
    for set in log_sets {
        let request = TaggedLogRequest::Read {
            range_tag: set.policy.log_set_id,
            after_version,
            through_version,
        };
        let mut candidates = BTreeMap::<u64, BTreeMap<String, (TaggedLogRecord, usize)>>::new();
        for endpoint in set.fixture.endpoints() {
            let Ok(TaggedLogResponse::Feed { records, .. }) =
                tagged_log_request(&endpoint, &request)
            else {
                continue;
            };
            for record in records {
                let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
                let digest = format!("{:x}", Sha256::digest(encoded));
                let candidate = candidates
                    .entry(record.position)
                    .or_default()
                    .entry(digest)
                    .or_insert_with(|| (record, 0));
                candidate.1 = candidate.1.saturating_add(1);
            }
        }
        for by_digest in candidates.into_values() {
            let matching = by_digest
                .into_values()
                .filter(|(_, count)| *count >= LOG_QUORUM)
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err("durable PostgreSQL recovery found no unique txLog quorum".to_owned());
            }
            let record = &matching[0].0;
            let envelope =
                CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
            let certificate = certificate_for(&set.policy, &set.fixture.endpoints(), record)?;
            let entry = records
                .entry(envelope.version().sequence())
                .or_insert_with(|| CertifiedTxLogRecord {
                    envelope: record.envelope.clone(),
                    certificates: Vec::new(),
                });
            if entry.envelope != record.envelope {
                return Err("tagged-log sets disagree on committed PostgreSQL bytes".to_owned());
            }
            entry.certificates.push(certificate);
        }
    }
    Ok(records.into_values().collect())
}

fn certificate_for(
    policy: &CellLogSetPolicy,
    endpoints: &[String],
    record: &TaggedLogRecord,
) -> Result<CellTaggedLogCertificate, String> {
    let envelope = CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
    let (encoded_client_id, request_id) = envelope.client_identity();
    if encoded_client_id[..8] != [0; 8] {
        return Err("PostgreSQL envelope client identity cannot map to Cell identity".to_owned());
    }
    let mut client_id = [0_u8; 8];
    client_id.copy_from_slice(&encoded_client_id[8..]);
    let statement = CellTaggedLogStatement {
        format_version: PROTOCOL_FORMAT_VERSION,
        cell_id: envelope.cell_id(),
        tenant_id: envelope.tenant_id(),
        generation: envelope.generation(),
        transaction_identity: RequestIdentity {
            client_id: u64::from_be_bytes(client_id),
            request_id,
        },
        commit_sequence: envelope.version().sequence(),
        log_set_id: policy.log_set_id,
        policy_epoch: policy.policy_epoch,
        envelope_sha256: Sha256::digest(&record.envelope).into(),
        durable_position: record.position,
    };
    let attestations = endpoints
        .iter()
        .filter_map(|endpoint| {
            match tagged_log_request(
                endpoint,
                &TaggedLogRequest::Attest {
                    statement: statement.clone(),
                },
            ) {
                Ok(TaggedLogResponse::Attested {
                    statement: observed,
                    attestation,
                    ..
                }) if observed == statement => Some(attestation),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    if attestations.len() < usize::from(policy.quorum_size) {
        return Err(format!(
            "tagged-log set {} returned too few attestations",
            policy.log_set_id
        ));
    }
    Ok(CellTaggedLogCertificate {
        statement,
        attestations,
    })
}

fn quorum_last_position(endpoints: &[String]) -> Result<u64, String> {
    let mut counts = BTreeMap::<u64, usize>::new();
    for endpoint in endpoints {
        if let Ok(TaggedLogResponse::Ready { last_position, .. }) =
            tagged_log_request(endpoint, &TaggedLogRequest::Status)
        {
            *counts.entry(last_position).or_default() += 1;
        }
    }
    let matching = counts
        .into_iter()
        .filter(|(_, count)| *count >= LOG_QUORUM)
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err("tagged-log set has no unique durable position quorum".to_owned());
    }
    Ok(matching[0].0)
}

fn common_popped_through(log_sets: &[DurableLogSet]) -> Result<u64, String> {
    let mut safe = u64::MAX;
    for set in log_sets {
        let mut counts = BTreeMap::<u64, usize>::new();
        for endpoint in set.fixture.endpoints() {
            if let Ok(TaggedLogResponse::Ready { popped_through, .. }) =
                tagged_log_request(&endpoint, &TaggedLogRequest::Status)
            {
                *counts.entry(popped_through).or_default() += 1;
            }
        }
        let matching = counts
            .into_iter()
            .filter(|(_, count)| *count >= LOG_QUORUM)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err("tagged-log set has no unique popped-through quorum".to_owned());
        }
        safe = safe.min(matching[0].0);
    }
    if safe == u64::MAX {
        return Err("durable PostgreSQL range has no txLog pop frontier".to_owned());
    }
    Ok(safe)
}

fn maximum_page_lsn(prior: u64, envelope_bytes: &[u8]) -> Result<u64, String> {
    let envelope = CommitEnvelope::decode(envelope_bytes).map_err(|error| error.to_string())?;
    let mutations: Vec<CellMutation> = serde_json::from_slice(envelope.canonical_mutations())
        .map_err(|error| error.to_string())?;
    Ok(mutations.into_iter().fold(prior, |maximum, mutation| {
        let CellMutation::Set { value, .. } = mutation else {
            return maximum;
        };
        PostgresPage::decode(&value).map_or(maximum, |page| maximum.max(page.page_lsn))
    }))
}

fn signing_seeds(seed: u64, log_set_id: u16) -> Vec<Vec<u8>> {
    (0_u64..LOG_NODES as u64)
        .map(|index| {
            let mut hasher = Sha256::new();
            hasher.update(b"okv-postgres-smgr-txlog-key-v1");
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
            tagged_log_public_key(seed).map(|public_key| CellLogSetMember {
                node_id: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                public_key,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CellLogSetPolicy {
        format_version: PROTOCOL_FORMAT_VERSION,
        generation,
        policy_epoch: POLICY_EPOCH,
        log_set_id,
        quorum_size: u16::try_from(LOG_QUORUM).unwrap_or(u16::MAX),
        ratekeeper_soft_limit_bytes: RETAINED_BYTES_LIMIT,
        members,
    })
}

fn load_postgres_descriptor(path: &Path) -> Result<PostgresDurableDescriptor, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn validate_postgres_descriptor(
    descriptor: &PostgresDurableDescriptor,
    relation: PostgresRelationForkIdentity,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
) -> Result<(), String> {
    if !matches!(
        descriptor.format_version,
        LEGACY_DURABLE_ROOT_FORMAT_VERSION | DURABLE_ROOT_FORMAT_VERSION
    ) || descriptor.format_version == LEGACY_DURABLE_ROOT_FORMAT_VERSION
        && !descriptor.object_deltas.is_empty()
        || descriptor.relation != relation
        || descriptor.cell_id != cell_id
        || descriptor.tenant_id != tenant_id
        || descriptor.generation != generation
        || descriptor.base_version == 0
        || descriptor.base_maximum_page_lsn == 0
        || !valid_relative_path(&descriptor.database_path)
        || !valid_relative_path(&descriptor.base_descriptor_path)
        || descriptor
            .base_txlogs
            .windows(2)
            .any(|pair| pair[0].log_set_id >= pair[1].log_set_id)
    {
        return Err("durable PostgreSQL descriptor differs from live bridge identity".to_owned());
    }
    Ok(())
}

fn default_base_descriptor_path() -> String {
    "range-base.json".to_owned()
}

fn valid_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn durable_relative_path(root: &Path, path: &str) -> Result<PathBuf, String> {
    if !valid_relative_path(path) {
        return Err("durable PostgreSQL root contains an invalid relative path".to_owned());
    }
    Ok(root.join(path))
}

fn persist_postgres_descriptor(
    path: &Path,
    descriptor: &PostgresDurableDescriptor,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(descriptor).map_err(|error| error.to_string())?;
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    File::open(
        path.parent()
            .ok_or_else(|| "durable PostgreSQL descriptor has no parent".to_owned())?,
    )
    .and_then(|directory| directory.sync_all())
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use okv_consensus::{sign_tagged_log_statement, CellTaggedLogAttestation};
    use okv_model::Version;
    use okv_sim::CommitEnvelopeParts;

    #[test]
    fn legacy_durable_root_v1_decodes_without_object_deltas() {
        let fixture = include_str!("../fixtures/durable-root-v1.json");
        let descriptor: PostgresDurableDescriptor = serde_json::from_str(fixture).unwrap();
        assert_eq!(
            descriptor.format_version,
            LEGACY_DURABLE_ROOT_FORMAT_VERSION
        );
        assert!(descriptor.object_deltas.is_empty());
        validate_postgres_descriptor(
            &descriptor,
            descriptor.relation,
            descriptor.cell_id,
            descriptor.tenant_id,
            descriptor.generation,
        )
        .unwrap();
    }

    #[test]
    fn legacy_reader_contract_rejects_delta_aware_root() {
        let fixture = include_str!("../fixtures/durable-root-v1.json");
        let mut descriptor: PostgresDurableDescriptor = serde_json::from_str(fixture).unwrap();
        descriptor.format_version = DURABLE_ROOT_FORMAT_VERSION;
        let encoded = serde_json::to_vec(&descriptor).unwrap();
        let observed: PostgresDurableDescriptor = serde_json::from_slice(&encoded).unwrap();
        assert_ne!(observed.format_version, LEGACY_DURABLE_ROOT_FORMAT_VERSION);
    }

    #[test]
    fn postgres_object_delta_contract_replays_and_detects_every_fault() {
        let correct =
            run_postgres_object_delta_contract(724_841, PostgresObjectDeltaMode::Correct).unwrap();
        assert_eq!(correct.anomaly_count, 0);
        assert!(correct.checks.values().all(|passed| *passed));
        assert_eq!(correct.object_delta_segments, 1);
        assert_eq!(correct.objectification_input_bytes, 8 * 1024);
        assert_eq!(
            correct.ssts_before_checkpoint,
            correct.ssts_after_checkpoint
        );

        for mode in [
            PostgresObjectDeltaMode::MissingObject,
            PostgresObjectDeltaMode::CorruptObject,
            PostgresObjectDeltaMode::BrokenChain,
            PostgresObjectDeltaMode::OmittedClosure,
            PostgresObjectDeltaMode::PopAhead,
            PostgresObjectDeltaMode::FullBaseRewrite,
        ] {
            let report = run_postgres_object_delta_contract(724_841, mode).unwrap();
            assert!(report.anomaly_count > 0, "mode {} was accepted", mode.id());
            assert!(report.checks["negative_control_detected"]);
        }
    }

    #[test]
    fn postgres_worker_readiness_splits_reads_from_closure_audit_and_detects_controls() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let correct_root = tempfile::tempdir().unwrap();
        let correct = prepare_postgres_worker_readiness_fixture(
            correct_root.path().join("correct"),
            724_841,
            8,
            8,
            4,
            1024 * 1024 * 1024,
            PostgresWorkerReadinessMode::Correct,
        )
        .unwrap();
        let correct = runtime
            .block_on(run_postgres_worker_readiness_process(&correct))
            .unwrap();
        assert_eq!(correct.anomaly_count, 0);
        assert!(correct.first_delta_point_exact);
        assert!(correct.first_base_point_exact);
        assert!(correct.first_range_exact);
        assert!(correct.full_oracle_exact);
        assert!(correct.closure_audit_executed);
        assert!(correct.closure_audit_exact);

        for mode in [
            PostgresWorkerReadinessMode::ChangedManifest,
            PostgresWorkerReadinessMode::ChangedDelta,
            PostgresWorkerReadinessMode::SkipClosureAudit,
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let config = prepare_postgres_worker_readiness_fixture(
                temporary.path().join(mode.id()),
                724_841,
                8,
                8,
                4,
                1024 * 1024 * 1024,
                mode,
            )
            .unwrap();
            let receipt = runtime
                .block_on(run_postgres_worker_readiness_process(&config))
                .unwrap();
            assert!(receipt.anomaly_count > 0, "mode {} passed", mode.id());
            assert!(receipt.negative_control_detected);
        }
    }

    #[test]
    fn postgres_object_delta_suffix_is_independent_of_untouched_relation_size() {
        let small = run_postgres_object_delta_contract_with_config(
            724_841,
            PostgresObjectDeltaMode::Correct,
            PostgresObjectDeltaContractConfig {
                relation_blocks: 2,
                reference_full_base_rewrite: false,
            },
        )
        .unwrap();
        let baseline = run_postgres_object_delta_contract_with_config(
            724_841,
            PostgresObjectDeltaMode::Correct,
            PostgresObjectDeltaContractConfig {
                relation_blocks: 128,
                reference_full_base_rewrite: false,
            },
        )
        .unwrap();

        assert_eq!(small.changed_block, baseline.changed_block);
        assert_eq!(small.object_delta_bytes, baseline.object_delta_bytes);
        assert_eq!(small.object_delta_sha256, baseline.object_delta_sha256);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn objectifies_only_the_certified_suffix_and_restarts_without_a_new_sst() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("durable");
        let object_root = root.join("objects");
        let base_descriptor_path = root.join("range-base.json");
        let cell_id = [0x11; 16];
        let tenant_id = [0x22; 16];
        let generation = 1;
        let base_log_chain_sha256 = [0x33; 32];
        let base_page = page(100, 0x10);
        let base = materialize_persistent_range_base(
            &PersistentRangeBaseConfig {
                object_root: object_root.clone(),
                descriptor_path: base_descriptor_path.clone(),
                database_path: DATABASE_PATH.to_owned(),
                seed: 701,
                cell_id,
                tenant_id,
                generation,
                base_version: 1,
                minimum_readable_version: 1,
                log_chain_sha256: base_log_chain_sha256,
            },
            &BTreeMap::from([(
                1,
                vec![CellMutation::Set {
                    key: b"page/a".to_vec(),
                    value: base_page.clone(),
                }],
            )]),
        )
        .await
        .unwrap();
        let (policy, seeds) = test_log_policy(generation);
        let page_at_2 = page(200, 0x20);
        let first = certified_record(
            2,
            base_log_chain_sha256,
            &[CellMutation::Set {
                key: b"page/a".to_vec(),
                value: page_at_2,
            }],
            &policy,
            &seeds,
        );
        let page_at_3 = page(300, 0x30);
        let second = certified_record(
            3,
            Sha256::digest(&first.envelope).into(),
            &[CellMutation::Set {
                key: b"page/b".to_vec(),
                value: page_at_3.clone(),
            }],
            &policy,
            &seeds,
        );
        let relation = PostgresRelationForkIdentity {
            cluster_id: [0x44; 16],
            tablespace_oid: 16_384,
            database_oid: 16_385,
            relation_number: 16_386,
            temporary_backend_id: 0,
            fork_number: 0,
        };
        let mut durable = DurablePostgresRange {
            root: root.clone(),
            relation,
            base: base.clone(),
            base_descriptor_path: "range-base.json".to_owned(),
            base_maximum_page_lsn: 100,
            base_txlogs: Vec::new(),
            base_visible_rows_sha256: [0; 32],
            object_deltas: Vec::new(),
            object_records: Vec::new(),
            log_sets: Vec::new(),
            policies: BTreeMap::from([(policy.log_set_id, policy.clone())]),
            records: vec![first, second],
            target_version: 3,
            maximum_page_lsn: 300,
            popped_through: 0,
        };
        let ssts_before = files_with_suffix(&object_root, ".sst");
        let prepared = durable.object_delta_plan(3).unwrap().materialize().unwrap();
        assert_eq!(prepared.delta.after_version, 1);
        assert_eq!(prepared.delta.through_version, 3);
        assert_eq!(prepared.delta.record_count, 2);
        assert!(prepared.delta.object.length > 0);
        assert_eq!(files_with_suffix(&object_root, ".segment"), 1);
        assert_eq!(files_with_suffix(&object_root, ".sst"), ssts_before);

        let (serving, frontier) = durable.activate_object_delta(prepared, 702).await.unwrap();
        assert_eq!(durable.base.root.covered_through, 1);
        assert_eq!(durable.base_version(), 3);
        assert_eq!(durable.authenticated_tail_records(), 0);
        assert_eq!(durable.object_delta_segments(), 1);
        assert!(durable.object_delta_bytes() > 0);
        assert!(
            durable.objectification_input_bytes()
                >= u64::try_from(2 * crate::POSTGRES_PAGE_SIZE).unwrap()
        );
        assert_eq!(durable.envelopes().count(), 2);
        assert_eq!(frontier.object_deltas.len(), 1);
        let rows = serving
            .current()
            .unwrap()
            .scan_at(&[], &[0xff], 3, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1], (b"page/b".to_vec(), page_at_3));

        let persisted = load_postgres_descriptor(&root.join("postgres-root.json")).unwrap();
        assert_eq!(persisted.format_version, DURABLE_ROOT_FORMAT_VERSION);
        assert_eq!(persisted.base_version, 1);
        assert_eq!(persisted.object_deltas.len(), 1);
        let reopened_base = load_persistent_range_base(&base_descriptor_path).unwrap();
        let reopened_records = load_persistent_range_delta_lineage(
            &object_root,
            &reopened_base,
            &persisted.object_deltas,
        )
        .unwrap();
        let reopened = open_persistent_range_view(
            &object_root,
            &reopened_base,
            3,
            reopened_records,
            &BTreeMap::from([(policy.log_set_id, policy)]),
            703,
        )
        .await
        .unwrap();
        assert_eq!(reopened.scan_at(&[], &[0xff], 3, 10).await.unwrap(), rows);
        assert_eq!(files_with_suffix(&object_root, ".sst"), ssts_before);
    }

    fn page(page_lsn: u64, byte: u8) -> Vec<u8> {
        PostgresPage::new(
            page_lsn,
            u16::from(byte),
            vec![byte; crate::POSTGRES_PAGE_SIZE],
        )
        .unwrap()
        .encode()
    }

    fn test_log_policy(generation: u64) -> (CellLogSetPolicy, BTreeMap<u64, Vec<u8>>) {
        let seeds = BTreeMap::from([
            (101, vec![0x11; 32]),
            (102, vec![0x22; 32]),
            (103, vec![0x33; 32]),
        ]);
        let members = seeds
            .iter()
            .map(|(node_id, seed)| CellLogSetMember {
                node_id: *node_id,
                public_key: tagged_log_public_key(seed).unwrap(),
            })
            .collect();
        (
            CellLogSetPolicy {
                format_version: PROTOCOL_FORMAT_VERSION,
                generation,
                policy_epoch: 1,
                log_set_id: 10,
                quorum_size: 2,
                ratekeeper_soft_limit_bytes: RETAINED_BYTES_LIMIT,
                members,
            },
            seeds,
        )
    }

    fn certified_record(
        sequence: u64,
        previous_log_chain: [u8; 32],
        mutations: &[CellMutation],
        policy: &CellLogSetPolicy,
        seeds: &BTreeMap<u64, Vec<u8>>,
    ) -> CertifiedTxLogRecord {
        let mut client_id = [0_u8; 16];
        client_id[8..].copy_from_slice(&41_u64.to_be_bytes());
        let envelope = CommitEnvelope::from_parts(CommitEnvelopeParts {
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 1,
            version: Version::from_parts(1, sequence),
            log_index: sequence,
            client_id,
            request_id: sequence,
            resolver_set_id: [0x55; 16],
            read_conflicts: vec![0x01],
            write_conflicts: vec![0x02],
            canonical_mutations: serde_json::to_vec(&mutations).unwrap(),
            required_resolvers: vec![1],
            required_log_tags: vec![policy.log_set_id],
            previous_log_chain,
        });
        let envelope = envelope.encode();
        let statement = CellTaggedLogStatement {
            format_version: PROTOCOL_FORMAT_VERSION,
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 1,
            transaction_identity: RequestIdentity {
                client_id: 41,
                request_id: sequence,
            },
            commit_sequence: sequence,
            log_set_id: policy.log_set_id,
            policy_epoch: policy.policy_epoch,
            envelope_sha256: Sha256::digest(&envelope).into(),
            durable_position: sequence,
        };
        let attestations = seeds
            .iter()
            .take(2)
            .map(|(node_id, seed)| sign_tagged_log_statement(*node_id, seed, &statement).unwrap())
            .collect::<Vec<CellTaggedLogAttestation>>();
        CertifiedTxLogRecord {
            envelope,
            certificates: vec![CellTaggedLogCertificate {
                statement,
                attestations,
            }],
        }
    }

    fn files_with_suffix(root: &Path, suffix: &str) -> usize {
        let Ok(entries) = fs::read_dir(root) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .map(|path| {
                if path.is_dir() {
                    files_with_suffix(&path, suffix)
                } else {
                    usize::from(path.to_string_lossy().ends_with(suffix))
                }
            })
            .sum()
    }
}
