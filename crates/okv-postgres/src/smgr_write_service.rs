//! Mutable `PostgreSQL` storage-manager probe over Cell commits and fresh Range Engines.

use crate::smgr_durable::{
    DurablePostgresRange, PostgresObjectDeltaPlan, PreparedPostgresObjectDelta,
};
use crate::smgr_stable::{publication_pop_policy, PostgresStablePublisher};
use crate::{
    admit_postgres_page_write, plan_postgres_page_commit, verify_postgres_page_commit,
    PostgresPage, PostgresPageCommitContext, PostgresPageCommitOperation, PostgresPageReadSnapshot,
    PostgresPageReader, PostgresPageWriteBatch, PostgresPublicationAuthorityConfig,
    PostgresRelationForkIdentity, PostgresTransactionAuthorityConfig, POSTGRES_PAGE_SIZE,
};
use async_trait::async_trait;
use okv_consensus::{
    CellKeyRange, CellMutation, CellProcessFixture, CellProcessPrototypeMode, CellReadVersion,
    CellStateSnapshot, CellTransactionClient, CellTransactionCommand, CellTransactionStatus,
    RequestIdentity,
};
use okv_object::{
    build_fixture_range_serving_state, serve_range_read_listener, ClientRangeMapSnapshot,
    ClientRangeRoute, KvReadClient, KvReadClientConfig, KvReadRouter, KvReadRouterConfig,
    RangeEngineId, RangeMapSource, RangeReadAssignment, RangeReadProtocolConfig, RangeServingState,
    RANGE_SERVING_FIXTURE_CELL_ID, RANGE_SERVING_FIXTURE_TENANT_ID,
};
use okv_sim::CommitEnvelope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::Instant;

const PROTOCOL_MAGIC: &[u8; 8] = b"OKVPGS02";
const RESPONSE_MAGIC: &[u8; 8] = b"OKVPGR02";
const REQUEST_HEADER_BYTES: usize = 80;
const RESPONSE_HEADER_BYTES: usize = 40;
const OPERATION_READ: u32 = 1;
const OPERATION_WRITE_EXISTING: u32 = 2;
const OPERATION_NBLOCKS: u32 = 3;
const OPERATION_STABLE: u32 = 4;
const RESPONSE_OK: u32 = 0;
const RESPONSE_ERROR: u32 = 1;
const MAXIMUM_ERROR_BYTES: usize = 4 * 1024;
const MAXIMUM_PAGES_PER_REQUEST: usize = 128;
const BOOTSTRAP_PAGES_PER_BATCH: usize = 16;
const RANGE_ID: RangeEngineId = RangeEngineId(201);
const ROUTING_EPOCH: u64 = 1;
const MAP_VERSION: u64 = 1;

/// One selected permanent relation served through the mutable callback probe.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresSmgrWriteServiceConfig {
    pub seed: u64,
    pub listen_address: String,
    pub source_file: PathBuf,
    pub status_file: PathBuf,
    pub cell_process_executable: PathBuf,
    pub cluster_id: [u8; 16],
    pub tablespace_oid: u32,
    pub database_oid: u32,
    pub relation_number: u32,
    pub temporary_backend_id: u32,
    pub fork_number: u8,
    pub maximum_page_lsn: u64,
    pub maximum_blocks_per_read: usize,
    #[serde(default)]
    pub durable_root: Option<PathBuf>,
    #[serde(default)]
    pub publication_authority: Option<PostgresPublicationAuthorityConfig>,
    #[serde(default)]
    pub transaction_authority: Option<PostgresTransactionAuthorityConfig>,
}

/// Current machine-readable bridge state, also published to `status_file`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresSmgrWriteServiceStatus {
    pub listen_address: String,
    pub source_file: PathBuf,
    pub relation_number: u32,
    pub objectkv_version: u64,
    pub maximum_page_lsn: u64,
    pub nblocks: u32,
    pub committed_write_batches: u64,
    pub fresh_range_engine_views: u64,
    pub base_objectkv_version: u64,
    pub authenticated_txlog_records: u64,
    pub recovered_durable_state: bool,
    pub stable_objectkv_version: u64,
    pub stable_object_frontier: u64,
    pub stable_maximum_page_lsn: u64,
    pub stable_postgres_wal_flush_lsn: u64,
    pub stable_sync_last_duration_millis: u64,
    pub stable_authority_term: u64,
    pub stable_authority_index: u64,
    pub stable_manifest_sha256: Option<String>,
    pub txlog_popped_through: u64,
    pub txlog_pop_certificates: u64,
    pub txlog_pop_error: Option<String>,
    pub objectification_ready_version: u64,
    pub objectification_lag_versions: u64,
    pub objectification_last_duration_millis: u64,
    pub objectification_error: Option<String>,
    #[serde(default)]
    pub object_delta_segments: u64,
    #[serde(default)]
    pub object_delta_bytes: u64,
    #[serde(default)]
    pub objectification_input_bytes: u64,
    #[serde(default)]
    pub object_delta_layers: u64,
    #[serde(default)]
    pub object_compaction_debt_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SmgrRequest {
    operation: u32,
    tablespace_oid: u32,
    database_oid: u32,
    relation_number: u32,
    temporary_backend_id: u32,
    fork_number: u32,
    first_block: u32,
    block_count: u32,
    expected_objectkv_version: u64,
    wal_or_page_lsn: u64,
    previous_nblocks: u32,
    resulting_nblocks: u32,
    request_id: [u8; 16],
}

struct MutableBridgeState {
    seed: u64,
    listen_address: String,
    source_file: PathBuf,
    status_file: PathBuf,
    relation: PostgresRelationForkIdentity,
    maximum_blocks_per_read: usize,
    context: PostgresPageCommitContext,
    client: CellTransactionClient,
    mutations: BTreeMap<u64, Vec<CellMutation>>,
    reader: Arc<PostgresPageReader>,
    objectkv_version: u64,
    maximum_page_lsn: u64,
    nblocks: u32,
    committed_write_batches: u64,
    fresh_range_engine_views: u64,
    base_objectkv_version: u64,
    authenticated_txlog_records: u64,
    recovered_durable_state: bool,
    durable: Option<DurablePostgresRange>,
    stable_publisher: Option<PostgresStablePublisher>,
    stable_objectkv_version: u64,
    stable_object_frontier: u64,
    stable_maximum_page_lsn: u64,
    stable_postgres_wal_flush_lsn: u64,
    stable_sync_last_duration_millis: u64,
    stable_authority_term: u64,
    stable_authority_index: u64,
    stable_manifest_sha256: Option<String>,
    txlog_popped_through: u64,
    txlog_pop_certificates: u64,
    txlog_pop_error: Option<String>,
    objectifier: Arc<ObjectificationCoordinator>,
    objectification_ready_version: u64,
    objectification_last_duration_millis: u64,
    objectification_error: Option<String>,
}

struct ObjectificationCapture {
    objectkv_version: u64,
    plan: PostgresObjectDeltaPlan,
}

struct ReadyObjectDelta {
    version: u64,
    prepared: PreparedPostgresObjectDelta,
    duration_millis: u64,
}

#[derive(Default)]
struct ObjectificationQueue {
    runner_active: bool,
    running_version: Option<u64>,
    pending: Option<ObjectificationCapture>,
    ready: Option<ReadyObjectDelta>,
}

#[derive(Default)]
struct ObjectificationCoordinator {
    queue: Mutex<ObjectificationQueue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectificationSchedule {
    Ignore,
    StartRunner,
    Queue,
    ReplacePending,
}

fn objectification_schedule(
    runner_active: bool,
    running_version: Option<u64>,
    pending_version: Option<u64>,
    ready_version: Option<u64>,
    requested_version: u64,
) -> ObjectificationSchedule {
    let newest_known = [running_version, pending_version, ready_version]
        .into_iter()
        .flatten()
        .max();
    if newest_known.is_some_and(|version| version >= requested_version) {
        ObjectificationSchedule::Ignore
    } else if !runner_active {
        ObjectificationSchedule::StartRunner
    } else if pending_version.is_some() {
        ObjectificationSchedule::ReplacePending
    } else {
        ObjectificationSchedule::Queue
    }
}

impl ObjectificationCoordinator {
    async fn enqueue(&self, capture: ObjectificationCapture) -> ObjectificationSchedule {
        let mut queue = self.queue.lock().await;
        let schedule = objectification_schedule(
            queue.runner_active,
            queue.running_version,
            queue
                .pending
                .as_ref()
                .map(|pending| pending.objectkv_version),
            queue.ready.as_ref().map(|ready| ready.version),
            capture.objectkv_version,
        );
        match schedule {
            ObjectificationSchedule::Ignore => {}
            ObjectificationSchedule::StartRunner => {
                queue.runner_active = true;
                queue.pending = Some(capture);
            }
            ObjectificationSchedule::Queue | ObjectificationSchedule::ReplacePending => {
                queue.pending = Some(capture);
            }
        }
        schedule
    }

    async fn next_capture(&self) -> Option<ObjectificationCapture> {
        let mut queue = self.queue.lock().await;
        let capture = queue.pending.take();
        if let Some(capture) = &capture {
            queue.running_version = Some(capture.objectkv_version);
        } else {
            queue.running_version = None;
            queue.runner_active = false;
        }
        capture
    }

    async fn is_superseded(&self, version: u64) -> bool {
        let queue = self.queue.lock().await;
        queue.running_version != Some(version)
            || queue
                .pending
                .as_ref()
                .is_some_and(|pending| pending.objectkv_version > version)
    }

    async fn complete(&self, ready: ReadyObjectDelta) {
        let mut queue = self.queue.lock().await;
        if queue.running_version != Some(ready.version) {
            return;
        }
        queue.running_version = None;
        if queue
            .ready
            .as_ref()
            .is_none_or(|current| current.version < ready.version)
        {
            queue.ready = Some(ready);
        }
    }

    async fn finish_without_base(&self, version: u64) {
        let mut queue = self.queue.lock().await;
        if queue.running_version == Some(version) {
            queue.running_version = None;
        }
    }

    async fn take_latest(
        &self,
        after_version: u64,
        through_version: u64,
    ) -> Option<ReadyObjectDelta> {
        let mut queue = self.queue.lock().await;
        if queue
            .ready
            .as_ref()
            .is_some_and(|ready| ready.version <= after_version)
        {
            queue.ready = None;
        }
        if queue
            .ready
            .as_ref()
            .is_some_and(|ready| ready.version <= through_version)
        {
            queue.ready.take()
        } else {
            None
        }
    }
}

struct StaticRangeMapSource {
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    snapshot: ClientRangeMapSnapshot,
}

#[async_trait]
impl RangeMapSource for StaticRangeMapSource {
    async fn snapshot(
        &self,
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
    ) -> Result<ClientRangeMapSnapshot, String> {
        if cell_id != self.cell_id || tenant_id != self.tenant_id {
            return Err("mutable PostgreSQL bridge received the wrong session identity".to_owned());
        }
        Ok(self.snapshot.clone())
    }
}

/// Bootstrap one real relation into a frozen versioned view, serve mutable page
/// requests through Cell commits, and rebuild a fresh Range Engine after every
/// accepted write batch.
///
/// The Cell fixture and in-memory object store make this a literal callback
/// probe, not production durability or checkpoint authority.
///
/// # Errors
///
/// Returns an error for invalid configuration, import, Cell commit, Range
/// Engine construction, status publication, or listener failure.
pub fn run_postgres_smgr_write_service(
    config: PostgresSmgrWriteServiceConfig,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(serve(config))
}

#[allow(clippy::too_many_lines)]
async fn serve(config: PostgresSmgrWriteServiceConfig) -> Result<(), String> {
    validate_config(&config)?;
    let cell_process_executable = config.cell_process_executable.clone();
    let mut owned_cell_fixture = if config.transaction_authority.is_none() {
        Some(CellProcessFixture::start(
            config.seed,
            CellProcessPrototypeMode::Correct,
            &cell_process_executable,
        )?)
    } else {
        None
    };
    let (snapshot, client) = if let Some(external) = &config.transaction_authority {
        let client = CellTransactionClient::new(external.endpoints.clone())?;
        let snapshot = client.linearizable_snapshot().await?;
        if snapshot.cell_id != external.cell_id
            || snapshot.tenant_id != external.tenant_id
            || snapshot.generation != external.generation
        {
            return Err(
                "external PostgreSQL transaction authority differs from configured identity"
                    .to_owned(),
            );
        }
        (snapshot, client)
    } else {
        let fixture = owned_cell_fixture
            .as_mut()
            .ok_or_else(|| "owned PostgreSQL transaction authority is absent".to_owned())?;
        let baseline = fixture.run_history().await?;
        if baseline.anomaly_count != 0 {
            return Err("mutable bridge Cell fixture baseline has anomalies".to_owned());
        }
        let snapshot = fixture.linearizable_cell_snapshot().await?;
        let client = CellTransactionClient::new(fixture.endpoints())?;
        (snapshot, client)
    };
    let context = PostgresPageCommitContext {
        cell_id: snapshot.cell_id,
        tenant_id: snapshot.tenant_id,
        generation: snapshot.generation,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: vec![10, 20],
    };
    let relation = relation_identity(&config);
    let base_objectkv_version = snapshot.latest_sequence;
    let base_envelope = snapshot
        .committed_envelopes
        .last()
        .ok_or_else(|| "mutable bridge Cell baseline omitted its commit chain".to_owned())?;
    let base_log_chain_sha256 = Sha256::digest(base_envelope).into();
    let durable_exists = config
        .durable_root
        .as_ref()
        .is_some_and(|root| root.join("postgres-root.json").exists());
    let bootstrap = if durable_exists {
        None
    } else {
        let (pages, maximum_page_lsn) = read_source_pages(&config)?;
        let mutations = bootstrap_pages(
            &pages,
            base_objectkv_version,
            &context,
            relation,
            maximum_page_lsn,
        )?;
        Some((mutations, maximum_page_lsn))
    };
    let mutations = bootstrap
        .as_ref()
        .map_or_else(BTreeMap::new, |(mutations, _)| mutations.clone());
    let txlog_pop_policy = config
        .publication_authority
        .as_ref()
        .and_then(publication_pop_policy);
    let (
        reader,
        objectkv_version,
        observed_base_objectkv_version,
        observed_maximum_page_lsn,
        authenticated_txlog_records,
        recovered_txlog_popped_through,
        recovered_durable_state,
        durable,
    ) = if let Some(root) = config.durable_root.clone() {
        let opened = DurablePostgresRange::open_or_bootstrap(
            root,
            config.seed,
            &cell_process_executable,
            relation,
            context.cell_id,
            context.tenant_id,
            context.generation,
            base_objectkv_version,
            base_log_chain_sha256,
            &context.durable_log_tags,
            bootstrap.as_ref().map(|(mutations, _)| mutations),
            bootstrap.as_ref().map(|(_, maximum)| *maximum),
            txlog_pop_policy.as_ref(),
        )
        .await?;
        if opened.recovered_existing && opened.target_version > base_objectkv_version {
            replay_cell_tail(&client, base_objectkv_version, opened.durable.envelopes()).await?;
        }
        let reader = build_reader_from_serving_state(
            context.cell_id,
            context.tenant_id,
            opened.serving,
            config.maximum_blocks_per_read,
        )
        .await?;
        let observed_base_objectkv_version = opened.durable.base_version();
        (
            reader,
            opened.target_version,
            observed_base_objectkv_version,
            opened.maximum_page_lsn,
            opened.authenticated_tail_records,
            opened.popped_through,
            opened.recovered_existing,
            Some(opened.durable),
        )
    } else {
        let observed_maximum_page_lsn = bootstrap
            .as_ref()
            .map(|(_, maximum)| *maximum)
            .ok_or_else(|| "in-memory bridge omitted bootstrap pages".to_owned())?;
        let reader = build_reader(
            config.seed,
            base_objectkv_version,
            &mutations,
            config.maximum_blocks_per_read,
        )
        .await?;
        (
            reader,
            base_objectkv_version,
            base_objectkv_version,
            observed_maximum_page_lsn,
            0,
            0,
            false,
            None,
        )
    };
    let mut stable_publisher = if let Some(publication) = config.publication_authority.clone() {
        let durable = durable.as_ref().ok_or_else(|| {
            "PostgreSQL publication authority requires a durable object base".to_owned()
        })?;
        Some(
            PostgresStablePublisher::open(
                durable.object_root(),
                relation,
                context.generation,
                publication,
                durable,
            )
            .await?,
        )
    } else {
        None
    };
    let stable = stable_publisher
        .as_mut()
        .and_then(|publisher| publisher.current());
    let stable_objectkv_version = stable.map_or(0, |receipt| receipt.objectkv_version);
    let stable_object_frontier = stable.map_or(0, |receipt| receipt.object_frontier);
    let stable_maximum_page_lsn = stable.map_or(0, |receipt| receipt.maximum_page_lsn);
    let stable_postgres_wal_flush_lsn = stable.map_or(0, |receipt| receipt.postgres_wal_flush_lsn);
    let stable_authority_term = stable.map_or(0, |receipt| receipt.authority_term);
    let stable_authority_index = stable.map_or(0, |receipt| receipt.authority_index);
    let stable_manifest_sha256 = stable.map(|receipt| receipt.manifest_sha256.clone());
    let nblocks = reader
        .read_nblocks(relation, objectkv_version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "fresh Range Engine omitted imported relation extent".to_owned())?
        .nblocks;
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(|error| error.to_string())?;
    let listen_address = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .to_string();
    let objectifier = Arc::new(ObjectificationCoordinator::default());
    let state = Arc::new(Mutex::new(MutableBridgeState {
        seed: config.seed,
        listen_address,
        source_file: config.source_file,
        status_file: config.status_file,
        relation,
        maximum_blocks_per_read: config.maximum_blocks_per_read,
        context,
        client,
        mutations,
        reader,
        objectkv_version,
        maximum_page_lsn: observed_maximum_page_lsn,
        nblocks,
        committed_write_batches: 0,
        fresh_range_engine_views: 1,
        base_objectkv_version: observed_base_objectkv_version,
        authenticated_txlog_records,
        recovered_durable_state,
        durable,
        stable_publisher,
        stable_objectkv_version,
        stable_object_frontier,
        stable_maximum_page_lsn,
        stable_postgres_wal_flush_lsn,
        stable_sync_last_duration_millis: 0,
        stable_authority_term,
        stable_authority_index,
        stable_manifest_sha256,
        txlog_popped_through: recovered_txlog_popped_through,
        txlog_pop_certificates: 0,
        txlog_pop_error: None,
        objectifier,
        objectification_ready_version: observed_base_objectkv_version,
        objectification_last_duration_millis: 0,
        objectification_error: None,
    }));
    {
        let state = state.lock().await;
        publish_status(&state)?;
        println!(
            "{}",
            serde_json::to_string(&status(&state)).map_err(|error| error.to_string())?
        );
    }

    loop {
        let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = Box::pin(serve_connection(stream, state)).await {
                eprintln!("objectKV PostgreSQL mutable request failed: {error}");
            }
        });
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<MutableBridgeState>>,
) -> Result<(), String> {
    let mut header = [0_u8; REQUEST_HEADER_BYTES];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| error.to_string())?;
    let request = match parse_request(&header) {
        Ok(request) => request,
        Err(error) => return write_error(&mut stream, &error, 0, 0).await,
    };
    match request.operation {
        OPERATION_READ => serve_read(&mut stream, &state, request).await,
        OPERATION_WRITE_EXISTING => Box::pin(serve_write(&mut stream, &state, request)).await,
        OPERATION_NBLOCKS => serve_nblocks(&mut stream, &state, request).await,
        OPERATION_STABLE => Box::pin(serve_stable(&mut stream, &state, request)).await,
        other => {
            write_error(
                &mut stream,
                &format!("unknown storage-manager operation {other}"),
                0,
                0,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn serve_stable(
    stream: &mut TcpStream,
    shared_state: &Arc<Mutex<MutableBridgeState>>,
    request: SmgrRequest,
) -> Result<(), String> {
    let stable_started = Instant::now();
    let (target_version, active_base_version, coordinator, capture) = {
        let state = shared_state.lock().await;
        if let Err(error) = validate_request(&state, request, false) {
            return write_state_error(stream, &error, &state).await;
        }
        if request.first_block != 0
            || request.block_count != 0
            || request.previous_nblocks != 0
            || request.resulting_nblocks != 0
            || request.wal_or_page_lsn < state.maximum_page_lsn
        {
            return write_state_error(
                stream,
                "PostgreSQL stable request does not cover the current page frontier",
                &state,
            )
            .await;
        }
        if state.stable_publisher.is_none() || state.durable.is_none() {
            return write_state_error(
                stream,
                "PostgreSQL stable request requires publication and durable authorities",
                &state,
            )
            .await;
        }
        (
            state.objectkv_version,
            state.base_objectkv_version,
            Arc::clone(&state.objectifier),
            if state.objectkv_version > state.base_objectkv_version {
                match objectification_capture(&state) {
                    Ok(capture) => Some(capture),
                    Err(error) => return write_state_error(stream, &error, &state).await,
                }
            } else {
                None
            },
        )
    };
    if let Some(capture) = capture {
        enqueue_objectification(Arc::clone(shared_state), Arc::clone(&coordinator), capture).await;
    }
    let ready = coordinator
        .take_latest(active_base_version, target_version)
        .await;
    let mut state = shared_state.lock().await;
    if ready
        .as_ref()
        .is_some_and(|ready| request.wal_or_page_lsn < ready.prepared.maximum_page_lsn())
    {
        return write_state_error(
            stream,
            "PostgreSQL stable request is behind the prepared object base",
            &state,
        )
        .await;
    }
    let Some(mut publisher) = state.stable_publisher.take() else {
        return write_state_error(
            stream,
            "PostgreSQL stable request has no publication authority",
            &state,
        )
        .await;
    };
    let Some(mut durable) = state.durable.take() else {
        state.stable_publisher = Some(publisher);
        return write_state_error(
            stream,
            "PostgreSQL stable request has no durable range",
            &state,
        )
        .await;
    };
    if let Some(ready) = ready {
        let (serving, _) = match durable
            .activate_object_delta(ready.prepared, state.seed ^ target_version)
            .await
        {
            Ok(activated) => activated,
            Err(error) => {
                state.durable = Some(durable);
                state.stable_publisher = Some(publisher);
                return write_state_error(stream, &error, &state).await;
            }
        };
        let reader = match build_reader_from_serving_state(
            state.context.cell_id,
            state.context.tenant_id,
            serving,
            state.maximum_blocks_per_read,
        )
        .await
        {
            Ok(reader) => reader,
            Err(error) => {
                state.durable = Some(durable);
                state.stable_publisher = Some(publisher);
                return write_state_error(stream, &error, &state).await;
            }
        };
        state.reader = reader;
        state.objectification_last_duration_millis = ready.duration_millis;
        state.fresh_range_engine_views = state.fresh_range_engine_views.saturating_add(1);
    }
    state.base_objectkv_version = durable.base_version();
    state.authenticated_txlog_records = durable.authenticated_tail_records();
    let object_snapshot = CellStateSnapshot {
        cell_id: state.context.cell_id,
        tenant_id: state.context.tenant_id,
        generation: state.context.generation,
        latest_sequence: durable.base_version(),
        ..CellStateSnapshot::default()
    };
    let receipt = match publisher
        .publish(
            &durable,
            target_version,
            request.wal_or_page_lsn,
            object_snapshot,
        )
        .await
    {
        Ok(receipt) => (
            receipt.objectkv_version,
            receipt.object_frontier,
            receipt.maximum_page_lsn,
            receipt.postgres_wal_flush_lsn,
            receipt.authority_term,
            receipt.authority_index,
            receipt.manifest_sha256.clone(),
        ),
        Err(error) => {
            state.durable = Some(durable);
            state.stable_publisher = Some(publisher);
            state.stable_sync_last_duration_millis =
                u64::try_from(stable_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            publish_status(&state)?;
            return write_state_error(stream, &error, &state).await;
        }
    };
    match publisher.pop_published_prefix(&mut durable).await {
        Ok(Some(pop)) => {
            state.txlog_popped_through = pop.object_frontier;
            state.txlog_pop_certificates =
                u64::try_from(pop.certificates.len()).unwrap_or(u64::MAX);
            state.txlog_pop_error = None;
        }
        Ok(None) => {}
        Err(error) => state.txlog_pop_error = Some(error),
    }
    state.authenticated_txlog_records = durable.authenticated_tail_records();
    state.durable = Some(durable);
    state.stable_publisher = Some(publisher);
    state.stable_objectkv_version = receipt.0;
    state.stable_object_frontier = receipt.1;
    state.stable_maximum_page_lsn = receipt.2;
    state.stable_postgres_wal_flush_lsn = receipt.3;
    state.stable_authority_term = receipt.4;
    state.stable_authority_index = receipt.5;
    state.stable_manifest_sha256 = Some(receipt.6);
    state.stable_sync_last_duration_millis =
        u64::try_from(stable_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    publish_status(&state)?;
    write_response(
        stream,
        RESPONSE_OK,
        OPERATION_STABLE,
        0,
        state.nblocks,
        state.stable_objectkv_version,
        state.stable_maximum_page_lsn,
    )
    .await
}

fn objectification_capture(state: &MutableBridgeState) -> Result<ObjectificationCapture, String> {
    let plan = state
        .durable
        .as_ref()
        .ok_or_else(|| "PostgreSQL objectifier has no durable range".to_owned())?
        .object_delta_plan(state.objectkv_version)?;
    Ok(ObjectificationCapture {
        objectkv_version: state.objectkv_version,
        plan,
    })
}

async fn enqueue_objectification(
    state: Arc<Mutex<MutableBridgeState>>,
    coordinator: Arc<ObjectificationCoordinator>,
    capture: ObjectificationCapture,
) {
    if coordinator.enqueue(capture).await != ObjectificationSchedule::StartRunner {
        return;
    }
    tokio::spawn(async move {
        while let Some(capture) = coordinator.next_capture().await {
            Box::pin(objectify_capture(&state, &coordinator, capture)).await;
        }
    });
}

async fn objectify_capture(
    state: &Arc<Mutex<MutableBridgeState>>,
    coordinator: &ObjectificationCoordinator,
    capture: ObjectificationCapture,
) {
    let version = capture.objectkv_version;
    let started = Instant::now();
    let result = async {
        if coordinator.is_superseded(version).await {
            return Ok(None);
        }
        capture.plan.materialize().map(Some)
    }
    .await;
    let duration_millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match result {
        Ok(Some(prepared)) => {
            coordinator
                .complete(ReadyObjectDelta {
                    version,
                    prepared,
                    duration_millis,
                })
                .await;
            let mut state = state.lock().await;
            state.objectification_ready_version = state.objectification_ready_version.max(version);
            state.objectification_last_duration_millis = duration_millis;
            state.objectification_error = None;
            let _ = publish_status(&state);
        }
        Ok(None) => coordinator.finish_without_base(version).await,
        Err(error) => {
            coordinator.finish_without_base(version).await;
            let mut state = state.lock().await;
            state.objectification_error = Some(error);
            let _ = publish_status(&state);
        }
    }
}

async fn serve_read(
    stream: &mut TcpStream,
    state: &Arc<Mutex<MutableBridgeState>>,
    request: SmgrRequest,
) -> Result<(), String> {
    let (reader, relation, snapshot, count) = {
        let state = state.lock().await;
        let objectkv_version = match validate_request(&state, request, true) {
            Ok(objectkv_version) => objectkv_version,
            Err(error) => {
                return write_error(
                    stream,
                    &error,
                    state.objectkv_version,
                    state.maximum_page_lsn,
                )
                .await;
            }
        };
        let count = usize::try_from(request.block_count)
            .map_err(|_| "block count does not fit this process".to_owned())?;
        (
            Arc::clone(&state.reader),
            state.relation,
            PostgresPageReadSnapshot {
                objectkv_version,
                maximum_page_lsn: if request.expected_objectkv_version == 0 {
                    state.maximum_page_lsn
                } else {
                    request.wal_or_page_lsn
                },
            },
            count,
        )
    };
    match reader
        .read_pages(relation.page(request.first_block), count, snapshot)
        .await
    {
        Ok(pages) => write_pages(stream, &pages, snapshot).await,
        Err(error) => {
            write_error(
                stream,
                &error.to_string(),
                snapshot.objectkv_version,
                snapshot.maximum_page_lsn,
            )
            .await
        }
    }
}

async fn serve_nblocks(
    stream: &mut TcpStream,
    state: &Arc<Mutex<MutableBridgeState>>,
    request: SmgrRequest,
) -> Result<(), String> {
    let (reader, relation, objectkv_version, maximum_page_lsn) = {
        let state = state.lock().await;
        let objectkv_version = match validate_request(&state, request, false) {
            Ok(objectkv_version) => objectkv_version,
            Err(error) => {
                return write_error(
                    stream,
                    &error,
                    state.objectkv_version,
                    state.maximum_page_lsn,
                )
                .await;
            }
        };
        (
            Arc::clone(&state.reader),
            state.relation,
            objectkv_version,
            state.maximum_page_lsn,
        )
    };
    match reader.read_nblocks(relation, objectkv_version).await {
        Ok(Some(extent)) => {
            write_response(
                stream,
                RESPONSE_OK,
                OPERATION_NBLOCKS,
                0,
                extent.nblocks,
                objectkv_version,
                maximum_page_lsn,
            )
            .await
        }
        Ok(None) => {
            write_error(
                stream,
                "relation extent is missing",
                objectkv_version,
                maximum_page_lsn,
            )
            .await
        }
        Err(error) => {
            write_error(
                stream,
                &error.to_string(),
                objectkv_version,
                maximum_page_lsn,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn serve_write(
    stream: &mut TcpStream,
    state: &Arc<Mutex<MutableBridgeState>>,
    request: SmgrRequest,
) -> Result<(), String> {
    let count = usize::try_from(request.block_count)
        .map_err(|_| "write block count does not fit this process".to_owned())?;
    let payload_bytes = count
        .checked_mul(POSTGRES_PAGE_SIZE)
        .ok_or_else(|| "write payload length overflows".to_owned())?;
    let mut payload = vec![0_u8; payload_bytes];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|error| error.to_string())?;
    let mut state = state.lock().await;
    let objectkv_version = match validate_request(&state, request, true) {
        Ok(objectkv_version) => objectkv_version,
        Err(error) => return write_state_error(stream, &error, &state).await,
    };
    if request.previous_nblocks != state.nblocks || request.resulting_nblocks != state.nblocks {
        return write_state_error(
            stream,
            "existing page write changed or mismatched the authoritative extent",
            &state,
        )
        .await;
    }
    let pages = match decode_pages(&payload) {
        Ok(pages) => pages,
        Err(error) => return write_state_error(stream, &error, &state).await,
    };
    let admission = match admit_postgres_page_write(&PostgresPageWriteBatch {
        first: state.relation.page(request.first_block),
        expected_objectkv_version: objectkv_version,
        postgres_wal_flush_lsn: request.wal_or_page_lsn,
        request_id: request.request_id,
        pages,
    })
    .map_err(|error| error.to_string())
    {
        Ok(admission) => admission,
        Err(error) => return write_state_error(stream, &error, &state).await,
    };
    let plan = match plan_postgres_page_commit(
        &admission,
        PostgresPageCommitOperation::WriteExisting,
        state.nblocks,
        state.nblocks,
        &state.context,
    )
    .map_err(|error| error.to_string())
    {
        Ok(plan) => plan,
        Err(error) => return write_state_error(stream, &error, &state).await,
    };
    let command = plan.command.encode().map_err(|error| error.to_string())?;
    let response = state.client.commit_app_data(&command).await?;
    let receipt =
        verify_postgres_page_commit(&plan, &response).map_err(|error| error.to_string())?;
    let committed_envelope = response
        .cell_transaction
        .as_ref()
        .and_then(|outcome| outcome.envelope.clone())
        .ok_or_else(|| "verified PostgreSQL Cell receipt omitted its envelope".to_owned())?;
    let prior_version = state.objectkv_version;
    let reader = if let Some(mut durable) = state.durable.take() {
        let serving = match durable
            .append_and_open(committed_envelope, state.seed)
            .await
        {
            Ok(serving) => serving,
            Err(error) => {
                state.durable = Some(durable);
                return Err(error);
            }
        };
        state.authenticated_txlog_records = durable.authenticated_tail_records();
        state.durable = Some(durable);
        build_reader_from_serving_state(
            state.context.cell_id,
            state.context.tenant_id,
            serving,
            state.maximum_blocks_per_read,
        )
        .await?
    } else {
        append_mutations(
            &mut state.mutations,
            prior_version,
            receipt.committed_objectkv_version,
            plan.command.mutations.clone(),
        )?;
        build_reader(
            state.seed ^ receipt.committed_objectkv_version,
            receipt.committed_objectkv_version,
            &state.mutations,
            state.maximum_blocks_per_read,
        )
        .await?
    };
    let maximum_page_lsn = state.maximum_page_lsn.max(receipt.maximum_page_lsn);
    let verified_extent = reader
        .read_nblocks(state.relation, receipt.committed_objectkv_version)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "fresh Range Engine omitted committed relation extent".to_owned())?;
    if verified_extent.nblocks != state.nblocks {
        return Err("fresh Range Engine changed committed relation extent".to_owned());
    }
    state.reader = reader;
    state.objectkv_version = receipt.committed_objectkv_version;
    state.maximum_page_lsn = maximum_page_lsn;
    state.committed_write_batches = state.committed_write_batches.saturating_add(1);
    state.fresh_range_engine_views = state.fresh_range_engine_views.saturating_add(1);
    publish_status(&state)?;
    let response = (
        state.nblocks,
        state.objectkv_version,
        state.maximum_page_lsn,
    );
    drop(state);
    write_response(
        stream,
        RESPONSE_OK,
        OPERATION_WRITE_EXISTING,
        0,
        response.0,
        response.1,
        response.2,
    )
    .await
}

fn validate_config(config: &PostgresSmgrWriteServiceConfig) -> Result<(), String> {
    if config.listen_address.is_empty()
        || config.source_file.as_os_str().is_empty()
        || config.status_file.as_os_str().is_empty()
        || config.cell_process_executable.as_os_str().is_empty()
        || config.maximum_page_lsn == 0
        || config.maximum_blocks_per_read == 0
        || config.maximum_blocks_per_read > MAXIMUM_PAGES_PER_REQUEST
        || config
            .durable_root
            .as_ref()
            .is_some_and(|root| root.as_os_str().is_empty())
        || config.publication_authority.is_some() && config.durable_root.is_none()
        || config
            .transaction_authority
            .as_ref()
            .is_some_and(|authority| {
                authority.endpoints.is_empty()
                    || authority.endpoints.iter().any(String::is_empty)
                    || authority.generation == 0
            })
        || config.publication_authority.is_some() && config.transaction_authority.is_none()
    {
        return Err(
            "mutable page service requires paths, a WAL frontier, and a 1..=128 read bound"
                .to_owned(),
        );
    }
    Ok(())
}

fn validate_request(
    state: &MutableBridgeState,
    request: SmgrRequest,
    require_blocks: bool,
) -> Result<u64, String> {
    validate_relation_identity(state, request)?;
    let objectkv_version =
        resolve_request_version(request.expected_objectkv_version, state.objectkv_version)?;
    let block_count = usize::try_from(request.block_count)
        .map_err(|_| "block count does not fit this process".to_owned())?;
    if require_blocks && (block_count == 0 || block_count > state.maximum_blocks_per_read) {
        return Err(format!(
            "storage-manager request block count is outside 1..={} ",
            state.maximum_blocks_per_read
        ));
    }
    request
        .first_block
        .checked_add(request.block_count)
        .ok_or_else(|| "storage-manager block range overflows".to_owned())?;
    Ok(objectkv_version)
}

fn resolve_request_version(expected: u64, current: u64) -> Result<u64, String> {
    if expected == 0 || expected == current {
        return Ok(current);
    }
    Err(format!(
        "storage-manager request expected objectKV version {expected}, current version is {current}"
    ))
}

fn validate_relation_identity(
    state: &MutableBridgeState,
    request: SmgrRequest,
) -> Result<(), String> {
    if request.tablespace_oid != state.relation.tablespace_oid
        || request.database_oid != state.relation.database_oid
        || request.relation_number != state.relation.relation_number
        || request.temporary_backend_id != state.relation.temporary_backend_id
        || request.fork_number != u32::from(state.relation.fork_number)
    {
        return Err("storage-manager request relation identity mismatch".to_owned());
    }
    Ok(())
}

fn read_source_pages(
    config: &PostgresSmgrWriteServiceConfig,
) -> Result<(Vec<PostgresPage>, u64), String> {
    let bytes = fs::read(&config.source_file).map_err(|error| error.to_string())?;
    if bytes.is_empty() || bytes.len() % POSTGRES_PAGE_SIZE != 0 {
        return Err(format!(
            "relation file length {} is not a positive multiple of {}",
            bytes.len(),
            POSTGRES_PAGE_SIZE
        ));
    }
    if bytes.len() / POSTGRES_PAGE_SIZE > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
        return Err("relation has more blocks than its extent encoding supports".to_owned());
    }
    let pages = bytes
        .chunks_exact(POSTGRES_PAGE_SIZE)
        .enumerate()
        .map(|(block, bytes)| {
            let page_lsn = read_page_lsn(bytes)?;
            if page_lsn > config.maximum_page_lsn {
                return Err(format!(
                    "relation block {block} page LSN {page_lsn} exceeds configured frontier {}",
                    config.maximum_page_lsn
                ));
            }
            PostgresPage::new(page_lsn, read_native_u16(bytes, 8)?, bytes.to_vec())
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let observed_maximum_page_lsn = pages
        .iter()
        .map(|page| page.page_lsn)
        .max()
        .ok_or_else(|| "relation contains no PostgreSQL pages".to_owned())?;
    Ok((pages, observed_maximum_page_lsn))
}

fn bootstrap_pages(
    pages: &[PostgresPage],
    starting_version: u64,
    context: &PostgresPageCommitContext,
    relation: PostgresRelationForkIdentity,
    wal_flush_lsn: u64,
) -> Result<BTreeMap<u64, Vec<CellMutation>>, String> {
    let mut mutations = BTreeMap::new();
    for version in 1..starting_version {
        mutations.insert(version, Vec::new());
    }
    let mut bootstrap_mutations = Vec::new();
    let mut final_extent_mutation = None;
    let mut previous_nblocks = 0_u32;
    for (batch_index, chunk) in pages.chunks(BOOTSTRAP_PAGES_PER_BATCH).enumerate() {
        let page_count = u32::try_from(chunk.len()).map_err(|error| error.to_string())?;
        let resulting_nblocks = previous_nblocks
            .checked_add(page_count)
            .ok_or_else(|| "imported relation extent overflows".to_owned())?;
        let request_id = bootstrap_request_id(u64::try_from(batch_index).unwrap_or(u64::MAX));
        let admission = admit_postgres_page_write(&PostgresPageWriteBatch {
            first: relation.page(previous_nblocks),
            expected_objectkv_version: starting_version,
            postgres_wal_flush_lsn: wal_flush_lsn,
            request_id,
            pages: chunk.to_vec(),
        })
        .map_err(|error| error.to_string())?;
        let plan = plan_postgres_page_commit(
            &admission,
            PostgresPageCommitOperation::Extend,
            previous_nblocks,
            resulting_nblocks,
            context,
        )
        .map_err(|error| error.to_string())?;
        let mut batch_mutations = plan.command.mutations;
        let extent_mutation = batch_mutations
            .pop()
            .ok_or_else(|| "bootstrap plan omitted relation extent mutation".to_owned())?;
        bootstrap_mutations.extend(batch_mutations);
        final_extent_mutation = Some(extent_mutation);
        previous_nblocks = resulting_nblocks;
    }
    bootstrap_mutations.push(
        final_extent_mutation
            .ok_or_else(|| "bootstrap relation produced no extent mutation".to_owned())?,
    );
    mutations.insert(starting_version, bootstrap_mutations);
    Ok(mutations)
}

fn append_mutations(
    history: &mut BTreeMap<u64, Vec<CellMutation>>,
    prior_version: u64,
    commit_version: u64,
    mutations: Vec<CellMutation>,
) -> Result<(), String> {
    if commit_version <= prior_version {
        return Err("Cell commit version did not advance".to_owned());
    }
    for version in prior_version.saturating_add(1)..commit_version {
        history.insert(version, Vec::new());
    }
    if history.insert(commit_version, mutations).is_some() {
        return Err("Cell commit version already exists in bridge history".to_owned());
    }
    Ok(())
}

async fn replay_cell_tail<'a>(
    client: &CellTransactionClient,
    base_version: u64,
    envelopes: impl Iterator<Item = &'a [u8]>,
) -> Result<(), String> {
    let mut prior_version = base_version;
    for encoded in envelopes {
        let envelope = CommitEnvelope::decode(encoded).map_err(|error| error.to_string())?;
        let version = envelope.version().sequence();
        if version <= prior_version {
            return Err("durable PostgreSQL Cell replay is not strictly advancing".to_owned());
        }
        let (encoded_client_id, request_id) = envelope.client_identity();
        if encoded_client_id[..8] != [0; 8] {
            return Err("durable PostgreSQL Cell replay has an invalid client identity".to_owned());
        }
        let mut client_id = [0_u8; 8];
        client_id.copy_from_slice(&encoded_client_id[8..]);
        let command = CellTransactionCommand {
            identity: RequestIdentity {
                client_id: u64::from_be_bytes(client_id),
                request_id,
            },
            credential: None,
            cell_id: envelope.cell_id(),
            tenant_id: envelope.tenant_id(),
            generation: envelope.generation(),
            read_version: CellReadVersion {
                generation: envelope.generation(),
                sequence: prior_version,
            },
            read_conflicts: serde_json::from_slice::<Vec<CellKeyRange>>(
                envelope.canonical_read_conflicts(),
            )
            .map_err(|error| error.to_string())?,
            write_conflicts: serde_json::from_slice::<Vec<CellKeyRange>>(
                envelope.canonical_write_conflicts(),
            )
            .map_err(|error| error.to_string())?,
            mutations: serde_json::from_slice::<Vec<CellMutation>>(envelope.canonical_mutations())
                .map_err(|error| error.to_string())?,
            partitioned_resolution: None,
            accepted_resolvers: envelope.required_resolvers().to_vec(),
            durable_log_tags: envelope.required_log_tags().to_vec(),
        };
        let response = client
            .commit_app_data(&command.encode().map_err(|error| error.to_string())?)
            .await?;
        let outcome = response
            .cell_transaction
            .ok_or_else(|| "Cell replay returned no transaction outcome".to_owned())?;
        if outcome.status != CellTransactionStatus::Committed
            || outcome.commit_sequence != Some(version)
            || outcome.envelope.as_deref() != Some(encoded)
        {
            return Err(format!(
                "Cell replay did not reproduce durable PostgreSQL envelope {version}"
            ));
        }
        prior_version = version;
    }
    Ok(())
}

async fn build_reader(
    seed: u64,
    objectkv_version: u64,
    mutations: &BTreeMap<u64, Vec<CellMutation>>,
    maximum_blocks_per_read: usize,
) -> Result<Arc<PostgresPageReader>, String> {
    let serving_state =
        build_fixture_range_serving_state(seed, objectkv_version, objectkv_version, mutations)
            .await?;
    build_reader_from_serving_state(
        RANGE_SERVING_FIXTURE_CELL_ID,
        RANGE_SERVING_FIXTURE_TENANT_ID,
        serving_state,
        maximum_blocks_per_read,
    )
    .await
}

async fn build_reader_from_serving_state(
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    serving_state: Arc<RangeServingState>,
    maximum_blocks_per_read: usize,
) -> Result<Arc<PostgresPageReader>, String> {
    let range_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let range_address = range_listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .to_string();
    let router = Arc::new(KvReadRouter::new(KvReadRouterConfig {
        cell_id,
        max_in_flight: 32,
        max_key_bytes: 256,
        max_scan_rows: maximum_blocks_per_read,
    })?);
    router.assign(
        RangeReadAssignment {
            tenant_id,
            range_id: RANGE_ID,
            routing_epoch: ROUTING_EPOCH,
            start: vec![0],
            end: vec![0xff],
        },
        serving_state,
    )?;
    let protocol = range_protocol(maximum_blocks_per_read)?;
    tokio::spawn(serve_range_read_listener(range_listener, protocol, router));
    let route = ClientRangeRoute {
        endpoint: range_address,
        range_id: RANGE_ID,
        routing_epoch: ROUTING_EPOCH,
        start: vec![0],
        end: vec![0xff],
    };
    let snapshot = ClientRangeMapSnapshot {
        cell_id,
        tenant_id,
        map_version: MAP_VERSION,
        routes: vec![route],
    };
    let source = Arc::new(StaticRangeMapSource {
        cell_id,
        tenant_id,
        snapshot: snapshot.clone(),
    });
    let client = Arc::new(
        KvReadClient::new(
            cell_id,
            tenant_id,
            KvReadClientConfig {
                protocol,
                max_route_refreshes: 1,
            },
            snapshot,
            source,
        )
        .map_err(|error| error.to_string())?,
    );
    Ok(Arc::new(PostgresPageReader::new(client)))
}

fn parse_request(header: &[u8; REQUEST_HEADER_BYTES]) -> Result<SmgrRequest, String> {
    if &header[..8] != PROTOCOL_MAGIC {
        return Err("invalid mutable storage-manager request magic".to_owned());
    }
    let mut request_id = [0_u8; 16];
    request_id.copy_from_slice(&header[64..80]);
    Ok(SmgrRequest {
        operation: read_be_u32(header, 8)?,
        tablespace_oid: read_be_u32(header, 12)?,
        database_oid: read_be_u32(header, 16)?,
        relation_number: read_be_u32(header, 20)?,
        temporary_backend_id: read_be_u32(header, 24)?,
        fork_number: read_be_u32(header, 28)?,
        first_block: read_be_u32(header, 32)?,
        block_count: read_be_u32(header, 36)?,
        expected_objectkv_version: read_be_u64(header, 40)?,
        wal_or_page_lsn: read_be_u64(header, 48)?,
        previous_nblocks: read_be_u32(header, 56)?,
        resulting_nblocks: read_be_u32(header, 60)?,
        request_id,
    })
}

async fn write_pages(
    stream: &mut TcpStream,
    pages: &[PostgresPage],
    snapshot: PostgresPageReadSnapshot,
) -> Result<(), String> {
    let page_count = u32::try_from(pages.len()).map_err(|error| error.to_string())?;
    let payload_bytes = pages
        .len()
        .checked_mul(POSTGRES_PAGE_SIZE)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or_else(|| "page response length overflows protocol".to_owned())?;
    write_response(
        stream,
        RESPONSE_OK,
        OPERATION_READ,
        payload_bytes,
        page_count,
        snapshot.objectkv_version,
        snapshot.maximum_page_lsn,
    )
    .await?;
    for page in pages {
        stream
            .write_all(&page.bytes)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn write_error(
    stream: &mut TcpStream,
    error: &str,
    objectkv_version: u64,
    maximum_page_lsn: u64,
) -> Result<(), String> {
    let error = error.as_bytes();
    let bounded = &error[..error.len().min(MAXIMUM_ERROR_BYTES)];
    let payload_bytes = u32::try_from(bounded.len()).map_err(|error| error.to_string())?;
    write_response(
        stream,
        RESPONSE_ERROR,
        0,
        payload_bytes,
        0,
        objectkv_version,
        maximum_page_lsn,
    )
    .await?;
    stream
        .write_all(bounded)
        .await
        .map_err(|error| error.to_string())
}

async fn write_state_error(
    stream: &mut TcpStream,
    error: &str,
    state: &MutableBridgeState,
) -> Result<(), String> {
    write_error(
        stream,
        error,
        state.objectkv_version,
        state.maximum_page_lsn,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn write_response(
    stream: &mut TcpStream,
    status: u32,
    operation: u32,
    payload_bytes: u32,
    value: u32,
    objectkv_version: u64,
    maximum_page_lsn: u64,
) -> Result<(), String> {
    let mut header = [0_u8; RESPONSE_HEADER_BYTES];
    header[..8].copy_from_slice(RESPONSE_MAGIC);
    header[8..12].copy_from_slice(&status.to_be_bytes());
    header[12..16].copy_from_slice(&operation.to_be_bytes());
    header[16..20].copy_from_slice(&payload_bytes.to_be_bytes());
    header[20..24].copy_from_slice(&value.to_be_bytes());
    header[24..32].copy_from_slice(&objectkv_version.to_be_bytes());
    header[32..40].copy_from_slice(&maximum_page_lsn.to_be_bytes());
    stream
        .write_all(&header)
        .await
        .map_err(|error| error.to_string())
}

fn decode_pages(payload: &[u8]) -> Result<Vec<PostgresPage>, String> {
    if payload.is_empty() || payload.len() % POSTGRES_PAGE_SIZE != 0 {
        return Err("write payload is not a positive number of PostgreSQL pages".to_owned());
    }
    payload
        .chunks_exact(POSTGRES_PAGE_SIZE)
        .map(|bytes| {
            PostgresPage::new(
                read_page_lsn(bytes)?,
                read_native_u16(bytes, 8)?,
                bytes.to_vec(),
            )
            .map_err(|error| error.to_string())
        })
        .collect()
}

fn relation_identity(config: &PostgresSmgrWriteServiceConfig) -> PostgresRelationForkIdentity {
    PostgresRelationForkIdentity {
        cluster_id: config.cluster_id,
        tablespace_oid: config.tablespace_oid,
        database_oid: config.database_oid,
        relation_number: config.relation_number,
        temporary_backend_id: config.temporary_backend_id,
        fork_number: config.fork_number,
    }
}

fn status(state: &MutableBridgeState) -> PostgresSmgrWriteServiceStatus {
    let object_delta_segments = state
        .durable
        .as_ref()
        .map_or(0, DurablePostgresRange::object_delta_segments);
    let object_delta_bytes = state
        .durable
        .as_ref()
        .map_or(0, DurablePostgresRange::object_delta_bytes);
    let objectification_input_bytes = state
        .durable
        .as_ref()
        .map_or(0, DurablePostgresRange::objectification_input_bytes);
    PostgresSmgrWriteServiceStatus {
        listen_address: state.listen_address.clone(),
        source_file: state.source_file.clone(),
        relation_number: state.relation.relation_number,
        objectkv_version: state.objectkv_version,
        maximum_page_lsn: state.maximum_page_lsn,
        nblocks: state.nblocks,
        committed_write_batches: state.committed_write_batches,
        fresh_range_engine_views: state.fresh_range_engine_views,
        base_objectkv_version: state.base_objectkv_version,
        authenticated_txlog_records: state.authenticated_txlog_records,
        recovered_durable_state: state.recovered_durable_state,
        stable_objectkv_version: state.stable_objectkv_version,
        stable_object_frontier: state.stable_object_frontier,
        stable_maximum_page_lsn: state.stable_maximum_page_lsn,
        stable_postgres_wal_flush_lsn: state.stable_postgres_wal_flush_lsn,
        stable_sync_last_duration_millis: state.stable_sync_last_duration_millis,
        stable_authority_term: state.stable_authority_term,
        stable_authority_index: state.stable_authority_index,
        stable_manifest_sha256: state.stable_manifest_sha256.clone(),
        txlog_popped_through: state.txlog_popped_through,
        txlog_pop_certificates: state.txlog_pop_certificates,
        txlog_pop_error: state.txlog_pop_error.clone(),
        objectification_ready_version: state.objectification_ready_version,
        objectification_lag_versions: state
            .objectkv_version
            .saturating_sub(state.objectification_ready_version),
        objectification_last_duration_millis: state.objectification_last_duration_millis,
        objectification_error: state.objectification_error.clone(),
        object_delta_segments,
        object_delta_bytes,
        objectification_input_bytes,
        object_delta_layers: object_delta_segments,
        object_compaction_debt_bytes: object_delta_bytes,
    }
}

fn publish_status(state: &MutableBridgeState) -> Result<(), String> {
    let rendered = serde_json::to_vec(&status(state)).map_err(|error| error.to_string())?;
    let temporary = state.status_file.with_extension("tmp");
    fs::write(&temporary, rendered).map_err(|error| error.to_string())?;
    fs::rename(temporary, &state.status_file).map_err(|error| error.to_string())
}

fn bootstrap_request_id(batch: u64) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"objectkv/postgres/smgr-bootstrap/v1");
    digest.update(batch.to_be_bytes());
    let digest = digest.finalize();
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(&digest[..16]);
    identity
}

fn read_page_lsn(page: &[u8]) -> Result<u64, String> {
    let high = u64::from(read_native_u32(page, 0)?);
    let low = u64::from(read_native_u32(page, 4)?);
    Ok((high << 32) | low)
}

fn read_native_u16(bytes: &[u8], offset: usize) -> Result<u16, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(2))
        .ok_or_else(|| "truncated PostgreSQL page header".to_owned())?;
    Ok(u16::from_ne_bytes([bytes[0], bytes[1]]))
}

fn read_native_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| "truncated PostgreSQL page header".to_owned())?;
    Ok(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_be_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| "truncated mutable storage-manager request".to_owned())?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_be_u64(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let bytes = bytes
        .get(offset..offset.saturating_add(8))
        .ok_or_else(|| "truncated mutable storage-manager request".to_owned())?;
    Ok(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn range_protocol(maximum_blocks_per_read: usize) -> Result<RangeReadProtocolConfig, String> {
    let max_frame_bytes = maximum_blocks_per_read
        .checked_mul(POSTGRES_PAGE_SIZE + 256)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| "range-read frame bound overflows".to_owned())?;
    Ok(RangeReadProtocolConfig {
        max_frame_bytes,
        request_timeout_millis: 5_000,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_versioned_write_request() {
        let mut request = [0_u8; REQUEST_HEADER_BYTES];
        request[..8].copy_from_slice(PROTOCOL_MAGIC);
        request[8..12].copy_from_slice(&OPERATION_WRITE_EXISTING.to_be_bytes());
        request[12..16].copy_from_slice(&1663_u32.to_be_bytes());
        request[16..20].copy_from_slice(&5_u32.to_be_bytes());
        request[20..24].copy_from_slice(&16_402_u32.to_be_bytes());
        request[24..28].copy_from_slice(&0_u32.to_be_bytes());
        request[28..32].copy_from_slice(&0_u32.to_be_bytes());
        request[32..36].copy_from_slice(&7_u32.to_be_bytes());
        request[36..40].copy_from_slice(&2_u32.to_be_bytes());
        request[40..48].copy_from_slice(&41_u64.to_be_bytes());
        request[48..56].copy_from_slice(&900_u64.to_be_bytes());
        request[56..60].copy_from_slice(&148_u32.to_be_bytes());
        request[60..64].copy_from_slice(&148_u32.to_be_bytes());
        request[64..80].copy_from_slice(&[0x81; 16]);
        assert_eq!(
            parse_request(&request).unwrap(),
            SmgrRequest {
                operation: OPERATION_WRITE_EXISTING,
                tablespace_oid: 1663,
                database_oid: 5,
                relation_number: 16_402,
                temporary_backend_id: 0,
                fork_number: 0,
                first_block: 7,
                block_count: 2,
                expected_objectkv_version: 41,
                wal_or_page_lsn: 900,
                previous_nblocks: 148,
                resulting_nblocks: 148,
                request_id: [0x81; 16],
            }
        );
    }

    #[test]
    fn resolves_current_or_exact_versions_and_refuses_stale_versions() {
        assert_eq!(resolve_request_version(0, 41).unwrap(), 41);
        assert_eq!(resolve_request_version(41, 41).unwrap(), 41);
        assert_eq!(
            resolve_request_version(40, 41).unwrap_err(),
            "storage-manager request expected objectKV version 40, current version is 41"
        );
    }

    #[test]
    fn decodes_native_page_lsn_for_write_admission() {
        let mut page = vec![0_u8; POSTGRES_PAGE_SIZE];
        page[..4].copy_from_slice(&0x1234_5678_u32.to_ne_bytes());
        page[4..8].copy_from_slice(&0x90ab_cdef_u32.to_ne_bytes());
        assert_eq!(
            decode_pages(&page).unwrap()[0].page_lsn,
            0x1234_5678_90ab_cdef
        );
    }

    #[test]
    fn appends_only_a_strictly_advancing_history() {
        let mut history = BTreeMap::from([(1, Vec::new())]);
        append_mutations(&mut history, 1, 4, vec![]).unwrap();
        assert_eq!(
            history.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert!(append_mutations(&mut history, 4, 4, vec![]).is_err());
    }

    #[test]
    fn objectifier_keeps_one_runner_and_only_the_latest_pending_version() {
        assert_eq!(
            objectification_schedule(false, None, None, None, 9),
            ObjectificationSchedule::StartRunner
        );
        assert_eq!(
            objectification_schedule(true, Some(9), None, None, 10),
            ObjectificationSchedule::Queue
        );
        assert_eq!(
            objectification_schedule(true, Some(9), Some(10), None, 11),
            ObjectificationSchedule::ReplacePending
        );
        assert_eq!(
            objectification_schedule(true, Some(9), Some(11), None, 10),
            ObjectificationSchedule::Ignore
        );
        assert_eq!(
            objectification_schedule(false, None, None, Some(11), 11),
            ObjectificationSchedule::Ignore
        );
        assert_eq!(
            objectification_schedule(false, None, None, Some(11), 12),
            ObjectificationSchedule::StartRunner
        );
    }
}
