//! Independent-process `PostgreSQL` page-read contract.

use crate::{
    PostgresPage, PostgresPageBridgeError, PostgresPageIdentity, PostgresPageReadSnapshot,
    PostgresPageReader, POSTGRES_PAGE_SIZE,
};
use async_trait::async_trait;
use okv_consensus::CellMutation;
use okv_object::{
    build_fixture_range_serving_state, request_range_read, serve_range_read_listener,
    ClientRangeMapSnapshot, ClientRangeRoute, KvReadClient, KvReadClientConfig, KvReadRouter,
    KvReadRouterConfig, RangeEngineId, RangeMapSource, RangeReadAssignment,
    RangeReadProtocolConfig, RoutedRangeReadRequest, RANGE_SERVING_FIXTURE_CELL_ID,
    RANGE_SERVING_FIXTURE_TENANT_ID,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::net::TcpListener as StdTcpListener;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

const BASE_VERSION: u64 = 1;
const TARGET_VERSION: u64 = 2;
const MAXIMUM_PAGE_LSN: u64 = 800;
const LEFT_RANGE: RangeEngineId = RangeEngineId(91);
const RIGHT_RANGE: RangeEngineId = RangeEngineId(92);
const LEFT_EPOCH: u64 = 11;
const RIGHT_EPOCH: u64 = 12;
const FIRST_BLOCK: u32 = 7;
const BLOCK_COUNT: usize = 3;

/// Unsafe subject selected by the `PostgreSQL` page-read process suite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPageReadProcessMode {
    Correct,
    MissingPage,
    CorruptPayload,
    ChangeObjectKvVersion,
    PageLsnAhead,
}

impl PostgresPageReadProcessMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::MissingPage => "missing_page",
            Self::CorruptPayload => "corrupt_payload",
            Self::ChangeObjectKvVersion => "change_objectkv_version",
            Self::PageLsnAhead => "page_lsn_ahead",
        }
    }
}

/// Child-process configuration for one page-serving KV Runtime.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresPageReadProcessConfig {
    pub seed: u64,
    pub mode: PostgresPageReadProcessMode,
    pub listen_address: String,
}

/// Stable semantic receipt from one page-read client and worker history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresPageReadProcessReceipt {
    pub seed: u64,
    pub mode: PostgresPageReadProcessMode,
    pub worker_process_starts: u64,
    pub worker_process_kills: u64,
    pub route_refreshes: u64,
    pub requested_objectkv_version: u64,
    pub observed_objectkv_version: u64,
    pub maximum_page_lsn: u64,
    pub expected_pages: u64,
    pub observed_pages: u64,
    pub vector_duration_nanos: u64,
    pub point_duration_nanos: u64,
    pub vector_error: Option<String>,
    pub point_error: Option<String>,
    pub checks: BTreeMap<String, bool>,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

struct WorkerChild(Child);

impl Deref for WorkerChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WorkerChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for WorkerChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct ProcessRangeMapSource {
    snapshot: ClientRangeMapSnapshot,
    refreshes: AtomicU64,
}

#[async_trait]
impl RangeMapSource for ProcessRangeMapSource {
    async fn snapshot(
        &self,
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
    ) -> Result<ClientRangeMapSnapshot, String> {
        if cell_id != RANGE_SERVING_FIXTURE_CELL_ID || tenant_id != RANGE_SERVING_FIXTURE_TENANT_ID
        {
            return Err("PostgreSQL page fixture received the wrong session identity".to_owned());
        }
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot.clone())
    }
}

/// Run encoded page reads through a fresh independent KV Runtime.
///
/// # Errors
///
/// Returns an error when the process, routing, or fixture cannot execute.
pub fn run_postgres_page_read_process_contract(
    seed: u64,
    mode: PostgresPageReadProcessMode,
    executable: &Path,
) -> Result<PostgresPageReadProcessReceipt, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_controller(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_controller(
    seed: u64,
    mode: PostgresPageReadProcessMode,
    executable: &Path,
) -> Result<PostgresPageReadProcessReceipt, String> {
    let listen_address = reserve_address()?;
    let config = PostgresPageReadProcessConfig {
        seed,
        mode,
        listen_address: listen_address.clone(),
    };
    let mut child = spawn_worker(executable, &config)?;
    let protocol = protocol_config();
    let first_identity = page_identity(FIRST_BLOCK);
    let split = page_identity(FIRST_BLOCK + 2).encode_key();
    let readiness = RoutedRangeReadRequest::point(
        RANGE_SERVING_FIXTURE_CELL_ID,
        RANGE_SERVING_FIXTURE_TENANT_ID,
        LEFT_RANGE,
        LEFT_EPOCH,
        TARGET_VERSION,
        first_identity.encode_key(),
    );
    wait_until_ready(&listen_address, &readiness, protocol, &mut child).await?;

    let old_map = ClientRangeMapSnapshot {
        cell_id: RANGE_SERVING_FIXTURE_CELL_ID,
        tenant_id: RANGE_SERVING_FIXTURE_TENANT_ID,
        map_version: 1,
        routes: vec![client_route(
            &listen_address,
            RangeEngineId(90),
            10,
            vec![0],
            vec![0xff],
        )],
    };
    let source = Arc::new(ProcessRangeMapSource {
        snapshot: ClientRangeMapSnapshot {
            cell_id: RANGE_SERVING_FIXTURE_CELL_ID,
            tenant_id: RANGE_SERVING_FIXTURE_TENANT_ID,
            map_version: 2,
            routes: vec![
                client_route(
                    &listen_address,
                    LEFT_RANGE,
                    LEFT_EPOCH,
                    vec![0],
                    split.clone(),
                ),
                client_route(&listen_address, RIGHT_RANGE, RIGHT_EPOCH, split, vec![0xff]),
            ],
        },
        refreshes: AtomicU64::new(0),
    });
    let client = Arc::new(
        KvReadClient::new(
            RANGE_SERVING_FIXTURE_CELL_ID,
            RANGE_SERVING_FIXTURE_TENANT_ID,
            KvReadClientConfig {
                protocol,
                max_route_refreshes: 2,
            },
            old_map,
            source.clone(),
        )
        .map_err(|error| error.to_string())?,
    );
    let reader = PostgresPageReader::new(client);
    let observed_objectkv_version = if mode == PostgresPageReadProcessMode::ChangeObjectKvVersion {
        BASE_VERSION
    } else {
        TARGET_VERSION
    };
    let snapshot = PostgresPageReadSnapshot {
        objectkv_version: observed_objectkv_version,
        maximum_page_lsn: MAXIMUM_PAGE_LSN,
    };
    let vector_started = Instant::now();
    let vector = reader
        .read_pages(first_identity, BLOCK_COUNT, snapshot)
        .await;
    let vector_duration_nanos =
        u64::try_from(vector_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let point_started = Instant::now();
    let point = reader
        .read_page(page_identity(FIRST_BLOCK + 1), snapshot)
        .await;
    let point_duration_nanos =
        u64::try_from(point_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let expected = expected_pages(seed)?;
    let observed_pages = vector.as_ref().map_or(0, Vec::len);
    let vector_exact = vector.as_ref().is_ok_and(|pages| pages == &expected);
    let point_exact = point
        .as_ref()
        .is_ok_and(|page| page.as_ref() == expected.get(1));
    let second_range_included = vector
        .as_ref()
        .is_ok_and(|pages| pages.get(2) == expected.get(2));
    let vector_error = vector.as_ref().err().map(ToString::to_string);
    let point_error = point.as_ref().err().map(ToString::to_string);
    let subject_behavior_exact = subject_behavior_exact(mode, &vector, &point);

    child.kill().map_err(|error| error.to_string())?;
    child.wait().map_err(|error| error.to_string())?;
    let route_refreshes = source.refreshes.load(Ordering::SeqCst);
    let checks = BTreeMap::from([
        (
            "objectkv_version_preserved".to_owned(),
            observed_objectkv_version == TARGET_VERSION,
        ),
        ("point_page_exact".to_owned(), point_exact),
        ("route_refreshed_once".to_owned(), route_refreshes == 1),
        ("second_range_included".to_owned(), second_range_included),
        ("subject_behavior_exact".to_owned(), subject_behavior_exact),
        ("vector_pages_exact".to_owned(), vector_exact),
        ("worker_killed".to_owned(), true),
    ]);
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let semantic = (
        seed,
        mode,
        route_refreshes,
        TARGET_VERSION,
        observed_objectkv_version,
        MAXIMUM_PAGE_LSN,
        expected.len(),
        observed_pages,
        &vector_error,
        &point_error,
        &checks,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(PostgresPageReadProcessReceipt {
        seed,
        mode,
        worker_process_starts: 1,
        worker_process_kills: 1,
        route_refreshes,
        requested_objectkv_version: TARGET_VERSION,
        observed_objectkv_version,
        maximum_page_lsn: MAXIMUM_PAGE_LSN,
        expected_pages: u64::try_from(expected.len()).unwrap_or(u64::MAX),
        observed_pages: u64::try_from(observed_pages).unwrap_or(u64::MAX),
        vector_duration_nanos,
        point_duration_nanos,
        vector_error,
        point_error,
        checks,
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

/// Start one real page-serving worker process.
///
/// # Errors
///
/// Returns an error when encoded fixture pages, the authority-bound view,
/// assignments, or the listener cannot be created.
pub async fn run_postgres_page_read_process_worker(
    config: PostgresPageReadProcessConfig,
) -> Result<(), String> {
    let mutations = fixture_mutations(config.seed, config.mode)?;
    let state =
        build_fixture_range_serving_state(config.seed, BASE_VERSION, TARGET_VERSION, &mutations)
            .await?;
    let router = Arc::new(KvReadRouter::new(KvReadRouterConfig {
        cell_id: RANGE_SERVING_FIXTURE_CELL_ID,
        max_in_flight: 16,
        max_key_bytes: 256,
        max_scan_rows: 16,
    })?);
    let split = page_identity(FIRST_BLOCK + 2).encode_key();
    router.assign(
        RangeReadAssignment {
            tenant_id: RANGE_SERVING_FIXTURE_TENANT_ID,
            range_id: LEFT_RANGE,
            routing_epoch: LEFT_EPOCH,
            start: vec![0],
            end: split.clone(),
        },
        Arc::clone(&state),
    )?;
    router.assign(
        RangeReadAssignment {
            tenant_id: RANGE_SERVING_FIXTURE_TENANT_ID,
            range_id: RIGHT_RANGE,
            routing_epoch: RIGHT_EPOCH,
            start: split,
            end: vec![0xff],
        },
        state,
    )?;
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(|error| error.to_string())?;
    serve_range_read_listener(listener, protocol_config(), router).await
}

fn fixture_mutations(
    seed: u64,
    mode: PostgresPageReadProcessMode,
) -> Result<BTreeMap<u64, Vec<CellMutation>>, String> {
    let mut base = Vec::new();
    for block_number in FIRST_BLOCK..FIRST_BLOCK + u32::try_from(BLOCK_COUNT).unwrap_or(0) {
        if mode == PostgresPageReadProcessMode::MissingPage && block_number == FIRST_BLOCK + 1 {
            continue;
        }
        let page_lsn = if mode == PostgresPageReadProcessMode::PageLsnAhead
            && block_number == FIRST_BLOCK + 2
        {
            MAXIMUM_PAGE_LSN + 100
        } else {
            700 + u64::from(block_number - FIRST_BLOCK)
        };
        base.push(CellMutation::Set {
            key: page_identity(block_number).encode_key(),
            value: page(seed, block_number, 1, page_lsn)?.encode(),
        });
    }
    let mut update = Vec::new();
    if mode != PostgresPageReadProcessMode::MissingPage {
        let mut value = page(seed, FIRST_BLOCK + 1, 2, MAXIMUM_PAGE_LSN)?.encode();
        if mode == PostgresPageReadProcessMode::CorruptPayload {
            let last = value
                .len()
                .checked_sub(1)
                .ok_or_else(|| "encoded PostgreSQL page is empty".to_owned())?;
            value[last] ^= 0xff;
        }
        update.push(CellMutation::Set {
            key: page_identity(FIRST_BLOCK + 1).encode_key(),
            value,
        });
    }
    Ok(BTreeMap::from([
        (BASE_VERSION, base),
        (TARGET_VERSION, update),
    ]))
}

fn expected_pages(seed: u64) -> Result<Vec<PostgresPage>, String> {
    Ok(vec![
        page(seed, FIRST_BLOCK, 1, 700)?,
        page(seed, FIRST_BLOCK + 1, 2, MAXIMUM_PAGE_LSN)?,
        page(seed, FIRST_BLOCK + 2, 1, 702)?,
    ])
}

fn page(seed: u64, block_number: u32, revision: u8, page_lsn: u64) -> Result<PostgresPage, String> {
    let byte = seed.to_le_bytes()[0]
        .wrapping_add(block_number.to_le_bytes()[0])
        .wrapping_add(revision);
    PostgresPage::new(
        page_lsn,
        u16::try_from(block_number)
            .unwrap_or(u16::MAX)
            .wrapping_mul(31)
            .wrapping_add(u16::from(revision)),
        vec![byte; POSTGRES_PAGE_SIZE],
    )
    .map_err(|error| error.to_string())
}

fn page_identity(block_number: u32) -> PostgresPageIdentity {
    PostgresPageIdentity {
        cluster_id: [0x61; 16],
        tablespace_oid: 1663,
        database_oid: 5,
        relation_number: 16_384,
        temporary_backend_id: 0,
        fork_number: 0,
        block_number,
    }
}

fn subject_behavior_exact(
    mode: PostgresPageReadProcessMode,
    vector: &Result<Vec<PostgresPage>, PostgresPageBridgeError>,
    point: &Result<Option<PostgresPage>, PostgresPageBridgeError>,
) -> bool {
    match mode {
        PostgresPageReadProcessMode::Correct
        | PostgresPageReadProcessMode::ChangeObjectKvVersion => vector.is_ok() && point.is_ok(),
        PostgresPageReadProcessMode::MissingPage => matches!(
            vector,
            Err(PostgresPageBridgeError::MissingPage { block_number })
                if *block_number == FIRST_BLOCK + 1
        ),
        PostgresPageReadProcessMode::CorruptPayload => {
            matches!(
                vector,
                Err(PostgresPageBridgeError::PagePayloadChecksumMismatch)
            ) && matches!(
                point,
                Err(PostgresPageBridgeError::PagePayloadChecksumMismatch)
            )
        }
        PostgresPageReadProcessMode::PageLsnAhead => matches!(
            vector,
            Err(PostgresPageBridgeError::PageLsnBeyondSnapshot { page_lsn, maximum })
                if *page_lsn == MAXIMUM_PAGE_LSN + 100 && *maximum == MAXIMUM_PAGE_LSN
        ),
    }
}

fn client_route(
    endpoint: &str,
    range_id: RangeEngineId,
    routing_epoch: u64,
    start: Vec<u8>,
    end: Vec<u8>,
) -> ClientRangeRoute {
    ClientRangeRoute {
        endpoint: endpoint.to_owned(),
        range_id,
        routing_epoch,
        start,
        end,
    }
}

fn spawn_worker(
    executable: &Path,
    config: &PostgresPageReadProcessConfig,
) -> Result<WorkerChild, String> {
    let config = serde_json::to_string(config).map_err(|error| error.to_string())?;
    Command::new(executable)
        .arg("postgres-page-read-node")
        .arg("--config-json")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map(WorkerChild)
        .map_err(|error| error.to_string())
}

async fn wait_until_ready(
    endpoint: &str,
    request: &RoutedRangeReadRequest,
    protocol: RangeReadProtocolConfig,
    child: &mut Child,
) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..500 {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            return Err(format!(
                "PostgreSQL page worker exited before readiness: {status}"
            ));
        }
        match request_range_read(endpoint, request, protocol).await {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(error)) => last = format!("readiness refused: {error:?}"),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(format!(
        "PostgreSQL page worker did not become ready: {last}"
    ))
}

fn reserve_address() -> Result<String, String> {
    let listener = StdTcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?;
    let address = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .to_string();
    drop(listener);
    Ok(address)
}

const fn protocol_config() -> RangeReadProtocolConfig {
    RangeReadProtocolConfig {
        max_frame_bytes: 128 * 1024,
        request_timeout_millis: 2_000,
    }
}
