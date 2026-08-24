//! Independent-process contract for routed KV Runtime reads.

use crate::range_serving_concurrency::{
    build_final_range_serving_state, final_range_serving_rows, range_serving_rows_at,
};
use crate::{
    request_range_read, serve_range_read_listener, ClientRangeMapSnapshot, ClientRangeRoute,
    KvReadClient, KvReadClientConfig, KvReadRouter, KvReadRouterConfig, RangeEngineId,
    RangeMapSource, RangeReadAssignment, RangeReadProtocolConfig, RoutedRangeReadError,
    RoutedRangeReadReply, RoutedRangeReadRequest,
};
use async_trait::async_trait;
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

const CELL_ID: [u8; 16] = [0x41; 16];
const TENANT_ID: [u8; 16] = [0x52; 16];
const LEFT_RANGE: RangeEngineId = RangeEngineId(71);
const RIGHT_RANGE: RangeEngineId = RangeEngineId(72);
const LEFT_EPOCH: u64 = 9;
const RIGHT_EPOCH: u64 = 10;
const TARGET_VERSION: u64 = 9;
const POINT_READS: u64 = 64;
const SCAN_READS: u64 = 16;

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

/// Unsafe subject for the independent-process routed-read gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeReadProcessMode {
    Correct,
    AcceptStaleRoute,
    AcceptCrossingScan,
    AcceptWrongValue,
    SkipWorkerKill,
    RouteRefreshFixture,
}

impl RangeReadProcessMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AcceptStaleRoute => "accept_stale_route",
            Self::AcceptCrossingScan => "accept_crossing_scan",
            Self::AcceptWrongValue => "accept_wrong_value",
            Self::SkipWorkerKill => "skip_worker_kill",
            Self::RouteRefreshFixture => "route_refresh_fixture",
        }
    }
}

/// Unsafe subject for the fixed-snapshot route-refresh process gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeRouteRefreshMode {
    Correct,
    KeepStaleMap,
    ChangeSnapshotVersion,
    SkipSecondRange,
}

impl RangeRouteRefreshMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::KeepStaleMap => "keep_stale_map",
            Self::ChangeSnapshotVersion => "change_snapshot_version",
            Self::SkipSecondRange => "skip_second_range",
        }
    }
}

/// Child-process configuration for one routed-read server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeReadProcessConfig {
    pub seed: u64,
    pub mode: RangeReadProcessMode,
    pub listen_address: String,
}

/// Stable receipt from one server lifecycle and client history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeReadProcessReceipt {
    pub seed: u64,
    pub mode: RangeReadProcessMode,
    pub worker_process_starts: u64,
    pub worker_process_kills: u64,
    pub point_reads: u64,
    pub scan_reads: u64,
    pub point_latency_nanos: Vec<u64>,
    pub scan_latency_nanos: Vec<u64>,
    pub stale_route_refusals: u64,
    pub crossing_scan_refusals: u64,
    pub unavailable_snapshot_refusals: u64,
    pub killed_worker_refusals: u64,
    pub wrong_values: u64,
    pub checks: BTreeMap<String, bool>,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Stable semantic receipt from client refresh and multi-range fan-out.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeRouteRefreshReceipt {
    pub seed: u64,
    pub mode: RangeRouteRefreshMode,
    pub worker_process_starts: u64,
    pub worker_process_kills: u64,
    pub route_refreshes: u64,
    pub requested_read_version: u64,
    pub observed_read_version: u64,
    pub expected_rows: u64,
    pub observed_rows: u64,
    pub checks: BTreeMap<String, bool>,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Run one routed-read history through a fresh server process.
///
/// # Errors
///
/// Returns an error when process, transport, or fixture setup cannot execute.
pub fn run_range_read_process_contract(
    seed: u64,
    mode: RangeReadProcessMode,
    executable: &Path,
) -> Result<RangeReadProcessReceipt, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_controller(seed, mode, executable))
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
        if cell_id != CELL_ID || tenant_id != TENANT_ID {
            return Err("route-refresh fixture received the wrong session identity".to_owned());
        }
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot.clone())
    }
}

/// Run client route refresh and multi-range fan-out through a fresh KV Runtime.
///
/// # Errors
///
/// Returns an error when the worker process or route-refresh fixture cannot run.
pub fn run_range_route_refresh_process_contract(
    seed: u64,
    mode: RangeRouteRefreshMode,
    executable: &Path,
) -> Result<RangeRouteRefreshReceipt, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_route_refresh_controller(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_route_refresh_controller(
    seed: u64,
    mode: RangeRouteRefreshMode,
    executable: &Path,
) -> Result<RangeRouteRefreshReceipt, String> {
    const FIXED_READ_VERSION: u64 = 8;
    const SPLIT: &[u8] = b"k5";

    let listen_address = reserve_address()?;
    let config = RangeReadProcessConfig {
        seed,
        mode: RangeReadProcessMode::RouteRefreshFixture,
        listen_address: listen_address.clone(),
    };
    let mut child = spawn_worker(executable, &config)?;
    let protocol = protocol_config();
    let readiness = RoutedRangeReadRequest::point(
        CELL_ID,
        TENANT_ID,
        LEFT_RANGE,
        LEFT_EPOCH,
        FIXED_READ_VERSION,
        b"a".to_vec(),
    );
    wait_until_ready(&listen_address, &readiness, protocol, &mut child).await?;

    let old_map = ClientRangeMapSnapshot {
        cell_id: CELL_ID,
        tenant_id: TENANT_ID,
        map_version: 1,
        routes: vec![client_route(
            &listen_address,
            RangeEngineId(70),
            8,
            b"a",
            b"z",
        )],
    };
    let current_routes = if mode == RangeRouteRefreshMode::SkipSecondRange {
        vec![client_route(
            &listen_address,
            LEFT_RANGE,
            LEFT_EPOCH,
            b"a",
            SPLIT,
        )]
    } else {
        vec![
            client_route(&listen_address, LEFT_RANGE, LEFT_EPOCH, b"a", SPLIT),
            client_route(&listen_address, RIGHT_RANGE, RIGHT_EPOCH, SPLIT, b"z"),
        ]
    };
    let source_snapshot = if mode == RangeRouteRefreshMode::KeepStaleMap {
        old_map.clone()
    } else {
        ClientRangeMapSnapshot {
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            map_version: 2,
            routes: current_routes,
        }
    };
    let source = Arc::new(ProcessRangeMapSource {
        snapshot: source_snapshot,
        refreshes: AtomicU64::new(0),
    });
    let client = KvReadClient::new(
        CELL_ID,
        TENANT_ID,
        KvReadClientConfig {
            protocol,
            max_route_refreshes: 2,
        },
        old_map,
        source.clone(),
    )
    .map_err(|error| error.to_string())?;
    let observed_read_version = if mode == RangeRouteRefreshMode::ChangeSnapshotVersion {
        FIXED_READ_VERSION + 1
    } else {
        FIXED_READ_VERSION
    };
    let scan = client.scan_at(b"a", b"z", observed_read_version, 100).await;
    let point = client.point_at(b"a", observed_read_version).await;
    let expected = range_serving_rows_at(FIXED_READ_VERSION);
    let observed_rows = scan.as_ref().map_or(0, Vec::len);
    let scan_exact = scan.as_ref().is_ok_and(|rows| rows == &expected);
    let point_exact = point
        .as_ref()
        .is_ok_and(|value| value.as_deref() == Some(b"a8"));
    let second_range_present = scan.as_ref().is_ok_and(|rows| {
        rows.iter()
            .any(|(key, value)| key == b"k8" && value == b"v8")
    });

    child.kill().map_err(|error| error.to_string())?;
    child.wait().map_err(|error| error.to_string())?;
    let route_refreshes = source.refreshes.load(Ordering::SeqCst);
    let checks = BTreeMap::from([
        (
            "fixed_snapshot_preserved".to_owned(),
            observed_read_version == FIXED_READ_VERSION,
        ),
        ("point_exact_after_refresh".to_owned(), point_exact),
        ("route_refreshed_once".to_owned(), route_refreshes == 1),
        ("scan_exact_after_refresh".to_owned(), scan_exact),
        ("second_range_included".to_owned(), second_range_present),
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
        FIXED_READ_VERSION,
        observed_read_version,
        expected.len(),
        observed_rows,
        &checks,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(RangeRouteRefreshReceipt {
        seed,
        mode,
        worker_process_starts: 1,
        worker_process_kills: 1,
        route_refreshes,
        requested_read_version: FIXED_READ_VERSION,
        observed_read_version,
        expected_rows: u64::try_from(expected.len()).unwrap_or(u64::MAX),
        observed_rows: u64::try_from(observed_rows).unwrap_or(u64::MAX),
        checks,
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

fn client_route(
    endpoint: &str,
    range_id: RangeEngineId,
    routing_epoch: u64,
    start: &[u8],
    end: &[u8],
) -> ClientRangeRoute {
    ClientRangeRoute {
        endpoint: endpoint.to_owned(),
        range_id,
        routing_epoch,
        start: start.to_vec(),
        end: end.to_vec(),
    }
}

#[allow(clippy::too_many_lines)]
async fn run_controller(
    seed: u64,
    mode: RangeReadProcessMode,
    executable: &Path,
) -> Result<RangeReadProcessReceipt, String> {
    let listen_address = reserve_address()?;
    let config = RangeReadProcessConfig {
        seed,
        mode,
        listen_address: listen_address.clone(),
    };
    let mut child = spawn_worker(executable, &config)?;
    let protocol = protocol_config();
    let left_epoch = if mode == RangeReadProcessMode::AcceptStaleRoute {
        LEFT_EPOCH - 1
    } else {
        LEFT_EPOCH
    };
    let first = RoutedRangeReadRequest::point(
        CELL_ID,
        TENANT_ID,
        LEFT_RANGE,
        left_epoch,
        TARGET_VERSION,
        b"a".to_vec(),
    );
    wait_until_ready(&listen_address, &first, protocol, &mut child).await?;

    let mut point_latency_nanos = Vec::new();
    let mut scan_latency_nanos = Vec::new();
    let mut wrong_values = 0_u64;
    for _ in 0..POINT_READS {
        let started = Instant::now();
        let reply = request_range_read(&listen_address, &first, protocol)
            .await?
            .map_err(|error| format!("point read refused: {error:?}"))?;
        point_latency_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let exact = matches!(
            reply,
            RoutedRangeReadReply::Point {
                value: Some(ref value),
                ref stamp,
            } if value == b"a9"
                && stamp.range_id == LEFT_RANGE
                && stamp.routing_epoch == left_epoch
                && stamp.applied_frontier == TARGET_VERSION
        );
        wrong_values = wrong_values.saturating_add(u64::from(!exact));
    }

    let scan_end = if mode == RangeReadProcessMode::AcceptCrossingScan {
        b"z".to_vec()
    } else {
        b"m".to_vec()
    };
    let scan_request = RoutedRangeReadRequest::scan(
        CELL_ID,
        TENANT_ID,
        LEFT_RANGE,
        left_epoch,
        TARGET_VERSION,
        b"a".to_vec(),
        scan_end,
        100,
    );
    for _ in 0..SCAN_READS {
        let started = Instant::now();
        let reply = request_range_read(&listen_address, &scan_request, protocol)
            .await?
            .map_err(|error| format!("scan read refused: {error:?}"))?;
        scan_latency_nanos.push(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let exact = matches!(
            reply,
            RoutedRangeReadReply::Scan { rows, ref stamp }
                if rows == final_range_serving_rows()
                    && stamp.range_id == LEFT_RANGE
                    && stamp.routing_epoch == left_epoch
        );
        wrong_values = wrong_values.saturating_add(u64::from(!exact));
    }
    if mode == RangeReadProcessMode::AcceptWrongValue {
        wrong_values = wrong_values.saturating_add(1);
    }

    let stale = RoutedRangeReadRequest::point(
        CELL_ID,
        TENANT_ID,
        LEFT_RANGE,
        LEFT_EPOCH - 1,
        TARGET_VERSION,
        b"a".to_vec(),
    );
    let stale_route_refused = matches!(
        request_range_read(&listen_address, &stale, protocol).await?,
        Err(RoutedRangeReadError::StaleRoute { .. })
    );
    let crossing = RoutedRangeReadRequest::scan(
        CELL_ID,
        TENANT_ID,
        LEFT_RANGE,
        left_epoch,
        TARGET_VERSION,
        b"a".to_vec(),
        b"z".to_vec(),
        100,
    );
    let crossing_scan_refused = matches!(
        request_range_read(&listen_address, &crossing, protocol).await?,
        Err(RoutedRangeReadError::ScanCrossesRange { .. })
    );
    let unavailable = RoutedRangeReadRequest::point(
        CELL_ID,
        TENANT_ID,
        LEFT_RANGE,
        left_epoch,
        TARGET_VERSION + 1,
        b"a".to_vec(),
    );
    let unavailable_snapshot_refused = matches!(
        request_range_read(&listen_address, &unavailable, protocol).await?,
        Err(RoutedRangeReadError::SnapshotUnavailable { .. })
    );

    let worker_process_kills = if mode == RangeReadProcessMode::SkipWorkerKill {
        0
    } else {
        child.kill().map_err(|error| error.to_string())?;
        child.wait().map_err(|error| error.to_string())?;
        1
    };
    let killed_worker_refused = request_range_read(&listen_address, &first, protocol)
        .await
        .is_err();
    if worker_process_kills == 0 {
        let _ = child.kill();
        let _ = child.wait();
    }

    let checks = BTreeMap::from([
        ("point_reads_exact".to_owned(), wrong_values == 0),
        ("scan_reads_exact".to_owned(), wrong_values == 0),
        ("stale_route_refused".to_owned(), stale_route_refused),
        ("crossing_scan_refused".to_owned(), crossing_scan_refused),
        (
            "unavailable_snapshot_refused".to_owned(),
            unavailable_snapshot_refused,
        ),
        (
            "worker_kill_exercised".to_owned(),
            worker_process_kills == 1,
        ),
        ("killed_worker_refused".to_owned(), killed_worker_refused),
    ]);
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let semantic = (
        seed,
        mode,
        POINT_READS,
        SCAN_READS,
        stale_route_refused,
        crossing_scan_refused,
        unavailable_snapshot_refused,
        worker_process_kills,
        killed_worker_refused,
        wrong_values,
        &checks,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(RangeReadProcessReceipt {
        seed,
        mode,
        worker_process_starts: 1,
        worker_process_kills,
        point_reads: POINT_READS,
        scan_reads: SCAN_READS,
        point_latency_nanos,
        scan_latency_nanos,
        stale_route_refusals: u64::from(stale_route_refused),
        crossing_scan_refusals: u64::from(crossing_scan_refused),
        unavailable_snapshot_refusals: u64::from(unavailable_snapshot_refused),
        killed_worker_refusals: u64::from(killed_worker_refused),
        wrong_values,
        checks,
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

/// Start one bounded routed-read server process.
///
/// # Errors
///
/// Returns an error when the real serving fixture, router, or listener fails.
pub async fn run_range_read_process_worker(config: RangeReadProcessConfig) -> Result<(), String> {
    let state = build_final_range_serving_state(config.seed).await?;
    let router = Arc::new(KvReadRouter::new(KvReadRouterConfig {
        cell_id: CELL_ID,
        max_in_flight: 32,
        max_key_bytes: 256,
        max_scan_rows: 1_024,
    })?);
    let left_epoch = if config.mode == RangeReadProcessMode::AcceptStaleRoute {
        LEFT_EPOCH - 1
    } else {
        LEFT_EPOCH
    };
    let left_end = match config.mode {
        RangeReadProcessMode::AcceptCrossingScan => b"z".to_vec(),
        RangeReadProcessMode::RouteRefreshFixture => b"k5".to_vec(),
        _ => b"m".to_vec(),
    };
    router.assign(
        RangeReadAssignment {
            tenant_id: TENANT_ID,
            range_id: LEFT_RANGE,
            routing_epoch: left_epoch,
            start: b"a".to_vec(),
            end: left_end.clone(),
        },
        Arc::clone(&state),
    )?;
    if config.mode != RangeReadProcessMode::AcceptCrossingScan {
        router.assign(
            RangeReadAssignment {
                tenant_id: TENANT_ID,
                range_id: RIGHT_RANGE,
                routing_epoch: RIGHT_EPOCH,
                start: left_end,
                end: b"z".to_vec(),
            },
            state,
        )?;
    }
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(|error| error.to_string())?;
    serve_range_read_listener(listener, protocol_config(), router).await
}

fn spawn_worker(executable: &Path, config: &RangeReadProcessConfig) -> Result<WorkerChild, String> {
    let config = serde_json::to_string(config).map_err(|error| error.to_string())?;
    Command::new(executable)
        .arg("range-read-service-node")
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
                "range-read worker exited before readiness: {status}"
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
    Err(format!("range-read worker did not become ready: {last}"))
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
        max_frame_bytes: 64 * 1024,
        request_timeout_millis: 2_000,
    }
}
