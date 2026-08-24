//! Bounded routed point and range reads for one disposable KV Runtime.

use crate::{RangeEngineId, RangeServingState, RangeServingViewError};
use okv_model::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

const RANGE_READ_PROTOCOL_VERSION: u16 = 1;

/// Process-wide limits for direct reads served by one KV Runtime.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KvReadRouterConfig {
    pub cell_id: [u8; 16],
    pub max_in_flight: usize,
    pub max_key_bytes: usize,
    pub max_scan_rows: usize,
}

/// Stable local assignment identity supplied by the cell `RangeMap`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeReadAssignment {
    pub tenant_id: [u8; 16],
    pub range_id: RangeEngineId,
    pub routing_epoch: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// One bounded direct-read request. The caller repeats the `RangeMap` identity
/// it routed through so a stale client is fenced at the serving process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutedRangeReadRequest {
    pub protocol_version: u16,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub expected_range_id: RangeEngineId,
    pub routing_epoch: u64,
    pub read_version: u64,
    pub operation: RangeReadOperation,
}

impl RoutedRangeReadRequest {
    #[must_use]
    pub fn point(
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
        range_id: RangeEngineId,
        routing_epoch: u64,
        read_version: u64,
        key: Vec<u8>,
    ) -> Self {
        Self {
            protocol_version: RANGE_READ_PROTOCOL_VERSION,
            cell_id,
            tenant_id,
            expected_range_id: range_id,
            routing_epoch,
            read_version,
            operation: RangeReadOperation::Point { key },
        }
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
        range_id: RangeEngineId,
        routing_epoch: u64,
        read_version: u64,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    ) -> Self {
        Self {
            protocol_version: RANGE_READ_PROTOCOL_VERSION,
            cell_id,
            tenant_id,
            expected_range_id: range_id,
            routing_epoch,
            read_version,
            operation: RangeReadOperation::Scan { start, end, limit },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeReadOperation {
    Point {
        key: Vec<u8>,
    },
    Scan {
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    },
}

/// Routing and serving generation returned with every successful read.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeReadStamp {
    pub range_id: RangeEngineId,
    pub routing_epoch: u64,
    pub transaction_generation: u64,
    pub base_frontier: u64,
    pub applied_frontier: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutedRangeReadReply {
    Point {
        stamp: RangeReadStamp,
        value: Option<Vec<u8>>,
    },
    Scan {
        stamp: RangeReadStamp,
        rows: Vec<Row>,
    },
}

/// Typed refusal from routing, snapshot, resource, or storage validation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutedRangeReadError {
    UnsupportedProtocol {
        requested: u16,
        supported: u16,
    },
    WrongCell,
    TenantNotAssigned,
    RangeNotAssigned,
    StaleRoute {
        expected_range_id: RangeEngineId,
        current_range_id: RangeEngineId,
        expected_epoch: u64,
        current_epoch: u64,
    },
    KeyOutsideRange,
    ScanCrossesRange {
        split_at: Vec<u8>,
    },
    InvalidRange,
    KeyTooLarge {
        maximum: usize,
    },
    ScanLimitExceeded {
        maximum: usize,
    },
    SnapshotExpired {
        requested: u64,
        minimum: u64,
    },
    SnapshotUnavailable {
        requested: u64,
        applied: u64,
    },
    Overloaded,
    DeadlineExceeded,
    FrameTooLarge {
        maximum: usize,
    },
    MalformedRequest,
    StorageUnavailable,
}

struct ServingRange {
    assignment: RangeReadAssignment,
    state: Arc<RangeServingState>,
}

type TenantRanges = BTreeMap<Vec<u8>, Arc<ServingRange>>;
type LocalRangeMap = BTreeMap<[u8; 16], TenantRanges>;

/// Process-level router for every locally assigned Range Engine.
pub struct KvReadRouter {
    config: KvReadRouterConfig,
    tenants: RwLock<LocalRangeMap>,
    in_flight: Arc<Semaphore>,
}

impl KvReadRouter {
    /// Create one empty KV Runtime read router.
    ///
    /// # Errors
    ///
    /// Returns an error when a declared process bound is zero.
    pub fn new(config: KvReadRouterConfig) -> Result<Self, String> {
        if config.max_in_flight == 0 || config.max_key_bytes == 0 || config.max_scan_rows == 0 {
            return Err("KV read-router bounds must be positive".to_owned());
        }
        Ok(Self {
            config,
            tenants: RwLock::new(BTreeMap::new()),
            in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
        })
    }

    /// Install one non-overlapping local range assignment.
    ///
    /// # Errors
    ///
    /// Refuses invalid bounds, duplicate starts, overlap, and a serving view
    /// whose cell or tenant identity differs from the assignment.
    pub fn assign(
        &self,
        assignment: RangeReadAssignment,
        state: Arc<RangeServingState>,
    ) -> Result<(), String> {
        if assignment.start >= assignment.end || assignment.routing_epoch == 0 {
            return Err("range assignment requires ordered bounds and a nonzero epoch".to_owned());
        }
        let view = state.current().map_err(|error| error.to_string())?;
        if view.cell_id() != self.config.cell_id || view.tenant_id() != assignment.tenant_id {
            return Err("range assignment identity differs from its serving view".to_owned());
        }
        let mut tenants = self
            .tenants
            .write()
            .map_err(|_| "KV read-router assignment state is poisoned".to_owned())?;
        let ranges = tenants.entry(assignment.tenant_id).or_default();
        if let Some((_, preceding)) = ranges.range(..=assignment.start.clone()).next_back() {
            if preceding.assignment.end > assignment.start {
                return Err("range assignment overlaps its predecessor".to_owned());
            }
        }
        if let Some((following_start, _)) = ranges.range(assignment.start.clone()..).next() {
            if following_start < &assignment.end {
                return Err("range assignment overlaps its successor".to_owned());
            }
        }
        let start = assignment.start.clone();
        if ranges
            .insert(start, Arc::new(ServingRange { assignment, state }))
            .is_some()
        {
            return Err("range assignment start is already present".to_owned());
        }
        Ok(())
    }

    /// Route and execute one exact-version read through one immutable view.
    ///
    /// # Errors
    ///
    /// Returns a typed refusal without storage I/O when routing, bounds, or
    /// process capacity validation fails.
    pub async fn execute(
        &self,
        request: RoutedRangeReadRequest,
    ) -> Result<RoutedRangeReadReply, RoutedRangeReadError> {
        let _permit = Arc::clone(&self.in_flight)
            .try_acquire_owned()
            .map_err(|_| RoutedRangeReadError::Overloaded)?;
        if request.protocol_version != RANGE_READ_PROTOCOL_VERSION {
            return Err(RoutedRangeReadError::UnsupportedProtocol {
                requested: request.protocol_version,
                supported: RANGE_READ_PROTOCOL_VERSION,
            });
        }
        if request.cell_id != self.config.cell_id {
            return Err(RoutedRangeReadError::WrongCell);
        }
        let route_key = match &request.operation {
            RangeReadOperation::Point { key } => key,
            RangeReadOperation::Scan { start, .. } => start,
        };
        if route_key.len() > self.config.max_key_bytes {
            return Err(RoutedRangeReadError::KeyTooLarge {
                maximum: self.config.max_key_bytes,
            });
        }
        let serving = self.lookup(request.tenant_id, route_key)?;
        if request.expected_range_id != serving.assignment.range_id
            || request.routing_epoch != serving.assignment.routing_epoch
        {
            return Err(RoutedRangeReadError::StaleRoute {
                expected_range_id: request.expected_range_id,
                current_range_id: serving.assignment.range_id,
                expected_epoch: request.routing_epoch,
                current_epoch: serving.assignment.routing_epoch,
            });
        }
        validate_operation(&self.config, &serving.assignment, &request.operation)?;
        let view = serving
            .state
            .current()
            .map_err(|_| RoutedRangeReadError::StorageUnavailable)?;
        let stamp = RangeReadStamp {
            range_id: serving.assignment.range_id,
            routing_epoch: serving.assignment.routing_epoch,
            transaction_generation: view.generation(),
            base_frontier: view.base_frontier(),
            applied_frontier: view.target_version(),
        };
        match request.operation {
            RangeReadOperation::Point { key } => view
                .get_at(&key, request.read_version)
                .await
                .map(|value| RoutedRangeReadReply::Point { stamp, value })
                .map_err(|error| map_view_error(&error)),
            RangeReadOperation::Scan { start, end, limit } => view
                .scan_at(&start, &end, request.read_version, limit)
                .await
                .map(|rows| RoutedRangeReadReply::Scan { stamp, rows })
                .map_err(|error| map_view_error(&error)),
        }
    }

    fn lookup(
        &self,
        tenant_id: [u8; 16],
        key: &[u8],
    ) -> Result<Arc<ServingRange>, RoutedRangeReadError> {
        let tenants = self
            .tenants
            .read()
            .map_err(|_| RoutedRangeReadError::StorageUnavailable)?;
        let ranges = tenants
            .get(&tenant_id)
            .ok_or(RoutedRangeReadError::TenantNotAssigned)?;
        let serving = ranges
            .range(..=key.to_vec())
            .next_back()
            .map(|(_, serving)| Arc::clone(serving))
            .ok_or(RoutedRangeReadError::RangeNotAssigned)?;
        if key >= serving.assignment.end.as_slice() {
            return Err(RoutedRangeReadError::RangeNotAssigned);
        }
        Ok(serving)
    }
}

fn validate_operation(
    config: &KvReadRouterConfig,
    assignment: &RangeReadAssignment,
    operation: &RangeReadOperation,
) -> Result<(), RoutedRangeReadError> {
    match operation {
        RangeReadOperation::Point { key } => {
            if key.as_slice() < assignment.start.as_slice()
                || key.as_slice() >= assignment.end.as_slice()
            {
                return Err(RoutedRangeReadError::KeyOutsideRange);
            }
        }
        RangeReadOperation::Scan { start, end, limit } => {
            if start >= end {
                return Err(RoutedRangeReadError::InvalidRange);
            }
            if start.len() > config.max_key_bytes || end.len() > config.max_key_bytes {
                return Err(RoutedRangeReadError::KeyTooLarge {
                    maximum: config.max_key_bytes,
                });
            }
            if *limit > config.max_scan_rows {
                return Err(RoutedRangeReadError::ScanLimitExceeded {
                    maximum: config.max_scan_rows,
                });
            }
            if start.as_slice() < assignment.start.as_slice()
                || start.as_slice() >= assignment.end.as_slice()
            {
                return Err(RoutedRangeReadError::KeyOutsideRange);
            }
            if end.as_slice() > assignment.end.as_slice() {
                return Err(RoutedRangeReadError::ScanCrossesRange {
                    split_at: assignment.end.clone(),
                });
            }
        }
    }
    Ok(())
}

fn map_view_error(error: &RangeServingViewError) -> RoutedRangeReadError {
    match error {
        RangeServingViewError::SnapshotExpired { requested, minimum } => {
            RoutedRangeReadError::SnapshotExpired {
                requested: *requested,
                minimum: *minimum,
            }
        }
        RangeServingViewError::SnapshotUnavailable { requested, applied } => {
            RoutedRangeReadError::SnapshotUnavailable {
                requested: *requested,
                applied: *applied,
            }
        }
        RangeServingViewError::InvalidReadRange { .. } => RoutedRangeReadError::InvalidRange,
        _ => RoutedRangeReadError::StorageUnavailable,
    }
}

/// Framing and deadline bounds for the prototype direct-read protocol.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeReadProtocolConfig {
    pub max_frame_bytes: usize,
    pub request_timeout_millis: u64,
}

/// Serve routed reads on an already bound listener.
///
/// # Errors
///
/// Returns only when accepting a connection fails. Individual malformed or
/// failed requests receive a typed response and do not stop the listener.
pub async fn serve_range_read_listener(
    listener: TcpListener,
    protocol: RangeReadProtocolConfig,
    router: Arc<KvReadRouter>,
) -> Result<(), String> {
    if protocol.max_frame_bytes == 0 || protocol.request_timeout_millis == 0 {
        return Err("range-read protocol bounds must be positive".to_owned());
    }
    loop {
        let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
        let router = Arc::clone(&router);
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream, protocol, router).await {
                eprintln!("range-read connection failed: {error}");
            }
        });
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    protocol: RangeReadProtocolConfig,
    router: Arc<KvReadRouter>,
) -> Result<(), String> {
    let request = read_frame::<RoutedRangeReadRequest>(&mut stream, protocol.max_frame_bytes).await;
    let reply = match request {
        Ok(request) => tokio::time::timeout(
            Duration::from_millis(protocol.request_timeout_millis),
            router.execute(request),
        )
        .await
        .unwrap_or(Err(RoutedRangeReadError::DeadlineExceeded)),
        Err(error) => Err(error),
    };
    write_reply_frame(&mut stream, &reply, protocol.max_frame_bytes).await
}

/// Send one bounded request over a fresh TCP connection.
///
/// # Errors
///
/// Returns transport and framing failures separately from typed server
/// refusals, which remain in the returned result.
pub async fn request_range_read(
    endpoint: &str,
    request: &RoutedRangeReadRequest,
    protocol: RangeReadProtocolConfig,
) -> Result<Result<RoutedRangeReadReply, RoutedRangeReadError>, String> {
    let mut stream = tokio::time::timeout(
        Duration::from_millis(protocol.request_timeout_millis),
        TcpStream::connect(endpoint),
    )
    .await
    .map_err(|_| format!("range-read connect timed out at {endpoint}"))?
    .map_err(|error| error.to_string())?;
    write_frame(&mut stream, request, protocol.max_frame_bytes).await?;
    tokio::time::timeout(
        Duration::from_millis(protocol.request_timeout_millis),
        read_frame(&mut stream, protocol.max_frame_bytes),
    )
    .await
    .map_err(|_| format!("range-read response timed out at {endpoint}"))?
    .map_err(|error| format!("range-read response failed: {error:?}"))
}

async fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut TcpStream,
    maximum: usize,
) -> Result<T, RoutedRangeReadError> {
    let length = stream
        .read_u32()
        .await
        .map_err(|_| RoutedRangeReadError::MalformedRequest)?;
    let length =
        usize::try_from(length).map_err(|_| RoutedRangeReadError::FrameTooLarge { maximum })?;
    if length > maximum {
        return Err(RoutedRangeReadError::FrameTooLarge { maximum });
    }
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|_| RoutedRangeReadError::MalformedRequest)?;
    serde_json::from_slice(&bytes).map_err(|_| RoutedRangeReadError::MalformedRequest)
}

async fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
    maximum: usize,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    if bytes.len() > maximum {
        return Err(format!("range-read response exceeds {maximum} bytes"));
    }
    let length = u32::try_from(bytes.len()).map_err(|error| error.to_string())?;
    stream
        .write_u32(length)
        .await
        .map_err(|error| error.to_string())?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|error| error.to_string())?;
    stream.flush().await.map_err(|error| error.to_string())
}

async fn write_reply_frame(
    stream: &mut TcpStream,
    reply: &Result<RoutedRangeReadReply, RoutedRangeReadError>,
    maximum: usize,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(reply).map_err(|error| error.to_string())?;
    if bytes.len() <= maximum {
        return write_encoded_frame(stream, &bytes).await;
    }
    let refused: Result<RoutedRangeReadReply, RoutedRangeReadError> =
        Err(RoutedRangeReadError::FrameTooLarge { maximum });
    let bytes = serde_json::to_vec(&refused).map_err(|error| error.to_string())?;
    write_encoded_frame(stream, &bytes).await
}

async fn write_encoded_frame(stream: &mut TcpStream, bytes: &[u8]) -> Result<(), String> {
    let length = u32::try_from(bytes.len()).map_err(|error| error.to_string())?;
    stream
        .write_u32(length)
        .await
        .map_err(|error| error.to_string())?;
    stream
        .write_all(bytes)
        .await
        .map_err(|error| error.to_string())?;
    stream.flush().await.map_err(|error| error.to_string())
}
