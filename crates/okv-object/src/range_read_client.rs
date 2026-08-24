//! Client-side routing, bounded refresh, and exact-version range fan-out.

use crate::{
    request_range_read, RangeEngineId, RangeReadProtocolConfig, RangeReadStamp,
    RoutedRangeReadError, RoutedRangeReadReply, RoutedRangeReadRequest,
};
use async_trait::async_trait;
use okv_model::Row;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};

/// One client-visible route selected by the cell `RangeMap` authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRangeRoute {
    pub endpoint: String,
    pub range_id: RangeEngineId,
    pub routing_epoch: u64,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// One immutable tenant-scoped `RangeMap` snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRangeMapSnapshot {
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub map_version: u64,
    pub routes: Vec<ClientRangeRoute>,
}

impl ClientRangeMapSnapshot {
    /// Validate identity, ordering, overlap, and route identity.
    ///
    /// Gaps are allowed so a partially populated directory can be represented,
    /// but any read entering a gap is refused locally.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot cannot be routed deterministically.
    pub fn validate(&self) -> Result<(), String> {
        if self.map_version == 0 {
            return Err("range-map version must be nonzero".to_owned());
        }
        if self.routes.is_empty() {
            return Err("range-map snapshot must contain a route".to_owned());
        }
        let mut prior_end: Option<&[u8]> = None;
        let mut range_ids = BTreeSet::new();
        for route in &self.routes {
            if route.endpoint.is_empty() {
                return Err("range-map endpoint must not be empty".to_owned());
            }
            if route.routing_epoch == 0 || route.start >= route.end {
                return Err(
                    "range-map route requires ordered bounds and a nonzero epoch".to_owned(),
                );
            }
            if let Some(end) = prior_end {
                if route.start.as_slice() < end {
                    return Err("range-map routes overlap or are out of order".to_owned());
                }
            }
            if !range_ids.insert(route.range_id) {
                return Err("range-map range identity is duplicated".to_owned());
            }
            prior_end = Some(route.end.as_slice());
        }
        Ok(())
    }

    fn route(&self, key: &[u8]) -> Option<&ClientRangeRoute> {
        self.routes
            .iter()
            .rev()
            .find(|route| route.start.as_slice() <= key && key < route.end.as_slice())
    }
}

/// Refresh source for authoritative tenant route snapshots.
#[async_trait]
pub trait RangeMapSource: Send + Sync {
    /// Return the latest complete snapshot visible to this client.
    ///
    /// # Errors
    ///
    /// Returns an error when the routing authority cannot produce a snapshot.
    async fn snapshot(
        &self,
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
    ) -> Result<ClientRangeMapSnapshot, String>;
}

/// Bounds for client-side route recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KvReadClientConfig {
    pub protocol: RangeReadProtocolConfig,
    pub max_route_refreshes: usize,
}

/// Client-visible direct-read failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvReadClientError {
    InvalidRangeMap(String),
    RouteNotFound { key: Vec<u8>, map_version: u64 },
    RouteRefreshFailed(String),
    RouteRefreshDidNotAdvance { current: u64, refreshed: u64 },
    RouteRefreshLimit { maximum: usize },
    InvalidScan,
    Transport(String),
    Server(RoutedRangeReadError),
    ProtocolViolation(String),
}

impl Display for KvReadClientError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for KvReadClientError {}

/// Tenant-scoped exact-read client with an atomically replaceable route view.
pub struct KvReadClient {
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    config: KvReadClientConfig,
    source: Arc<dyn RangeMapSource>,
    routes: RwLock<Arc<ClientRangeMapSnapshot>>,
}

impl KvReadClient {
    /// Create a client from one validated route snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for mismatched identity, invalid routes, or a zero
    /// refresh budget.
    pub fn new(
        cell_id: [u8; 16],
        tenant_id: [u8; 16],
        config: KvReadClientConfig,
        initial: ClientRangeMapSnapshot,
        source: Arc<dyn RangeMapSource>,
    ) -> Result<Self, KvReadClientError> {
        if config.max_route_refreshes == 0 {
            return Err(KvReadClientError::InvalidRangeMap(
                "route refresh budget must be positive".to_owned(),
            ));
        }
        validate_snapshot_identity(cell_id, tenant_id, &initial)?;
        Ok(Self {
            cell_id,
            tenant_id,
            config,
            source,
            routes: RwLock::new(Arc::new(initial)),
        })
    }

    /// Read one key at exactly `read_version`, refreshing a stale route without
    /// obtaining a new version.
    ///
    /// # Errors
    ///
    /// Returns a bounded routing, transport, server, or protocol failure.
    pub async fn point_at(
        &self,
        key: &[u8],
        read_version: u64,
    ) -> Result<Option<Vec<u8>>, KvReadClientError> {
        for refreshes in 0..=self.config.max_route_refreshes {
            let snapshot = self.current_snapshot()?;
            let route = snapshot
                .route(key)
                .ok_or_else(|| KvReadClientError::RouteNotFound {
                    key: key.to_vec(),
                    map_version: snapshot.map_version,
                })?;
            let request = RoutedRangeReadRequest::point(
                self.cell_id,
                self.tenant_id,
                route.range_id,
                route.routing_epoch,
                read_version,
                key.to_vec(),
            );
            match request_range_read(&route.endpoint, &request, self.config.protocol).await {
                Ok(Ok(RoutedRangeReadReply::Point { stamp, value })) => {
                    validate_stamp(route, read_version, &stamp)?;
                    return Ok(value);
                }
                Ok(Ok(RoutedRangeReadReply::Scan { .. })) => {
                    return Err(KvReadClientError::ProtocolViolation(
                        "point request received a scan reply".to_owned(),
                    ));
                }
                Ok(Err(error)) if route_retryable(&error) => {
                    self.refresh_or_limit(snapshot.map_version, refreshes)
                        .await?;
                }
                Ok(Err(error)) => return Err(KvReadClientError::Server(error)),
                Err(error) => {
                    self.refresh_or_limit(snapshot.map_version, refreshes)
                        .await?;
                    if refreshes == self.config.max_route_refreshes {
                        return Err(KvReadClientError::Transport(error));
                    }
                }
            }
        }
        Err(KvReadClientError::RouteRefreshLimit {
            maximum: self.config.max_route_refreshes,
        })
    }

    /// Scan an arbitrary tenant interval at exactly `read_version` by issuing
    /// single-range requests and merging their already ordered rows.
    ///
    /// Any retryable route failure discards the partial result, refreshes the
    /// map, and restarts the complete scan at the original version.
    ///
    /// # Errors
    ///
    /// Returns a bounded routing, transport, server, or protocol failure.
    pub async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        read_version: u64,
        limit: usize,
    ) -> Result<Vec<Row>, KvReadClientError> {
        if start >= end {
            return Err(KvReadClientError::InvalidScan);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        for refreshes in 0..=self.config.max_route_refreshes {
            let snapshot = self.current_snapshot()?;
            match self
                .scan_once(&snapshot, start, end, read_version, limit)
                .await
            {
                Ok(rows) => return Ok(rows),
                Err(ScanAttemptError::Fatal(error)) => return Err(error),
                Err(ScanAttemptError::Refresh) => {
                    self.refresh_or_limit(snapshot.map_version, refreshes)
                        .await?;
                }
                Err(ScanAttemptError::Transport(error)) => {
                    self.refresh_or_limit(snapshot.map_version, refreshes)
                        .await?;
                    if refreshes == self.config.max_route_refreshes {
                        return Err(KvReadClientError::Transport(error));
                    }
                }
            }
        }
        Err(KvReadClientError::RouteRefreshLimit {
            maximum: self.config.max_route_refreshes,
        })
    }

    async fn scan_once(
        &self,
        snapshot: &ClientRangeMapSnapshot,
        start: &[u8],
        end: &[u8],
        read_version: u64,
        limit: usize,
    ) -> Result<Vec<Row>, ScanAttemptError> {
        let mut cursor = start.to_vec();
        let mut rows = Vec::new();
        while cursor.as_slice() < end && rows.len() < limit {
            let route = snapshot.route(&cursor).ok_or_else(|| {
                ScanAttemptError::Fatal(KvReadClientError::RouteNotFound {
                    key: cursor.clone(),
                    map_version: snapshot.map_version,
                })
            })?;
            let sub_end = if route.end.as_slice() < end {
                route.end.clone()
            } else {
                end.to_vec()
            };
            let remaining = limit.saturating_sub(rows.len());
            let request = RoutedRangeReadRequest::scan(
                self.cell_id,
                self.tenant_id,
                route.range_id,
                route.routing_epoch,
                read_version,
                cursor.clone(),
                sub_end.clone(),
                remaining,
            );
            let reply = request_range_read(&route.endpoint, &request, self.config.protocol)
                .await
                .map_err(ScanAttemptError::Transport)?;
            match reply {
                Ok(RoutedRangeReadReply::Scan {
                    stamp,
                    rows: mut part,
                }) => {
                    validate_stamp(route, read_version, &stamp).map_err(ScanAttemptError::Fatal)?;
                    validate_rows(&part, &cursor, &sub_end, remaining)
                        .map_err(ScanAttemptError::Fatal)?;
                    rows.append(&mut part);
                    cursor = sub_end;
                }
                Ok(RoutedRangeReadReply::Point { .. }) => {
                    return Err(ScanAttemptError::Fatal(
                        KvReadClientError::ProtocolViolation(
                            "scan request received a point reply".to_owned(),
                        ),
                    ));
                }
                Err(error) if route_retryable(&error) => {
                    return Err(ScanAttemptError::Refresh);
                }
                Err(error) => {
                    return Err(ScanAttemptError::Fatal(KvReadClientError::Server(error)));
                }
            }
        }
        Ok(rows)
    }

    fn current_snapshot(&self) -> Result<Arc<ClientRangeMapSnapshot>, KvReadClientError> {
        self.routes
            .read()
            .map(|snapshot| Arc::clone(&snapshot))
            .map_err(|_| {
                KvReadClientError::InvalidRangeMap("cached range map is poisoned".to_owned())
            })
    }

    async fn refresh_or_limit(
        &self,
        prior_version: u64,
        refreshes: usize,
    ) -> Result<(), KvReadClientError> {
        if refreshes == self.config.max_route_refreshes {
            return Err(KvReadClientError::RouteRefreshLimit {
                maximum: self.config.max_route_refreshes,
            });
        }
        let refreshed_snapshot = self
            .source
            .snapshot(self.cell_id, self.tenant_id)
            .await
            .map_err(KvReadClientError::RouteRefreshFailed)?;
        validate_snapshot_identity(self.cell_id, self.tenant_id, &refreshed_snapshot)?;
        if refreshed_snapshot.map_version <= prior_version {
            return Err(KvReadClientError::RouteRefreshDidNotAdvance {
                current: prior_version,
                refreshed: refreshed_snapshot.map_version,
            });
        }
        let mut cached = self.routes.write().map_err(|_| {
            KvReadClientError::InvalidRangeMap("cached range map is poisoned".to_owned())
        })?;
        if refreshed_snapshot.map_version > cached.map_version {
            *cached = Arc::new(refreshed_snapshot);
        }
        Ok(())
    }
}

enum ScanAttemptError {
    Refresh,
    Transport(String),
    Fatal(KvReadClientError),
}

fn validate_snapshot_identity(
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    snapshot: &ClientRangeMapSnapshot,
) -> Result<(), KvReadClientError> {
    if snapshot.cell_id != cell_id || snapshot.tenant_id != tenant_id {
        return Err(KvReadClientError::InvalidRangeMap(
            "range-map identity differs from the client session".to_owned(),
        ));
    }
    snapshot
        .validate()
        .map_err(KvReadClientError::InvalidRangeMap)
}

fn route_retryable(error: &RoutedRangeReadError) -> bool {
    matches!(
        error,
        RoutedRangeReadError::StaleRoute { .. }
            | RoutedRangeReadError::RangeNotAssigned
            | RoutedRangeReadError::TenantNotAssigned
            | RoutedRangeReadError::ScanCrossesRange { .. }
    )
}

fn validate_stamp(
    route: &ClientRangeRoute,
    read_version: u64,
    stamp: &RangeReadStamp,
) -> Result<(), KvReadClientError> {
    if stamp.range_id != route.range_id || stamp.routing_epoch != route.routing_epoch {
        return Err(KvReadClientError::ProtocolViolation(
            "reply route identity differs from the selected route".to_owned(),
        ));
    }
    if stamp.applied_frontier < read_version {
        return Err(KvReadClientError::ProtocolViolation(
            "reply applied frontier is below the requested version".to_owned(),
        ));
    }
    Ok(())
}

fn validate_rows(
    rows: &[Row],
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<(), KvReadClientError> {
    if rows.len() > limit {
        return Err(KvReadClientError::ProtocolViolation(
            "scan reply exceeds the requested row limit".to_owned(),
        ));
    }
    let mut prior: Option<&[u8]> = None;
    for (key, _) in rows {
        if key.as_slice() < start || key.as_slice() >= end {
            return Err(KvReadClientError::ProtocolViolation(
                "scan reply contains a key outside the routed subrange".to_owned(),
            ));
        }
        if prior.is_some_and(|prior| prior >= key.as_slice()) {
            return Err(KvReadClientError::ProtocolViolation(
                "scan reply is not strictly ordered".to_owned(),
            ));
        }
        prior = Some(key);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range_serving_concurrency::{
        build_final_range_serving_state, final_range_serving_rows,
    };
    use crate::{serve_range_read_listener, KvReadRouter, KvReadRouterConfig, RangeReadAssignment};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::net::TcpListener;

    const CELL_ID: [u8; 16] = [0x41; 16];
    const TENANT_ID: [u8; 16] = [0x52; 16];

    struct StaticRangeMapSource {
        snapshot: ClientRangeMapSnapshot,
        refreshes: AtomicU64,
    }

    #[async_trait]
    impl RangeMapSource for StaticRangeMapSource {
        async fn snapshot(
            &self,
            cell_id: [u8; 16],
            tenant_id: [u8; 16],
        ) -> Result<ClientRangeMapSnapshot, String> {
            if cell_id != CELL_ID || tenant_id != TENANT_ID {
                return Err("unexpected range-map identity".to_owned());
            }
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            Ok(self.snapshot.clone())
        }
    }

    #[tokio::test]
    async fn refreshes_a_stale_route_and_fans_out_without_changing_snapshot() {
        let state = build_final_range_serving_state(1103).await.unwrap();
        let router = Arc::new(
            KvReadRouter::new(KvReadRouterConfig {
                cell_id: CELL_ID,
                max_in_flight: 8,
                max_key_bytes: 256,
                max_scan_rows: 100,
            })
            .unwrap(),
        );
        router
            .assign(
                RangeReadAssignment {
                    tenant_id: TENANT_ID,
                    range_id: RangeEngineId(81),
                    routing_epoch: 11,
                    start: b"a".to_vec(),
                    end: b"c".to_vec(),
                },
                Arc::clone(&state),
            )
            .unwrap();
        router
            .assign(
                RangeReadAssignment {
                    tenant_id: TENANT_ID,
                    range_id: RangeEngineId(82),
                    routing_epoch: 12,
                    start: b"c".to_vec(),
                    end: b"z".to_vec(),
                },
                state,
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let protocol = RangeReadProtocolConfig {
            max_frame_bytes: 16 * 1024,
            request_timeout_millis: 2_000,
        };
        let server = tokio::spawn(serve_range_read_listener(listener, protocol, router));

        let current = ClientRangeMapSnapshot {
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            map_version: 2,
            routes: vec![
                route(&endpoint, 81, 11, b"a", b"c"),
                route(&endpoint, 82, 12, b"c", b"z"),
            ],
        };
        let source = Arc::new(StaticRangeMapSource {
            snapshot: current,
            refreshes: AtomicU64::new(0),
        });
        let old_map = ClientRangeMapSnapshot {
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            map_version: 1,
            routes: vec![route(&endpoint, 71, 9, b"a", b"z")],
        };
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
        .unwrap();

        let rows = client.scan_at(b"a", b"z", 9, 100).await.unwrap();
        assert_eq!(rows, final_range_serving_rows());
        assert_eq!(source.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(
            client.point_at(b"k9", 9).await.unwrap(),
            Some(b"v9".to_vec())
        );
        assert_eq!(source.refreshes.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn refuses_a_refresh_that_does_not_advance_the_map() {
        let protocol = RangeReadProtocolConfig {
            max_frame_bytes: 16 * 1024,
            request_timeout_millis: 100,
        };
        let snapshot = ClientRangeMapSnapshot {
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            map_version: 1,
            routes: vec![route("127.0.0.1:1", 71, 9, b"a", b"z")],
        };
        let source = Arc::new(StaticRangeMapSource {
            snapshot: snapshot.clone(),
            refreshes: AtomicU64::new(0),
        });
        let client = KvReadClient::new(
            CELL_ID,
            TENANT_ID,
            KvReadClientConfig {
                protocol,
                max_route_refreshes: 1,
            },
            snapshot,
            source,
        )
        .unwrap();
        let error = client.point_at(b"a", 9).await.unwrap_err();
        assert_eq!(
            error,
            KvReadClientError::RouteRefreshDidNotAdvance {
                current: 1,
                refreshed: 1,
            }
        );
    }

    fn route(
        endpoint: &str,
        range_id: u64,
        routing_epoch: u64,
        start: &[u8],
        end: &[u8],
    ) -> ClientRangeRoute {
        ClientRangeRoute {
            endpoint: endpoint.to_owned(),
            range_id: RangeEngineId(range_id),
            routing_epoch,
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }
}
