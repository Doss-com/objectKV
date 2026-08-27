use crate::rpc::{read_response, write_request, ControlWrite, WriteAck, CLIENT_WRITE};
use crate::{
    ClientCommand, GenerationCredential, ObjectFrontierAttestation,
    ObjectFrontierCertificateStatement, ObjectFrontierLogPosition, ObjectFrontierRecord,
    RequestIdentity, TransactionBatchApplyResponse, TransactionBatchCommand,
};
use okv_transaction::{
    RetainedTransactionRecord, TransactionApplyResponse, TransactionCommand, TransactionStatus,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

const RETRY_ATTEMPTS: usize = 500;
const FRONTIER_MAGIC: &[u8] = b"OKVF1";
const OBJECT_FRONTIER_MAGIC: &[u8] = b"OKVO1";
pub(crate) const MAX_RETAINED_TRANSACTION_PAGE_RECORDS: u32 = 4_096;

/// One independently retryable transaction submitted through a batch entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionBatchItem {
    pub identity: RequestIdentity,
    pub credential: Option<GenerationCredential>,
    pub command: TransactionCommand,
}

/// Bounded commit-proxy policy for forming transaction-batch entries from
/// independent client requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionBatcherConfig {
    pub max_items: usize,
    pub max_entry_bytes: usize,
    pub max_delay: Duration,
    pub queue_capacity: usize,
}

impl TransactionBatcherConfig {
    fn validate(self) -> Result<Self, String> {
        if self.max_items == 0 || self.max_items > 32 {
            return Err("transaction batcher max_items must be between 1 and 32".to_owned());
        }
        if self.max_entry_bytes == 0 {
            return Err("transaction batcher max_entry_bytes must be positive".to_owned());
        }
        if self.max_delay.is_zero() {
            return Err("transaction batcher max_delay must be positive".to_owned());
        }
        if self.queue_capacity < self.max_items {
            return Err("transaction batcher queue_capacity must be at least max_items".to_owned());
        }
        Ok(self)
    }
}

/// Cumulative commit-proxy observations. These counters are diagnostic and do
/// not participate in acknowledgement or recovery.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionBatcherStats {
    pub accepted_items: u64,
    pub backpressure_rejections: u64,
    pub oversized_rejections: u64,
    pub batches: u64,
    pub resolved_items: u64,
    pub failed_batches: u64,
    pub item_bound_closures: u64,
    pub byte_bound_closures: u64,
    pub delay_bound_closures: u64,
    pub credential_bound_closures: u64,
    pub sender_closed_closures: u64,
    pub max_observed_batch_items: u64,
    pub max_observed_entry_bytes: u64,
}

#[derive(Debug, Default)]
struct TransactionBatcherCounters {
    accepted_items: AtomicU64,
    backpressure_rejections: AtomicU64,
    oversized_rejections: AtomicU64,
    batches: AtomicU64,
    resolved_items: AtomicU64,
    failed_batches: AtomicU64,
    item_bound_closures: AtomicU64,
    byte_bound_closures: AtomicU64,
    delay_bound_closures: AtomicU64,
    credential_bound_closures: AtomicU64,
    sender_closed_closures: AtomicU64,
    max_observed_batch_items: AtomicU64,
    max_observed_entry_bytes: AtomicU64,
}

impl TransactionBatcherCounters {
    fn snapshot(&self) -> TransactionBatcherStats {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        TransactionBatcherStats {
            accepted_items: load(&self.accepted_items),
            backpressure_rejections: load(&self.backpressure_rejections),
            oversized_rejections: load(&self.oversized_rejections),
            batches: load(&self.batches),
            resolved_items: load(&self.resolved_items),
            failed_batches: load(&self.failed_batches),
            item_bound_closures: load(&self.item_bound_closures),
            byte_bound_closures: load(&self.byte_bound_closures),
            delay_bound_closures: load(&self.delay_bound_closures),
            credential_bound_closures: load(&self.credential_bound_closures),
            sender_closed_closures: load(&self.sender_closed_closures),
            max_observed_batch_items: load(&self.max_observed_batch_items),
            max_observed_entry_bytes: load(&self.max_observed_entry_bytes),
        }
    }
}

struct PendingTransaction {
    item: TransactionBatchItem,
    result: oneshot::Sender<Result<TransactionApplyResponse, String>>,
}

/// One bounded FIFO commit-proxy batcher over a replicated transaction-log
/// client.
#[derive(Clone)]
pub struct TransactionBatcher {
    sender: mpsc::Sender<PendingTransaction>,
    config: TransactionBatcherConfig,
    counters: Arc<TransactionBatcherCounters>,
}

impl std::fmt::Debug for TransactionBatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionBatcher")
            .field("config", &self.config)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl TransactionBatcher {
    /// Start one batcher task inside the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when any queue, delay, item, or byte bound is invalid.
    pub fn start(
        client: TransactionLogClient,
        config: TransactionBatcherConfig,
    ) -> Result<Self, String> {
        let config = config.validate()?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let counters = Arc::new(TransactionBatcherCounters::default());
        tokio::spawn(run_transaction_batcher(
            client,
            receiver,
            config,
            Arc::clone(&counters),
        ));
        Ok(Self {
            sender,
            config,
            counters,
        })
    }

    /// Admit and resolve one independent transaction request.
    ///
    /// # Errors
    ///
    /// Returns explicit backpressure before replication when the queue is full,
    /// rejects an individually oversized entry, or returns the replicated
    /// transaction error for this request.
    pub async fn commit(
        &self,
        item: TransactionBatchItem,
    ) -> Result<TransactionApplyResponse, String> {
        let one_item_bytes = transaction_batch_entry_bytes(std::slice::from_ref(&item))?;
        if one_item_bytes > self.config.max_entry_bytes {
            self.counters
                .oversized_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(format!(
                "transaction batcher item encodes to {one_item_bytes} bytes above {}",
                self.config.max_entry_bytes
            ));
        }
        let (result, receiver) = oneshot::channel();
        match self.sender.try_send(PendingTransaction { item, result }) {
            Ok(()) => {
                self.counters.accepted_items.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.counters
                    .backpressure_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err("transaction batcher queue is full".to_owned());
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err("transaction batcher is closed".to_owned());
            }
        }
        receiver
            .await
            .map_err(|_| "transaction batcher stopped before resolving request".to_owned())?
    }

    /// Read cumulative local batching observations.
    #[must_use]
    pub fn stats(&self) -> TransactionBatcherStats {
        self.counters.snapshot()
    }
}

async fn run_transaction_batcher(
    client: TransactionLogClient,
    mut receiver: mpsc::Receiver<PendingTransaction>,
    config: TransactionBatcherConfig,
    counters: Arc<TransactionBatcherCounters>,
) {
    let mut carried = None;
    loop {
        let first = match carried.take() {
            Some(pending) => pending,
            None => match receiver.recv().await {
                Some(pending) => pending,
                None => break,
            },
        };
        let mut pending = vec![first];
        let deadline = tokio::time::Instant::now() + config.max_delay;
        loop {
            if pending.len() >= config.max_items {
                counters.item_bound_closures.fetch_add(1, Ordering::Relaxed);
                break;
            }
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => {
                    counters.delay_bound_closures.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                next = receiver.recv() => {
                    let Some(next) = next else {
                        counters.sender_closed_closures.fetch_add(1, Ordering::Relaxed);
                        break;
                    };
                    if next.item.credential != pending[0].item.credential {
                        counters.credential_bound_closures.fetch_add(1, Ordering::Relaxed);
                        carried = Some(next);
                        break;
                    }
                    let mut candidate = pending
                        .iter()
                        .map(|item| item.item.clone())
                        .collect::<Vec<_>>();
                    candidate.push(next.item.clone());
                    match transaction_batch_entry_bytes(&candidate) {
                        Ok(bytes) if bytes <= config.max_entry_bytes => pending.push(next),
                        Ok(_) => {
                            counters.byte_bound_closures.fetch_add(1, Ordering::Relaxed);
                            carried = Some(next);
                            break;
                        }
                        Err(error) => {
                            let _ = next.result.send(Err(error));
                        }
                    }
                }
            }
        }
        resolve_transaction_batch(&client, pending, &counters).await;
    }
}

async fn resolve_transaction_batch(
    client: &TransactionLogClient,
    pending: Vec<PendingTransaction>,
    counters: &TransactionBatcherCounters,
) {
    let items = pending
        .iter()
        .map(|request| request.item.clone())
        .collect::<Vec<_>>();
    let entry_bytes = transaction_batch_entry_bytes(&items).unwrap_or(usize::MAX);
    counters.batches.fetch_add(1, Ordering::Relaxed);
    counters.max_observed_batch_items.fetch_max(
        u64::try_from(items.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    counters.max_observed_entry_bytes.fetch_max(
        u64::try_from(entry_bytes).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    match client.commit_batch(&items).await {
        Ok(response)
            if response.items.len() == pending.len()
                && response
                    .items
                    .iter()
                    .zip(&pending)
                    .all(|(result, request)| result.identity == request.item.identity) =>
        {
            for (request, result) in pending.into_iter().zip(response.items) {
                let outcome = result.transaction.ok_or_else(|| {
                    format!(
                        "transaction batch item {:?} failed: {:?}",
                        result.identity, result.error
                    )
                });
                if outcome.is_ok() {
                    counters.resolved_items.fetch_add(1, Ordering::Relaxed);
                }
                let _ = request.result.send(outcome);
            }
        }
        Ok(_) => {
            counters.failed_batches.fetch_add(1, Ordering::Relaxed);
            for request in pending {
                let _ = request.result.send(Err(
                    "transaction batch response identities changed".to_owned()
                ));
            }
        }
        Err(error) => {
            counters.failed_batches.fetch_add(1, Ordering::Relaxed);
            for request in pending {
                let _ = request.result.send(Err(error.clone()));
            }
        }
    }
}

/// One monotonic exact-retry cutoff for a stable transaction client.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TransactionRetryFloor {
    pub client_id: u64,
    pub through_request_id: u64,
}

/// Replicated advancement of the resolver and transaction-retry frontiers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionFrontierAdvance {
    pub sequence: u64,
    pub conflict_retention_floor: u64,
    pub retry_floors: Vec<TransactionRetryFloor>,
}

impl TransactionFrontierAdvance {
    /// Encode one split-frontier command inside a generation-fenced client
    /// command.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = FRONTIER_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(FRONTIER_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// Exact response retained for the latest split-frontier command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionFrontierApplyResponse {
    pub applied_log_index: u64,
    pub sequence: u64,
    pub conflict_retention_floor: u64,
    pub retry_floors: Vec<TransactionRetryFloor>,
    pub pruned_conflict_versions: u64,
    pub pruned_retry_outcomes: u64,
}

/// Generation-bound request to make one immutable object closure authoritative
/// through its covered version and physically pop the recovery stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectFrontierAdvance {
    pub frontier: ObjectFrontierRecord,
}

impl ObjectFrontierAdvance {
    /// Encode one object-frontier transition inside a generation-fenced client
    /// command.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = OBJECT_FRONTIER_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(OBJECT_FRONTIER_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// Exact response retained for the latest physical object-frontier apply.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectFrontierApplyResponse {
    pub applied_log_position: ObjectFrontierLogPosition,
    pub frontier: ObjectFrontierRecord,
    pub prior_retention_floor: u64,
    pub retention_floor: u64,
    pub popped_records: u64,
}

/// One frozen, paginated read from the retained transaction stream.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetainedTransactionReadRequest {
    pub after_version_exclusive: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_batch_order_exclusive: Option<u16>,
    pub through_version_inclusive: Option<u64>,
    pub max_records: u32,
}

/// One linearizable page from the retained transaction stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetainedTransactionReadResponse {
    pub format_version: u32,
    pub retention_floor: u64,
    pub high_watermark: u64,
    pub target_version: u64,
    pub next_after_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_batch_order: Option<u16>,
    pub complete: bool,
    pub records: Vec<RetainedTransactionRecord>,
}

/// Non-mutating request for exact state-machine storage accounting.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionLogStorageStatsRequest {
    pub projected_retention_floor: Option<u64>,
}

/// Exact serialized state accounting used by the bounded-state falsifier.
#[doc(hidden)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionLogStorageStats {
    pub format_version: u32,
    pub high_watermark: u64,
    pub retention_floor: u64,
    pub projected_retention_floor: u64,
    pub conflict_retention_floor: u64,
    pub retry_clients: u64,
    pub live_keys: u64,
    pub retained_conflict_versions: u64,
    pub durable_outcomes: u64,
    pub request_fingerprints: u64,
    pub transaction_retry_outcomes: u64,
    pub transaction_retry_fingerprints: u64,
    pub retained_records: u64,
    pub projected_retained_records: u64,
    pub snapshot_bytes: u64,
    pub projected_snapshot_bytes: u64,
    pub transaction_authority_bytes: u64,
    pub serving_state_bytes: u64,
    pub resolver_state_bytes: u64,
    pub transaction_retry_state_bytes: u64,
    pub transaction_frontier_state_bytes: u64,
    pub retained_transactions_bytes: u64,
    pub projected_retained_transactions_bytes: u64,
    pub durable_outcomes_bytes: u64,
    pub request_fingerprints_bytes: u64,
}

/// Client for committed transactions and the authority-owned recovery stream.
#[derive(Clone, Debug)]
pub struct TransactionLogClient {
    endpoints: Vec<String>,
}

impl TransactionLogClient {
    /// Create a client over one bounded data-authority endpoint set.
    ///
    /// # Errors
    ///
    /// Returns an error when no valid endpoint is supplied.
    pub fn new(endpoints: Vec<String>) -> Result<Self, String> {
        if endpoints.is_empty() || endpoints.iter().any(String::is_empty) {
            return Err("transaction-log client requires non-empty data endpoints".to_owned());
        }
        Ok(Self { endpoints })
    }

    /// Commit one deterministic transaction through the replicated authority.
    ///
    /// # Errors
    ///
    /// Returns an error when no endpoint commits or resolves the request within
    /// the bounded retry budget.
    pub async fn commit(
        &self,
        identity: RequestIdentity,
        command: &TransactionCommand,
    ) -> Result<TransactionApplyResponse, String> {
        self.commit_internal(identity, command, None).await
    }

    /// Commit one bounded ordered set of independent transactions through one
    /// quorum-durable Raft application entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch is invalid or no endpoint commits and
    /// resolves it within the bounded retry budget.
    pub async fn commit_batch(
        &self,
        items: &[TransactionBatchItem],
    ) -> Result<TransactionBatchApplyResponse, String> {
        let request = transaction_batch_write_request(items)?;
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control::<_, WriteAck>(self.endpoint(attempt), CLIENT_WRITE, &request).await {
                Ok(ack) => match transaction_batch_response(ack) {
                    Ok(response) => return Ok(response),
                    Err(error) => last = error,
                },
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("transaction batch could not be committed: {last}"))
    }

    /// Submit one transaction batch attempt to the first endpoint.
    ///
    /// # Errors
    ///
    /// Returns the first transport, consensus, or application rejection.
    #[doc(hidden)]
    pub async fn commit_batch_once(
        &self,
        items: &[TransactionBatchItem],
    ) -> Result<TransactionBatchApplyResponse, String> {
        let request = transaction_batch_write_request(items)?;
        let ack = control::<_, WriteAck>(self.endpoint(0), CLIENT_WRITE, &request).await?;
        transaction_batch_response(ack)
    }

    /// Commit one transaction through a generation-fenced data authority.
    ///
    /// # Errors
    ///
    /// Returns an error when generation authorization, consensus, or the
    /// transaction authority rejects the request.
    pub async fn commit_fenced(
        &self,
        identity: RequestIdentity,
        credential: &GenerationCredential,
        command: &TransactionCommand,
    ) -> Result<TransactionApplyResponse, String> {
        self.commit_internal(identity, command, Some(credential))
            .await
    }

    async fn commit_internal(
        &self,
        identity: RequestIdentity,
        command: &TransactionCommand,
        credential: Option<&GenerationCredential>,
    ) -> Result<TransactionApplyResponse, String> {
        let request = transaction_write_request(identity, credential, command)?;
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control::<_, WriteAck>(self.endpoint(attempt), CLIENT_WRITE, &request).await {
                Ok(ack) => match transaction_response(ack) {
                    Ok(response) => return Ok(response),
                    Err(error) => last = error,
                },
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("transaction could not be committed: {last}"))
    }

    /// Submit one transaction attempt to the first configured endpoint.
    ///
    /// This is reserved for semantic rejection probes where retrying the same
    /// application error would obscure the result.
    ///
    /// # Errors
    ///
    /// Returns the first transport, consensus, or application rejection.
    #[doc(hidden)]
    pub async fn commit_once(
        &self,
        identity: RequestIdentity,
        command: &TransactionCommand,
    ) -> Result<TransactionApplyResponse, String> {
        let request = transaction_write_request(identity, None, command)?;
        let ack = control::<_, WriteAck>(self.endpoint(0), CLIENT_WRITE, &request).await?;
        transaction_response(ack)
    }

    /// Submit one generation-fenced transaction attempt to the first endpoint.
    ///
    /// This is reserved for semantic rejection probes where retrying an
    /// expected application error would append redundant rejected entries.
    ///
    /// # Errors
    ///
    /// Returns the first transport, authorization, consensus, or application
    /// rejection.
    #[doc(hidden)]
    pub async fn commit_fenced_once(
        &self,
        identity: RequestIdentity,
        credential: &GenerationCredential,
        command: &TransactionCommand,
    ) -> Result<TransactionApplyResponse, String> {
        let request = transaction_write_request(identity, Some(credential), command)?;
        let ack = control::<_, WriteAck>(self.endpoint(0), CLIENT_WRITE, &request).await?;
        transaction_response(ack)
    }

    /// Observe the deliberately unsafe acknowledgement-before-quorum subject.
    ///
    /// This bypasses normal response validation only for the frozen durability
    /// poison. Production callers must use `commit` or `commit_fenced`.
    ///
    /// # Errors
    ///
    /// Returns a transport error or an acknowledgement whose committed bit is
    /// false.
    #[doc(hidden)]
    pub async fn acknowledge_without_outcome_once(
        &self,
        identity: RequestIdentity,
        command: &TransactionCommand,
    ) -> Result<(), String> {
        let request = transaction_write_request(identity, None, command)?;
        let ack = control::<_, WriteAck>(self.endpoint(0), CLIENT_WRITE, &request).await?;
        if ack.committed {
            Ok(())
        } else {
            Err("unsafe acknowledgement did not claim commit".to_owned())
        }
    }

    /// Observe acknowledgement of a batch before quorum durability.
    ///
    /// # Errors
    ///
    /// Returns a transport error or an acknowledgement whose committed bit is
    /// false.
    #[doc(hidden)]
    pub async fn acknowledge_batch_without_outcome_once(
        &self,
        items: &[TransactionBatchItem],
    ) -> Result<(), String> {
        let request = transaction_batch_write_request(items)?;
        let ack = control::<_, WriteAck>(self.endpoint(0), CLIENT_WRITE, &request).await?;
        if ack.committed {
            Ok(())
        } else {
            Err("unsafe batch acknowledgement did not claim commit".to_owned())
        }
    }

    /// Atomically advance the resolver and transaction-retry retention floors.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is rejected or no endpoint resolves
    /// it within the bounded retry budget.
    pub async fn advance_frontiers(
        &self,
        identity: RequestIdentity,
        advance: &TransactionFrontierAdvance,
    ) -> Result<TransactionFrontierApplyResponse, String> {
        self.advance_frontiers_internal(identity, None, advance)
            .await
    }

    /// Atomically advance resolver and retry retention floors on a
    /// generation-fenced data authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the credential is stale, the frontier command is
    /// rejected, or no endpoint resolves it within the bounded retry budget.
    pub async fn advance_frontiers_fenced(
        &self,
        identity: RequestIdentity,
        credential: &GenerationCredential,
        advance: &TransactionFrontierAdvance,
    ) -> Result<TransactionFrontierApplyResponse, String> {
        self.advance_frontiers_internal(identity, Some(credential), advance)
            .await
    }

    async fn advance_frontiers_internal(
        &self,
        identity: RequestIdentity,
        credential: Option<&GenerationCredential>,
        advance: &TransactionFrontierAdvance,
    ) -> Result<TransactionFrontierApplyResponse, String> {
        let app_data = ClientCommand {
            identity,
            credential: credential.cloned(),
            payload: advance.encode().map_err(|error| error.to_string())?,
        }
        .encode()
        .map_err(|error| error.to_string())?;
        let request = ControlWrite {
            app_data,
            drop_reply_after_commit: false,
            credential: credential.cloned(),
        };
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control::<_, WriteAck>(self.endpoint(attempt), CLIENT_WRITE, &request).await {
                Ok(ack) => match frontier_response(ack) {
                    Ok(response) => return Ok(response),
                    Err(error) => last = error,
                },
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "transaction frontiers could not be advanced: {last}"
        ))
    }

    /// Physically pop the retained recovery stream behind an exact pending
    /// object frontier.
    ///
    /// # Errors
    ///
    /// Returns an error when publication state does not retain the exact
    /// frontier, generation fencing rejects the command, replicated apply
    /// rejects the bounds, or no endpoint resolves the outcome.
    pub async fn advance_object_frontier(
        &self,
        identity: RequestIdentity,
        credential: &GenerationCredential,
        advance: &ObjectFrontierAdvance,
    ) -> Result<ObjectFrontierApplyResponse, String> {
        let app_data = ClientCommand {
            identity,
            credential: Some(credential.clone()),
            payload: advance.encode().map_err(|error| error.to_string())?,
        }
        .encode()
        .map_err(|error| error.to_string())?;
        let request = ControlWrite {
            app_data,
            drop_reply_after_commit: false,
            credential: Some(credential.clone()),
        };
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control::<_, WriteAck>(self.endpoint(attempt), CLIENT_WRITE, &request).await {
                Ok(ack) => match object_frontier_response(ack) {
                    Ok(response) => return Ok(response),
                    Err(error) => last = error,
                },
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!("object frontier could not be advanced: {last}"))
    }

    /// Submit one physical object-frontier attempt without retrying a semantic
    /// rejection. Reserved for falsifier controls.
    ///
    /// # Errors
    ///
    /// Returns the first transport, authorization, consensus, or apply error.
    #[doc(hidden)]
    pub async fn advance_object_frontier_once(
        &self,
        identity: RequestIdentity,
        credential: &GenerationCredential,
        advance: &ObjectFrontierAdvance,
    ) -> Result<ObjectFrontierApplyResponse, String> {
        let app_data = ClientCommand {
            identity,
            credential: Some(credential.clone()),
            payload: advance.encode().map_err(|error| error.to_string())?,
        }
        .encode()
        .map_err(|error| error.to_string())?;
        let request = ControlWrite {
            app_data,
            drop_reply_after_commit: false,
            credential: Some(credential.clone()),
        };
        let ack = control::<_, WriteAck>(self.endpoint(0), CLIENT_WRITE, &request).await?;
        object_frontier_response(ack)
    }

    /// Collect local applied-frontier attestations from reachable data voters.
    ///
    /// The caller passes the returned set to the publication authority, which
    /// performs membership and quorum verification. Unreachable voters are
    /// omitted; an empty result is an error.
    ///
    /// # Errors
    ///
    /// Returns an error when no configured voter can attest the exact statement.
    pub async fn attest_object_frontier(
        &self,
        statement: &ObjectFrontierCertificateStatement,
    ) -> Result<Vec<ObjectFrontierAttestation>, String> {
        let mut attestations = Vec::new();
        let mut errors = Vec::new();
        for endpoint in &self.endpoints {
            match control(endpoint, crate::rpc::OBJECT_FRONTIER_ATTEST, statement).await {
                Ok(attestation) => attestations.push(attestation),
                Err(error) => errors.push(error),
            }
        }
        if attestations.is_empty() {
            Err(format!(
                "no data voter attested the object frontier: {}",
                errors.join("; ")
            ))
        } else {
            Ok(attestations)
        }
    }

    /// Read one linearizable, frozen page from the retained transaction stream.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid or unavailable suffix, or when no data
    /// endpoint serves a linearizable page within the bounded retry budget.
    pub async fn read(
        &self,
        request: RetainedTransactionReadRequest,
    ) -> Result<RetainedTransactionReadResponse, String> {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control(
                self.endpoint(attempt),
                crate::rpc::TRANSACTION_LOG_READ,
                &request,
            )
            .await
            {
                Ok(response) => return Ok(response),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "retained transaction page could not be read: {last}"
        ))
    }

    /// Read exact state-machine bytes and one non-mutating stream-pop projection.
    ///
    /// # Errors
    ///
    /// Returns an error when no data endpoint serves a linearizable response or
    /// the projected floor is outside the retained stream bounds.
    #[doc(hidden)]
    pub async fn storage_stats(
        &self,
        request: TransactionLogStorageStatsRequest,
    ) -> Result<TransactionLogStorageStats, String> {
        let mut last = String::new();
        for attempt in 0..RETRY_ATTEMPTS {
            match control(
                self.endpoint(attempt),
                crate::rpc::TRANSACTION_LOG_STORAGE_STATS,
                &request,
            )
            .await
            {
                Ok(response) => return Ok(response),
                Err(error) => last = error,
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(format!(
            "transaction-log storage stats could not be read: {last}"
        ))
    }

    fn endpoint(&self, attempt: usize) -> &str {
        &self.endpoints[attempt % self.endpoints.len()]
    }
}

fn transaction_response(ack: WriteAck) -> Result<TransactionApplyResponse, String> {
    if !ack.committed {
        return Err("transaction command was not quorum committed".to_owned());
    }
    let response = ack
        .response
        .and_then(|response| response.transaction)
        .ok_or_else(|| "transaction response is absent".to_owned())?;
    if matches!(response.status, TransactionStatus::Rejected { .. }) {
        return Err(format!("transaction was rejected: {:?}", response.status));
    }
    Ok(response)
}

fn transaction_batch_response(ack: WriteAck) -> Result<TransactionBatchApplyResponse, String> {
    if !ack.committed {
        return Err("transaction batch was not quorum committed".to_owned());
    }
    ack.response
        .and_then(|response| response.transaction_batch)
        .ok_or_else(|| "transaction batch response is absent".to_owned())
}

fn transaction_batch_write_request(items: &[TransactionBatchItem]) -> Result<ControlWrite, String> {
    if items.is_empty() || items.len() > 32 {
        return Err("transaction batch must contain between 1 and 32 items".to_owned());
    }
    let credential = items[0].credential.clone();
    if items.iter().any(|item| item.credential != credential) {
        return Err("transaction batch items must use one generation credential".to_owned());
    }
    let commands = items
        .iter()
        .map(|item| {
            Ok(ClientCommand {
                identity: item.identity,
                credential: item.credential.clone(),
                payload: item.command.encode().map_err(|error| error.to_string())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let app_data = TransactionBatchCommand { commands }
        .encode()
        .map_err(|error| error.to_string())?;
    Ok(ControlWrite {
        app_data,
        drop_reply_after_commit: false,
        credential,
    })
}

fn transaction_batch_entry_bytes(items: &[TransactionBatchItem]) -> Result<usize, String> {
    transaction_batch_write_request(items).map(|request| request.app_data.len())
}

fn transaction_write_request(
    identity: RequestIdentity,
    credential: Option<&GenerationCredential>,
    command: &TransactionCommand,
) -> Result<ControlWrite, String> {
    let app_data = ClientCommand {
        identity,
        credential: credential.cloned(),
        payload: command.encode().map_err(|error| error.to_string())?,
    }
    .encode()
    .map_err(|error| error.to_string())?;
    Ok(ControlWrite {
        app_data,
        drop_reply_after_commit: false,
        credential: credential.cloned(),
    })
}

fn frontier_response(ack: WriteAck) -> Result<TransactionFrontierApplyResponse, String> {
    if !ack.committed {
        return Err("transaction frontier command was not quorum committed".to_owned());
    }
    ack.response
        .and_then(|response| response.transaction_frontier)
        .ok_or_else(|| "transaction frontier response is absent".to_owned())
}

fn object_frontier_response(ack: WriteAck) -> Result<ObjectFrontierApplyResponse, String> {
    if !ack.committed {
        return Err("object frontier command was not quorum committed".to_owned());
    }
    ack.response
        .and_then(|response| response.object_frontier)
        .ok_or_else(|| "object frontier response is absent".to_owned())
}

async fn control<Req, Resp>(endpoint: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: for<'de> Deserialize<'de>,
{
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("connect timed out at {endpoint}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, kind, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<Resp, String> =
        tokio::time::timeout(Duration::from_secs(8), read_response(&mut stream))
            .await
            .map_err(|_| format!("response timed out at {endpoint}"))?
            .map_err(|error| error.to_string())?;
    response
}

#[cfg(test)]
mod tests {
    use super::{
        transaction_batch_entry_bytes, TransactionBatchItem, TransactionBatcherConfig,
        TransactionLogClient,
    };
    use crate::RequestIdentity;
    use okv_transaction::{KeyRange, Mutation, TransactionCommand};
    use std::time::Duration;

    #[test]
    fn endpoint_set_must_be_non_empty() {
        assert!(TransactionLogClient::new(Vec::new()).is_err());
        assert!(TransactionLogClient::new(vec![String::new()]).is_err());
        assert!(TransactionLogClient::new(vec!["127.0.0.1:1".to_owned()]).is_ok());
    }

    #[test]
    fn batcher_bounds_are_fail_closed() {
        let valid = TransactionBatcherConfig {
            max_items: 16,
            max_entry_bytes: 256 * 1_024,
            max_delay: Duration::from_millis(2),
            queue_capacity: 2_048,
        };
        assert!(valid.validate().is_ok());
        assert!(TransactionBatcherConfig {
            max_items: 0,
            ..valid
        }
        .validate()
        .is_err());
        assert!(TransactionBatcherConfig {
            queue_capacity: 8,
            ..valid
        }
        .validate()
        .is_err());
        assert!(TransactionBatcherConfig {
            max_delay: Duration::ZERO,
            ..valid
        }
        .validate()
        .is_err());
    }

    #[test]
    fn exact_batch_entry_bytes_grow_with_items_and_values() {
        let item = |request_id, value_bytes| TransactionBatchItem {
            identity: RequestIdentity {
                client_id: 7,
                request_id,
            },
            credential: None,
            command: TransactionCommand {
                read_version: 0,
                read_conflicts: Vec::new(),
                write_conflicts: vec![KeyRange::point(b"key")],
                mutations: vec![Mutation::Set {
                    key: b"key".to_vec(),
                    value: vec![1; value_bytes],
                }],
            },
        };
        let one = transaction_batch_entry_bytes(&[item(1, 8)]).expect("encode one item");
        let two =
            transaction_batch_entry_bytes(&[item(1, 8), item(2, 8)]).expect("encode two items");
        let large = transaction_batch_entry_bytes(&[item(1, 1_024)]).expect("encode large item");
        assert!(two > one);
        assert!(large > one);
    }
}
