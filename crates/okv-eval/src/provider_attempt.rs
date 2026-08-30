//! Provider-call evidence for the RFC-0046 cold-read boundary.

use async_trait::async_trait;
use bytes::Bytes;
use okv_object::{
    Backend, BackendDescriptor, BackendRead, RevisionToken, StoreError, WriteCondition,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

tokio::task_local! {
    static LOGICAL_OPERATION_ID: u64;
}

/// Run one future with a logical operation identity visible to every provider
/// attempt issued by that future.
///
/// Nested provider calls retain their own provider operation IDs. This scoped
/// identity only correlates those attempts with the application operation that
/// caused them.
pub async fn scope_logical_operation<F>(logical_operation_id: u64, future: F) -> F::Output
where
    F: Future,
{
    LOGICAL_OPERATION_ID
        .scope(logical_operation_id, future)
        .await
}

/// Lifecycle phase for one provider call.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAttemptPhase {
    Started,
    Completed,
}

/// One event emitted immediately before or after a provider call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAttemptEventV1 {
    pub schema_version: u32,
    pub sequence: u64,
    pub operation_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_operation_id: Option<u64>,
    pub attempt_ordinal: u32,
    pub subject: String,
    pub provider: String,
    pub phase: ProviderAttemptPhase,
    pub api: String,
    pub object_key: String,
    pub requested_range: Option<Range<u64>>,
    pub expected_revision: Option<RevisionToken>,
    pub started_unix_nanos: u64,
    pub started_monotonic_nanos: u64,
    pub result: Option<String>,
    pub returned_revision: Option<RevisionToken>,
    pub object_length: Option<u64>,
    pub returned_range: Option<Range<u64>>,
    pub request_payload_bytes: u64,
    pub response_payload_bytes: u64,
    pub elapsed_nanos: u64,
}

/// A `Backend` wrapper that records one start and one completion event per call.
#[derive(Debug)]
pub struct ProviderAttemptBackend {
    inner: Arc<dyn Backend>,
    subject: String,
    provider: String,
    next_operation_id: AtomicU64,
    next_sequence: AtomicU64,
    monotonic_epoch: Instant,
    events: Mutex<Vec<ProviderAttemptEventV1>>,
}

impl ProviderAttemptBackend {
    /// Wrap a backend under one immutable evaluation subject name.
    ///
    /// # Errors
    ///
    /// Returns an error when the subject is empty.
    pub fn new(inner: Arc<dyn Backend>, subject: impl Into<String>) -> Result<Self, String> {
        let subject = subject.into();
        if subject.trim().is_empty() {
            return Err("provider-attempt subject must not be empty".to_owned());
        }
        let provider = inner.descriptor().id;
        Ok(Self {
            inner,
            subject,
            provider,
            next_operation_id: AtomicU64::new(1),
            next_sequence: AtomicU64::new(1),
            monotonic_epoch: Instant::now(),
            events: Mutex::new(Vec::new()),
        })
    }

    /// Return a stable snapshot of all events in emission order.
    #[must_use]
    pub fn events(&self) -> Vec<ProviderAttemptEventV1> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Clear recorded events without changing provider state or identifiers.
    pub fn clear_events(&self) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn start(
        &self,
        api: &str,
        object_key: &str,
        requested_range: Option<Range<u64>>,
        expected_revision: Option<&RevisionToken>,
        request_payload_bytes: u64,
    ) -> AttemptContext {
        let operation_id = self.next_operation_id.fetch_add(1, Ordering::SeqCst);
        let started_unix_nanos = unix_nanos();
        let started = Instant::now();
        let started_monotonic_nanos = u64::try_from(
            started
                .saturating_duration_since(self.monotonic_epoch)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);
        let context = AttemptContext {
            operation_id,
            logical_operation_id: LOGICAL_OPERATION_ID.try_with(|value| *value).ok(),
            api: api.to_owned(),
            object_key: object_key.to_owned(),
            requested_range,
            expected_revision: expected_revision.cloned(),
            request_payload_bytes,
            started_unix_nanos,
            started_monotonic_nanos,
            started,
        };
        self.push_event(ProviderAttemptEventV1 {
            schema_version: 1,
            sequence: self.next_sequence.fetch_add(1, Ordering::SeqCst),
            operation_id,
            logical_operation_id: context.logical_operation_id,
            attempt_ordinal: 1,
            subject: self.subject.clone(),
            provider: self.provider.clone(),
            phase: ProviderAttemptPhase::Started,
            api: context.api.clone(),
            object_key: context.object_key.clone(),
            requested_range: context.requested_range.clone(),
            expected_revision: context.expected_revision.clone(),
            started_unix_nanos,
            started_monotonic_nanos,
            result: None,
            returned_revision: None,
            object_length: None,
            returned_range: None,
            request_payload_bytes,
            response_payload_bytes: 0,
            elapsed_nanos: 0,
        });
        context
    }

    fn complete_read(&self, context: AttemptContext, result: &Result<BackendRead, StoreError>) {
        let (result_name, revision, object_length, returned_range, response_payload_bytes) =
            match result {
                Ok(read) => (
                    "ok".to_owned(),
                    Some(read.revision.clone()),
                    Some(read.object_length),
                    Some(read.returned_range.clone()),
                    u64::try_from(read.bytes.len()).unwrap_or(u64::MAX),
                ),
                Err(error) => (error.class.to_string(), None, None, None, 0),
            };
        self.complete(
            context,
            result_name,
            revision,
            object_length,
            returned_range,
            response_payload_bytes,
        );
    }

    fn complete_revision(
        &self,
        context: AttemptContext,
        result: &Result<RevisionToken, StoreError>,
    ) {
        let (result_name, revision) = match result {
            Ok(revision) => ("ok".to_owned(), Some(revision.clone())),
            Err(error) => (error.class.to_string(), None),
        };
        self.complete(context, result_name, revision, None, None, 0);
    }

    fn complete_unit(&self, context: AttemptContext, result: &Result<(), StoreError>) {
        let result_name = result
            .as_ref()
            .map_or_else(|error| error.class.to_string(), |()| "ok".to_owned());
        self.complete(context, result_name, None, None, None, 0);
    }

    fn complete_list(&self, context: AttemptContext, result: &Result<Vec<String>, StoreError>) {
        let result_name = result
            .as_ref()
            .map_or_else(|error| error.class.to_string(), |_| "ok".to_owned());
        self.complete(context, result_name, None, None, None, 0);
    }

    fn complete(
        &self,
        context: AttemptContext,
        result: String,
        returned_revision: Option<RevisionToken>,
        object_length: Option<u64>,
        returned_range: Option<Range<u64>>,
        response_payload_bytes: u64,
    ) {
        let elapsed_nanos = u64::try_from(context.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.push_event(ProviderAttemptEventV1 {
            schema_version: 1,
            sequence: self.next_sequence.fetch_add(1, Ordering::SeqCst),
            operation_id: context.operation_id,
            logical_operation_id: context.logical_operation_id,
            attempt_ordinal: 1,
            subject: self.subject.clone(),
            provider: self.provider.clone(),
            phase: ProviderAttemptPhase::Completed,
            api: context.api,
            object_key: context.object_key,
            requested_range: context.requested_range,
            expected_revision: context.expected_revision,
            started_unix_nanos: context.started_unix_nanos,
            started_monotonic_nanos: context.started_monotonic_nanos,
            result: Some(result),
            returned_revision,
            object_length,
            returned_range,
            request_payload_bytes: context.request_payload_bytes,
            response_payload_bytes,
            elapsed_nanos,
        });
    }

    fn push_event(&self, event: ProviderAttemptEventV1) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
    }
}

#[derive(Debug)]
struct AttemptContext {
    operation_id: u64,
    logical_operation_id: Option<u64>,
    api: String,
    object_key: String,
    requested_range: Option<Range<u64>>,
    expected_revision: Option<RevisionToken>,
    request_payload_bytes: u64,
    started_unix_nanos: u64,
    started_monotonic_nanos: u64,
    started: Instant,
}

#[async_trait]
impl Backend for ProviderAttemptBackend {
    fn descriptor(&self) -> BackendDescriptor {
        self.inner.descriptor()
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        condition: WriteCondition,
    ) -> Result<RevisionToken, StoreError> {
        let context = self.start(
            "put",
            key,
            None,
            None,
            u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        );
        let result = self.inner.put(key, bytes, condition).await;
        self.complete_revision(context, &result);
        result
    }

    async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
        expected: Option<&RevisionToken>,
    ) -> Result<BackendRead, StoreError> {
        let context = self.start("get", key, range.clone(), expected, 0);
        let result = self.inner.get(key, range, expected).await;
        self.complete_read(context, &result);
        result
    }

    async fn delete(&self, key: &str, expected: Option<&RevisionToken>) -> Result<(), StoreError> {
        let context = self.start("delete", key, None, expected, 0);
        let result = self.inner.delete(key, expected).await;
        self.complete_unit(context, &result);
        result
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let context = self.start("list", prefix, None, None, 0);
        let result = self.inner.list(prefix).await;
        self.complete_list(context, &result);
        result
    }
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::{scope_logical_operation, ProviderAttemptBackend, ProviderAttemptPhase};
    use bytes::Bytes;
    use okv_object::{memory_backend, Backend, WriteCondition};
    use std::sync::Arc;

    #[tokio::test]
    async fn records_one_ordered_pair_for_each_provider_call() {
        let backend = Arc::new(
            ProviderAttemptBackend::new(memory_backend(), "candidate").expect("observed backend"),
        );
        let revision = backend
            .put(
                "fixture/data",
                Bytes::from_static(b"0123456789"),
                WriteCondition::Create,
            )
            .await
            .expect("put");
        backend.clear_events();
        let read = backend
            .get("fixture/data", Some(2..6), Some(&revision))
            .await
            .expect("range read");
        assert_eq!(read.bytes, Bytes::from_static(b"2345"));

        let events = backend.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence + 1, events[1].sequence);
        assert_eq!(events[0].operation_id, events[1].operation_id);
        assert_eq!(events[0].phase, ProviderAttemptPhase::Started);
        assert_eq!(events[1].phase, ProviderAttemptPhase::Completed);
        assert_eq!(events[0].requested_range, Some(2..6));
        assert_eq!(events[1].returned_range, Some(2..6));
        assert_eq!(events[1].response_payload_bytes, 4);
        assert_eq!(events[1].result.as_deref(), Some("ok"));
        assert_eq!(events[1].attempt_ordinal, 1);
    }

    #[tokio::test]
    async fn preserves_error_class_and_zero_response_bytes() {
        let backend = Arc::new(
            ProviderAttemptBackend::new(memory_backend(), "raw_range_control")
                .expect("observed backend"),
        );
        assert!(backend.get("missing", None, None).await.is_err());
        let events = backend.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].result.as_deref(), Some("not_found"));
        assert_eq!(events[1].response_payload_bytes, 0);
    }

    #[tokio::test]
    async fn binds_provider_attempts_to_the_scoped_logical_operation() {
        let backend = Arc::new(
            ProviderAttemptBackend::new(memory_backend(), "candidate").expect("observed backend"),
        );
        let revision = backend
            .put(
                "fixture/data",
                Bytes::from_static(b"0123456789"),
                WriteCondition::Create,
            )
            .await
            .expect("put");
        backend.clear_events();

        scope_logical_operation(77, async {
            backend
                .get("fixture/data", Some(1..3), Some(&revision))
                .await
                .expect("first range");
            backend
                .get("fixture/data", Some(4..6), Some(&revision))
                .await
                .expect("second range");
        })
        .await;

        let events = backend.events();
        assert_eq!(events.len(), 4);
        assert!(events
            .iter()
            .all(|event| event.logical_operation_id == Some(77)));
        assert_ne!(events[0].operation_id, events[2].operation_id);
    }
}
