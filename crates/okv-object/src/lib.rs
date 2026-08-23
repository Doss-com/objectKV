//! Object-store correctness boundary and conformance probes for objectKV.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    Error as ObjectStoreError, GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    UpdateVersion,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

pub const OBJECT_STORE_DRIVER_VERSION: &str = "0.14.1";

mod publication_adapter;

pub use publication_adapter::{
    run_publication_adapter_contract, PublicationAdapterMode, PublicationAdapterReport,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceProfile {
    Segment,
    Authority,
}

impl Display for ConformanceProfile {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Segment => formatter.write_str("segment"),
            Self::Authority => formatter.write_str("authority"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BackendDescriptor {
    pub id: String,
    pub driver: String,
    pub driver_version: String,
    pub server_version: String,
    pub conditional_primitive: String,
    pub guarded_delete: bool,
    pub delete_strategy: String,
}

#[derive(Clone, Debug, Default)]
pub struct ConformanceOptions {
    pub inject_immutable_overwrite_bug: bool,
    pub inject_list_authority_bug: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RevisionToken {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl RevisionToken {
    fn is_present(&self) -> bool {
        self.e_tag.is_some() || self.version.is_some()
    }

    fn matches(&self, other: &Self) -> bool {
        match (&self.version, &other.version) {
            (Some(expected), Some(actual)) => expected == actual,
            _ => match (&self.e_tag, &other.e_tag) {
                (Some(expected), Some(actual)) => expected == actual,
                _ => false,
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectIdentity {
    pub revision: RevisionToken,
    pub length: u64,
    pub sha256: String,
}

#[derive(Clone, Debug)]
pub enum WriteCondition {
    Overwrite,
    Create,
    Update(RevisionToken),
}

impl WriteCondition {
    fn api_name(&self) -> &'static str {
        match self {
            Self::Overwrite => "put.overwrite",
            Self::Create => "put.create",
            Self::Update(_) => "put.update",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    AlreadyExists,
    PreconditionFailed,
    NotFound,
    Corrupt,
    RetryableUnobserved,
    RetryableUnknown,
    Throttled,
    PermissionDenied,
    Unsupported,
    Other,
}

impl Display for ErrorClass {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyExists => "already_exists",
            Self::PreconditionFailed => "precondition_failed",
            Self::NotFound => "not_found",
            Self::Corrupt => "corrupt",
            Self::RetryableUnobserved => "retryable_unobserved",
            Self::RetryableUnknown => "retryable_unknown",
            Self::Throttled => "throttled",
            Self::PermissionDenied => "permission_denied",
            Self::Unsupported => "unsupported",
            Self::Other => "other",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreError {
    pub class: ErrorClass,
    pub detail: String,
}

impl StoreError {
    fn new(class: ErrorClass, detail: impl Into<String>) -> Self {
        Self {
            class,
            detail: detail.into(),
        }
    }
}

impl Display for StoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.class, self.detail)
    }
}

impl std::error::Error for StoreError {}

#[derive(Clone, Debug)]
pub struct BackendRead {
    pub bytes: Bytes,
    pub revision: RevisionToken,
    pub object_length: u64,
    pub returned_range: Range<u64>,
}

#[async_trait]
pub trait Backend: Debug + Send + Sync {
    fn descriptor(&self) -> BackendDescriptor;

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        condition: WriteCondition,
    ) -> Result<RevisionToken, StoreError>;

    async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
        expected: Option<&RevisionToken>,
    ) -> Result<BackendRead, StoreError>;

    async fn delete(&self, key: &str, expected: Option<&RevisionToken>) -> Result<(), StoreError>;

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

#[derive(Debug)]
pub struct ObjectStoreBackend {
    descriptor: BackendDescriptor,
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreBackend {
    #[must_use]
    pub fn new(descriptor: BackendDescriptor, store: Arc<dyn ObjectStore>) -> Self {
        Self { descriptor, store }
    }
}

#[async_trait]
impl Backend for ObjectStoreBackend {
    fn descriptor(&self) -> BackendDescriptor {
        self.descriptor.clone()
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        condition: WriteCondition,
    ) -> Result<RevisionToken, StoreError> {
        let mode = match condition {
            WriteCondition::Overwrite => PutMode::Overwrite,
            WriteCondition::Create => PutMode::Create,
            WriteCondition::Update(revision) => PutMode::Update(UpdateVersion {
                e_tag: revision.e_tag,
                version: revision.version,
            }),
        };
        let options = PutOptions {
            mode,
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(&ObjectPath::from(key), bytes.into(), options)
            .await
            .map_err(|error| classify_object_store_error(&error))?;
        Ok(RevisionToken {
            e_tag: result.e_tag,
            version: result.version,
        })
    }

    async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
        expected: Option<&RevisionToken>,
    ) -> Result<BackendRead, StoreError> {
        let mut options = GetOptions::new();
        if let Some(range) = range {
            options = options.with_range(Some(range));
        }
        if let Some(expected) = expected {
            if let Some(e_tag) = &expected.e_tag {
                options = options.with_if_match(Some(e_tag.clone()));
            }
            if let Some(version) = &expected.version {
                options = options.with_version(Some(version.clone()));
            }
        }
        let result = self
            .store
            .get_opts(&ObjectPath::from(key), options)
            .await
            .map_err(|error| classify_object_store_error(&error))?;
        let revision = RevisionToken {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let object_length = result.meta.size;
        let returned_range = result.range.clone();
        let bytes = result
            .bytes()
            .await
            .map_err(|error| classify_object_store_error(&error))?;
        Ok(BackendRead {
            bytes,
            revision,
            object_length,
            returned_range,
        })
    }

    async fn delete(&self, key: &str, expected: Option<&RevisionToken>) -> Result<(), StoreError> {
        if expected.is_some() {
            return Err(StoreError::new(
                ErrorClass::Unsupported,
                "the shared object_store API has no guarded delete operation",
            ));
        }
        self.store
            .delete(&ObjectPath::from(key))
            .await
            .map_err(|error| classify_object_store_error(&error))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.store
            .list(Some(&ObjectPath::from(prefix)))
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .map_err(|error| classify_object_store_error(&error))
    }
}

fn classify_object_store_error(error: &ObjectStoreError) -> StoreError {
    let class = match error {
        ObjectStoreError::AlreadyExists { .. } => ErrorClass::AlreadyExists,
        ObjectStoreError::Precondition { .. } | ObjectStoreError::NotModified { .. } => {
            ErrorClass::PreconditionFailed
        }
        ObjectStoreError::NotFound { .. } => ErrorClass::NotFound,
        ObjectStoreError::NotImplemented { .. } | ObjectStoreError::NotSupported { .. } => {
            ErrorClass::Unsupported
        }
        ObjectStoreError::PermissionDenied { .. } | ObjectStoreError::Unauthenticated { .. } => {
            ErrorClass::PermissionDenied
        }
        ObjectStoreError::Generic { .. } => ErrorClass::RetryableUnknown,
        _ => ErrorClass::Other,
    };
    StoreError::new(class, "object_store operation failed")
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RequestStats {
    pub requests: Vec<RequestStat>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RequestStat {
    pub api: String,
    pub result: String,
    pub count: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub elapsed_micros: u64,
}

#[derive(Clone, Debug, Default)]
struct StatAggregate {
    count: u64,
    request_bytes: u64,
    response_bytes: u64,
    elapsed_micros: u64,
}

#[derive(Debug)]
pub struct ObservedBackend {
    inner: Arc<dyn Backend>,
    stats: Mutex<BTreeMap<(String, String), StatAggregate>>,
}

impl ObservedBackend {
    #[must_use]
    pub fn new(inner: Arc<dyn Backend>) -> Self {
        Self {
            inner,
            stats: Mutex::new(BTreeMap::new()),
        }
    }

    fn record(
        &self,
        api: &str,
        error: Option<&StoreError>,
        request_bytes: u64,
        response_bytes: u64,
        started: Instant,
    ) {
        let result = error.map_or_else(|| "ok".to_owned(), |error| error.class.to_string());
        let elapsed = started.elapsed().as_micros();
        let elapsed_micros = u64::try_from(elapsed).unwrap_or(u64::MAX);
        let mut stats = self
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let aggregate = stats.entry((api.to_owned(), result)).or_default();
        aggregate.count = aggregate.count.saturating_add(1);
        aggregate.request_bytes = aggregate.request_bytes.saturating_add(request_bytes);
        aggregate.response_bytes = aggregate.response_bytes.saturating_add(response_bytes);
        aggregate.elapsed_micros = aggregate.elapsed_micros.saturating_add(elapsed_micros);
    }

    #[must_use]
    pub fn stats(&self) -> RequestStats {
        let stats = self
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        RequestStats {
            requests: stats
                .iter()
                .map(|((api, result), aggregate)| RequestStat {
                    api: api.clone(),
                    result: result.clone(),
                    count: aggregate.count,
                    request_bytes: aggregate.request_bytes,
                    response_bytes: aggregate.response_bytes,
                    elapsed_micros: aggregate.elapsed_micros,
                })
                .collect(),
        }
    }
}

#[async_trait]
impl Backend for ObservedBackend {
    fn descriptor(&self) -> BackendDescriptor {
        self.inner.descriptor()
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        condition: WriteCondition,
    ) -> Result<RevisionToken, StoreError> {
        let api = condition.api_name();
        let request_bytes = byte_len(&bytes);
        let started = Instant::now();
        let result = self.inner.put(key, bytes, condition).await;
        self.record(api, result.as_ref().err(), request_bytes, 0, started);
        result
    }

    async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
        expected: Option<&RevisionToken>,
    ) -> Result<BackendRead, StoreError> {
        let api = if range.is_some() { "get.range" } else { "get" };
        let started = Instant::now();
        let result = self.inner.get(key, range, expected).await;
        let response_bytes = result.as_ref().map_or(0, |read| byte_len(&read.bytes));
        self.record(api, result.as_ref().err(), 0, response_bytes, started);
        result
    }

    async fn delete(&self, key: &str, expected: Option<&RevisionToken>) -> Result<(), StoreError> {
        let api = if expected.is_some() {
            "delete.guarded"
        } else {
            "delete"
        };
        let started = Instant::now();
        let result = self.inner.delete(key, expected).await;
        self.record(api, result.as_ref().err(), 0, 0, started);
        result
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let started = Instant::now();
        let result = self.inner.list(prefix).await;
        self.record("list", result.as_ref().err(), 0, 0, started);
        result
    }
}

#[derive(Debug)]
pub struct FaultBackend {
    inner: Arc<dyn Backend>,
    lose_next_put_response: AtomicBool,
    lose_next_delete_response: AtomicBool,
    corrupt_next_get: AtomicBool,
    shorten_next_get: AtomicBool,
    stale_next_list: AtomicBool,
    list_calls: AtomicU64,
}

impl FaultBackend {
    #[must_use]
    pub fn new(inner: Arc<dyn Backend>) -> Self {
        Self {
            inner,
            lose_next_put_response: AtomicBool::new(false),
            lose_next_delete_response: AtomicBool::new(false),
            corrupt_next_get: AtomicBool::new(false),
            shorten_next_get: AtomicBool::new(false),
            stale_next_list: AtomicBool::new(false),
            list_calls: AtomicU64::new(0),
        }
    }

    pub fn lose_next_put_response(&self) {
        self.lose_next_put_response.store(true, Ordering::SeqCst);
    }

    pub fn lose_next_delete_response(&self) {
        self.lose_next_delete_response.store(true, Ordering::SeqCst);
    }

    pub fn corrupt_next_get(&self) {
        self.corrupt_next_get.store(true, Ordering::SeqCst);
    }

    pub fn shorten_next_get(&self) {
        self.shorten_next_get.store(true, Ordering::SeqCst);
    }

    pub fn stale_next_list(&self) {
        self.stale_next_list.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn list_calls(&self) -> u64 {
        self.list_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Backend for FaultBackend {
    fn descriptor(&self) -> BackendDescriptor {
        self.inner.descriptor()
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        condition: WriteCondition,
    ) -> Result<RevisionToken, StoreError> {
        let result = self.inner.put(key, bytes, condition).await?;
        if self.lose_next_put_response.swap(false, Ordering::SeqCst) {
            Err(StoreError::new(
                ErrorClass::RetryableUnknown,
                "injected lost successful write response",
            ))
        } else {
            Ok(result)
        }
    }

    async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
        expected: Option<&RevisionToken>,
    ) -> Result<BackendRead, StoreError> {
        let mut read = self.inner.get(key, range, expected).await?;
        if self.shorten_next_get.swap(false, Ordering::SeqCst) && !read.bytes.is_empty() {
            read.bytes = read.bytes.slice(..read.bytes.len() - 1);
        }
        if self.corrupt_next_get.swap(false, Ordering::SeqCst) && !read.bytes.is_empty() {
            let mut bytes = read.bytes.to_vec();
            bytes[0] ^= 0x80;
            read.bytes = Bytes::from(bytes);
        }
        Ok(read)
    }

    async fn delete(&self, key: &str, expected: Option<&RevisionToken>) -> Result<(), StoreError> {
        self.inner.delete(key, expected).await?;
        if self.lose_next_delete_response.swap(false, Ordering::SeqCst) {
            Err(StoreError::new(
                ErrorClass::RetryableUnknown,
                "injected lost successful delete response",
            ))
        } else {
            Ok(())
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        if self.stale_next_list.swap(false, Ordering::SeqCst) {
            Ok(Vec::new())
        } else {
            self.inner.list(prefix).await
        }
    }
}

#[derive(Debug)]
struct BrokenConditionalBackend {
    inner: Arc<dyn Backend>,
}

#[async_trait]
impl Backend for BrokenConditionalBackend {
    fn descriptor(&self) -> BackendDescriptor {
        let mut descriptor = self.inner.descriptor();
        descriptor.id.push_str("-broken-overwrite");
        descriptor
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        _condition: WriteCondition,
    ) -> Result<RevisionToken, StoreError> {
        self.inner.put(key, bytes, WriteCondition::Overwrite).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
        expected: Option<&RevisionToken>,
    ) -> Result<BackendRead, StoreError> {
        self.inner.get(key, range, expected).await
    }

    async fn delete(&self, key: &str, expected: Option<&RevisionToken>) -> Result<(), StoreError> {
        self.inner.delete(key, expected).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix).await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutOutcome {
    Created,
    ExistingIdentical,
    LostResponseRecovered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateOutcome {
    Updated,
    LostResponseRecovered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteOutcome {
    Deleted,
    LostResponseRecovered,
    AlreadyAbsent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeletePermit {
    key: String,
    identity: ObjectIdentity,
    plan_id: String,
    authority_revision: u64,
}

impl DeletePermit {
    fn new(
        key: impl Into<String>,
        identity: ObjectIdentity,
        plan_id: impl Into<String>,
        authority_revision: u64,
    ) -> Self {
        Self {
            key: key.into(),
            identity,
            plan_id: plan_id.into(),
            authority_revision,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    #[must_use]
    pub const fn authority_revision(&self) -> u64 {
        self.authority_revision
    }
}

#[derive(Debug)]
pub struct ObjectClient {
    backend: Arc<dyn Backend>,
}

impl ObjectClient {
    #[must_use]
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    #[must_use]
    pub fn descriptor(&self) -> BackendDescriptor {
        self.backend.descriptor()
    }

    /// Discover candidate names. Callers must never interpret this as liveness.
    ///
    /// # Errors
    ///
    /// Returns a classified backend error.
    pub async fn list_candidates(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.backend.list(prefix).await
    }

    /// Delete an immutable object while an authority-owned reservation remains live.
    ///
    /// # Errors
    ///
    /// Returns a classified identity, delete, or unknown-outcome error.
    pub async fn delete_reserved(
        &self,
        permit: &DeletePermit,
    ) -> Result<DeleteOutcome, StoreError> {
        let guarded = self.backend.descriptor().guarded_delete;
        if !guarded {
            self.get_full_verified(
                &permit.key,
                Some(&permit.identity.revision),
                permit.identity.length,
                &permit.identity.sha256,
            )
            .await?;
        }

        let expected = guarded.then_some(&permit.identity.revision);
        match self.backend.delete(&permit.key, expected).await {
            Ok(()) => Ok(DeleteOutcome::Deleted),
            Err(error) if error.class == ErrorClass::NotFound => Ok(DeleteOutcome::AlreadyAbsent),
            Err(error) if error.class == ErrorClass::RetryableUnknown => {
                match self.backend.get(&permit.key, None, None).await {
                    Err(read_error) if read_error.class == ErrorClass::NotFound => {
                        Ok(DeleteOutcome::LostResponseRecovered)
                    }
                    Ok(read) => {
                        if read.object_length == permit.identity.length
                            && byte_len(&read.bytes) == permit.identity.length
                            && sha256(&read.bytes) == permit.identity.sha256
                        {
                            Err(StoreError::new(
                                ErrorClass::RetryableUnknown,
                                "delete outcome remains unresolved while exact object exists",
                            ))
                        } else {
                            Err(StoreError::new(
                                ErrorClass::Corrupt,
                                "delete outcome read found a different immutable identity",
                            ))
                        }
                    }
                    Err(read_error) => Err(read_error),
                }
            }
            Err(error) if guarded && error.class == ErrorClass::Unsupported => {
                Err(StoreError::new(
                    ErrorClass::Unsupported,
                    "backend declared guarded delete but rejected the operation",
                ))
            }
            Err(error) => Err(error),
        }
    }

    /// Create one immutable object or resolve an identical retry.
    ///
    /// # Errors
    ///
    /// Returns a classified backend, identity-conflict, or verification error.
    pub async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<(PutOutcome, ObjectIdentity), StoreError> {
        let expected_length = byte_len(&bytes);
        let expected_digest = sha256(&bytes);
        match self.backend.put(key, bytes, WriteCondition::Create).await {
            Ok(revision) => {
                let identity = self
                    .get_full_verified(key, Some(&revision), expected_length, &expected_digest)
                    .await?;
                Ok((PutOutcome::Created, identity))
            }
            Err(error) if error.class == ErrorClass::AlreadyExists => {
                let identity = self
                    .get_full_verified(key, None, expected_length, &expected_digest)
                    .await
                    .map_err(|read_error| {
                        if read_error.class == ErrorClass::Corrupt {
                            StoreError::new(ErrorClass::Corrupt, "immutable identity conflict")
                        } else {
                            read_error
                        }
                    })?;
                Ok((PutOutcome::ExistingIdentical, identity))
            }
            Err(error) if error.class == ErrorClass::RetryableUnknown => {
                let identity = self
                    .get_full_verified(key, None, expected_length, &expected_digest)
                    .await?;
                Ok((PutOutcome::LostResponseRecovered, identity))
            }
            Err(error) => Err(error),
        }
    }

    /// Conditionally replace a small authority object at one exact revision.
    ///
    /// # Errors
    ///
    /// Returns a precondition, unknown-outcome, backend, or verification error.
    pub async fn compare_and_put(
        &self,
        key: &str,
        expected: &ObjectIdentity,
        bytes: Bytes,
    ) -> Result<(UpdateOutcome, ObjectIdentity), StoreError> {
        let expected_length = byte_len(&bytes);
        let expected_digest = sha256(&bytes);
        match self
            .backend
            .put(
                key,
                bytes,
                WriteCondition::Update(expected.revision.clone()),
            )
            .await
        {
            Ok(revision) => {
                let identity = self
                    .get_full_verified(key, Some(&revision), expected_length, &expected_digest)
                    .await?;
                Ok((UpdateOutcome::Updated, identity))
            }
            Err(error) if error.class == ErrorClass::RetryableUnknown => {
                let identity = self
                    .get_full_verified(key, None, expected_length, &expected_digest)
                    .await?;
                Ok((UpdateOutcome::LostResponseRecovered, identity))
            }
            Err(error) => Err(error),
        }
    }

    /// Read and verify an entire named object against length, digest, and revision.
    ///
    /// # Errors
    ///
    /// Returns a classified read or corruption error.
    pub async fn get_full_verified(
        &self,
        key: &str,
        expected_revision: Option<&RevisionToken>,
        expected_length: u64,
        expected_digest: &str,
    ) -> Result<ObjectIdentity, StoreError> {
        self.read_full_verified(key, expected_revision, expected_length, expected_digest)
            .await
            .map(|(_, identity)| identity)
    }

    /// Read and return exact bytes plus their verified identity.
    ///
    /// # Errors
    ///
    /// Returns a classified read or corruption error.
    pub async fn read_full_verified(
        &self,
        key: &str,
        expected_revision: Option<&RevisionToken>,
        expected_length: u64,
        expected_digest: &str,
    ) -> Result<(Bytes, ObjectIdentity), StoreError> {
        let read = self.backend.get(key, None, expected_revision).await?;
        if read.returned_range != (0..expected_length)
            || read.object_length != expected_length
            || byte_len(&read.bytes) != expected_length
            || sha256(&read.bytes) != expected_digest
        {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "full object length, range, or digest mismatch",
            ));
        }
        if let Some(expected_revision) = expected_revision {
            if !expected_revision.matches(&read.revision) {
                return Err(StoreError::new(
                    ErrorClass::Corrupt,
                    "object revision changed during verified read",
                ));
            }
        }
        let identity = ObjectIdentity {
            revision: read.revision,
            length: read.object_length,
            sha256: expected_digest.to_owned(),
        };
        Ok((read.bytes, identity))
    }

    /// Read and verify one byte range at an exact object revision.
    ///
    /// # Errors
    ///
    /// Returns a classified read or corruption error.
    pub async fn get_range_verified(
        &self,
        key: &str,
        range: Range<u64>,
        expected: &ObjectIdentity,
        expected_range_digest: &str,
    ) -> Result<Bytes, StoreError> {
        let read = self
            .backend
            .get(key, Some(range.clone()), Some(&expected.revision))
            .await?;
        let expected_length = range.end.saturating_sub(range.start);
        if read.returned_range != range
            || read.object_length != expected.length
            || byte_len(&read.bytes) != expected_length
            || !expected.revision.matches(&read.revision)
            || sha256(&read.bytes) != expected_range_digest
        {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "range length, revision, or digest mismatch",
            ));
        }
        Ok(read.bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseStatus {
    Pass,
    Fail,
    Unsupported,
}

#[derive(Clone, Debug, Serialize)]
pub struct CaseResult {
    pub id: String,
    pub required: bool,
    pub status: CaseStatus,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct CapabilityEvidence {
    pub id: String,
    pub supported: bool,
    pub evidence_case: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceVerdict {
    Pass,
    Fail,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConformanceReport {
    pub schema_version: u32,
    pub contract: String,
    pub profile: ConformanceProfile,
    pub backend: BackendDescriptor,
    pub capabilities: Vec<CapabilityEvidence>,
    pub cases: Vec<CaseResult>,
    pub stats: RequestStats,
    pub verdict: ConformanceVerdict,
    pub failure_count: u64,
}

impl ConformanceReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.verdict == ConformanceVerdict::Pass
    }
}

/// Validate a conformance report against the repository-owned report schema.
///
/// # Errors
///
/// Returns an error when the embedded schema is invalid or the report does not
/// conform to it.
pub fn validate_conformance_report(report: &ConformanceReport) -> Result<(), StoreError> {
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../evals/schema/object-store-conformance.schema.json"
    ))
    .map_err(|_| StoreError::new(ErrorClass::Other, "invalid embedded conformance schema"))?;
    let value = serde_json::to_value(report).map_err(|_| {
        StoreError::new(ErrorClass::Other, "failed to serialize conformance report")
    })?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|_| StoreError::new(ErrorClass::Other, "invalid conformance validator"))?;
    let failures: Vec<String> = validator
        .iter_errors(&value)
        .map(|error| error.to_string())
        .collect();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(StoreError::new(
            ErrorClass::Other,
            format!("conformance result failed schema: {}", failures.join("; ")),
        ))
    }
}

fn record_case(
    cases: &mut Vec<CaseResult>,
    id: &str,
    required: bool,
    result: Result<String, StoreError>,
) {
    match result {
        Ok(detail) => cases.push(CaseResult {
            id: id.to_owned(),
            required,
            status: CaseStatus::Pass,
            detail,
        }),
        Err(error) => cases.push(CaseResult {
            id: id.to_owned(),
            required,
            status: if error.class == ErrorClass::Unsupported {
                CaseStatus::Unsupported
            } else {
                CaseStatus::Fail
            },
            detail: format!("{}: {}", error.class, error.detail),
        }),
    }
}

/// Execute the objectKV named-object conformance contract.
#[allow(clippy::too_many_lines)]
pub async fn run_conformance(
    backend: Arc<dyn Backend>,
    profile: ConformanceProfile,
    options: &ConformanceOptions,
) -> ConformanceReport {
    let backend: Arc<dyn Backend> = if options.inject_immutable_overwrite_bug {
        Arc::new(BrokenConditionalBackend { inner: backend })
    } else {
        backend
    };
    let observed = Arc::new(ObservedBackend::new(backend));
    let base: Arc<dyn Backend> = observed.clone();
    let client = ObjectClient::new(base.clone());
    let prefix = format!("scratch/okv-conformance/{}/", Uuid::new_v4());
    let mut cases = Vec::new();

    record_case(
        &mut cases,
        "named_read_after_write",
        true,
        case_named_read_after_write(&client, &prefix).await,
    );
    record_case(
        &mut cases,
        "identity_token",
        true,
        case_identity_token(&client, &prefix).await,
    );
    record_case(
        &mut cases,
        "immutable_create_idempotency",
        true,
        case_immutable_create_idempotency(&client, &prefix).await,
    );
    record_case(
        &mut cases,
        "lost_create_response_recovery",
        true,
        case_lost_create_response(base.clone(), &prefix).await,
    );
    record_case(
        &mut cases,
        "exact_range_read",
        true,
        case_exact_range_read(&client, &prefix).await,
    );
    record_case(
        &mut cases,
        "short_range_rejected",
        true,
        case_short_range_rejected(base.clone(), &prefix).await,
    );
    record_case(
        &mut cases,
        "checksum_corruption_rejected",
        true,
        case_checksum_corruption_rejected(base.clone(), &prefix).await,
    );
    record_case(
        &mut cases,
        "list_not_authority",
        true,
        case_list_not_authority(base.clone(), &prefix, options.inject_list_authority_bug).await,
    );
    record_case(
        &mut cases,
        "guarded_delete_or_immutable_fallback",
        true,
        case_guarded_delete_or_fallback(base.clone(), &client, &prefix).await,
    );

    if profile == ConformanceProfile::Authority {
        record_case(
            &mut cases,
            "conditional_root_update",
            true,
            case_conditional_root_update(&client, &prefix).await,
        );
        record_case(
            &mut cases,
            "conditional_update_race",
            true,
            case_conditional_update_race(base.clone(), &prefix).await,
        );
        record_case(
            &mut cases,
            "lost_update_response_recovery",
            true,
            case_lost_update_response(base, &prefix).await,
        );
    }

    let failure_count = u64::try_from(
        cases
            .iter()
            .filter(|case| case.required && case.status != CaseStatus::Pass)
            .count(),
    )
    .unwrap_or(u64::MAX);
    let backend = observed.descriptor();
    let capabilities = capability_evidence(&cases, &backend);
    ConformanceReport {
        schema_version: 1,
        contract: "okv-object-store-v1".to_owned(),
        profile,
        backend,
        capabilities,
        cases,
        stats: observed.stats(),
        verdict: if failure_count == 0 {
            ConformanceVerdict::Pass
        } else {
            ConformanceVerdict::Fail
        },
        failure_count,
    }
}

fn capability_evidence(
    cases: &[CaseResult],
    backend: &BackendDescriptor,
) -> Vec<CapabilityEvidence> {
    let passed = |id: &str| {
        cases
            .iter()
            .any(|case| case.id == id && case.status == CaseStatus::Pass)
    };
    vec![
        CapabilityEvidence {
            id: "strong_named_read_after_write".to_owned(),
            supported: passed("named_read_after_write"),
            evidence_case: "named_read_after_write".to_owned(),
        },
        CapabilityEvidence {
            id: "conditional_create".to_owned(),
            supported: passed("immutable_create_idempotency"),
            evidence_case: "immutable_create_idempotency".to_owned(),
        },
        CapabilityEvidence {
            id: "conditional_update".to_owned(),
            supported: passed("conditional_root_update") && passed("conditional_update_race"),
            evidence_case: "conditional_root_update+conditional_update_race".to_owned(),
        },
        CapabilityEvidence {
            id: "exact_range_read".to_owned(),
            supported: passed("exact_range_read") && passed("short_range_rejected"),
            evidence_case: "exact_range_read+short_range_rejected".to_owned(),
        },
        CapabilityEvidence {
            id: "guarded_delete".to_owned(),
            supported: backend.guarded_delete,
            evidence_case: "guarded_delete_or_immutable_fallback".to_owned(),
        },
    ]
}

async fn case_named_read_after_write(
    client: &ObjectClient,
    prefix: &str,
) -> Result<String, StoreError> {
    let bytes = Bytes::from_static(b"named-read-after-write");
    let key = immutable_key(prefix, &bytes);
    let (_, identity) = client.put_if_absent(&key, bytes.clone()).await?;
    client
        .get_full_verified(
            &key,
            Some(&identity.revision),
            identity.length,
            &identity.sha256,
        )
        .await?;
    Ok("exact named bytes visible after successful create".to_owned())
}

async fn case_identity_token(client: &ObjectClient, prefix: &str) -> Result<String, StoreError> {
    let bytes = Bytes::from_static(b"identity-token");
    let key = immutable_key(prefix, &bytes);
    let (_, identity) = client.put_if_absent(&key, bytes).await?;
    if !identity.revision.is_present() {
        return Err(StoreError::new(
            ErrorClass::Unsupported,
            "backend returned neither version nor ETag",
        ));
    }
    Ok("version or ETag preserved across publication and read".to_owned())
}

async fn case_immutable_create_idempotency(
    client: &ObjectClient,
    prefix: &str,
) -> Result<String, StoreError> {
    let original = Bytes::from_static(b"immutable-original");
    let conflicting = Bytes::from_static(b"immutable-conflict");
    let key = immutable_key(prefix, &original);
    client.put_if_absent(&key, original.clone()).await?;
    let (outcome, _) = client.put_if_absent(&key, original.clone()).await?;
    if outcome != PutOutcome::ExistingIdentical {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "identical retry did not resolve as existing identical bytes",
        ));
    }
    let error = match client.put_if_absent(&key, conflicting).await {
        Ok(_) => {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "conflicting immutable create unexpectedly succeeded",
            ));
        }
        Err(error) => error,
    };
    if error.class != ErrorClass::Corrupt {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "conflicting immutable create returned the wrong error class",
        ));
    }
    let digest = sha256(&original);
    client
        .get_full_verified(&key, None, byte_len(&original), &digest)
        .await?;
    Ok("identical retry accepted and conflicting bytes rejected".to_owned())
}

async fn case_lost_create_response(
    backend: Arc<dyn Backend>,
    prefix: &str,
) -> Result<String, StoreError> {
    let fault = Arc::new(FaultBackend::new(backend));
    fault.lose_next_put_response();
    let client = ObjectClient::new(fault);
    let bytes = Bytes::from_static(b"lost-create-response");
    let key = immutable_key(prefix, &bytes);
    let (outcome, _) = client.put_if_absent(&key, bytes).await?;
    if outcome != PutOutcome::LostResponseRecovered {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "lost create response was not recovered by identity read",
        ));
    }
    Ok("unknown create outcome recovered by exact named read".to_owned())
}

async fn case_exact_range_read(client: &ObjectClient, prefix: &str) -> Result<String, StoreError> {
    let bytes = Bytes::from_static(b"0123456789abcdef");
    let key = immutable_key(prefix, &bytes);
    let (_, identity) = client.put_if_absent(&key, bytes).await?;
    let expected = Bytes::from_static(b"345678");
    let actual = client
        .get_range_verified(&key, 3..9, &identity, &sha256(&expected))
        .await?;
    if actual != expected {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "range bytes differ from requested immutable slice",
        ));
    }
    Ok("range, revision, length, and digest matched".to_owned())
}

async fn case_short_range_rejected(
    backend: Arc<dyn Backend>,
    prefix: &str,
) -> Result<String, StoreError> {
    let fault = Arc::new(FaultBackend::new(backend));
    let client = ObjectClient::new(fault.clone());
    let bytes = Bytes::from_static(b"short-range-fixture");
    let key = immutable_key(prefix, &bytes);
    let (_, identity) = client.put_if_absent(&key, bytes.clone()).await?;
    let range = 2..8;
    let expected = bytes.slice(2..8);
    fault.shorten_next_get();
    let error = match client
        .get_range_verified(&key, range, &identity, &sha256(&expected))
        .await
    {
        Ok(_) => {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "short range unexpectedly passed verification",
            ));
        }
        Err(error) => error,
    };
    if error.class != ErrorClass::Corrupt {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "short range was not classified as corrupt",
        ));
    }
    Ok("injected short success response rejected".to_owned())
}

async fn case_checksum_corruption_rejected(
    backend: Arc<dyn Backend>,
    prefix: &str,
) -> Result<String, StoreError> {
    let fault = Arc::new(FaultBackend::new(backend));
    let client = ObjectClient::new(fault.clone());
    let bytes = Bytes::from_static(b"checksum-corruption-fixture");
    let key = immutable_key(prefix, &bytes);
    let (_, identity) = client.put_if_absent(&key, bytes).await?;
    fault.corrupt_next_get();
    let error = match client
        .get_full_verified(
            &key,
            Some(&identity.revision),
            identity.length,
            &identity.sha256,
        )
        .await
    {
        Ok(_) => {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "corrupt bytes unexpectedly passed verification",
            ));
        }
        Err(error) => error,
    };
    if error.class != ErrorClass::Corrupt {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "corrupt bytes returned the wrong error class",
        ));
    }
    Ok("injected checksum corruption rejected".to_owned())
}

async fn case_list_not_authority(
    backend: Arc<dyn Backend>,
    prefix: &str,
    inject_list_authority_bug: bool,
) -> Result<String, StoreError> {
    let fault = Arc::new(FaultBackend::new(backend));
    let client = ObjectClient::new(fault.clone());
    let bytes = Bytes::from_static(b"list-is-not-authority");
    let key = immutable_key(prefix, &bytes);
    let (_, identity) = client.put_if_absent(&key, bytes).await?;
    fault.stale_next_list();
    if inject_list_authority_bug {
        let listed = fault.list(prefix).await?;
        if !listed.iter().any(|candidate| candidate == &key) {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "injected LIST-authority algorithm lost a live named object",
            ));
        }
    } else {
        client
            .get_full_verified(
                &key,
                Some(&identity.revision),
                identity.length,
                &identity.sha256,
            )
            .await?;
        if fault.list_calls() != 0 {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "named read unexpectedly consulted LIST",
            ));
        }
    }
    Ok("named correctness path made zero LIST requests".to_owned())
}

async fn case_guarded_delete_or_fallback(
    backend: Arc<dyn Backend>,
    client: &ObjectClient,
    prefix: &str,
) -> Result<String, StoreError> {
    let bytes = Bytes::from_static(b"guarded-delete-probe");
    let key = immutable_key(prefix, &bytes);
    let (_, identity) = client.put_if_absent(&key, bytes).await?;
    match backend.delete(&key, Some(&identity.revision)).await {
        Ok(()) => Ok("exact revision guarded delete supported".to_owned()),
        Err(error) if error.class == ErrorClass::Unsupported => Ok(
            "guarded delete unsupported; immutable digest keys require reachability-horizon GC"
                .to_owned(),
        ),
        Err(error) => Err(error),
    }
}

async fn case_conditional_root_update(
    client: &ObjectClient,
    prefix: &str,
) -> Result<String, StoreError> {
    let key = format!("{prefix}authority/root-conditional");
    let initial = Bytes::from_static(b"root-generation-1");
    let (_, identity) = client.put_if_absent(&key, initial).await?;
    let updated = Bytes::from_static(b"root-generation-2");
    let (_, new_identity) = client.compare_and_put(&key, &identity, updated).await?;
    let stale_error = match client
        .compare_and_put(&key, &identity, Bytes::from_static(b"stale-generation"))
        .await
    {
        Ok(_) => {
            return Err(StoreError::new(
                ErrorClass::Corrupt,
                "stale root update unexpectedly succeeded",
            ));
        }
        Err(error) => error,
    };
    if stale_error.class != ErrorClass::PreconditionFailed {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "stale root update returned the wrong error class",
        ));
    }
    client
        .get_full_verified(
            &key,
            Some(&new_identity.revision),
            new_identity.length,
            &new_identity.sha256,
        )
        .await?;
    Ok("one root generation advanced and stale identity was fenced".to_owned())
}

async fn case_conditional_update_race(
    backend: Arc<dyn Backend>,
    prefix: &str,
) -> Result<String, StoreError> {
    let client = Arc::new(ObjectClient::new(backend));
    let key = format!("{prefix}authority/root-race");
    let (_, identity) = client
        .put_if_absent(&key, Bytes::from_static(b"root-race-initial"))
        .await?;
    let first_client = client.clone();
    let second_client = client.clone();
    let first_key = key.clone();
    let second_key = key;
    let first_identity = identity.clone();
    let second_identity = identity;
    let first = async move {
        first_client
            .compare_and_put(
                &first_key,
                &first_identity,
                Bytes::from_static(b"root-race-a"),
            )
            .await
    };
    let second = async move {
        second_client
            .compare_and_put(
                &second_key,
                &second_identity,
                Bytes::from_static(b"root-race-b"),
            )
            .await
    };
    let (first, second) = tokio::join!(first, second);
    let successes = u8::from(first.is_ok()) + u8::from(second.is_ok());
    let preconditions = u8::from(matches!(
        first,
        Err(StoreError {
            class: ErrorClass::PreconditionFailed,
            ..
        })
    )) + u8::from(matches!(
        second,
        Err(StoreError {
            class: ErrorClass::PreconditionFailed,
            ..
        })
    ));
    if successes != 1 || preconditions != 1 {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "concurrent conditional root updates did not produce one winner",
        ));
    }
    Ok("two same-revision writers produced one winner and one fenced loser".to_owned())
}

async fn case_lost_update_response(
    backend: Arc<dyn Backend>,
    prefix: &str,
) -> Result<String, StoreError> {
    let fault = Arc::new(FaultBackend::new(backend));
    let client = ObjectClient::new(fault.clone());
    let key = format!("{prefix}authority/root-lost-response");
    let (_, identity) = client
        .put_if_absent(&key, Bytes::from_static(b"root-before-lost-response"))
        .await?;
    fault.lose_next_put_response();
    let (outcome, _) = client
        .compare_and_put(
            &key,
            &identity,
            Bytes::from_static(b"root-after-lost-response"),
        )
        .await?;
    if outcome != UpdateOutcome::LostResponseRecovered {
        return Err(StoreError::new(
            ErrorClass::Corrupt,
            "lost update response was not recovered by intended root identity",
        ));
    }
    Ok("unknown root update outcome recovered by full transition read".to_owned())
}

fn immutable_key(prefix: &str, bytes: &[u8]) -> String {
    format!("{prefix}segments/sha256/{}", sha256(bytes))
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn byte_len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

#[must_use]
pub fn memory_backend() -> Arc<dyn Backend> {
    Arc::new(ObjectStoreBackend::new(
        BackendDescriptor {
            id: "memory".to_owned(),
            driver: "apache-object_store".to_owned(),
            driver_version: OBJECT_STORE_DRIVER_VERSION.to_owned(),
            server_version: "in-process".to_owned(),
            conditional_primitive: "etag".to_owned(),
            guarded_delete: false,
            delete_strategy: "immutable-digest-reachability-horizon".to_owned(),
        },
        Arc::new(InMemory::new()),
    ))
}

/// Build the local filesystem adapter rooted at a caller-owned directory.
///
/// # Errors
///
/// Returns an error when the root or filesystem adapter cannot be created.
pub fn filesystem_backend(root: &Path) -> Result<Arc<dyn Backend>, StoreError> {
    std::fs::create_dir_all(root).map_err(|_| {
        StoreError::new(
            ErrorClass::Other,
            "failed to create filesystem conformance root",
        )
    })?;
    let store = LocalFileSystem::new_with_prefix(root)
        .map_err(|error| classify_object_store_error(&error))?;
    Ok(Arc::new(ObjectStoreBackend::new(
        BackendDescriptor {
            id: "filesystem".to_owned(),
            driver: "apache-object_store".to_owned(),
            driver_version: OBJECT_STORE_DRIVER_VERSION.to_owned(),
            server_version: "local-filesystem".to_owned(),
            conditional_primitive: "unsupported".to_owned(),
            guarded_delete: false,
            delete_strategy: "immutable-digest-reachability-horizon".to_owned(),
        },
        Arc::new(store),
    )))
}

/// Build the S3-compatible `MinIO` adapter from `OKV_S3_*` environment values.
///
/// # Errors
///
/// Returns an error for missing configuration or an invalid client build.
pub fn minio_backend_from_env() -> Result<Arc<dyn Backend>, StoreError> {
    let endpoint = required_env("OKV_S3_ENDPOINT")?;
    let bucket = required_env("OKV_S3_BUCKET")?;
    let access_key = required_env("OKV_S3_ACCESS_KEY_ID")?;
    let secret_key = required_env("OKV_S3_SECRET_ACCESS_KEY")?;
    let region = env::var("OKV_S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let server_version =
        env::var("OKV_OBJECT_SERVER_VERSION").unwrap_or_else(|_| "unrecorded".to_owned());
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint(endpoint)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_region(region)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .map_err(|error| classify_object_store_error(&error))?;
    Ok(Arc::new(ObjectStoreBackend::new(
        BackendDescriptor {
            id: "minio".to_owned(),
            driver: "apache-object_store".to_owned(),
            driver_version: OBJECT_STORE_DRIVER_VERSION.to_owned(),
            server_version,
            conditional_primitive: "etag-if-match".to_owned(),
            guarded_delete: false,
            delete_strategy: "immutable-digest-reachability-horizon".to_owned(),
        },
        Arc::new(store),
    )))
}

/// Build the GCS adapter from standard Google credentials and `OKV_GCS_BUCKET`.
///
/// # Errors
///
/// Returns an error for missing configuration or an invalid client build.
pub fn gcs_backend_from_env() -> Result<Arc<dyn Backend>, StoreError> {
    let bucket = required_env("OKV_GCS_BUCKET")?;
    let server_version =
        env::var("OKV_OBJECT_SERVER_VERSION").unwrap_or_else(|_| "google-cloud-storage".to_owned());
    let store = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(|error| classify_object_store_error(&error))?;
    Ok(Arc::new(ObjectStoreBackend::new(
        BackendDescriptor {
            id: "gcs".to_owned(),
            driver: "apache-object_store".to_owned(),
            driver_version: OBJECT_STORE_DRIVER_VERSION.to_owned(),
            server_version,
            conditional_primitive: "generation-match".to_owned(),
            guarded_delete: false,
            delete_strategy: "immutable-digest-reachability-horizon".to_owned(),
        },
        Arc::new(store),
    )))
}

fn required_env(name: &str) -> Result<String, StoreError> {
    env::var(name).map_err(|_| {
        StoreError::new(
            ErrorClass::Other,
            format!("required environment variable {name} is not set"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{
        filesystem_backend, memory_backend, run_conformance, validate_conformance_report,
        CaseStatus, ConformanceOptions, ConformanceProfile,
    };
    use std::path::PathBuf;

    fn temporary_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("okv-object-{label}-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn memory_passes_authority_contract() {
        let report = run_conformance(
            memory_backend(),
            ConformanceProfile::Authority,
            &ConformanceOptions::default(),
        )
        .await;
        assert!(report.passed(), "{:#?}", report.cases);
        validate_conformance_report(&report).expect("schema-valid report");
    }

    #[tokio::test]
    async fn filesystem_passes_segment_but_not_authority_contract() {
        let segment_root = temporary_root("segment");
        let segment = run_conformance(
            filesystem_backend(&segment_root).expect("filesystem backend"),
            ConformanceProfile::Segment,
            &ConformanceOptions::default(),
        )
        .await;
        assert!(segment.passed(), "{:#?}", segment.cases);

        let authority_root = temporary_root("authority");
        let authority = run_conformance(
            filesystem_backend(&authority_root).expect("filesystem backend"),
            ConformanceProfile::Authority,
            &ConformanceOptions::default(),
        )
        .await;
        assert!(!authority.passed());
        assert!(authority.cases.iter().any(|case| {
            case.id == "conditional_root_update" && case.status == CaseStatus::Unsupported
        }));

        std::fs::remove_dir_all(segment_root).expect("remove segment fixture");
        std::fs::remove_dir_all(authority_root).expect("remove authority fixture");
    }

    #[tokio::test]
    async fn immutable_overwrite_negative_control_fails() {
        let report = run_conformance(
            memory_backend(),
            ConformanceProfile::Segment,
            &ConformanceOptions {
                inject_immutable_overwrite_bug: true,
                inject_list_authority_bug: false,
            },
        )
        .await;
        assert!(!report.passed());
    }

    #[tokio::test]
    async fn list_authority_negative_control_fails() {
        let report = run_conformance(
            memory_backend(),
            ConformanceProfile::Segment,
            &ConformanceOptions {
                inject_immutable_overwrite_bug: false,
                inject_list_authority_bug: true,
            },
        )
        .await;
        assert!(!report.passed());
    }
}
