#![allow(clippy::result_large_err)]

//! First integrated objectKV kernel boundary.
//!
//! [`SingleRange`] composes one replicated generation and publication
//! authority, one replicated transaction authority, one immutable row-object
//! base, and the retained transaction suffix after that base. It is an
//! experimental one-range boundary, not a production routing or serving lease.

use okv_consensus::{
    GenerationClient, GenerationCredential, GenerationPhase, PublicationClient,
    PublicationObjectKind, RequestIdentity, RetainedTransactionReadRequest,
    RetainedTransactionReadResponse, RetainedTransactionRecord, TransactionLogClient,
};
pub use okv_consensus::{
    TransactionKeyRange as ResidentKeyRange, TransactionMutation as ResidentMutation,
};
use okv_object::{
    content_sha256, decode_full_row_object, read_indexed_point, read_point_from_full_object,
    Backend, ObservedBackend, PointReadOutcome, RequestStats, RowObjectManifestV1, RowSegmentIndex,
};
use okv_transaction::{KeyRange, TransactionApplyResponse, TransactionCommand, TransactionStatus};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

const MAX_RETAINED_PAGE_RECORDS: u32 = 4_096;

/// Configuration for one experimental integrated range.
#[derive(Debug)]
pub struct SingleRangeConfig {
    pub authority_endpoints: Vec<String>,
    pub transaction_endpoints: Vec<String>,
    pub publication_root: String,
    pub object_backend: Arc<dyn Backend>,
    pub max_page_records: u32,
    pub serving_image: Option<Box<dyn ServingImage>>,
    pub resident_engine: Option<Arc<dyn ResidentRangeEngine>>,
}

impl SingleRangeConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.authority_endpoints.is_empty()
            || self.authority_endpoints.iter().any(String::is_empty)
            || self.transaction_endpoints.is_empty()
            || self.transaction_endpoints.iter().any(String::is_empty)
            || self.publication_root.is_empty()
            || self.max_page_records == 0
            || self.max_page_records > MAX_RETAINED_PAGE_RECORDS
            || (self.serving_image.is_some() && self.resident_engine.is_some())
        {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "single range requires non-empty endpoints and root, a page bound in 1..=4096, and at most one resident provider",
            ));
        }
        Ok(())
    }
}

/// Stable error category at the experimental kernel boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    InvalidConfiguration,
    AuthorityUnavailable,
    GenerationChanged,
    PublicationRootMissing,
    ManifestInvalid,
    RecoveryUnavailable,
    RecoveryOrder,
    ReadCoverage,
    ObjectRead,
    ServingImage,
    Commit,
}

/// Classified objectKV kernel failure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Error {
    pub kind: ErrorKind,
    pub detail: String,
}

impl Error {
    fn new(kind: ErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.detail)
    }
}

impl std::error::Error for Error {}

/// Cursor for the authority-owned retained transaction stream.
///
/// `batch_order = None` means resume after the complete scalar commit version.
/// `Some(order)` means resume within that version after the named batch item.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamCursor {
    pub commit_version: u64,
    pub batch_order: Option<u16>,
}

impl StreamCursor {
    #[must_use]
    pub const fn after_complete_version(commit_version: u64) -> Self {
        Self {
            commit_version,
            batch_order: None,
        }
    }

    const fn request(
        self,
        target: Option<u64>,
        max_records: u32,
    ) -> RetainedTransactionReadRequest {
        RetainedTransactionReadRequest {
            after_version_exclusive: self.commit_version,
            after_batch_order_exclusive: self.batch_order,
            through_version_inclusive: target,
            max_records,
        }
    }

    fn contains_later(self, record: &RetainedTransactionRecord) -> bool {
        record.commit_version > self.commit_version
            || (record.commit_version == self.commit_version
                && self
                    .batch_order
                    .is_some_and(|order| record.batch_order > order))
    }
}

/// Exact point-read result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ReadOutcome {
    Value(Vec<u8>),
    Tombstone,
    Absent,
}

/// One object-durable point state installed into a disposable serving image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServingImageRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// Complete activation evidence returned by one serving-image provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServingImageReceipt {
    pub provider: String,
    pub generation: u64,
    pub covered_through: u64,
    pub records: u64,
    pub local_bytes: u64,
}

/// One retained transaction translated into the resident-engine boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentTransactionRecord {
    pub commit_version: u64,
    pub batch_order: u16,
    pub mutations: Vec<ResidentMutation>,
}

impl From<&RetainedTransactionRecord> for ResidentTransactionRecord {
    fn from(record: &RetainedTransactionRecord) -> Self {
        Self {
            commit_version: record.commit_version,
            batch_order: record.batch_order,
            mutations: record.command.mutations.clone(),
        }
    }
}

/// Complete, already verified object state supplied to a resident engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentActivationRequest {
    pub generation: u64,
    pub object_root: String,
    pub object_durable_version: u64,
    pub owned_range: ResidentRangeBounds,
    pub object_first_key: Vec<u8>,
    pub object_last_key: Vec<u8>,
    pub records: Vec<ServingImageRecord>,
}

/// Half-open assigned key range. Missing bounds represent negative or positive
/// infinity for the current single-range kernel.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResidentRangeBounds {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
}

impl ResidentRangeBounds {
    /// Return whether one key belongs to this half-open range.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_deref().is_none_or(|start| key >= start)
            && self.end.as_deref().is_none_or(|end| key < end)
    }
}

/// One ordered retained-stream page applied atomically by a resident engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentAdvanceRequest {
    pub generation: u64,
    pub start: StreamCursor,
    pub end: StreamCursor,
    pub target_version: u64,
    pub records: Vec<ResidentTransactionRecord>,
}

/// Current activation and applied-frontier evidence from a resident engine.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResidentEngineReceipt {
    pub provider: String,
    pub generation: u64,
    pub object_root: String,
    pub object_durable_version: u64,
    pub applied: StreamCursor,
    pub owned_range: ResidentRangeBounds,
    pub object_first_key: Vec<u8>,
    pub object_last_key: Vec<u8>,
    pub records: u64,
    pub local_bytes: u64,
}

/// One version-bound view owned by a resident engine.
pub trait ResidentSnapshot: std::fmt::Debug + Send + Sync {
    /// Read one exact point without consulting object storage or an external
    /// MVCC overlay.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot cannot prove exact coverage for the
    /// requested key.
    fn get(&self, key: &[u8]) -> Result<ReadOutcome, String>;
}

/// Disposable engine whose correctness is established when state activates or
/// advances, rather than rebuilt around every point lookup.
pub trait ResidentRangeEngine: std::fmt::Debug + Send + Sync {
    /// Install one verified object closure into an empty engine.
    ///
    /// # Errors
    ///
    /// Returns an error when the closure, generation, range, or local budget
    /// cannot be validated and installed completely.
    fn activate(&self, request: ResidentActivationRequest)
        -> Result<ResidentEngineReceipt, String>;

    /// Apply one validated retained-stream page and its frontier atomically.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation or cursor is stale, the page is
    /// invalid, or its mutations and frontier cannot be applied together.
    fn advance(&self, request: ResidentAdvanceRequest) -> Result<ResidentEngineReceipt, String>;

    /// Bind one exact logical version to an engine-owned snapshot view.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation is stale or the requested version
    /// lies outside the engine's proven coverage.
    fn snapshot(
        self: Arc<Self>,
        generation: u64,
        read_version: u64,
    ) -> Result<Box<dyn ResidentSnapshot>, String>;

    /// Return the current provider receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the engine is not activated or has failed closed.
    fn receipt(&self) -> Result<ResidentEngineReceipt, String>;
}

/// Provider-neutral disposable point-serving image.
///
/// The first contract installs one complete object snapshot. Incremental tail
/// apply, range iteration, and partial admission remain separate later gates.
pub trait ServingImage: std::fmt::Debug + Send {
    /// Install an empty provider from the complete object state through one
    /// object-durable version.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot validate or install the
    /// complete state at the requested generation and frontier.
    fn activate(
        &mut self,
        generation: u64,
        covered_through: u64,
        records: Vec<ServingImageRecord>,
    ) -> Result<ServingImageReceipt, String>;

    /// Read one point from a completely activated image.
    ///
    /// # Errors
    ///
    /// Returns an error when the generation or coverage does not match the
    /// activated image, or when the provider read fails.
    fn get(&self, generation: u64, covered_through: u64, key: &[u8])
        -> Result<ReadOutcome, String>;
}

/// Coverage owned by one open range instance.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Coverage {
    pub object_durable_version: u64,
    pub recovered_version: u64,
}

/// Bounded work performed by one retained-stream catch-up.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatchUpReceipt {
    pub start: StreamCursor,
    pub end: StreamCursor,
    pub target_version: u64,
    pub pages: u64,
    pub records_applied: u64,
    pub response_payload_bytes: u64,
    pub batch_cursor_resumes: u64,
}

/// Authoritative state selected while opening one range.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OpenReceipt {
    pub generation: u64,
    pub logical_txlog_root: String,
    pub manifest_key: String,
    pub object_durable_version: u64,
    pub recovered_version: u64,
    pub row_segment_count: u64,
    pub row_index_closure_bytes: u64,
    pub row_data_closure_bytes: u64,
    pub serving_image: Option<ServingImageReceipt>,
    pub resident_engine: Option<ResidentEngineReceipt>,
    pub catch_up: CatchUpReceipt,
}

/// Commit result plus the local recovery work required for read-your-write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitReceipt {
    pub response: TransactionApplyResponse,
    pub catch_up: Option<CatchUpReceipt>,
}

/// Cumulative counters for one range-process lifetime.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeStats {
    pub txlog_read_requests: u64,
    pub txlog_records_applied: u64,
    pub txlog_response_payload_bytes: u64,
    pub manifest_requests: u64,
    pub index_requests: u64,
    pub data_range_requests: u64,
    pub data_full_requests: u64,
    pub point_actions: u64,
    pub range_clear_actions: u64,
    pub tail_resident_bytes: u64,
    pub serving_image_records: u64,
    pub serving_image_local_bytes: u64,
    pub resident_engine_records: u64,
    pub resident_engine_local_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MutationStamp {
    commit_version: u64,
    batch_order: u16,
    mutation_ordinal: usize,
}

#[derive(Clone, Debug)]
struct PointAction {
    stamp: MutationStamp,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct RangeClearAction {
    stamp: MutationStamp,
    range: KeyRange,
}

#[derive(Default)]
struct TailOverlay {
    points: BTreeMap<Vec<u8>, Vec<PointAction>>,
    range_clears: Vec<RangeClearAction>,
    point_actions: u64,
    range_clear_actions: u64,
    resident_bytes: u64,
}

impl TailOverlay {
    fn apply(&mut self, record: &RetainedTransactionRecord) {
        for (mutation_ordinal, mutation) in record.command.mutations.iter().enumerate() {
            let stamp = MutationStamp {
                commit_version: record.commit_version,
                batch_order: record.batch_order,
                mutation_ordinal,
            };
            match mutation {
                ResidentMutation::Set { key, value } => {
                    self.resident_bytes = self
                        .resident_bytes
                        .saturating_add(byte_len(key).saturating_add(byte_len(value)));
                    self.point_actions = self.point_actions.saturating_add(1);
                    self.points
                        .entry(key.clone())
                        .or_default()
                        .push(PointAction {
                            stamp,
                            value: Some(value.clone()),
                        });
                }
                ResidentMutation::Clear { key } => {
                    self.resident_bytes = self.resident_bytes.saturating_add(byte_len(key));
                    self.point_actions = self.point_actions.saturating_add(1);
                    self.points
                        .entry(key.clone())
                        .or_default()
                        .push(PointAction { stamp, value: None });
                }
                ResidentMutation::ClearRange { range } => {
                    self.resident_bytes = self.resident_bytes.saturating_add(
                        byte_len(&range.start).saturating_add(byte_len(&range.end)),
                    );
                    self.range_clear_actions = self.range_clear_actions.saturating_add(1);
                    self.range_clears.push(RangeClearAction {
                        stamp,
                        range: range.clone(),
                    });
                }
            }
        }
    }

    fn read(&self, key: &[u8], version: u64) -> Option<ReadOutcome> {
        let point = self.points.get(key).and_then(|actions| {
            actions
                .iter()
                .rev()
                .find(|action| action.stamp.commit_version <= version)
        });
        let clear = self
            .range_clears
            .iter()
            .rev()
            .find(|clear| clear.stamp.commit_version <= version && clear.range.contains(key));
        match (point, clear) {
            (None, None) => None,
            (Some(point), None) => Some(point_outcome(point)),
            (Some(point), Some(clear)) if point.stamp > clear.stamp => Some(point_outcome(point)),
            (None | Some(_), Some(_)) => Some(ReadOutcome::Tombstone),
        }
    }
}

/// Experimental one-range composition of transaction authority, txLog tail,
/// and immutable object base.
pub struct SingleRange {
    generation: u64,
    logical_txlog_root: String,
    credential: GenerationCredential,
    manifest: RowObjectManifestV1,
    backend: Arc<ObservedBackend>,
    txlog: TransactionLogClient,
    max_page_records: u32,
    cursor: StreamCursor,
    overlay: TailOverlay,
    indexes: BTreeMap<String, RowSegmentIndex>,
    hydrated: BTreeMap<String, (RowSegmentIndex, Vec<u8>)>,
    serving_image: Option<Box<dyn ServingImage>>,
    resident_engine: Option<Arc<dyn ResidentRangeEngine>>,
    stats: RangeStats,
}

impl SingleRange {
    /// Open one range exclusively from linearizable authority state, immutable
    /// objects, and the retained transaction stream.
    ///
    /// # Errors
    ///
    /// Fails closed when configuration, generation, publication, object
    /// identity, retained-stream ordering, or coverage is invalid.
    #[allow(clippy::too_many_lines)]
    pub async fn open(config: SingleRangeConfig) -> Result<(Self, OpenReceipt), Error> {
        config.validate()?;
        let generation_client = GenerationClient::new(config.authority_endpoints.clone())
            .map_err(|error| Error::new(ErrorKind::InvalidConfiguration, error))?;
        let publication_client = PublicationClient::new(config.authority_endpoints)
            .map_err(|error| Error::new(ErrorKind::InvalidConfiguration, error))?;
        let generation_before = generation_client
            .read()
            .await
            .map_err(|error| Error::new(ErrorKind::AuthorityUnavailable, error))?;
        let publication = publication_client
            .read()
            .await
            .map_err(|error| Error::new(ErrorKind::AuthorityUnavailable, error))?;
        let generation_after = generation_client
            .read()
            .await
            .map_err(|error| Error::new(ErrorKind::AuthorityUnavailable, error))?;
        if generation_before != generation_after
            || generation_before.phase != GenerationPhase::Active
        {
            return Err(Error::new(
                ErrorKind::GenerationChanged,
                "generation changed or was not active around the publication-root read",
            ));
        }
        let logical_txlog_root = generation_before.wal_root.clone().ok_or_else(|| {
            Error::new(ErrorKind::GenerationChanged, "active txLog root is absent")
        })?;
        let transaction_system_id =
            generation_before
                .transaction_system_id
                .clone()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::GenerationChanged,
                        "active transaction-system identity is absent",
                    )
                })?;
        let manifest_reference =
            publication
                .roots
                .get(&config.publication_root)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::PublicationRootMissing,
                        "named publication root is absent",
                    )
                })?;
        if manifest_reference.kind != PublicationObjectKind::Manifest {
            return Err(Error::new(
                ErrorKind::PublicationRootMissing,
                "named publication root does not reference a manifest",
            ));
        }
        let backend = Arc::new(ObservedBackend::new(config.object_backend));
        let manifest_read = backend
            .get(&manifest_reference.key, None, None)
            .await
            .map_err(|error| Error::new(ErrorKind::ObjectRead, error.to_string()))?;
        if u64::try_from(manifest_read.bytes.len()).unwrap_or(u64::MAX) != manifest_reference.length
            || content_sha256(&manifest_read.bytes) != manifest_reference.sha256
        {
            return Err(Error::new(
                ErrorKind::ManifestInvalid,
                "published manifest length or digest does not match authority",
            ));
        }
        let manifest = RowObjectManifestV1::decode(&manifest_read.bytes)
            .map_err(|error| Error::new(ErrorKind::ManifestInvalid, error))?;
        if manifest.generation != generation_before.generation {
            return Err(Error::new(
                ErrorKind::ManifestInvalid,
                "published manifest belongs to another generation",
            ));
        }
        let txlog = TransactionLogClient::new(config.transaction_endpoints)
            .map_err(|error| Error::new(ErrorKind::InvalidConfiguration, error))?;
        let cursor = StreamCursor::after_complete_version(manifest.covered_through);
        let mut range = Self {
            generation: generation_before.generation,
            logical_txlog_root: logical_txlog_root.clone(),
            credential: GenerationCredential {
                generation: generation_before.generation,
                transaction_system_id,
            },
            manifest,
            backend,
            txlog,
            max_page_records: config.max_page_records,
            cursor,
            overlay: TailOverlay::default(),
            indexes: BTreeMap::new(),
            hydrated: BTreeMap::new(),
            serving_image: config.serving_image,
            resident_engine: config.resident_engine,
            stats: RangeStats {
                manifest_requests: 1,
                ..RangeStats::default()
            },
        };
        let serving_image = range.activate_serving_image().await?;
        range
            .activate_resident_engine(&manifest_reference.key)
            .await?;
        let catch_up = range.catch_up(None).await?;
        let resident_engine = range
            .resident_engine
            .as_ref()
            .map(|engine| engine.receipt())
            .transpose()
            .map_err(|error| Error::new(ErrorKind::ServingImage, error))?;
        let receipt = OpenReceipt {
            generation: range.generation,
            logical_txlog_root,
            manifest_key: manifest_reference.key.clone(),
            object_durable_version: range.manifest.covered_through,
            recovered_version: catch_up.target_version,
            row_segment_count: u64::try_from(range.manifest.segments.len()).unwrap_or(u64::MAX),
            row_index_closure_bytes: range
                .manifest
                .segments
                .iter()
                .map(|segment| segment.index_bytes)
                .sum(),
            row_data_closure_bytes: range
                .manifest
                .segments
                .iter()
                .map(|segment| segment.data_bytes)
                .sum(),
            serving_image,
            resident_engine,
            catch_up,
        };
        Ok((range, receipt))
    }

    /// Commit one transaction and catch this range up through a committed
    /// response before returning it.
    ///
    /// # Errors
    ///
    /// Returns a classified error when generation fencing, quorum commit, or
    /// subsequent retained-stream catch-up fails.
    pub async fn commit(
        &mut self,
        identity: RequestIdentity,
        command: &TransactionCommand,
    ) -> Result<CommitReceipt, Error> {
        let response = self
            .txlog
            .commit_fenced(identity, &self.credential, command)
            .await
            .map_err(|error| Error::new(ErrorKind::Commit, error))?;
        let catch_up = match response.status {
            TransactionStatus::Committed { commit_version } => {
                Some(self.catch_up(Some(commit_version)).await?)
            }
            TransactionStatus::Conflict { .. } | TransactionStatus::Rejected { .. } => None,
        };
        Ok(CommitReceipt { response, catch_up })
    }

    /// Catch up to a frozen target, or to the transaction high watermark when
    /// `target_version` is absent.
    ///
    /// # Errors
    ///
    /// Fails closed on an unavailable suffix, invalid frozen target, cursor
    /// regression, duplicate record, skipped batch item, or response mismatch.
    pub async fn catch_up(&mut self, target_version: Option<u64>) -> Result<CatchUpReceipt, Error> {
        let start = self.cursor;
        let mut frozen_target = target_version;
        let mut receipt = CatchUpReceipt {
            start,
            end: start,
            target_version: target_version.unwrap_or(start.commit_version),
            ..CatchUpReceipt::default()
        };
        loop {
            let request = self.cursor.request(frozen_target, self.max_page_records);
            let page = self
                .txlog
                .read(request)
                .await
                .map_err(|error| Error::new(ErrorKind::RecoveryUnavailable, error))?;
            receipt.pages = receipt.pages.saturating_add(1);
            self.stats.txlog_read_requests = self.stats.txlog_read_requests.saturating_add(1);
            let response_bytes = u64::try_from(
                serde_json::to_vec(&page)
                    .map_err(|error| Error::new(ErrorKind::RecoveryOrder, error.to_string()))?
                    .len(),
            )
            .unwrap_or(u64::MAX);
            receipt.response_payload_bytes = receipt
                .response_payload_bytes
                .saturating_add(response_bytes);
            self.stats.txlog_response_payload_bytes = self
                .stats
                .txlog_response_payload_bytes
                .saturating_add(response_bytes);
            if let Some(expected) = frozen_target {
                if page.target_version != expected {
                    return Err(Error::new(
                        ErrorKind::RecoveryOrder,
                        "retained page changed its frozen target",
                    ));
                }
            } else {
                frozen_target = Some(page.target_version);
            }
            let next = validate_page(self.cursor, &page)?;
            if let Some(engine) = &self.resident_engine {
                let request = ResidentAdvanceRequest {
                    generation: self.generation,
                    start: self.cursor,
                    end: next,
                    target_version: page.target_version,
                    records: page
                        .records
                        .iter()
                        .map(ResidentTransactionRecord::from)
                        .collect(),
                };
                engine
                    .advance(request)
                    .map_err(|error| Error::new(ErrorKind::ServingImage, error))?;
            }
            for record in &page.records {
                if self.resident_engine.is_none() {
                    self.overlay.apply(record);
                } else {
                    note_resident_record(&mut self.stats, record);
                }
                receipt.records_applied = receipt.records_applied.saturating_add(1);
                self.stats.txlog_records_applied =
                    self.stats.txlog_records_applied.saturating_add(1);
            }
            self.cursor = next;
            if !page.complete && self.cursor.batch_order.is_some() {
                receipt.batch_cursor_resumes = receipt.batch_cursor_resumes.saturating_add(1);
            }
            if page.complete {
                if self.resident_engine.is_none() {
                    self.stats.point_actions = self.overlay.point_actions;
                    self.stats.range_clear_actions = self.overlay.range_clear_actions;
                    self.stats.tail_resident_bytes = self.overlay.resident_bytes;
                } else if let Some(engine) = &self.resident_engine {
                    let engine_receipt = engine
                        .receipt()
                        .map_err(|error| Error::new(ErrorKind::ServingImage, error))?;
                    self.stats.resident_engine_records = engine_receipt.records;
                    self.stats.resident_engine_local_bytes = engine_receipt.local_bytes;
                }
                receipt.end = self.cursor;
                receipt.target_version = page.target_version;
                return Ok(receipt);
            }
        }
    }

    /// Read one exact point from the recovered tail or immutable object base.
    ///
    /// # Errors
    ///
    /// Fails closed outside `[object_durable_version, recovered_version]` or
    /// when an index or selected object block cannot be verified.
    pub async fn get(&mut self, key: &[u8], version: u64) -> Result<ReadOutcome, Error> {
        if version < self.manifest.covered_through || version > self.cursor.commit_version {
            return Err(Error::new(
                ErrorKind::ReadCoverage,
                format!(
                    "read version {version} is outside recovered coverage [{}, {}]",
                    self.manifest.covered_through, self.cursor.commit_version
                ),
            ));
        }
        if self.resident_engine.is_some() {
            return self
                .resident_snapshot(version)?
                .get(key)
                .map_err(|error| Error::new(ErrorKind::ServingImage, error));
        }
        if let Some(outcome) = self.overlay.read(key, version) {
            return Ok(outcome);
        }
        if let Some(image) = &self.serving_image {
            return image
                .get(self.generation, self.manifest.covered_through, key)
                .map_err(|error| Error::new(ErrorKind::ServingImage, error));
        }
        let Some(reference) = self.manifest.locate(key).cloned() else {
            return Ok(ReadOutcome::Absent);
        };
        let point = if let Some((index, data)) = self.hydrated.get(&reference.data_key) {
            read_point_from_full_object(data, index, key, self.manifest.covered_through)
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?
        } else {
            if !self.indexes.contains_key(&reference.index_key) {
                let index_read = self
                    .backend
                    .get(&reference.index_key, None, None)
                    .await
                    .map_err(|error| Error::new(ErrorKind::ObjectRead, error.to_string()))?;
                let index = RowSegmentIndex::decode(&index_read.bytes)
                    .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
                reference
                    .validate_index(&index_read.bytes, &index)
                    .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
                self.stats.index_requests = self.stats.index_requests.saturating_add(1);
                self.indexes.insert(reference.index_key.clone(), index);
            }
            let index = self.indexes.get(&reference.index_key).ok_or_else(|| {
                Error::new(ErrorKind::ObjectRead, "selected row index was not cached")
            })?;
            let point = read_indexed_point(
                self.backend.as_ref(),
                &reference.data_key,
                None,
                index,
                key,
                self.manifest.covered_through,
            )
            .await
            .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
            if point.data_bytes > 0 {
                self.stats.data_range_requests = self.stats.data_range_requests.saturating_add(1);
            }
            point
        };
        Ok(match point.outcome {
            PointReadOutcome::Value(value) => ReadOutcome::Value(value.to_vec()),
            PointReadOutcome::Tombstone => ReadOutcome::Tombstone,
            PointReadOutcome::Absent => ReadOutcome::Absent,
        })
    }

    /// Bind one exact version to the native resident data plane.
    ///
    /// Generation, coverage, and applied-frontier checks occur once here. The
    /// returned snapshot performs no object reads and does not consult the
    /// external tail overlay.
    ///
    /// # Errors
    ///
    /// Returns an error when no native engine is active or the requested
    /// version is outside complete resident coverage.
    pub fn resident_snapshot(&self, version: u64) -> Result<Box<dyn ResidentSnapshot>, Error> {
        if version < self.manifest.covered_through
            || version > self.cursor.commit_version
            || self.cursor.batch_order.is_some()
        {
            return Err(Error::new(
                ErrorKind::ReadCoverage,
                format!(
                    "read version {version} is outside complete resident coverage [{}, {}]",
                    self.manifest.covered_through, self.cursor.commit_version
                ),
            ));
        }
        self.resident_engine
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::ServingImage,
                    "native resident engine is not configured",
                )
            })?
            .clone()
            .snapshot(self.generation, version)
            .map_err(|error| Error::new(ErrorKind::ServingImage, error))
    }

    /// Preload and verify every data object into the disposable range process.
    /// This is an explicit latency-versus-memory choice and is not required for
    /// correctness.
    ///
    /// # Errors
    ///
    /// Returns an error when any index or data object fails closure validation.
    pub async fn preload(&mut self) -> Result<(), Error> {
        for reference in &self.manifest.segments {
            let index_read = self
                .backend
                .get(&reference.index_key, None, None)
                .await
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error.to_string()))?;
            let index = RowSegmentIndex::decode(&index_read.bytes)
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
            reference
                .validate_index(&index_read.bytes, &index)
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
            let data_read = self
                .backend
                .get(&reference.data_key, None, None)
                .await
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error.to_string()))?;
            if u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX) != reference.data_bytes
                || content_sha256(&data_read.bytes) != reference.data_sha256
            {
                return Err(Error::new(
                    ErrorKind::ObjectRead,
                    "preloaded data object does not match the manifest",
                ));
            }
            self.stats.index_requests = self.stats.index_requests.saturating_add(1);
            self.stats.data_full_requests = self.stats.data_full_requests.saturating_add(1);
            self.indexes
                .insert(reference.index_key.clone(), index.clone());
            self.hydrated.insert(
                reference.data_key.clone(),
                (index, data_read.bytes.to_vec()),
            );
        }
        Ok(())
    }

    async fn activate_serving_image(&mut self) -> Result<Option<ServingImageReceipt>, Error> {
        if self.serving_image.is_none() {
            return Ok(None);
        }
        let records = self.load_complete_object_state().await?;
        let expected_records = u64::try_from(records.len()).unwrap_or(u64::MAX);
        let image = self.serving_image.as_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::ServingImage,
                "serving image disappeared during activation",
            )
        })?;
        let receipt = image
            .activate(self.generation, self.manifest.covered_through, records)
            .map_err(|error| Error::new(ErrorKind::ServingImage, error))?;
        if receipt.provider.is_empty()
            || receipt.generation != self.generation
            || receipt.covered_through != self.manifest.covered_through
            || receipt.records != expected_records
        {
            return Err(Error::new(
                ErrorKind::ServingImage,
                "serving-image activation receipt does not match the selected range",
            ));
        }
        self.stats.serving_image_records = receipt.records;
        self.stats.serving_image_local_bytes = receipt.local_bytes;
        Ok(Some(receipt))
    }

    async fn activate_resident_engine(&mut self, object_root: &str) -> Result<(), Error> {
        let Some(engine) = self.resident_engine.clone() else {
            return Ok(());
        };
        let records = self.load_complete_object_state().await?;
        let expected_records = u64::try_from(records.len()).unwrap_or(u64::MAX);
        let object_first_key = self
            .manifest
            .segments
            .first()
            .map(|segment| segment.first_key.clone())
            .ok_or_else(|| Error::new(ErrorKind::ServingImage, "resident range is empty"))?;
        let object_last_key = self
            .manifest
            .segments
            .last()
            .map(|segment| segment.last_key.clone())
            .ok_or_else(|| Error::new(ErrorKind::ServingImage, "resident range is empty"))?;
        let owned_range = ResidentRangeBounds::default();
        let receipt = engine
            .activate(ResidentActivationRequest {
                generation: self.generation,
                object_root: object_root.to_owned(),
                object_durable_version: self.manifest.covered_through,
                owned_range: owned_range.clone(),
                object_first_key: object_first_key.clone(),
                object_last_key: object_last_key.clone(),
                records,
            })
            .map_err(|error| Error::new(ErrorKind::ServingImage, error))?;
        if receipt.provider.is_empty()
            || receipt.generation != self.generation
            || receipt.object_root != object_root
            || receipt.object_durable_version != self.manifest.covered_through
            || receipt.applied
                != StreamCursor::after_complete_version(self.manifest.covered_through)
            || receipt.owned_range != owned_range
            || receipt.object_first_key != object_first_key
            || receipt.object_last_key != object_last_key
            || receipt.records != expected_records
        {
            return Err(Error::new(
                ErrorKind::ServingImage,
                "resident-engine activation receipt does not match the selected range",
            ));
        }
        self.stats.resident_engine_records = receipt.records;
        self.stats.resident_engine_local_bytes = receipt.local_bytes;
        Ok(())
    }

    async fn load_complete_object_state(&mut self) -> Result<Vec<ServingImageRecord>, Error> {
        let references = self.manifest.segments.clone();
        let mut visible = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
        for reference in references {
            let index_read = self
                .backend
                .get(&reference.index_key, None, None)
                .await
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error.to_string()))?;
            let index = RowSegmentIndex::decode(&index_read.bytes)
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
            reference
                .validate_index(&index_read.bytes, &index)
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
            let data_read = self
                .backend
                .get(&reference.data_key, None, None)
                .await
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error.to_string()))?;
            if u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX) != reference.data_bytes
                || content_sha256(&data_read.bytes) != reference.data_sha256
            {
                return Err(Error::new(
                    ErrorKind::ObjectRead,
                    "serving-image data object does not match the manifest",
                ));
            }
            let decoded = decode_full_row_object(&data_read.bytes, &index)
                .map_err(|error| Error::new(ErrorKind::ObjectRead, error))?;
            for record in decoded {
                if record.version > self.manifest.covered_through {
                    return Err(Error::new(
                        ErrorKind::ServingImage,
                        "serving-image record exceeds object-durable coverage",
                    ));
                }
                visible.entry(record.key).or_insert(record.value);
            }
            self.stats.index_requests = self.stats.index_requests.saturating_add(1);
            self.stats.data_full_requests = self.stats.data_full_requests.saturating_add(1);
            self.indexes.insert(reference.index_key, index);
        }
        Ok(visible
            .into_iter()
            .map(|(key, value)| ServingImageRecord { key, value })
            .collect())
    }

    #[must_use]
    pub const fn coverage(&self) -> Coverage {
        Coverage {
            object_durable_version: self.manifest.covered_through,
            recovered_version: self.cursor.commit_version,
        }
    }

    #[must_use]
    pub const fn cursor(&self) -> StreamCursor {
        self.cursor
    }

    #[must_use]
    pub const fn stats(&self) -> RangeStats {
        self.stats
    }

    #[must_use]
    pub fn logical_txlog_root(&self) -> &str {
        &self.logical_txlog_root
    }

    #[must_use]
    pub fn object_stats(&self) -> RequestStats {
        self.backend.stats()
    }
}

fn validate_page(
    cursor: StreamCursor,
    page: &RetainedTransactionReadResponse,
) -> Result<StreamCursor, Error> {
    if page.format_version != 1
        || cursor.commit_version < page.retention_floor
        || page.target_version > page.high_watermark
        || page.target_version < cursor.commit_version
    {
        return Err(Error::new(
            ErrorKind::RecoveryOrder,
            "retained page header is inconsistent with the requested cursor",
        ));
    }
    let mut prior = cursor;
    for record in &page.records {
        if !prior.contains_later(record) || record.commit_version > page.target_version {
            return Err(Error::new(
                ErrorKind::RecoveryOrder,
                "retained records are duplicated, regressed, or outside the frozen target",
            ));
        }
        prior = StreamCursor {
            commit_version: record.commit_version,
            batch_order: Some(record.batch_order),
        };
    }
    if page.complete {
        if page.next_after_version != page.target_version || page.next_after_batch_order.is_some() {
            return Err(Error::new(
                ErrorKind::RecoveryOrder,
                "complete retained page did not close at the frozen target",
            ));
        }
        Ok(StreamCursor::after_complete_version(page.target_version))
    } else {
        if page.records.is_empty()
            || page.next_after_version != prior.commit_version
            || page.next_after_batch_order != prior.batch_order
        {
            return Err(Error::new(
                ErrorKind::RecoveryOrder,
                "incomplete retained page did not advance by its final versionstamp",
            ));
        }
        Ok(prior)
    }
}

fn point_outcome(action: &PointAction) -> ReadOutcome {
    action
        .value
        .clone()
        .map_or(ReadOutcome::Tombstone, ReadOutcome::Value)
}

fn byte_len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

fn note_resident_record(stats: &mut RangeStats, record: &RetainedTransactionRecord) {
    for mutation in &record.command.mutations {
        match mutation {
            ResidentMutation::Set { key, value } => {
                stats.point_actions = stats.point_actions.saturating_add(1);
                stats.tail_resident_bytes = stats
                    .tail_resident_bytes
                    .saturating_add(byte_len(key).saturating_add(byte_len(value)));
            }
            ResidentMutation::Clear { key } => {
                stats.point_actions = stats.point_actions.saturating_add(1);
                stats.tail_resident_bytes = stats.tail_resident_bytes.saturating_add(byte_len(key));
            }
            ResidentMutation::ClearRange { range } => {
                stats.range_clear_actions = stats.range_clear_actions.saturating_add(1);
                stats.tail_resident_bytes = stats
                    .tail_resident_bytes
                    .saturating_add(byte_len(&range.start).saturating_add(byte_len(&range.end)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(mutations: Vec<ResidentMutation>) -> TransactionCommand {
        TransactionCommand {
            read_version: 0,
            read_conflicts: Vec::new(),
            write_conflicts: Vec::new(),
            mutations,
        }
    }

    fn record(
        commit_version: u64,
        batch_order: u16,
        mutations: Vec<ResidentMutation>,
    ) -> RetainedTransactionRecord {
        RetainedTransactionRecord {
            commit_version,
            batch_order,
            command: command(mutations),
        }
    }

    #[test]
    fn shared_version_batch_order_selects_the_later_transaction() {
        let mut overlay = TailOverlay::default();
        overlay.apply(&record(
            12,
            0,
            vec![ResidentMutation::Set {
                key: b"k".to_vec(),
                value: b"first".to_vec(),
            }],
        ));
        overlay.apply(&record(
            12,
            1,
            vec![ResidentMutation::Set {
                key: b"k".to_vec(),
                value: b"second".to_vec(),
            }],
        ));
        assert_eq!(
            overlay.read(b"k", 12),
            Some(ReadOutcome::Value(b"second".to_vec()))
        );
    }

    #[test]
    fn mutation_ordinal_orders_point_after_range_clear() {
        let mut overlay = TailOverlay::default();
        overlay.apply(&record(
            13,
            2,
            vec![
                ResidentMutation::ClearRange {
                    range: KeyRange {
                        start: b"a".to_vec(),
                        end: b"z".to_vec(),
                    },
                },
                ResidentMutation::Set {
                    key: b"k".to_vec(),
                    value: b"after".to_vec(),
                },
            ],
        ));
        assert_eq!(
            overlay.read(b"k", 13),
            Some(ReadOutcome::Value(b"after".to_vec()))
        );
    }

    #[test]
    fn incomplete_page_carries_batch_order_cursor() {
        let page = RetainedTransactionReadResponse {
            format_version: 1,
            retention_floor: 10,
            high_watermark: 12,
            target_version: 12,
            next_after_version: 12,
            next_after_batch_order: Some(0),
            complete: false,
            records: vec![record(
                12,
                0,
                vec![ResidentMutation::Clear { key: b"k".to_vec() }],
            )],
        };
        assert_eq!(
            validate_page(StreamCursor::after_complete_version(10), &page).unwrap(),
            StreamCursor {
                commit_version: 12,
                batch_order: Some(0),
            }
        );
    }

    #[test]
    fn scalar_cursor_rejects_another_record_at_the_same_version() {
        let cursor = StreamCursor::after_complete_version(12);
        assert!(!cursor.contains_later(&record(
            12,
            1,
            vec![ResidentMutation::Clear { key: b"k".to_vec() }],
        )));
    }
}
