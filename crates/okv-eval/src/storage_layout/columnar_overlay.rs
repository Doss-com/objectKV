//! Columnar permanent media with a disposable range-local serving overlay.

use super::{
    array, as_usize, branch_manifest_bytes, content_sha256, elapsed_micros, elapsed_ns,
    expected_outcome, key_u64, latency_summary, live_logical_bytes, logical_digest, operation_keys,
    rate, ratio, response_bytes, row_group_ranges, successful_requests, Arc, BTreeMap, Backend,
    Bytes, ColumnarCacheAdmissionMode, ColumnarCacheAdmissionSample, ColumnarDataFusionMode,
    ColumnarDataFusionSample, Deserialize, Digest, Instant, LogicalHistory, ObservedBackend,
    PointReadOutcome, ProjectedRow, Range, RowRecord, Serialize, Sha256, StorageLayoutMode,
    StorageLayoutProfile, StorageLayoutSample, ValueFields, WriteCondition, FORMAT_VERSION,
    GENERATION,
};
use crate::t28_layout::{
    GenerationPinnedChildBackend, TypedLayoutChildV1, TypedLayoutObjectRoleV1, TypedLayoutSubjectV1,
};
use arrow::array::{ArrayRef, Int64Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;
use okv_htap::{RangeStripeSource, RangeStripeTableProvider};
use std::collections::{BTreeSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

const PROJECTION_KEY: &str = "layout/columnar/projection.okcp";
const PAYLOAD_KEY: &str = "layout/columnar/payload.okcv";
const INDEX_KEY: &str = "layout/columnar/index.okci";
const MANIFEST_KEY: &str = "layout/columnar/active-manifest";
const PROJECTION_MAGIC: &[u8; 4] = b"OKCP";
const INDEX_MAGIC: &[u8; 4] = b"OKCI";
const MANIFEST_MAGIC: &[u8; 6] = b"OKVCM1";
const RANGE_DIGEST_BYTES: usize = 16;
const FULL_DIGEST_BYTES: usize = 32;
const PROJECTION_HEADER_BYTES: usize = 10;
const PROJECTION_RECORD_BYTES: usize = 61;
const MAX_COLUMNAR_ENTRIES: usize = 1_000_000;
const PAYLOAD_PAGE_ROWS: usize = 32;

pub(super) async fn prepare_t28_columnar_layout(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
    backend: &dyn Backend,
) -> Result<Vec<(String, TypedLayoutObjectRoleV1)>, String> {
    prepare_columnar_layout(profile, history, backend).await?;
    Ok(vec![
        (MANIFEST_KEY.to_owned(), TypedLayoutObjectRoleV1::Manifest),
        (INDEX_KEY.to_owned(), TypedLayoutObjectRoleV1::Index),
        (PAYLOAD_KEY.to_owned(), TypedLayoutObjectRoleV1::Payload),
        (
            PROJECTION_KEY.to_owned(),
            TypedLayoutObjectRoleV1::Projection,
        ),
    ])
}

pub(super) fn minimum_overlay_cache_bytes(profile: &StorageLayoutProfile) -> Result<usize, String> {
    let payload_page = profile
        .opaque_payload_bytes
        .checked_mul(PAYLOAD_PAGE_ROWS)
        .ok_or_else(|| "columnar payload page size overflow".to_owned())?;
    let projection_stripe = profile
        .columnar_block_rows
        .checked_mul(PROJECTION_RECORD_BYTES)
        .and_then(|bytes| bytes.checked_add(PROJECTION_HEADER_BYTES))
        .ok_or_else(|| "columnar projection stripe size overflow".to_owned())?;
    Ok(payload_page.max(projection_stripe))
}

#[derive(Clone, Debug)]
struct ProjectionRecord {
    key: u64,
    version: u64,
    fields: Option<ValueFields>,
    payload_offset: u64,
    payload_length: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectionFence {
    first_key: u64,
    last_key: u64,
    offset: u64,
    length: u64,
    digest: [u8; RANGE_DIGEST_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ColumnarIndex {
    generation: u64,
    projection_length: u64,
    projection_digest: [u8; FULL_DIGEST_BYTES],
    payload_length: u64,
    payload_digest: [u8; FULL_DIGEST_BYTES],
    payload_page_bytes: u32,
    projection_fences: Vec<ProjectionFence>,
    payload_page_digests: Vec<[u8; RANGE_DIGEST_BYTES]>,
}

impl ColumnarIndex {
    fn locate_projection(&self, key: u64) -> Option<usize> {
        let mut lower = 0_usize;
        let mut upper = self.projection_fences.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.projection_fences[middle].first_key <= key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let index = lower.checked_sub(1)?;
        (key <= self.projection_fences[index].last_key).then_some(index)
    }

    fn projection_range(&self, index: usize) -> Result<Range<u64>, String> {
        let fence = self
            .projection_fences
            .get(index)
            .ok_or_else(|| "columnar projection fence is absent".to_owned())?;
        Ok(fence.offset..fence.offset.saturating_add(fence.length))
    }

    fn payload_page(&self, payload_offset: u64) -> Result<usize, String> {
        let page_bytes = u64::from(self.payload_page_bytes);
        if payload_offset >= self.payload_length || page_bytes == 0 {
            return Err("columnar payload offset is outside the object".to_owned());
        }
        as_usize(payload_offset / page_bytes)
    }

    fn payload_range(&self, index: usize) -> Result<Range<u64>, String> {
        if index >= self.payload_page_digests.len() {
            return Err("columnar payload page is absent".to_owned());
        }
        let page_bytes = u64::from(self.payload_page_bytes);
        let start = u64::try_from(index)
            .unwrap_or(u64::MAX)
            .checked_mul(page_bytes)
            .ok_or_else(|| "columnar payload page offset overflow".to_owned())?;
        Ok(start..start.saturating_add(page_bytes).min(self.payload_length))
    }

    fn validate(&self) -> Result<(), String> {
        if self.generation == 0
            || self.projection_length == 0
            || self.payload_length == 0
            || self.payload_page_bytes == 0
            || self.projection_fences.is_empty()
            || self.projection_fences.len() > MAX_COLUMNAR_ENTRIES
            || self.payload_page_digests.is_empty()
            || self.payload_page_digests.len() > MAX_COLUMNAR_ENTRIES
        {
            return Err("invalid columnar index header".to_owned());
        }
        let mut expected_offset = 0_u64;
        for (position, fence) in self.projection_fences.iter().enumerate() {
            if fence.first_key > fence.last_key
                || fence.offset != expected_offset
                || fence.length == 0
            {
                return Err("invalid columnar projection fence".to_owned());
            }
            if let Some(previous) = position
                .checked_sub(1)
                .and_then(|index| self.projection_fences.get(index))
            {
                if previous.last_key >= fence.first_key {
                    return Err("columnar projection fences overlap".to_owned());
                }
            }
            expected_offset = expected_offset
                .checked_add(fence.length)
                .ok_or_else(|| "columnar projection length overflow".to_owned())?;
        }
        if expected_offset != self.projection_length {
            return Err("columnar projection fences do not close the object".to_owned());
        }
        let page_bytes = u64::from(self.payload_page_bytes);
        let expected_pages = self.payload_length.saturating_add(page_bytes - 1) / page_bytes;
        if u64::try_from(self.payload_page_digests.len()).unwrap_or(u64::MAX) != expected_pages {
            return Err("columnar payload pages do not close the object".to_owned());
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(INDEX_MAGIC);
        encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.generation.to_be_bytes());
        encoded.extend_from_slice(&self.projection_length.to_be_bytes());
        encoded.extend_from_slice(&self.projection_digest);
        encoded.extend_from_slice(&self.payload_length.to_be_bytes());
        encoded.extend_from_slice(&self.payload_digest);
        encoded.extend_from_slice(&self.payload_page_bytes.to_be_bytes());
        encoded.extend_from_slice(
            &u32::try_from(self.projection_fences.len())
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(
            &u32::try_from(self.payload_page_digests.len())
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        for fence in &self.projection_fences {
            encoded.extend_from_slice(&fence.first_key.to_be_bytes());
            encoded.extend_from_slice(&fence.last_key.to_be_bytes());
            encoded.extend_from_slice(&fence.offset.to_be_bytes());
            encoded.extend_from_slice(&fence.length.to_be_bytes());
            encoded.extend_from_slice(&fence.digest);
        }
        for digest in &self.payload_page_digests {
            encoded.extend_from_slice(digest);
        }
        let checksum = Sha256::digest(&encoded);
        encoded.extend_from_slice(&checksum);
        Ok(encoded)
    }

    fn decode(encoded: &[u8]) -> Result<Self, String> {
        if encoded.len() < FULL_DIGEST_BYTES {
            return Err("columnar index is truncated".to_owned());
        }
        let payload_length = encoded.len() - FULL_DIGEST_BYTES;
        if Sha256::digest(&encoded[..payload_length]).as_slice() != &encoded[payload_length..] {
            return Err("columnar index checksum mismatch".to_owned());
        }
        let mut cursor = ColumnCursor::new(&encoded[..payload_length]);
        if cursor.array::<4>()? != *INDEX_MAGIC || cursor.u16()? != FORMAT_VERSION {
            return Err("unsupported columnar index format".to_owned());
        }
        let generation = cursor.u64()?;
        let projection_length = cursor.u64()?;
        let projection_digest = cursor.array::<FULL_DIGEST_BYTES>()?;
        let payload_object_length = cursor.u64()?;
        let payload_digest = cursor.array::<FULL_DIGEST_BYTES>()?;
        let payload_page_bytes = cursor.u32()?;
        let projection_count = as_usize(u64::from(cursor.u32()?))?;
        let payload_page_count = as_usize(u64::from(cursor.u32()?))?;
        if projection_count > MAX_COLUMNAR_ENTRIES || payload_page_count > MAX_COLUMNAR_ENTRIES {
            return Err("columnar index entry count exceeds the format bound".to_owned());
        }
        let mut projection_fences = Vec::with_capacity(projection_count);
        for _ in 0..projection_count {
            projection_fences.push(ProjectionFence {
                first_key: cursor.u64()?,
                last_key: cursor.u64()?,
                offset: cursor.u64()?,
                length: cursor.u64()?,
                digest: cursor.array::<RANGE_DIGEST_BYTES>()?,
            });
        }
        let mut payload_page_digests = Vec::with_capacity(payload_page_count);
        for _ in 0..payload_page_count {
            payload_page_digests.push(cursor.array::<RANGE_DIGEST_BYTES>()?);
        }
        cursor.finish()?;
        let index = Self {
            generation,
            projection_length,
            projection_digest,
            payload_length: payload_object_length,
            payload_digest,
            payload_page_bytes,
            projection_fences,
            payload_page_digests,
        };
        index.validate()?;
        Ok(index)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ColumnarManifest {
    format_version: u16,
    generation: u64,
    covered_through: u64,
    layout: String,
    projection_key: String,
    projection_bytes: u64,
    projection_sha256: String,
    payload_key: String,
    payload_bytes: u64,
    payload_sha256: String,
    index_key: String,
    index_bytes: u64,
    index_sha256: String,
    capabilities: Vec<String>,
}

impl ColumnarManifest {
    fn encode(&self) -> Result<Vec<u8>, String> {
        let payload = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let mut encoded = MANIFEST_MAGIC.to_vec();
        encoded.extend_from_slice(&payload);
        let checksum = Sha256::digest(&encoded);
        encoded.extend_from_slice(&checksum);
        Ok(encoded)
    }

    fn decode(encoded: &[u8]) -> Result<Self, String> {
        if encoded.len() < MANIFEST_MAGIC.len() + FULL_DIGEST_BYTES
            || &encoded[..MANIFEST_MAGIC.len()] != MANIFEST_MAGIC
        {
            return Err("columnar manifest framing mismatch".to_owned());
        }
        let payload_end = encoded.len() - FULL_DIGEST_BYTES;
        if Sha256::digest(&encoded[..payload_end]).as_slice() != &encoded[payload_end..] {
            return Err("columnar manifest checksum mismatch".to_owned());
        }
        let manifest = serde_json::from_slice::<Self>(&encoded[MANIFEST_MAGIC.len()..payload_end])
            .map_err(|error| error.to_string())?;
        if manifest.format_version != FORMAT_VERSION
            || manifest.generation == 0
            || manifest.covered_through == 0
            || manifest.layout != StorageLayoutMode::ColumnarRangeOverlayCandidate.subject()
            || manifest.projection_key != PROJECTION_KEY
            || manifest.payload_key != PAYLOAD_KEY
            || manifest.index_key != INDEX_KEY
            || manifest.projection_bytes == 0
            || manifest.payload_bytes == 0
            || manifest.index_bytes == 0
        {
            return Err("invalid columnar manifest".to_owned());
        }
        Ok(manifest)
    }
}

struct EncodedColumnarLayout {
    projection: Vec<u8>,
    payload: Vec<u8>,
    index: ColumnarIndex,
    index_encoded: Vec<u8>,
    manifest: ColumnarManifest,
    manifest_encoded: Vec<u8>,
}

struct PreparedColumnarLayout {
    manifest: ColumnarManifest,
    index: ColumnarIndex,
    manifest_sha256: String,
    manifest_bytes: u64,
    index_bytes: u64,
    projection_bytes: u64,
    payload_bytes: u64,
    active_manifest_complete: bool,
}

/// Generation-pinned C5 reader state used by the RFC-0048 matched curve.
pub(super) struct T28ColumnarLayoutCore {
    backend: Arc<dyn Backend>,
    prepared: Arc<PreparedColumnarLayout>,
    read_version: u64,
}

/// One C5 provider plus counters that are not owned by the shared scheduler.
pub(super) struct T28ColumnarScanCore {
    provider: Arc<RangeStripeTableProvider>,
    source: Arc<ColumnarProjectionStripeSource>,
}

/// Stable snapshot of C5-specific object-fetch counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct T28ColumnarSourceSnapshot {
    pub projection_fetch_requests: u64,
    pub peak_fetch_bytes: u64,
    pub payload_requests: u64,
    pub payload_response_bytes: u64,
}

struct RangeEngineCache {
    projection_stripes: BTreeMap<usize, Vec<ProjectionRecord>>,
    payload_pages: BTreeMap<usize, Bytes>,
    insertion_order: VecDeque<CacheEntry>,
    admission_mode: ColumnarCacheAdmissionMode,
    ghost_order: VecDeque<CacheEntry>,
    ghost_entries: BTreeSet<CacheEntry>,
    ghost_capacity: usize,
    resident_bytes: u64,
    capacity_bytes: u64,
    evictions: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CacheEntry {
    Projection(usize),
    Payload(usize),
}

impl RangeEngineCache {
    fn new(capacity_bytes: usize) -> Self {
        Self::with_admission(capacity_bytes, ColumnarCacheAdmissionMode::FullAdmit, 0)
    }

    fn with_admission(
        capacity_bytes: usize,
        admission_mode: ColumnarCacheAdmissionMode,
        ghost_capacity: usize,
    ) -> Self {
        Self {
            projection_stripes: BTreeMap::new(),
            payload_pages: BTreeMap::new(),
            insertion_order: VecDeque::new(),
            admission_mode,
            ghost_order: VecDeque::new(),
            ghost_entries: BTreeSet::new(),
            ghost_capacity,
            resident_bytes: 0,
            capacity_bytes: u64::try_from(capacity_bytes).unwrap_or(u64::MAX),
            evictions: 0,
        }
    }

    fn record_ghost(&mut self, entry: CacheEntry) {
        if self.ghost_capacity == 0 || self.ghost_entries.contains(&entry) {
            return;
        }
        while self.ghost_entries.len() >= self.ghost_capacity {
            let Some(expired) = self.ghost_order.pop_front() else {
                break;
            };
            self.ghost_entries.remove(&expired);
        }
        self.ghost_entries.insert(entry);
        self.ghost_order.push_back(entry);
    }

    fn should_admit(&mut self, entry: CacheEntry) -> bool {
        match self.admission_mode {
            ColumnarCacheAdmissionMode::FullAdmit => true,
            ColumnarCacheAdmissionMode::NeverAdmitControl => false,
            ColumnarCacheAdmissionMode::GhostTwoChance => {
                if self.ghost_entries.remove(&entry) {
                    self.ghost_order.retain(|candidate| *candidate != entry);
                    true
                } else {
                    self.record_ghost(entry);
                    false
                }
            }
        }
    }

    fn reserve(&mut self, bytes: u64) -> bool {
        if bytes > self.capacity_bytes {
            return false;
        }
        while self.resident_bytes.saturating_add(bytes) > self.capacity_bytes {
            let Some(entry) = self.insertion_order.pop_front() else {
                break;
            };
            let removed = match entry {
                CacheEntry::Projection(index) => self
                    .projection_stripes
                    .remove(&index)
                    .map_or(0, |records| projection_encoded_bytes(records.len())),
                CacheEntry::Payload(index) => self
                    .payload_pages
                    .remove(&index)
                    .map_or(0, |page| u64::try_from(page.len()).unwrap_or(u64::MAX)),
            };
            self.resident_bytes = self.resident_bytes.saturating_sub(removed);
            if removed > 0 {
                self.evictions = self.evictions.saturating_add(1);
                self.record_ghost(entry);
            }
        }
        self.resident_bytes.saturating_add(bytes) <= self.capacity_bytes
    }

    fn insert_projection(&mut self, index: usize, records: Vec<ProjectionRecord>) {
        let entry = CacheEntry::Projection(index);
        if !self.should_admit(entry) {
            return;
        }
        let bytes = projection_encoded_bytes(records.len());
        if self.reserve(bytes) {
            self.resident_bytes = self.resident_bytes.saturating_add(bytes);
            self.projection_stripes.insert(index, records);
            self.insertion_order.push_back(entry);
        }
    }

    fn insert_payload(&mut self, index: usize, page: Bytes) {
        let entry = CacheEntry::Payload(index);
        if !self.should_admit(entry) {
            return;
        }
        let bytes = u64::try_from(page.len()).unwrap_or(u64::MAX);
        if self.reserve(bytes) {
            self.resident_bytes = self.resident_bytes.saturating_add(bytes);
            self.payload_pages.insert(index, page);
            self.insertion_order.push_back(entry);
        }
    }
}

struct ColumnarProjectionStripeSource {
    backend: Arc<dyn Backend>,
    prepared: Arc<PreparedColumnarLayout>,
    read_version: u64,
    mode: ColumnarDataFusionMode,
    scan_fetch_target_bytes: usize,
    scan_group: Mutex<Option<CachedProjectionGroup>>,
    projection_fetch_requests: AtomicU64,
    peak_fetch_bytes: AtomicU64,
    payload_requests: AtomicU64,
    payload_response_bytes: AtomicU64,
}

struct CachedProjectionGroup {
    first_stripe: usize,
    end_stripe: usize,
    object_offset: u64,
    bytes: Bytes,
}

impl CachedProjectionGroup {
    fn contains(&self, stripe_index: usize) -> bool {
        (self.first_stripe..self.end_stripe).contains(&stripe_index)
    }

    fn stripe_bytes(&self, index: &ColumnarIndex, stripe_index: usize) -> Result<Bytes, String> {
        let range = index.projection_range(stripe_index)?;
        let start = as_usize(
            range
                .start
                .checked_sub(self.object_offset)
                .ok_or_else(|| "cached scan group begins after its stripe".to_owned())?,
        )?;
        let end = start
            .checked_add(as_usize(range.end.saturating_sub(range.start))?)
            .ok_or_else(|| "cached scan-group slice overflow".to_owned())?;
        if end > self.bytes.len() {
            return Err("cached scan group does not cover its stripe".to_owned());
        }
        Ok(self.bytes.slice(start..end))
    }
}

impl Debug for ColumnarProjectionStripeSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ColumnarProjectionStripeSource")
            .field("stripes", &self.prepared.index.projection_fences.len())
            .field("read_version", &self.read_version)
            .field("mode", &self.mode)
            .field("scan_fetch_target_bytes", &self.scan_fetch_target_bytes)
            .finish_non_exhaustive()
    }
}

impl T28ColumnarLayoutCore {
    pub(super) async fn open(
        inner: Arc<dyn Backend>,
        child: &TypedLayoutChildV1,
        read_version: u64,
    ) -> Result<Self, String> {
        if child.subject != TypedLayoutSubjectV1::C5ColumnarMain
            || read_version == 0
            || read_version > child.covered_through_version
        {
            return Err("invalid RFC-0048 C5 reader identity or version".to_owned());
        }
        let backend: Arc<dyn Backend> = Arc::new(GenerationPinnedChildBackend::new(inner, child)?);
        let prepared = Arc::new(reopen_columnar_layout(backend.as_ref()).await?);
        if prepared.manifest.covered_through != child.covered_through_version
            || prepared.manifest.generation != prepared.index.generation
        {
            return Err("RFC-0048 C5 manifest coverage or format generation mismatch".to_owned());
        }

        let expected = [
            (
                MANIFEST_KEY,
                TypedLayoutObjectRoleV1::Manifest,
                prepared.manifest_bytes,
                prepared.manifest_sha256.as_str(),
            ),
            (
                INDEX_KEY,
                TypedLayoutObjectRoleV1::Index,
                prepared.index_bytes,
                prepared.manifest.index_sha256.as_str(),
            ),
            (
                PROJECTION_KEY,
                TypedLayoutObjectRoleV1::Projection,
                prepared.projection_bytes,
                prepared.manifest.projection_sha256.as_str(),
            ),
            (
                PAYLOAD_KEY,
                TypedLayoutObjectRoleV1::Payload,
                prepared.payload_bytes,
                prepared.manifest.payload_sha256.as_str(),
            ),
        ];
        let expected_keys = expected
            .iter()
            .map(|(key, _, _, _)| *key)
            .collect::<BTreeSet<_>>();
        let actual_keys = child
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<BTreeSet<_>>();
        if expected_keys != actual_keys {
            return Err("RFC-0048 C5 descriptor has unreachable or missing media".to_owned());
        }
        for (key, role, length, sha256) in expected {
            let identity = child
                .object(key)
                .filter(|object| object.role == role)
                .ok_or_else(|| "RFC-0048 C5 object role is absent".to_owned())?;
            if identity.length != length || identity.sha256 != sha256 {
                return Err("RFC-0048 C5 descriptor differs from its manifest".to_owned());
            }
        }

        Ok(Self {
            backend,
            prepared,
            read_version,
        })
    }

    pub(super) async fn point(
        &self,
        key: u64,
        read_version: u64,
    ) -> Result<PointReadOutcome, String> {
        if read_version == 0 || read_version > self.read_version {
            return Err("RFC-0048 C5 point version exceeds the opened snapshot".to_owned());
        }
        columnar_point(
            self.backend.as_ref(),
            self.prepared.as_ref(),
            key,
            read_version,
        )
        .await
    }

    pub(super) fn table_provider(&self, scan_fetch_target_bytes: usize) -> T28ColumnarScanCore {
        let source = Arc::new(ColumnarProjectionStripeSource {
            backend: Arc::clone(&self.backend),
            prepared: Arc::clone(&self.prepared),
            read_version: self.read_version,
            mode: ColumnarDataFusionMode::Correct,
            scan_fetch_target_bytes,
            scan_group: Mutex::new(None),
            projection_fetch_requests: AtomicU64::new(0),
            peak_fetch_bytes: AtomicU64::new(0),
            payload_requests: AtomicU64::new(0),
            payload_response_bytes: AtomicU64::new(0),
        });
        T28ColumnarScanCore {
            provider: Arc::new(RangeStripeTableProvider::new(source.clone())),
            source,
        }
    }

    pub(super) fn resident_metadata_bytes(&self) -> u64 {
        self.prepared
            .manifest_bytes
            .saturating_add(self.prepared.index_bytes)
    }
}

impl T28ColumnarScanCore {
    pub(super) fn provider(&self) -> Arc<RangeStripeTableProvider> {
        Arc::clone(&self.provider)
    }

    pub(super) fn source_snapshot(&self) -> T28ColumnarSourceSnapshot {
        T28ColumnarSourceSnapshot {
            projection_fetch_requests: self
                .source
                .projection_fetch_requests
                .load(Ordering::Relaxed),
            peak_fetch_bytes: self.source.peak_fetch_bytes.load(Ordering::Relaxed),
            payload_requests: self.source.payload_requests.load(Ordering::Relaxed),
            payload_response_bytes: self.source.payload_response_bytes.load(Ordering::Relaxed),
        }
    }
}

impl ColumnarProjectionStripeSource {
    async fn projection_stripe(
        &self,
        stripe_index: usize,
    ) -> Result<Vec<ProjectionRecord>, String> {
        if let Some(bytes) = self
            .scan_group
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|group| group.contains(stripe_index))
            .map(|group| group.stripe_bytes(&self.prepared.index, stripe_index))
            .transpose()?
        {
            return decode_projection_stripe(&bytes);
        }

        let fences = &self.prepared.index.projection_fences;
        let mut end_stripe = stripe_index;
        let mut fetch_bytes = 0_u64;
        let target = u64::try_from(self.scan_fetch_target_bytes).unwrap_or(u64::MAX);
        while end_stripe < fences.len() {
            let next = fences[end_stripe].length;
            if end_stripe > stripe_index
                && (target == 0 || fetch_bytes.saturating_add(next) > target)
            {
                break;
            }
            fetch_bytes = fetch_bytes.saturating_add(next);
            end_stripe = end_stripe.saturating_add(1);
        }
        let first = fences
            .get(stripe_index)
            .ok_or_else(|| "projection stripe is absent".to_owned())?;
        let last = fences
            .get(end_stripe.saturating_sub(1))
            .ok_or_else(|| "projection scan group is empty".to_owned())?;
        let range = first.offset..last.offset.saturating_add(last.length);
        let read = self
            .backend
            .get(
                &self.prepared.manifest.projection_key,
                Some(range.clone()),
                None,
            )
            .await
            .map_err(|error| error.to_string())?;
        if read.returned_range != range
            || u64::try_from(read.bytes.len()).unwrap_or(u64::MAX)
                != range.end.saturating_sub(range.start)
        {
            return Err("columnar scan-group range framing mismatch".to_owned());
        }
        for (index, fence) in fences
            .iter()
            .enumerate()
            .take(end_stripe)
            .skip(stripe_index)
        {
            let start = as_usize(fence.offset.saturating_sub(range.start))?;
            let end = start
                .checked_add(as_usize(fence.length)?)
                .ok_or_else(|| "columnar scan-group verification overflow".to_owned())?;
            let stripe = read
                .bytes
                .get(start..end)
                .ok_or_else(|| format!("scan group omits projection stripe {index}"))?;
            if range_digest(stripe) != fence.digest {
                return Err(format!("projection stripe {index} checksum mismatch"));
            }
        }
        let group_bytes = u64::try_from(read.bytes.len()).unwrap_or(u64::MAX);
        self.projection_fetch_requests
            .fetch_add(1, Ordering::Relaxed);
        self.peak_fetch_bytes
            .fetch_max(group_bytes, Ordering::Relaxed);
        let group = CachedProjectionGroup {
            first_stripe: stripe_index,
            end_stripe,
            object_offset: range.start,
            bytes: read.bytes,
        };
        let stripe = group.stripe_bytes(&self.prepared.index, stripe_index)?;
        *self
            .scan_group
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(group);
        decode_projection_stripe(&stripe)
    }
}

#[async_trait]
impl RangeStripeSource for ColumnarProjectionStripeSource {
    fn schema(&self) -> SchemaRef {
        projection_schema()
    }

    fn stripe_count(&self) -> usize {
        self.prepared.index.projection_fences.len()
    }

    async fn read_stripe(
        &self,
        stripe_index: usize,
        projection: Option<&[usize]>,
    ) -> DataFusionResult<RecordBatch> {
        let records = self
            .projection_stripe(stripe_index)
            .await
            .map_err(DataFusionError::Execution)?;
        if self.mode == ColumnarDataFusionMode::PayloadPrefetchPoison {
            let page_index = stripe_index % self.prepared.index.payload_page_digests.len();
            let payload =
                fetch_payload_page(self.backend.as_ref(), self.prepared.as_ref(), page_index)
                    .await
                    .map_err(DataFusionError::Execution)?;
            self.payload_requests.fetch_add(1, Ordering::Relaxed);
            self.payload_response_bytes.fetch_add(
                u64::try_from(payload.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        let rows = visible_projection_rows(&records, self.read_version);
        projection_batch(&rows, projection)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

pub(super) fn projection_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::UInt64, false),
        Field::new("tenant", DataType::UInt32, false),
        Field::new("category", DataType::UInt16, false),
        Field::new("quantity", DataType::Int64, false),
    ]))
}

fn visible_projection_rows(records: &[ProjectionRecord], read_version: u64) -> Vec<ProjectedRow> {
    let mut projected = Vec::new();
    let mut cursor = 0_usize;
    while cursor < records.len() {
        let key = records[cursor].key;
        let mut visible = None;
        while cursor < records.len() && records[cursor].key == key {
            if visible.is_none() && records[cursor].version <= read_version {
                visible = Some(&records[cursor]);
            }
            cursor += 1;
        }
        let Some(fields) = visible.and_then(|record| record.fields.as_ref()) else {
            continue;
        };
        projected.push(ProjectedRow {
            key,
            tenant: fields.tenant,
            category: fields.category,
            quantity: fields.quantity,
        });
    }
    projected
}

pub(super) fn projection_batch(
    rows: &[ProjectedRow],
    projection: Option<&[usize]>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let full_schema = projection_schema();
    let indices = projection.map_or_else(
        || (0..full_schema.fields().len()).collect(),
        <[usize]>::to_vec,
    );
    let arrays = indices
        .iter()
        .map(|index| match index {
            0 => Ok(Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.key).collect::<Vec<_>>(),
            )) as ArrayRef),
            1 => Ok(Arc::new(UInt32Array::from(
                rows.iter().map(|row| row.tenant).collect::<Vec<_>>(),
            )) as ArrayRef),
            2 => Ok(Arc::new(UInt16Array::from(
                rows.iter().map(|row| row.category).collect::<Vec<_>>(),
            )) as ArrayRef),
            3 => Ok(Arc::new(Int64Array::from(
                rows.iter().map(|row| row.quantity).collect::<Vec<_>>(),
            )) as ArrayRef),
            other => Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                "projection column {other} is outside the C5 schema"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(full_schema.project(&indices)?);
    RecordBatch::try_new_with_options(
        schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
}

fn aggregate_projection(rows: &[ProjectedRow]) -> Vec<(u32, i64, u64)> {
    let mut groups = BTreeMap::<u32, (i64, u64)>::new();
    for row in rows.iter().filter(|row| (8..=31).contains(&row.category)) {
        let entry = groups.entry(row.tenant).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(row.quantity);
        entry.1 = entry.1.saturating_add(1);
    }
    groups
        .into_iter()
        .map(|(tenant, (quantity, count))| (tenant, quantity, count))
        .collect()
}

fn decode_aggregate_batches(batches: &[RecordBatch]) -> Result<Vec<(u32, i64, u64)>, String> {
    let mut groups = Vec::new();
    for batch in batches {
        let tenant = batch
            .column_by_name("tenant")
            .and_then(|array| array.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| "DataFusion aggregate tenant column is absent".to_owned())?;
        let quantity = batch
            .column_by_name("total_quantity")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| "DataFusion aggregate quantity column is absent".to_owned())?;
        let count = batch
            .column_by_name("row_count")
            .ok_or_else(|| "DataFusion aggregate count column is absent".to_owned())?;
        for row in 0..batch.num_rows() {
            let count = if let Some(count) = count.as_any().downcast_ref::<UInt64Array>() {
                count.value(row)
            } else if let Some(count) = count.as_any().downcast_ref::<Int64Array>() {
                u64::try_from(count.value(row))
                    .map_err(|_| "DataFusion aggregate count is negative".to_owned())?
            } else {
                return Err("DataFusion aggregate count column has the wrong type".to_owned());
            };
            groups.push((tenant.value(row), quantity.value(row), count));
        }
    }
    groups.sort_unstable();
    Ok(groups)
}

#[allow(clippy::too_many_lines)]
pub(super) async fn run_columnar_datafusion_seed(
    mode: ColumnarDataFusionMode,
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
    history: &LogicalHistory,
    observed: Arc<ObservedBackend>,
    scan_fetch_target_bytes: usize,
) -> Result<ColumnarDataFusionSample, String> {
    let backend: Arc<dyn Backend> = observed.clone();
    let prepared = Arc::new(prepare_columnar_layout(profile, history, backend.as_ref()).await?);
    let read_version = profile.base_version.saturating_add(profile.delta_cycles);
    let expected_rows = history.final_rows(read_version);
    let expected = aggregate_projection(&expected_rows);
    observed.clear_stats();
    let source = Arc::new(ColumnarProjectionStripeSource {
        backend,
        prepared: Arc::clone(&prepared),
        read_version,
        mode,
        scan_fetch_target_bytes,
        scan_group: Mutex::new(None),
        projection_fetch_requests: AtomicU64::new(0),
        peak_fetch_bytes: AtomicU64::new(0),
        payload_requests: AtomicU64::new(0),
        payload_response_bytes: AtomicU64::new(0),
    });
    let provider = Arc::new(RangeStripeTableProvider::new(source.clone()));
    let source_stats = provider.stats();
    let context = SessionContext::new();
    context
        .register_table("c5", provider)
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let batches = context
        .sql(
            "SELECT tenant, SUM(quantity) AS total_quantity, COUNT(*) AS row_count \
             FROM c5 WHERE category BETWEEN 8 AND 31 GROUP BY tenant ORDER BY tenant",
        )
        .await
        .map_err(|error| error.to_string())?
        .collect()
        .await
        .map_err(|error| error.to_string())?;
    let query_seconds = started.elapsed().as_secs_f64();
    let actual = decode_aggregate_batches(&batches)?;
    let stats = source_stats.snapshot();
    let object_stats = observed.stats();
    let payload_requests = source.payload_requests.load(Ordering::Relaxed);
    let payload_response_bytes = source.payload_response_bytes.load(Ordering::Relaxed);
    let projection_fetch_requests = source.projection_fetch_requests.load(Ordering::Relaxed);
    let peak_fetch_bytes = source.peak_fetch_bytes.load(Ordering::Relaxed);
    let query_anomalies = u64::from(actual != expected).saturating_add(u64::from(
        stats.rows_emitted != u64::try_from(expected_rows.len()).unwrap_or(u64::MAX),
    ));
    let trace = serde_json::to_vec(&(
        mode.id(),
        &history.canonical_sha256,
        &expected,
        &actual,
        stats.rows_emitted,
        stats.stripes_read,
        projection_fetch_requests,
        payload_requests,
    ))
    .map_err(|error| error.to_string())?;
    Ok(ColumnarDataFusionSample {
        seed,
        repeat,
        mode: mode.id().to_owned(),
        canonical_history_sha256: history.canonical_sha256.clone(),
        trace_sha256: content_sha256(&trace),
        query_anomalies,
        expected_groups: u64::try_from(expected.len()).unwrap_or(u64::MAX),
        result_groups: u64::try_from(actual.len()).unwrap_or(u64::MAX),
        source_rows: stats.rows_emitted,
        source_stripes: stats.stripes_read,
        source_batches: stats.batches_emitted,
        scan_plans: stats.scan_plans,
        projection_pushdown_plans: stats.projection_pushdown_plans,
        peak_batch_rows: stats.peak_batch_rows,
        peak_batch_bytes: stats.peak_batch_bytes,
        scan_fetch_target_bytes: u64::try_from(scan_fetch_target_bytes).unwrap_or(u64::MAX),
        peak_fetch_bytes,
        maximum_projection_stripe_bytes: prepared
            .index
            .projection_fences
            .iter()
            .map(|fence| fence.length)
            .max()
            .unwrap_or(0),
        projection_fetch_requests,
        object_requests: successful_requests(&object_stats, &["get.range", "get"]),
        full_object_requests: successful_requests(&object_stats, &["get"]),
        object_response_bytes: response_bytes(&object_stats),
        opaque_payload_requests: payload_requests,
        opaque_payload_response_bytes: payload_response_bytes,
        list_requests: successful_requests(&object_stats, &["list"]),
        query_seconds,
        source_rows_per_second: rate(stats.rows_emitted, query_seconds),
        projection_bytes: prepared.projection_bytes,
        payload_bytes: prepared.payload_bytes,
        checksum_covered_ranges: true,
        poison_detected: mode == ColumnarDataFusionMode::PayloadPrefetchPoison
            && payload_requests > 0,
    })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::cast_precision_loss
)]
pub(super) async fn run_columnar_cache_admission_seed(
    mode: ColumnarCacheAdmissionMode,
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
    history: &LogicalHistory,
    observed: Arc<ObservedBackend>,
    cache_ratio_percent: u32,
    zipf_alpha: f64,
) -> Result<ColumnarCacheAdmissionSample, String> {
    let backend: Arc<dyn Backend> = observed.clone();
    let prepared = prepare_columnar_layout(profile, history, backend.as_ref()).await?;
    let durable_data_bytes = prepared
        .projection_bytes
        .saturating_add(prepared.payload_bytes);
    let capacity_bytes = durable_data_bytes
        .saturating_mul(u64::from(cache_ratio_percent))
        .saturating_div(100);
    let capacity = usize::try_from(capacity_bytes).unwrap_or(usize::MAX);
    let minimum_entry_bytes = minimum_overlay_cache_bytes(profile)?.max(1);
    let ghost_capacity = capacity
        .saturating_div(minimum_entry_bytes)
        .saturating_mul(2)
        .max(1);
    let mut cache = RangeEngineCache::with_admission(capacity, mode, ghost_capacity);
    let final_version = profile.base_version.saturating_add(profile.delta_cycles);
    let point_keys = zipf_operation_keys(
        profile.key_count,
        profile.point_operations,
        seed,
        zipf_alpha,
    )?;
    let mut point_anomalies = 0_u64;

    for _ in 0..2 {
        for key in &point_keys {
            let expected = expected_outcome(history.visible(*key, final_version));
            let (actual, _) = cached_columnar_point_with_hit(
                backend.as_ref(),
                &prepared,
                &mut cache,
                *key,
                final_version,
            )
            .await?;
            point_anomalies = point_anomalies.saturating_add(u64::from(actual != expected));
        }
    }

    observed.clear_stats();
    let mut pre_scan_hits = 0_u64;
    for key in &point_keys {
        let expected = expected_outcome(history.visible(*key, final_version));
        let (actual, hit) = cached_columnar_point_with_hit(
            backend.as_ref(),
            &prepared,
            &mut cache,
            *key,
            final_version,
        )
        .await?;
        point_anomalies = point_anomalies.saturating_add(u64::from(actual != expected));
        pre_scan_hits = pre_scan_hits.saturating_add(u64::from(hit));
    }
    let pre_scan_stats = observed.stats();

    observed.clear_stats();
    for key in 0..profile.key_count {
        let expected = expected_outcome(history.visible(key, final_version));
        let (actual, _) = cached_columnar_point_with_hit(
            backend.as_ref(),
            &prepared,
            &mut cache,
            key,
            final_version,
        )
        .await?;
        point_anomalies = point_anomalies.saturating_add(u64::from(actual != expected));
    }
    let pollution_stats = observed.stats();

    observed.clear_stats();
    let mut post_scan_hits = 0_u64;
    for key in &point_keys {
        let expected = expected_outcome(history.visible(*key, final_version));
        let (actual, hit) = cached_columnar_point_with_hit(
            backend.as_ref(),
            &prepared,
            &mut cache,
            *key,
            final_version,
        )
        .await?;
        point_anomalies = point_anomalies.saturating_add(u64::from(actual != expected));
        post_scan_hits = post_scan_hits.saturating_add(u64::from(hit));
    }
    let post_scan_stats = observed.stats();
    let operations = u64::try_from(point_keys.len()).unwrap_or(u64::MAX);
    let mut trace = Sha256::new();
    trace.update(seed.to_be_bytes());
    trace.update(cache_ratio_percent.to_be_bytes());
    trace.update(zipf_alpha.to_bits().to_be_bytes());
    for key in &point_keys {
        trace.update(key.to_be_bytes());
    }
    Ok(ColumnarCacheAdmissionSample {
        seed,
        repeat,
        mode: mode.id().to_owned(),
        cache_ratio_percent,
        zipf_alpha,
        trace_sha256: format!("{:x}", trace.finalize()),
        point_operations: operations,
        point_anomalies,
        pre_scan_hit_ratio: pre_scan_hits as f64 / operations.max(1) as f64,
        post_scan_hit_ratio: post_scan_hits as f64 / operations.max(1) as f64,
        pre_scan_object_requests: successful_requests(&pre_scan_stats, &["get.range", "get"]),
        post_scan_object_requests: successful_requests(&post_scan_stats, &["get.range", "get"]),
        pollution_object_requests: successful_requests(&pollution_stats, &["get.range", "get"]),
        pre_scan_response_bytes: response_bytes(&pre_scan_stats),
        post_scan_response_bytes: response_bytes(&post_scan_stats),
        pollution_response_bytes: response_bytes(&pollution_stats),
        resident_bytes: cache.resident_bytes,
        capacity_bytes: cache.capacity_bytes,
        ghost_entries: u64::try_from(cache.ghost_entries.len()).unwrap_or(u64::MAX),
        evictions: cache.evictions,
    })
}

#[allow(clippy::cast_precision_loss)]
fn zipf_operation_keys(
    key_count: u64,
    operations: usize,
    seed: u64,
    alpha: f64,
) -> Result<Vec<u64>, String> {
    if key_count == 0 {
        return Err("Zipf trace requires at least one key".to_owned());
    }
    let key_count_usize = usize::try_from(key_count).map_err(|error| error.to_string())?;
    let mut cumulative = Vec::with_capacity(key_count_usize);
    let mut total = 0.0_f64;
    for rank in 1..=key_count_usize {
        total += 1.0 / (rank as f64).powf(alpha);
        cumulative.push(total);
    }
    let multiplier = seed.wrapping_mul(2).wrapping_add(1);
    let keys = (0..operations)
        .map(|operation| {
            let mixed = splitmix64(seed ^ u64::try_from(operation).unwrap_or(u64::MAX));
            let sample = mixed as f64 / u64::MAX as f64;
            let target = sample * total;
            let rank = cumulative.partition_point(|value| *value < target);
            u64::try_from(rank)
                .unwrap_or(u64::MAX)
                .wrapping_mul(multiplier)
                .wrapping_add(seed)
                % key_count
        })
        .collect();
    Ok(keys)
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[allow(clippy::too_many_lines)]
pub(super) async fn run_columnar_overlay_seed(
    profile: &StorageLayoutProfile,
    seed: u64,
    repeat: u32,
    history: &LogicalHistory,
    observed: Arc<ObservedBackend>,
) -> Result<StorageLayoutSample, String> {
    let backend: Arc<dyn Backend> = observed.clone();
    let build_started = Instant::now();
    let prepared = prepare_columnar_layout(profile, history, backend.as_ref()).await?;
    let build_seconds = build_started.elapsed().as_secs_f64();
    let point_keys = operation_keys(profile.key_count, profile.point_operations, seed);

    observed.clear_stats();
    let mut point_latencies = Vec::with_capacity(point_keys.len());
    let mut point_anomalies = 0_u64;
    for (key, read_version) in &point_keys {
        let started = Instant::now();
        let expected = expected_outcome(history.visible(*key, *read_version));
        let actual = columnar_point(backend.as_ref(), &prepared, *key, *read_version).await?;
        point_latencies.push(elapsed_ns(started));
        point_anomalies = point_anomalies.saturating_add(u64::from(actual != expected));
    }
    let point_stats = observed.stats();

    observed.clear_stats();
    let mut cache = RangeEngineCache::new(profile.overlay_cache_bytes);
    let mut warm_point_anomalies = 0_u64;
    for (key, read_version) in &point_keys {
        let expected = expected_outcome(history.visible(*key, *read_version));
        let actual =
            cached_columnar_point(backend.as_ref(), &prepared, &mut cache, *key, *read_version)
                .await?;
        warm_point_anomalies = warm_point_anomalies.saturating_add(u64::from(actual != expected));
    }
    let overlay_fill_stats = observed.stats();

    observed.clear_stats();
    let mut warm_latencies = Vec::with_capacity(point_keys.len());
    for (key, read_version) in &point_keys {
        let started = Instant::now();
        let expected = expected_outcome(history.visible(*key, *read_version));
        let actual =
            cached_columnar_point(backend.as_ref(), &prepared, &mut cache, *key, *read_version)
                .await?;
        warm_latencies.push(elapsed_ns(started));
        warm_point_anomalies = warm_point_anomalies.saturating_add(u64::from(actual != expected));
    }
    let warm_stats = observed.stats();

    observed.clear_stats();
    let scan_started = Instant::now();
    let final_version = profile.base_version.saturating_add(profile.delta_cycles);
    let projected = columnar_projected_scan(backend.as_ref(), &prepared, final_version).await?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let scan_stats = observed.stats();
    let expected_projection = history.final_rows(final_version);
    let scan_anomalies = u64::from(projected != expected_projection);

    observed.clear_stats();
    let reopened = reopen_columnar_layout(backend.as_ref()).await?;
    let restart_key = point_keys[0];
    let restart_point =
        columnar_point(backend.as_ref(), &reopened, restart_key.0, restart_key.1).await?;
    let rebuilt = reconstruct_columnar_history(backend.as_ref(), &reopened).await?;
    let restart_stats = observed.stats();
    let restart_anomalies = u64::from(reopened.manifest != prepared.manifest)
        .saturating_add(u64::from(
            restart_point != expected_outcome(history.visible(restart_key.0, restart_key.1)),
        ))
        .saturating_add(u64::from(rebuilt != history.records));

    let compaction_written_bytes = columnar_compaction_written_bytes(profile, history)?;
    let data_bytes = prepared
        .projection_bytes
        .saturating_add(prepared.payload_bytes);
    let stored_bytes = data_bytes
        .saturating_add(prepared.index_bytes)
        .saturating_add(prepared.manifest_bytes);
    let live_logical_bytes = live_logical_bytes(profile, expected_projection.len())?;
    let branch_manifest = branch_manifest_bytes(
        StorageLayoutMode::ColumnarRangeOverlayCandidate.subject(),
        &prepared.manifest_sha256,
        stored_bytes,
    )?;
    let operations = u64::try_from(point_keys.len()).unwrap_or(u64::MAX);
    let rows = u64::try_from(expected_projection.len()).unwrap_or(u64::MAX);
    let point_summary = latency_summary(&mut point_latencies);
    let warm_summary = latency_summary(&mut warm_latencies);
    let all_stats = [
        &point_stats,
        &overlay_fill_stats,
        &warm_stats,
        &scan_stats,
        &restart_stats,
    ];
    Ok(StorageLayoutSample {
        seed,
        repeat,
        subject: StorageLayoutMode::ColumnarRangeOverlayCandidate
            .subject()
            .to_owned(),
        canonical_history_sha256: history.canonical_sha256.clone(),
        post_compaction_sha256: logical_digest(&rebuilt),
        point_operations: operations,
        point_anomalies,
        scan_anomalies,
        accounting_anomalies: 0,
        invalidation_anomalies: 0,
        point_latency_ns_p50: point_summary.0,
        point_latency_ns_p95: point_summary.1,
        point_latency_ns_p99: point_summary.2,
        point_requests: successful_requests(&point_stats, &["get.range", "get"]),
        point_full_object_requests: successful_requests(&point_stats, &["get"]),
        point_response_bytes: response_bytes(&point_stats),
        point_backend_elapsed_micros: elapsed_micros(&point_stats),
        overlay_fill_requests: successful_requests(&overlay_fill_stats, &["get.range", "get"]),
        overlay_fill_response_bytes: response_bytes(&overlay_fill_stats),
        overlay_resident_bytes: cache.resident_bytes,
        overlay_capacity_bytes: cache.capacity_bytes,
        warm_point_operations: operations,
        warm_point_anomalies,
        warm_point_latency_ns_p99: warm_summary.2,
        warm_point_requests: successful_requests(&warm_stats, &["get.range", "get"]),
        warm_point_response_bytes: response_bytes(&warm_stats),
        scan_requests: successful_requests(&scan_stats, &["get.range", "get"]),
        scan_response_bytes: response_bytes(&scan_stats),
        scan_opaque_payload_bytes: 0,
        scan_backend_elapsed_micros: elapsed_micros(&scan_stats),
        scan_rows: rows,
        scan_seconds,
        scan_rows_per_second: rate(rows, scan_seconds),
        manifest_bytes: prepared.manifest_bytes,
        index_bytes: prepared.index_bytes,
        data_bytes,
        stored_bytes,
        live_logical_bytes,
        storage_amplification: ratio(stored_bytes, live_logical_bytes),
        resident_index_bytes: prepared.index_bytes.saturating_add(prepared.manifest_bytes),
        build_seconds,
        build_rows_per_second: rate(
            u64::try_from(history.records.len()).unwrap_or(u64::MAX),
            build_seconds,
        ),
        compaction_written_bytes,
        logical_history_bytes: history.logical_history_bytes,
        compaction_write_amplification: ratio(
            compaction_written_bytes,
            history.logical_history_bytes,
        ),
        branch_incremental_bytes: u64::try_from(branch_manifest.len()).unwrap_or(u64::MAX),
        branch_shared_bytes: stored_bytes,
        active_manifest_complete: prepared.active_manifest_complete,
        list_requests: all_stats
            .into_iter()
            .map(|stats| successful_requests(stats, &["list"]))
            .sum(),
        checksum_covered_ranges: true,
        restart_requests: successful_requests(&restart_stats, &["get.range", "get"]),
        restart_response_bytes: response_bytes(&restart_stats),
        restart_anomalies,
        branch_reused_immutable_runs: true,
        poison_detected: false,
    })
}

async fn prepare_columnar_layout(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
    backend: &dyn Backend,
) -> Result<PreparedColumnarLayout, String> {
    let encoded = encode_columnar_layout(profile, &history.records)?;
    backend
        .put(
            PROJECTION_KEY,
            Bytes::from(encoded.projection.clone()),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    backend
        .put(
            PAYLOAD_KEY,
            Bytes::from(encoded.payload.clone()),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    backend
        .put(
            INDEX_KEY,
            Bytes::from(encoded.index_encoded.clone()),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    backend
        .put(
            MANIFEST_KEY,
            Bytes::from(encoded.manifest_encoded.clone()),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(PreparedColumnarLayout {
        manifest: encoded.manifest,
        index: encoded.index,
        manifest_sha256: content_sha256(&encoded.manifest_encoded),
        manifest_bytes: u64::try_from(encoded.manifest_encoded.len()).unwrap_or(u64::MAX),
        index_bytes: u64::try_from(encoded.index_encoded.len()).unwrap_or(u64::MAX),
        projection_bytes: u64::try_from(encoded.projection.len()).unwrap_or(u64::MAX),
        payload_bytes: u64::try_from(encoded.payload.len()).unwrap_or(u64::MAX),
        active_manifest_complete: true,
    })
}

async fn reopen_columnar_layout(backend: &dyn Backend) -> Result<PreparedColumnarLayout, String> {
    let manifest_read = backend
        .get(MANIFEST_KEY, None, None)
        .await
        .map_err(|error| error.to_string())?;
    let manifest = ColumnarManifest::decode(&manifest_read.bytes)?;
    let index_read = backend
        .get(&manifest.index_key, None, None)
        .await
        .map_err(|error| error.to_string())?;
    if u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX) != manifest.index_bytes
        || content_sha256(&index_read.bytes) != manifest.index_sha256
    {
        return Err("columnar manifest selected the wrong index object".to_owned());
    }
    let index = ColumnarIndex::decode(&index_read.bytes)?;
    if index.projection_length != manifest.projection_bytes
        || full_digest_hex(&index.projection_digest) != manifest.projection_sha256
        || index.payload_length != manifest.payload_bytes
        || full_digest_hex(&index.payload_digest) != manifest.payload_sha256
    {
        return Err("columnar manifest and index closure differ".to_owned());
    }
    let projection_bytes = manifest.projection_bytes;
    let payload_bytes = manifest.payload_bytes;
    Ok(PreparedColumnarLayout {
        manifest,
        index,
        manifest_sha256: content_sha256(&manifest_read.bytes),
        manifest_bytes: u64::try_from(manifest_read.bytes.len()).unwrap_or(u64::MAX),
        index_bytes: u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX),
        projection_bytes,
        payload_bytes,
        active_manifest_complete: true,
    })
}

async fn columnar_point(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
    key: u64,
    read_version: u64,
) -> Result<PointReadOutcome, String> {
    let Some(stripe_index) = prepared.index.locate_projection(key) else {
        return Ok(PointReadOutcome::Absent);
    };
    let records = fetch_projection_stripe(backend, prepared, stripe_index).await?;
    let Some(record) = records
        .iter()
        .find(|record| record.key == key && record.version <= read_version)
    else {
        return Ok(PointReadOutcome::Absent);
    };
    outcome_from_projection(backend, prepared, record).await
}

async fn cached_columnar_point(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
    cache: &mut RangeEngineCache,
    key: u64,
    read_version: u64,
) -> Result<PointReadOutcome, String> {
    cached_columnar_point_with_hit(backend, prepared, cache, key, read_version)
        .await
        .map(|(outcome, _)| outcome)
}

async fn cached_columnar_point_with_hit(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
    cache: &mut RangeEngineCache,
    key: u64,
    read_version: u64,
) -> Result<(PointReadOutcome, bool), String> {
    let Some(stripe_index) = prepared.index.locate_projection(key) else {
        return Ok((PointReadOutcome::Absent, true));
    };
    let projection_hit = cache.projection_stripes.contains_key(&stripe_index);
    let record = if let Some(records) = cache.projection_stripes.get(&stripe_index) {
        records
            .iter()
            .find(|record| record.key == key && record.version <= read_version)
            .cloned()
    } else {
        let range = prepared.index.projection_range(stripe_index)?;
        let raw = fetch_verified_range(
            backend,
            &prepared.manifest.projection_key,
            range,
            prepared.index.projection_fences[stripe_index].digest,
        )
        .await?;
        let records = decode_projection_stripe(&raw)?;
        let record = records
            .iter()
            .find(|record| record.key == key && record.version <= read_version)
            .cloned();
        cache.insert_projection(stripe_index, records);
        record
    };
    let Some(record) = record else {
        return Ok((PointReadOutcome::Absent, projection_hit));
    };
    let Some(fields) = record.fields else {
        return Ok((PointReadOutcome::Tombstone, projection_hit));
    };
    let page_index = prepared.index.payload_page(record.payload_offset)?;
    let payload_hit = cache.payload_pages.contains_key(&page_index);
    let payload = if let Some(page) = cache.payload_pages.get(&page_index) {
        payload_slice(
            &prepared.index,
            page_index,
            page,
            record.payload_offset,
            record.payload_length,
        )?
        .to_vec()
    } else {
        let raw = fetch_payload_page(backend, prepared, page_index).await?;
        let payload = payload_slice(
            &prepared.index,
            page_index,
            &raw,
            record.payload_offset,
            record.payload_length,
        )?
        .to_vec();
        cache.insert_payload(page_index, raw);
        payload
    };
    Ok((
        PointReadOutcome::Value(Bytes::from(ValueFields { payload, ..fields }.encode())),
        projection_hit && payload_hit,
    ))
}

async fn outcome_from_projection(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
    record: &ProjectionRecord,
) -> Result<PointReadOutcome, String> {
    let Some(fields) = record.fields.clone() else {
        return Ok(PointReadOutcome::Tombstone);
    };
    let page_index = prepared.index.payload_page(record.payload_offset)?;
    let page = fetch_payload_page(backend, prepared, page_index).await?;
    let payload = payload_slice(
        &prepared.index,
        page_index,
        &page,
        record.payload_offset,
        record.payload_length,
    )?;
    Ok(PointReadOutcome::Value(Bytes::from(
        ValueFields {
            payload: payload.to_vec(),
            ..fields
        }
        .encode(),
    )))
}

async fn columnar_projected_scan(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
    read_version: u64,
) -> Result<Vec<ProjectedRow>, String> {
    let mut projected = Vec::new();
    for stripe_index in 0..prepared.index.projection_fences.len() {
        let records = fetch_projection_stripe(backend, prepared, stripe_index).await?;
        let mut cursor = 0_usize;
        while cursor < records.len() {
            let key = records[cursor].key;
            let mut visible = None;
            while cursor < records.len() && records[cursor].key == key {
                if visible.is_none() && records[cursor].version <= read_version {
                    visible = Some(&records[cursor]);
                }
                cursor += 1;
            }
            let Some(fields) = visible.and_then(|record| record.fields.as_ref()) else {
                continue;
            };
            projected.push(ProjectedRow {
                key,
                tenant: fields.tenant,
                category: fields.category,
                quantity: fields.quantity,
            });
        }
    }
    Ok(projected)
}

#[allow(clippy::map_entry)]
async fn reconstruct_columnar_history(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
) -> Result<Vec<RowRecord>, String> {
    let mut records = Vec::new();
    let mut projection_hasher = Sha256::new();
    let mut payload_pages = BTreeMap::new();
    for stripe_index in 0..prepared.index.projection_fences.len() {
        let range = prepared.index.projection_range(stripe_index)?;
        let raw = fetch_verified_range(
            backend,
            &prepared.manifest.projection_key,
            range,
            prepared.index.projection_fences[stripe_index].digest,
        )
        .await?;
        projection_hasher.update(&raw);
        for record in decode_projection_stripe(&raw)? {
            let value = if let Some(fields) = record.fields {
                let page_index = prepared.index.payload_page(record.payload_offset)?;
                if !payload_pages.contains_key(&page_index) {
                    payload_pages.insert(
                        page_index,
                        fetch_payload_page(backend, prepared, page_index).await?,
                    );
                }
                let payload = payload_slice(
                    &prepared.index,
                    page_index,
                    &payload_pages[&page_index],
                    record.payload_offset,
                    record.payload_length,
                )?;
                Some(
                    ValueFields {
                        payload: payload.to_vec(),
                        ..fields
                    }
                    .encode(),
                )
            } else {
                None
            };
            records.push(RowRecord {
                key: record.key.to_be_bytes().to_vec(),
                version: record.version,
                value,
            });
        }
    }
    if projection_hasher.finalize().as_slice() != prepared.index.projection_digest {
        return Err("columnar projection full-object digest mismatch".to_owned());
    }
    let mut payload_hasher = Sha256::new();
    for page_index in 0..prepared.index.payload_page_digests.len() {
        if !payload_pages.contains_key(&page_index) {
            payload_pages.insert(
                page_index,
                fetch_payload_page(backend, prepared, page_index).await?,
            );
        }
        payload_hasher.update(&payload_pages[&page_index]);
    }
    if payload_hasher.finalize().as_slice() != prepared.index.payload_digest {
        return Err("columnar payload full-object digest mismatch".to_owned());
    }
    Ok(records)
}

async fn fetch_projection_stripe(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
    stripe_index: usize,
) -> Result<Vec<ProjectionRecord>, String> {
    let raw = fetch_verified_range(
        backend,
        &prepared.manifest.projection_key,
        prepared.index.projection_range(stripe_index)?,
        prepared.index.projection_fences[stripe_index].digest,
    )
    .await?;
    decode_projection_stripe(&raw)
}

async fn fetch_payload_page(
    backend: &dyn Backend,
    prepared: &PreparedColumnarLayout,
    page_index: usize,
) -> Result<Bytes, String> {
    fetch_verified_range(
        backend,
        &prepared.manifest.payload_key,
        prepared.index.payload_range(page_index)?,
        *prepared
            .index
            .payload_page_digests
            .get(page_index)
            .ok_or_else(|| "columnar payload page digest is absent".to_owned())?,
    )
    .await
}

async fn fetch_verified_range(
    backend: &dyn Backend,
    key: &str,
    range: Range<u64>,
    expected_digest: [u8; RANGE_DIGEST_BYTES],
) -> Result<Bytes, String> {
    let read = backend
        .get(key, Some(range.clone()), None)
        .await
        .map_err(|error| error.to_string())?;
    if read.returned_range != range
        || u64::try_from(read.bytes.len()).unwrap_or(u64::MAX)
            != read
                .returned_range
                .end
                .saturating_sub(read.returned_range.start)
        || range_digest(&read.bytes) != expected_digest
    {
        return Err("columnar range checksum or framing mismatch".to_owned());
    }
    Ok(read.bytes)
}

fn payload_slice<'a>(
    index: &ColumnarIndex,
    page_index: usize,
    page: &'a [u8],
    payload_offset: u64,
    payload_length: u32,
) -> Result<&'a [u8], String> {
    let page_range = index.payload_range(page_index)?;
    let relative = payload_offset
        .checked_sub(page_range.start)
        .ok_or_else(|| "columnar payload precedes its page".to_owned())?;
    let start = as_usize(relative)?;
    let end = start
        .checked_add(usize::try_from(payload_length).map_err(|error| error.to_string())?)
        .ok_or_else(|| "columnar payload slice overflow".to_owned())?;
    page.get(start..end)
        .ok_or_else(|| "columnar payload crosses its page".to_owned())
}

#[allow(clippy::too_many_lines)]
fn encode_columnar_layout(
    profile: &StorageLayoutProfile,
    records: &[RowRecord],
) -> Result<EncodedColumnarLayout, String> {
    if records.is_empty() {
        return Err("columnar layout requires records".to_owned());
    }
    let payload_page_bytes = profile
        .opaque_payload_bytes
        .checked_mul(PAYLOAD_PAGE_ROWS)
        .ok_or_else(|| "columnar payload page size overflow".to_owned())?;
    let payload_page_bytes_u32 =
        u32::try_from(payload_page_bytes).map_err(|error| error.to_string())?;
    let mut payload = Vec::new();
    let mut payload_references = Vec::with_capacity(records.len());
    for record in records {
        let Some(value) = &record.value else {
            payload_references.push((0_u64, 0_u32));
            continue;
        };
        let fields = ValueFields::decode(value)?;
        if fields.payload.len() > payload_page_bytes {
            return Err("one payload exceeds a columnar payload page".to_owned());
        }
        let page_offset = payload.len() % payload_page_bytes;
        if page_offset.saturating_add(fields.payload.len()) > payload_page_bytes {
            payload.resize(
                payload
                    .len()
                    .saturating_add(payload_page_bytes - page_offset),
                0,
            );
        }
        let offset = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        payload.extend_from_slice(&fields.payload);
        payload_references.push((
            offset,
            u32::try_from(fields.payload.len()).map_err(|error| error.to_string())?,
        ));
    }
    let payload_page_digests = payload
        .chunks(payload_page_bytes)
        .map(range_digest)
        .collect();

    let mut projection = Vec::new();
    let mut projection_fences = Vec::new();
    for range in row_group_ranges(records, profile.columnar_block_rows)? {
        let start = u64::try_from(projection.len()).unwrap_or(u64::MAX);
        let mut stripe = Vec::with_capacity(
            PROJECTION_HEADER_BYTES
                .saturating_add(range.len().saturating_mul(PROJECTION_RECORD_BYTES)),
        );
        stripe.extend_from_slice(PROJECTION_MAGIC);
        stripe.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        stripe.extend_from_slice(
            &u32::try_from(range.len())
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        for record_index in range.clone() {
            encode_projection_record(
                &mut stripe,
                &records[record_index],
                payload_references[record_index],
            )?;
        }
        let first_key = key_u64(&records[range.start].key)?;
        let last_key = key_u64(&records[range.end - 1].key)?;
        projection_fences.push(ProjectionFence {
            first_key,
            last_key,
            offset: start,
            length: u64::try_from(stripe.len()).unwrap_or(u64::MAX),
            digest: range_digest(&stripe),
        });
        projection.extend_from_slice(&stripe);
    }
    let index = ColumnarIndex {
        generation: GENERATION,
        projection_length: u64::try_from(projection.len()).unwrap_or(u64::MAX),
        projection_digest: full_digest(&projection),
        payload_length: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        payload_digest: full_digest(&payload),
        payload_page_bytes: payload_page_bytes_u32,
        projection_fences,
        payload_page_digests,
    };
    let index_encoded = index.encode()?;
    let manifest = ColumnarManifest {
        format_version: FORMAT_VERSION,
        generation: GENERATION,
        covered_through: records
            .iter()
            .map(|record| record.version)
            .max()
            .unwrap_or(1),
        layout: StorageLayoutMode::ColumnarRangeOverlayCandidate
            .subject()
            .to_owned(),
        projection_key: PROJECTION_KEY.to_owned(),
        projection_bytes: u64::try_from(projection.len()).unwrap_or(u64::MAX),
        projection_sha256: content_sha256(&projection),
        payload_key: PAYLOAD_KEY.to_owned(),
        payload_bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        payload_sha256: content_sha256(&payload),
        index_key: INDEX_KEY.to_owned(),
        index_bytes: u64::try_from(index_encoded.len()).unwrap_or(u64::MAX),
        index_sha256: content_sha256(&index_encoded),
        capabilities: vec![
            "indexed_mvcc_point".to_owned(),
            "projection_only_scan".to_owned(),
            "paged_opaque_payload".to_owned(),
            "disposable_range_engine_cache".to_owned(),
        ],
    };
    let manifest_encoded = manifest.encode()?;
    Ok(EncodedColumnarLayout {
        projection,
        payload,
        index,
        index_encoded,
        manifest,
        manifest_encoded,
    })
}

fn encode_projection_record(
    encoded: &mut Vec<u8>,
    record: &RowRecord,
    payload_reference: (u64, u32),
) -> Result<(), String> {
    encoded.extend_from_slice(&key_u64(&record.key)?.to_be_bytes());
    encoded.extend_from_slice(&record.version.to_be_bytes());
    let fields = record
        .value
        .as_ref()
        .map(|value| ValueFields::decode(value))
        .transpose()?;
    encoded.push(u8::from(fields.is_some()));
    let fields = fields.unwrap_or(ValueFields {
        tenant: 0,
        category: 0,
        flags: 0,
        quantity: 0,
        updated_version: 0,
        checksum: 0,
        payload: Vec::new(),
    });
    encoded.extend_from_slice(&fields.tenant.to_be_bytes());
    encoded.extend_from_slice(&fields.category.to_be_bytes());
    encoded.extend_from_slice(&fields.flags.to_be_bytes());
    encoded.extend_from_slice(&fields.quantity.to_be_bytes());
    encoded.extend_from_slice(&fields.updated_version.to_be_bytes());
    encoded.extend_from_slice(&fields.checksum.to_be_bytes());
    encoded.extend_from_slice(&payload_reference.0.to_be_bytes());
    encoded.extend_from_slice(&payload_reference.1.to_be_bytes());
    Ok(())
}

fn decode_projection_stripe(encoded: &[u8]) -> Result<Vec<ProjectionRecord>, String> {
    let mut cursor = ColumnCursor::new(encoded);
    if cursor.array::<4>()? != *PROJECTION_MAGIC || cursor.u16()? != FORMAT_VERSION {
        return Err("unsupported columnar projection stripe".to_owned());
    }
    let count = as_usize(u64::from(cursor.u32()?))?;
    if count == 0 || count > MAX_COLUMNAR_ENTRIES {
        return Err("invalid columnar projection record count".to_owned());
    }
    let expected = PROJECTION_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(PROJECTION_RECORD_BYTES)
                .ok_or_else(|| "columnar projection length overflow".to_owned())?,
        )
        .ok_or_else(|| "columnar projection length overflow".to_owned())?;
    if encoded.len() != expected {
        return Err("columnar projection stripe length mismatch".to_owned());
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let key = cursor.u64()?;
        let version = cursor.u64()?;
        let operation = cursor.u8()?;
        let fields = ValueFields {
            tenant: cursor.u32()?,
            category: cursor.u16()?,
            flags: cursor.u16()?,
            quantity: cursor.i64()?,
            updated_version: cursor.u64()?,
            checksum: cursor.u64()?,
            payload: Vec::new(),
        };
        let payload_offset = cursor.u64()?;
        let payload_length = cursor.u32()?;
        if version == 0
            || operation > 1
            || (operation == 0 && (payload_offset != 0 || payload_length != 0))
            || (operation == 1 && payload_length == 0)
        {
            return Err("invalid columnar projection record".to_owned());
        }
        if let Some(previous) = records.last() {
            let previous: &ProjectionRecord = previous;
            if previous.key > key || (previous.key == key && previous.version <= version) {
                return Err("columnar projection order is invalid".to_owned());
            }
        }
        records.push(ProjectionRecord {
            key,
            version,
            fields: (operation == 1).then_some(fields),
            payload_offset,
            payload_length,
        });
    }
    cursor.finish()?;
    Ok(records)
}

fn columnar_compaction_written_bytes(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
) -> Result<u64, String> {
    let mut total = columnar_media_bytes(profile, &history.base_records)?;
    for delta in &history.delta_records {
        if !delta.is_empty() {
            total = total.saturating_add(columnar_media_bytes(profile, delta)?);
        }
    }
    total = total.saturating_add(columnar_media_bytes(profile, &history.records)?);
    Ok(total)
}

fn columnar_media_bytes(
    profile: &StorageLayoutProfile,
    records: &[RowRecord],
) -> Result<u64, String> {
    let encoded = encode_columnar_layout(profile, records)?;
    Ok(u64::try_from(encoded.projection.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(encoded.payload.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(encoded.index_encoded.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(encoded.manifest_encoded.len()).unwrap_or(u64::MAX)))
}

fn range_digest(bytes: &[u8]) -> [u8; RANGE_DIGEST_BYTES] {
    let digest = Sha256::digest(bytes);
    let mut range = [0_u8; RANGE_DIGEST_BYTES];
    range.copy_from_slice(&digest[..RANGE_DIGEST_BYTES]);
    range
}

fn projection_encoded_bytes(record_count: usize) -> u64 {
    u64::try_from(
        PROJECTION_HEADER_BYTES
            .saturating_add(record_count.saturating_mul(PROJECTION_RECORD_BYTES)),
    )
    .unwrap_or(u64::MAX)
}

fn full_digest(bytes: &[u8]) -> [u8; FULL_DIGEST_BYTES] {
    Sha256::digest(bytes).into()
}

fn full_digest_hex(digest: &[u8; FULL_DIGEST_BYTES]) -> String {
    let mut encoded = String::with_capacity(FULL_DIGEST_BYTES * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

struct ColumnCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ColumnCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| "columnar cursor overflow".to_owned())?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "columnar media is truncated".to_owned())?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        array(self.take(N)?)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, String> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset != self.bytes.len() {
            return Err("columnar media has trailing bytes".to_owned());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::t28_layout::{TypedLayoutCapabilityV1, TypedLayoutObjectIdentityV1};
    use arrow::array::UInt64Array;
    use okv_object::{
        memory_backend, BackendDescriptor, BackendRead, ErrorClass, RevisionToken, StoreError,
    };

    #[derive(Debug)]
    struct NumericRevisionBackend {
        inner: Arc<dyn Backend>,
        generation: String,
    }

    #[async_trait]
    impl Backend for NumericRevisionBackend {
        fn descriptor(&self) -> BackendDescriptor {
            self.inner.descriptor()
        }

        async fn put(
            &self,
            _key: &str,
            _bytes: Bytes,
            _condition: WriteCondition,
        ) -> Result<RevisionToken, StoreError> {
            Err(StoreError {
                class: ErrorClass::PermissionDenied,
                detail: "read only".to_owned(),
            })
        }

        async fn get(
            &self,
            key: &str,
            range: Option<Range<u64>>,
            expected: Option<&RevisionToken>,
        ) -> Result<BackendRead, StoreError> {
            if expected.and_then(|value| value.version.as_deref()) != Some(self.generation.as_str())
            {
                return Err(StoreError {
                    class: ErrorClass::PreconditionFailed,
                    detail: "generation mismatch".to_owned(),
                });
            }
            let mut read = self.inner.get(key, range, None).await?;
            read.revision = RevisionToken {
                e_tag: None,
                version: Some(self.generation.clone()),
            };
            Ok(read)
        }

        async fn delete(
            &self,
            _key: &str,
            _expected: Option<&RevisionToken>,
        ) -> Result<(), StoreError> {
            Err(StoreError {
                class: ErrorClass::PermissionDenied,
                detail: "read only".to_owned(),
            })
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>, StoreError> {
            Err(StoreError {
                class: ErrorClass::PermissionDenied,
                detail: "no list".to_owned(),
            })
        }
    }

    #[test]
    fn compact_index_round_trips() {
        let index = ColumnarIndex {
            generation: 1,
            projection_length: 100,
            projection_digest: [1; FULL_DIGEST_BYTES],
            payload_length: 200,
            payload_digest: [2; FULL_DIGEST_BYTES],
            payload_page_bytes: 128,
            projection_fences: vec![ProjectionFence {
                first_key: 1,
                last_key: 9,
                offset: 0,
                length: 100,
                digest: [3; RANGE_DIGEST_BYTES],
            }],
            payload_page_digests: vec![[4; RANGE_DIGEST_BYTES], [5; RANGE_DIGEST_BYTES]],
        };
        let encoded = index.encode().expect("encode");
        assert_eq!(ColumnarIndex::decode(&encoded).expect("decode"), index);
    }

    #[tokio::test]
    async fn generation_pinned_c5_serves_point_and_exact_datafusion_projection() {
        let profile = StorageLayoutProfile {
            key_count: 1_024,
            canonical_live_row_bytes: 512,
            opaque_payload_bytes: 480,
            base_version: 1,
            delta_cycles: 4,
            update_fraction: 0.125,
            delete_fraction: 0.01,
            point_operations: 64,
            target_run_object_bytes: 512 * 1_024,
            row_block_bytes: 64 * 1_024,
            columnar_block_rows: 128,
            overlay_cache_bytes: 64 * 1_024,
            seeds: vec![5_699],
            repeats: 1,
        };
        let history = LogicalHistory::generate(&profile, 5_699).expect("history");
        let writable = memory_backend();
        let prepared = prepare_columnar_layout(&profile, &history, writable.as_ref())
            .await
            .expect("columnar layout");
        let generation = "101".to_owned();
        let mut objects = Vec::new();
        for (key, role) in [
            (MANIFEST_KEY, TypedLayoutObjectRoleV1::Manifest),
            (INDEX_KEY, TypedLayoutObjectRoleV1::Index),
            (PAYLOAD_KEY, TypedLayoutObjectRoleV1::Payload),
            (PROJECTION_KEY, TypedLayoutObjectRoleV1::Projection),
        ] {
            let read = writable
                .get(key, None, None)
                .await
                .expect("published object");
            objects.push(TypedLayoutObjectIdentityV1 {
                role,
                key: key.to_owned(),
                generation: generation.clone(),
                length: read.object_length,
                sha256: content_sha256(&read.bytes),
            });
        }
        let child = TypedLayoutChildV1::seal(
            TypedLayoutSubjectV1::C5ColumnarMain,
            "doss-objectkv-dev-okv-evals".to_owned(),
            history.canonical_sha256.clone(),
            "objectkv.t28.typed-row.v1".to_owned(),
            "bb".repeat(32),
            5,
            MANIFEST_KEY.to_owned(),
            vec![
                TypedLayoutCapabilityV1::Point,
                TypedLayoutCapabilityV1::ProjectedScan,
                TypedLayoutCapabilityV1::OpaquePayloadSplit,
            ],
            objects,
        )
        .expect("typed C5 child");
        let readonly: Arc<dyn Backend> = Arc::new(NumericRevisionBackend {
            inner: writable,
            generation,
        });
        let reader = T28ColumnarLayoutCore::open(readonly, &child, 5)
            .await
            .expect("open C5");
        assert_eq!(
            reader.point(7, 5).await.expect("C5 point"),
            expected_outcome(history.visible(7, 5))
        );
        assert_eq!(
            reader.resident_metadata_bytes(),
            prepared.manifest_bytes.saturating_add(prepared.index_bytes)
        );

        let scan = reader.table_provider(256 * 1_024);
        let context = SessionContext::new();
        context
            .register_table("c5", scan.provider())
            .expect("register C5");
        let batches = context
            .sql("SELECT key, tenant, category, quantity FROM c5 ORDER BY key")
            .await
            .expect("plan C5 query")
            .collect()
            .await
            .expect("execute C5 query");
        let keys = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("key")
                    .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                    .expect("key column")
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            history
                .final_rows(5)
                .iter()
                .map(|row| row.key)
                .collect::<Vec<_>>()
        );
        let snapshot = scan.source_snapshot();
        assert!(snapshot.projection_fetch_requests > 0);
        assert!(snapshot.peak_fetch_bytes <= 256 * 1_024);
        assert_eq!(snapshot.payload_requests, 0);
        assert_eq!(snapshot.payload_response_bytes, 0);
    }
}
