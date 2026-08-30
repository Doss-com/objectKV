//! RFC-0049 aligned columnar projection and payload frames.

#![allow(dead_code)]

use super::columnar_overlay::{projection_batch, projection_schema};
use super::{
    content_sha256, key_u64, logical_digest, Backend, LogicalHistory, PointReadOutcome,
    ProjectedRow, RowRecord, StorageLayoutProfile, ValueFields, WriteCondition,
};
use crate::t28_layout::{
    GenerationPinnedChildBackend, TypedLayoutObjectIdentityV1, TypedLayoutObjectRoleV1,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use okv_htap::{RangeStripeSource, RangeStripeTableProvider};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

const FORMAT_VERSION: u16 = 2;
const FORMAT_GENERATION: u64 = 1;
const GROUP_TARGET_ROWS: usize = 32;
const MAX_VERSIONS_PER_KEY: usize = 32;
const MAX_FRAME_PAIR_BYTES: usize = 32_762;
const INDEX_MAGIC: &[u8; 4] = b"OKI2";
const PROJECTION_MAGIC: &[u8; 4] = b"OKP2";
const PAYLOAD_MAGIC: &[u8; 4] = b"OKV2";
const MANIFEST_MAGIC: &[u8; 6] = b"OKVCM2";
const PROJECTION_LEAF_DOMAIN: &[u8] = b"okv-c5v2-projection-leaf-v1\0";
const PAYLOAD_LEAF_DOMAIN: &[u8] = b"okv-c5v2-payload-leaf-v1\0";
const NODE_DOMAIN: &[u8] = b"okv-c5v2-merkle-node-v1\0";
const DIGEST_BYTES: usize = 32;
const INDEX_ENTRY_BYTES: usize = 24;
const PROJECTION_RECORD_BYTES: usize = 57;
const FRAME_HEADER_BYTES: usize = 28;
const MAX_GROUPS: usize = 1_000_000;

pub(super) const PROJECTION_KEY: &str = "layout/columnar-v2/projection.okp2";
pub(super) const PAYLOAD_KEY: &str = "layout/columnar-v2/payload.okv2";
pub(super) const INDEX_KEY: &str = "layout/columnar-v2/index.oki2";
pub(super) const MANIFEST_KEY: &str = "layout/columnar-v2/active-manifest";

#[derive(Clone, Debug, Eq, PartialEq)]
struct AlignedIndexEntry {
    first_key: u64,
    projection_offset: u64,
    payload_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AlignedIndex {
    generation: u64,
    group_target_rows: u32,
    max_versions_per_key: u32,
    projection_length: u64,
    payload_length: u64,
    projection_root: [u8; DIGEST_BYTES],
    payload_root: [u8; DIGEST_BYTES],
    entries: Vec<AlignedIndexEntry>,
}

impl AlignedIndex {
    fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let mut encoded = Vec::with_capacity(
            108_usize
                .saturating_add(self.entries.len().saturating_mul(INDEX_ENTRY_BYTES))
                .saturating_add(DIGEST_BYTES),
        );
        encoded.extend_from_slice(INDEX_MAGIC);
        encoded.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&0_u16.to_be_bytes());
        encoded.extend_from_slice(&self.generation.to_be_bytes());
        encoded.extend_from_slice(&self.group_target_rows.to_be_bytes());
        encoded.extend_from_slice(&self.max_versions_per_key.to_be_bytes());
        encoded.extend_from_slice(
            &u32::try_from(self.entries.len())
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        encoded.extend_from_slice(&self.projection_length.to_be_bytes());
        encoded.extend_from_slice(&self.payload_length.to_be_bytes());
        encoded.extend_from_slice(&self.projection_root);
        encoded.extend_from_slice(&self.payload_root);
        for entry in &self.entries {
            encoded.extend_from_slice(&entry.first_key.to_be_bytes());
            encoded.extend_from_slice(&entry.projection_offset.to_be_bytes());
            encoded.extend_from_slice(&entry.payload_offset.to_be_bytes());
        }
        encoded.extend_from_slice(&Sha256::digest(&encoded));
        Ok(encoded)
    }

    fn decode(encoded: &[u8]) -> Result<Self, String> {
        if encoded.len() < DIGEST_BYTES {
            return Err("columnar v2 index is truncated".to_owned());
        }
        let body_end = encoded.len() - DIGEST_BYTES;
        if Sha256::digest(&encoded[..body_end]).as_slice() != &encoded[body_end..] {
            return Err("columnar v2 index checksum mismatch".to_owned());
        }
        let mut cursor = Cursor::new(&encoded[..body_end]);
        if cursor.array::<4>()? != *INDEX_MAGIC || cursor.u16()? != FORMAT_VERSION {
            return Err("unsupported columnar v2 index".to_owned());
        }
        if cursor.u16()? != 0 {
            return Err("columnar v2 index has unknown flags".to_owned());
        }
        let generation = cursor.u64()?;
        let group_target_rows = cursor.u32()?;
        let max_versions_per_key = cursor.u32()?;
        let group_count = usize::try_from(cursor.u32()?).map_err(|error| error.to_string())?;
        if group_count > MAX_GROUPS {
            return Err("columnar v2 group count exceeds the format bound".to_owned());
        }
        let projection_length = cursor.u64()?;
        let payload_length = cursor.u64()?;
        let projection_root = cursor.array::<DIGEST_BYTES>()?;
        let payload_root = cursor.array::<DIGEST_BYTES>()?;
        let mut entries = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            entries.push(AlignedIndexEntry {
                first_key: cursor.u64()?,
                projection_offset: cursor.u64()?,
                payload_offset: cursor.u64()?,
            });
        }
        cursor.finish()?;
        let index = Self {
            generation,
            group_target_rows,
            max_versions_per_key,
            projection_length,
            payload_length,
            projection_root,
            payload_root,
            entries,
        };
        index.validate()?;
        Ok(index)
    }

    fn validate(&self) -> Result<(), String> {
        if self.generation != FORMAT_GENERATION
            || usize::try_from(self.group_target_rows).unwrap_or(0) != GROUP_TARGET_ROWS
            || usize::try_from(self.max_versions_per_key).unwrap_or(0) != MAX_VERSIONS_PER_KEY
            || self.projection_length == 0
            || self.payload_length == 0
            || self.entries.is_empty()
            || self.entries.len() > MAX_GROUPS
        {
            return Err("invalid columnar v2 index header".to_owned());
        }
        for (ordinal, entry) in self.entries.iter().enumerate() {
            if ordinal == 0 && (entry.projection_offset != 0 || entry.payload_offset != 0) {
                return Err("columnar v2 offsets do not start at zero".to_owned());
            }
            let projection_end = self
                .entries
                .get(ordinal + 1)
                .map_or(self.projection_length, |next| next.projection_offset);
            let payload_end = self
                .entries
                .get(ordinal + 1)
                .map_or(self.payload_length, |next| next.payload_offset);
            if entry.projection_offset >= projection_end || entry.payload_offset >= payload_end {
                return Err("columnar v2 offsets are empty or non-monotonic".to_owned());
            }
            if let Some(next) = self.entries.get(ordinal + 1) {
                if entry.first_key >= next.first_key {
                    return Err("columnar v2 key fences are non-monotonic".to_owned());
                }
            }
        }
        let last = self
            .entries
            .last()
            .ok_or_else(|| "columnar v2 index has no entries".to_owned())?;
        if last.projection_offset >= self.projection_length
            || last.payload_offset >= self.payload_length
        {
            return Err("columnar v2 offsets do not close their objects".to_owned());
        }
        Ok(())
    }

    fn locate(&self, key: u64) -> Option<usize> {
        let mut lower = 0_usize;
        let mut upper = self.entries.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.entries[middle].first_key <= key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        lower.checked_sub(1)
    }

    fn projection_range(&self, ordinal: usize) -> Result<Range<usize>, String> {
        self.range(ordinal, true)
    }

    fn payload_range(&self, ordinal: usize) -> Result<Range<usize>, String> {
        self.range(ordinal, false)
    }

    fn range(&self, ordinal: usize, projection: bool) -> Result<Range<usize>, String> {
        let entry = self
            .entries
            .get(ordinal)
            .ok_or_else(|| "columnar v2 group is absent".to_owned())?;
        let start = if projection {
            entry.projection_offset
        } else {
            entry.payload_offset
        };
        let end = self.entries.get(ordinal + 1).map_or_else(
            || {
                if projection {
                    self.projection_length
                } else {
                    self.payload_length
                }
            },
            |next| {
                if projection {
                    next.projection_offset
                } else {
                    next.payload_offset
                }
            },
        );
        Ok(usize::try_from(start).map_err(|error| error.to_string())?
            ..usize::try_from(end).map_err(|error| error.to_string())?)
    }
}

#[derive(Clone, Debug)]
struct AlignedProjectionRecord {
    key: u64,
    version: u64,
    fields: Option<ValueFields>,
    payload_offset: u32,
    payload_length: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameKind {
    Projection,
    Payload,
}

impl FrameKind {
    const fn magic(self) -> &'static [u8; 4] {
        match self {
            Self::Projection => PROJECTION_MAGIC,
            Self::Payload => PAYLOAD_MAGIC,
        }
    }

    const fn leaf_domain(self) -> &'static [u8] {
        match self {
            Self::Projection => PROJECTION_LEAF_DOMAIN,
            Self::Payload => PAYLOAD_LEAF_DOMAIN,
        }
    }
}

#[derive(Clone, Debug)]
struct FrameBody {
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct FramedObject {
    bytes: Vec<u8>,
    frames: Vec<Vec<u8>>,
    root: [u8; DIGEST_BYTES],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AlignedManifest {
    format_version: u16,
    generation: u64,
    covered_through: u64,
    layout: String,
    projection_key: String,
    projection_bytes: u64,
    projection_sha256: String,
    projection_merkle_root: String,
    payload_key: String,
    payload_bytes: u64,
    payload_sha256: String,
    payload_merkle_root: String,
    index_key: String,
    index_bytes: u64,
    index_sha256: String,
    group_target_rows: u32,
    max_versions_per_key: u32,
    opaque_payload_bytes: u32,
    max_frame_pair_bytes: u32,
    capabilities: Vec<String>,
}

impl AlignedManifest {
    fn encode(&self) -> Result<Vec<u8>, String> {
        let json = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let mut body = MANIFEST_MAGIC.to_vec();
        body.extend_from_slice(&json);
        body.extend_from_slice(&Sha256::digest(&body));
        Ok(body)
    }

    fn decode(encoded: &[u8]) -> Result<Self, String> {
        if encoded.len() < MANIFEST_MAGIC.len() + DIGEST_BYTES
            || &encoded[..MANIFEST_MAGIC.len()] != MANIFEST_MAGIC
        {
            return Err("columnar v2 manifest framing mismatch".to_owned());
        }
        let body_end = encoded.len() - DIGEST_BYTES;
        if Sha256::digest(&encoded[..body_end]).as_slice() != &encoded[body_end..] {
            return Err("columnar v2 manifest checksum mismatch".to_owned());
        }
        let manifest = serde_json::from_slice::<Self>(&encoded[MANIFEST_MAGIC.len()..body_end])
            .map_err(|error| error.to_string())?;
        if manifest.format_version != FORMAT_VERSION
            || manifest.generation != FORMAT_GENERATION
            || manifest.layout != "c5_columnar_main_aligned"
            || manifest.projection_key != PROJECTION_KEY
            || manifest.payload_key != PAYLOAD_KEY
            || manifest.index_key != INDEX_KEY
            || usize::try_from(manifest.group_target_rows).unwrap_or(0) != GROUP_TARGET_ROWS
            || usize::try_from(manifest.max_versions_per_key).unwrap_or(0) != MAX_VERSIONS_PER_KEY
            || usize::try_from(manifest.max_frame_pair_bytes).unwrap_or(0) != MAX_FRAME_PAIR_BYTES
        {
            return Err("invalid columnar v2 manifest".to_owned());
        }
        Ok(manifest)
    }
}

#[derive(Clone, Debug)]
struct EncodedAlignedLayout {
    index: AlignedIndex,
    index_bytes: Vec<u8>,
    projection: FramedObject,
    payload: FramedObject,
    manifest: AlignedManifest,
    manifest_bytes: Vec<u8>,
    group_records: Vec<usize>,
    maximum_frame_pair_bytes: usize,
}

struct PreparedAlignedLayout {
    index: AlignedIndex,
    manifest: AlignedManifest,
    manifest_bytes: u64,
    index_bytes: u64,
}

/// Metadata-warm C5v2 reader over a generation-pinned immutable inventory.
pub(super) struct T28AlignedColumnarCore {
    backend: Arc<dyn Backend>,
    prepared: Arc<PreparedAlignedLayout>,
    read_version: u64,
    point_pairs: AtomicU64,
    overlapping_point_pairs: AtomicU64,
}

/// Exact logical history recovered from every object in one C5v2 child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct T28AlignedClosureRecovery {
    pub record_count: u64,
    pub live_row_count: u64,
    pub group_count: u64,
    pub projection_bytes: u64,
    pub payload_bytes: u64,
    pub canonical_history_sha256: String,
}

/// Runtime observation of the concurrent projection and payload gather.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct T28AlignedPointPairSnapshot {
    pub point_pairs: u64,
    pub overlapping_point_pairs: u64,
}

/// One C5v2 provider plus counters owned by its projection-only source.
pub(super) struct T28AlignedColumnarScanCore {
    provider: Arc<RangeStripeTableProvider>,
    source: Arc<AlignedProjectionStripeSource>,
}

/// Stable snapshot of C5v2 object-fetch counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct T28AlignedColumnarSourceSnapshot {
    pub projection_fetch_requests: u64,
    pub peak_fetch_bytes: u64,
    pub payload_requests: u64,
    pub payload_response_bytes: u64,
}

struct AlignedProjectionStripeSource {
    backend: Arc<dyn Backend>,
    prepared: Arc<PreparedAlignedLayout>,
    read_version: u64,
    scan_fetch_target_bytes: usize,
    scan_group: Mutex<Option<CachedAlignedProjectionGroup>>,
    projection_fetch_requests: AtomicU64,
    peak_fetch_bytes: AtomicU64,
}

struct CachedAlignedProjectionGroup {
    first_ordinal: usize,
    end_ordinal: usize,
    object_offset: usize,
    bytes: Bytes,
}

impl CachedAlignedProjectionGroup {
    fn contains(&self, ordinal: usize) -> bool {
        (self.first_ordinal..self.end_ordinal).contains(&ordinal)
    }

    fn frame_bytes(&self, index: &AlignedIndex, ordinal: usize) -> Result<Bytes, String> {
        let range = index.projection_range(ordinal)?;
        let start = range
            .start
            .checked_sub(self.object_offset)
            .ok_or_else(|| "cached C5v2 scan group begins after its frame".to_owned())?;
        let end = start
            .checked_add(range.len())
            .ok_or_else(|| "cached C5v2 scan-group slice overflow".to_owned())?;
        if end > self.bytes.len() {
            return Err("cached C5v2 scan group does not cover its frame".to_owned());
        }
        Ok(self.bytes.slice(start..end))
    }
}

impl Debug for AlignedProjectionStripeSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlignedProjectionStripeSource")
            .field("groups", &self.prepared.index.entries.len())
            .field("read_version", &self.read_version)
            .field("scan_fetch_target_bytes", &self.scan_fetch_target_bytes)
            .finish_non_exhaustive()
    }
}

/// Publish the independently frozen C5v2 bytes through a create-only backend.
pub(super) async fn prepare_t28_aligned_columnar_layout(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
    backend: &dyn Backend,
) -> Result<Vec<(String, TypedLayoutObjectRoleV1)>, String> {
    prepare_t28_aligned_columnar_records(profile, &history.records, backend).await
}

/// Publish one immutable C5v2 run for an already ordered set of MVCC records.
pub(super) async fn prepare_t28_aligned_columnar_records(
    profile: &StorageLayoutProfile,
    records: &[RowRecord],
    backend: &dyn Backend,
) -> Result<Vec<(String, TypedLayoutObjectRoleV1)>, String> {
    let encoded = encode_aligned_layout(records, profile.opaque_payload_bytes)?;
    for (key, bytes) in [
        (PROJECTION_KEY, encoded.projection.bytes),
        (PAYLOAD_KEY, encoded.payload.bytes),
        (INDEX_KEY, encoded.index_bytes),
        (MANIFEST_KEY, encoded.manifest_bytes),
    ] {
        backend
            .put(key, Bytes::from(bytes), WriteCondition::Create)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(vec![
        (MANIFEST_KEY.to_owned(), TypedLayoutObjectRoleV1::Manifest),
        (INDEX_KEY.to_owned(), TypedLayoutObjectRoleV1::Index),
        (
            PROJECTION_KEY.to_owned(),
            TypedLayoutObjectRoleV1::Projection,
        ),
        (PAYLOAD_KEY.to_owned(), TypedLayoutObjectRoleV1::Payload),
    ])
}

/// Return all bytes written by the frozen base, delta, and final-compaction
/// sequence without trusting provider accounting.
pub(super) fn aligned_compaction_written_bytes(
    profile: &StorageLayoutProfile,
    history: &LogicalHistory,
) -> Result<u64, String> {
    let mut total = aligned_media_bytes(profile, &history.base_records)?;
    for delta in &history.delta_records {
        if !delta.is_empty() {
            total = total.saturating_add(aligned_media_bytes(profile, delta)?);
        }
    }
    total = total.saturating_add(aligned_media_bytes(profile, &history.records)?);
    Ok(total)
}

fn aligned_media_bytes(
    profile: &StorageLayoutProfile,
    records: &[RowRecord],
) -> Result<u64, String> {
    let encoded = encode_aligned_layout(records, profile.opaque_payload_bytes)?;
    Ok(u64::try_from(encoded.manifest_bytes.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(encoded.index_bytes.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(encoded.projection.bytes.len()).unwrap_or(u64::MAX))
        .saturating_add(u64::try_from(encoded.payload.bytes.len()).unwrap_or(u64::MAX)))
}

impl T28AlignedColumnarCore {
    pub(super) async fn open(
        inner: Arc<dyn Backend>,
        objects: &[TypedLayoutObjectIdentityV1],
        covered_through_version: u64,
        read_version: u64,
    ) -> Result<Self, String> {
        if read_version == 0 || read_version > covered_through_version {
            return Err("invalid RFC-0049 C5v2 reader version".to_owned());
        }
        let backend: Arc<dyn Backend> = Arc::new(GenerationPinnedChildBackend::from_inventory(
            inner,
            "c5v2_aligned_columnar_main",
            objects,
        )?);
        let prepared = Arc::new(reopen_aligned_layout(backend.as_ref()).await?);
        if prepared.manifest.covered_through != covered_through_version {
            return Err("RFC-0049 C5v2 manifest coverage differs from its root".to_owned());
        }
        validate_inventory(objects, &prepared)?;
        Ok(Self {
            backend,
            prepared,
            read_version,
            point_pairs: AtomicU64::new(0),
            overlapping_point_pairs: AtomicU64::new(0),
        })
    }

    pub(super) async fn point(
        &self,
        key: u64,
        read_version: u64,
    ) -> Result<PointReadOutcome, String> {
        if read_version == 0 || read_version > self.read_version {
            return Err("RFC-0049 C5v2 point version exceeds the opened snapshot".to_owned());
        }
        let Some(ordinal) = self.prepared.index.locate(key) else {
            return Ok(PointReadOutcome::Absent);
        };
        let projection_range = self.prepared.index.projection_range(ordinal)?;
        let payload_range = self.prepared.index.payload_range(ordinal)?;
        let pair_bytes = projection_range
            .len()
            .checked_add(payload_range.len())
            .ok_or_else(|| "RFC-0049 C5v2 point byte count overflow".to_owned())?;
        if pair_bytes > MAX_FRAME_PAIR_BYTES {
            return Err("RFC-0049 C5v2 frame pair exceeds the point byte ceiling".to_owned());
        }
        let projection_range = to_u64_range(projection_range)?;
        let payload_range = to_u64_range(payload_range)?;
        let projection_request = async {
            let started = Instant::now();
            let read = self
                .backend
                .get(PROJECTION_KEY, Some(projection_range), None)
                .await;
            (read, started, Instant::now())
        };
        let payload_request = async {
            let started = Instant::now();
            let read = self
                .backend
                .get(PAYLOAD_KEY, Some(payload_range), None)
                .await;
            (read, started, Instant::now())
        };
        let (projection, payload) = tokio::join!(projection_request, payload_request);
        let (projection_read, projection_started, projection_finished) = projection;
        let (payload_read, payload_started, payload_finished) = payload;
        let projection_read = projection_read.map_err(|error| error.to_string())?;
        let payload_read = payload_read.map_err(|error| error.to_string())?;
        self.point_pairs.fetch_add(1, Ordering::Relaxed);
        if projection_started < payload_finished && payload_started < projection_finished {
            self.overlapping_point_pairs.fetch_add(1, Ordering::Relaxed);
        }
        decode_point_pair(
            &self.prepared.index,
            ordinal,
            &projection_read.bytes,
            &payload_read.bytes,
            key,
            read_version,
        )
    }

    pub(super) fn resident_metadata_bytes(&self) -> u64 {
        self.prepared
            .manifest_bytes
            .saturating_add(self.prepared.index_bytes)
    }

    pub(super) fn point_pair_snapshot(&self) -> T28AlignedPointPairSnapshot {
        T28AlignedPointPairSnapshot {
            point_pairs: self.point_pairs.load(Ordering::Relaxed),
            overlapping_point_pairs: self.overlapping_point_pairs.load(Ordering::Relaxed),
        }
    }

    pub(super) fn table_provider(
        &self,
        scan_fetch_target_bytes: usize,
    ) -> T28AlignedColumnarScanCore {
        let source = Arc::new(AlignedProjectionStripeSource {
            backend: Arc::clone(&self.backend),
            prepared: Arc::clone(&self.prepared),
            read_version: self.read_version,
            scan_fetch_target_bytes,
            scan_group: Mutex::new(None),
            projection_fetch_requests: AtomicU64::new(0),
            peak_fetch_bytes: AtomicU64::new(0),
        });
        T28AlignedColumnarScanCore {
            provider: Arc::new(RangeStripeTableProvider::new(source.clone())),
            source,
        }
    }

    /// Fetch and authenticate the complete projection and payload objects,
    /// reconstruct every retained MVCC record, and return its canonical digest.
    pub(super) async fn recover_complete_closure(
        &self,
    ) -> Result<T28AlignedClosureRecovery, String> {
        let projection_request = self.backend.get(PROJECTION_KEY, None, None);
        let payload_request = self.backend.get(PAYLOAD_KEY, None, None);
        let (projection, payload) = tokio::join!(projection_request, payload_request);
        let projection = projection.map_err(|error| error.to_string())?;
        let payload = payload.map_err(|error| error.to_string())?;
        if u64::try_from(projection.bytes.len()).unwrap_or(u64::MAX)
            != self.prepared.manifest.projection_bytes
            || content_sha256(&projection.bytes) != self.prepared.manifest.projection_sha256
            || u64::try_from(payload.bytes.len()).unwrap_or(u64::MAX)
                != self.prepared.manifest.payload_bytes
            || content_sha256(&payload.bytes) != self.prepared.manifest.payload_sha256
        {
            return Err("RFC-0049 C5v2 complete closure differs from its manifest".to_owned());
        }

        let mut records = Vec::new();
        for ordinal in 0..self.prepared.index.entries.len() {
            let projection_frame = projection
                .bytes
                .get(self.prepared.index.projection_range(ordinal)?)
                .ok_or_else(|| "RFC-0049 C5v2 complete projection range is absent".to_owned())?;
            let payload_frame = payload
                .bytes
                .get(self.prepared.index.payload_range(ordinal)?)
                .ok_or_else(|| "RFC-0049 C5v2 complete payload range is absent".to_owned())?;
            let (projection_count, projection_content) = decode_frame(
                FrameKind::Projection,
                projection_frame,
                ordinal,
                self.prepared.index.entries.len(),
                &self.prepared.index.projection_root,
            )?;
            let (payload_count, payload_content) = decode_frame(
                FrameKind::Payload,
                payload_frame,
                ordinal,
                self.prepared.index.entries.len(),
                &self.prepared.index.payload_root,
            )?;
            if projection_count != payload_count {
                return Err("RFC-0049 C5v2 complete paired frame record counts differ".to_owned());
            }
            let projection_records =
                decode_projection_content(projection_content, projection_count)?;
            if projection_records
                .first()
                .is_none_or(|first| first.key != self.prepared.index.entries[ordinal].first_key)
            {
                return Err("RFC-0049 C5v2 group fence differs from its records".to_owned());
            }
            if let (Some(previous), Some(first)) = (records.last(), projection_records.first()) {
                let previous: &RowRecord = previous;
                if key_u64(&previous.key)? >= first.key {
                    return Err("RFC-0049 C5v2 key history crosses a group fence".to_owned());
                }
            }
            for projection_record in projection_records {
                if let Some(previous) = records.last() {
                    let previous: &RowRecord = previous;
                    let previous_key = key_u64(&previous.key)?;
                    if previous_key > projection_record.key
                        || (previous_key == projection_record.key
                            && previous.version <= projection_record.version)
                    {
                        return Err("RFC-0049 C5v2 recovered history order is invalid".to_owned());
                    }
                }
                let key = projection_record.key.to_be_bytes();
                let record = if let Some(fields) = projection_record.fields {
                    let start = usize::try_from(projection_record.payload_offset)
                        .map_err(|error| error.to_string())?;
                    let end = start
                        .checked_add(
                            usize::try_from(projection_record.payload_length)
                                .map_err(|error| error.to_string())?,
                        )
                        .ok_or_else(|| {
                            "RFC-0049 C5v2 recovered payload slice overflow".to_owned()
                        })?;
                    let opaque_payload = payload_content.get(start..end).ok_or_else(|| {
                        "RFC-0049 C5v2 recovered payload slice is outside its frame".to_owned()
                    })?;
                    RowRecord::value(
                        key,
                        projection_record.version,
                        ValueFields {
                            payload: opaque_payload.to_vec(),
                            ..fields
                        }
                        .encode(),
                    )
                } else {
                    RowRecord::tombstone(key, projection_record.version)
                };
                records.push(record);
            }
        }

        let mut live_row_count = 0_u64;
        let mut previous_key = None;
        for record in &records {
            let key = key_u64(&record.key)?;
            if previous_key != Some(key) {
                if record.value.is_some() {
                    live_row_count = live_row_count.saturating_add(1);
                }
                previous_key = Some(key);
            }
        }
        Ok(T28AlignedClosureRecovery {
            record_count: u64::try_from(records.len()).unwrap_or(u64::MAX),
            live_row_count,
            group_count: u64::try_from(self.prepared.index.entries.len()).unwrap_or(u64::MAX),
            projection_bytes: u64::try_from(projection.bytes.len()).unwrap_or(u64::MAX),
            payload_bytes: u64::try_from(payload.bytes.len()).unwrap_or(u64::MAX),
            canonical_history_sha256: logical_digest(&records),
        })
    }
}

impl T28AlignedColumnarScanCore {
    pub(super) fn provider(&self) -> Arc<RangeStripeTableProvider> {
        Arc::clone(&self.provider)
    }

    pub(super) fn source_snapshot(&self) -> T28AlignedColumnarSourceSnapshot {
        T28AlignedColumnarSourceSnapshot {
            projection_fetch_requests: self
                .source
                .projection_fetch_requests
                .load(Ordering::Relaxed),
            peak_fetch_bytes: self.source.peak_fetch_bytes.load(Ordering::Relaxed),
            payload_requests: 0,
            payload_response_bytes: 0,
        }
    }
}

impl AlignedProjectionStripeSource {
    async fn projection_frame(&self, ordinal: usize) -> Result<Bytes, String> {
        if let Some(bytes) = self
            .scan_group
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|group| group.contains(ordinal))
            .map(|group| group.frame_bytes(&self.prepared.index, ordinal))
            .transpose()?
        {
            return Ok(bytes);
        }

        let mut end_ordinal = ordinal;
        let mut fetch_bytes = 0_usize;
        while end_ordinal < self.prepared.index.entries.len() {
            let next = self.prepared.index.projection_range(end_ordinal)?.len();
            if end_ordinal > ordinal
                && (self.scan_fetch_target_bytes == 0
                    || fetch_bytes.saturating_add(next) > self.scan_fetch_target_bytes)
            {
                break;
            }
            fetch_bytes = fetch_bytes.saturating_add(next);
            end_ordinal = end_ordinal.saturating_add(1);
        }
        let first = self.prepared.index.projection_range(ordinal)?;
        let last = self
            .prepared
            .index
            .projection_range(end_ordinal.saturating_sub(1))?;
        let range = first.start..last.end;
        let read = self
            .backend
            .get(PROJECTION_KEY, Some(to_u64_range(range.clone())?), None)
            .await
            .map_err(|error| error.to_string())?;
        if usize::try_from(read.returned_range.start).ok() != Some(range.start)
            || usize::try_from(read.returned_range.end).ok() != Some(range.end)
            || read.bytes.len() != range.len()
        {
            return Err("C5v2 projection scan-group range framing mismatch".to_owned());
        }
        let group_bytes = u64::try_from(read.bytes.len()).unwrap_or(u64::MAX);
        self.projection_fetch_requests
            .fetch_add(1, Ordering::Relaxed);
        self.peak_fetch_bytes
            .fetch_max(group_bytes, Ordering::Relaxed);
        let group = CachedAlignedProjectionGroup {
            first_ordinal: ordinal,
            end_ordinal,
            object_offset: range.start,
            bytes: read.bytes,
        };
        let frame = group.frame_bytes(&self.prepared.index, ordinal)?;
        *self
            .scan_group
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(group);
        Ok(frame)
    }
}

#[async_trait]
impl RangeStripeSource for AlignedProjectionStripeSource {
    fn schema(&self) -> SchemaRef {
        projection_schema()
    }

    fn stripe_count(&self) -> usize {
        self.prepared.index.entries.len()
    }

    async fn read_stripe(
        &self,
        stripe_index: usize,
        projection: Option<&[usize]>,
    ) -> DataFusionResult<RecordBatch> {
        let frame = self
            .projection_frame(stripe_index)
            .await
            .map_err(DataFusionError::Execution)?;
        let (record_count, content) = decode_frame(
            FrameKind::Projection,
            &frame,
            stripe_index,
            self.prepared.index.entries.len(),
            &self.prepared.index.projection_root,
        )
        .map_err(DataFusionError::Execution)?;
        let records =
            decode_projection_content(content, record_count).map_err(DataFusionError::Execution)?;
        let rows = visible_aligned_projection_rows(&records, self.read_version);
        projection_batch(&rows, projection)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
    }
}

fn visible_aligned_projection_rows(
    records: &[AlignedProjectionRecord],
    read_version: u64,
) -> Vec<ProjectedRow> {
    let mut projected = Vec::new();
    let mut cursor = 0_usize;
    while cursor < records.len() {
        let key = records[cursor].key;
        let mut visible = None;
        while cursor < records.len() && records[cursor].key == key {
            if visible.is_none() && records[cursor].version <= read_version {
                visible = Some(&records[cursor]);
            }
            cursor = cursor.saturating_add(1);
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

async fn reopen_aligned_layout(backend: &dyn Backend) -> Result<PreparedAlignedLayout, String> {
    let manifest_read = backend
        .get(MANIFEST_KEY, None, None)
        .await
        .map_err(|error| error.to_string())?;
    let manifest = AlignedManifest::decode(&manifest_read.bytes)?;
    let index_read = backend
        .get(INDEX_KEY, None, None)
        .await
        .map_err(|error| error.to_string())?;
    if u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX) != manifest.index_bytes
        || content_sha256(&index_read.bytes) != manifest.index_sha256
    {
        return Err("RFC-0049 C5v2 manifest selected the wrong index".to_owned());
    }
    let index = AlignedIndex::decode(&index_read.bytes)?;
    if index.projection_length != manifest.projection_bytes
        || index.payload_length != manifest.payload_bytes
        || digest_hex(&index.projection_root) != manifest.projection_merkle_root
        || digest_hex(&index.payload_root) != manifest.payload_merkle_root
    {
        return Err("RFC-0049 C5v2 manifest and index closure differ".to_owned());
    }
    Ok(PreparedAlignedLayout {
        index,
        manifest,
        manifest_bytes: u64::try_from(manifest_read.bytes.len()).unwrap_or(u64::MAX),
        index_bytes: u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX),
    })
}

fn validate_inventory(
    objects: &[TypedLayoutObjectIdentityV1],
    prepared: &PreparedAlignedLayout,
) -> Result<(), String> {
    let expected = [
        (
            MANIFEST_KEY,
            TypedLayoutObjectRoleV1::Manifest,
            prepared.manifest_bytes,
            None,
        ),
        (
            INDEX_KEY,
            TypedLayoutObjectRoleV1::Index,
            prepared.manifest.index_bytes,
            Some(prepared.manifest.index_sha256.as_str()),
        ),
        (
            PROJECTION_KEY,
            TypedLayoutObjectRoleV1::Projection,
            prepared.manifest.projection_bytes,
            Some(prepared.manifest.projection_sha256.as_str()),
        ),
        (
            PAYLOAD_KEY,
            TypedLayoutObjectRoleV1::Payload,
            prepared.manifest.payload_bytes,
            Some(prepared.manifest.payload_sha256.as_str()),
        ),
    ];
    let expected_keys = expected.iter().map(|row| row.0).collect::<BTreeSet<_>>();
    let actual_keys = objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<BTreeSet<_>>();
    if expected_keys != actual_keys {
        return Err("RFC-0049 C5v2 inventory has unreachable or missing media".to_owned());
    }
    for (key, role, length, sha256) in expected {
        let object = objects
            .iter()
            .find(|object| object.key == key && object.role == role)
            .ok_or_else(|| "RFC-0049 C5v2 object role is absent".to_owned())?;
        if object.length != length || sha256.is_some_and(|expected| object.sha256 != expected) {
            return Err("RFC-0049 C5v2 inventory differs from its manifest".to_owned());
        }
    }
    let manifest = objects
        .iter()
        .find(|object| object.key == MANIFEST_KEY)
        .ok_or_else(|| "RFC-0049 C5v2 manifest identity is absent".to_owned())?;
    if manifest.sha256
        != prepared
            .manifest
            .encode()
            .map(|bytes| content_sha256(&bytes))?
    {
        return Err("RFC-0049 C5v2 manifest identity differs from its bytes".to_owned());
    }
    Ok(())
}

fn decode_point_pair(
    index: &AlignedIndex,
    ordinal: usize,
    projection_frame: &[u8],
    payload_frame: &[u8],
    key: u64,
    read_version: u64,
) -> Result<PointReadOutcome, String> {
    let (projection_count, projection_content) = decode_frame(
        FrameKind::Projection,
        projection_frame,
        ordinal,
        index.entries.len(),
        &index.projection_root,
    )?;
    let (payload_count, payload_content) = decode_frame(
        FrameKind::Payload,
        payload_frame,
        ordinal,
        index.entries.len(),
        &index.payload_root,
    )?;
    if projection_count != payload_count {
        return Err("columnar v2 paired frame record counts differ".to_owned());
    }
    outcome_from_contents(
        projection_content,
        projection_count,
        payload_content,
        key,
        read_version,
    )
}

fn outcome_from_contents(
    projection_content: &[u8],
    projection_count: usize,
    payload_content: &[u8],
    key: u64,
    read_version: u64,
) -> Result<PointReadOutcome, String> {
    let records = decode_projection_content(projection_content, projection_count)?;
    let Some(record) = records
        .iter()
        .find(|record| record.key == key && record.version <= read_version)
    else {
        return Ok(PointReadOutcome::Absent);
    };
    let Some(fields) = record.fields.clone() else {
        return Ok(PointReadOutcome::Tombstone);
    };
    let start = usize::try_from(record.payload_offset).map_err(|error| error.to_string())?;
    let end = start
        .checked_add(usize::try_from(record.payload_length).map_err(|error| error.to_string())?)
        .ok_or_else(|| "columnar v2 payload slice overflow".to_owned())?;
    let payload = payload_content
        .get(start..end)
        .ok_or_else(|| "columnar v2 payload slice is outside its frame".to_owned())?;
    Ok(PointReadOutcome::Value(Bytes::from(
        ValueFields {
            payload: payload.to_vec(),
            ..fields
        }
        .encode(),
    )))
}

fn to_u64_range(range: Range<usize>) -> Result<Range<u64>, String> {
    Ok(
        u64::try_from(range.start).map_err(|error| error.to_string())?
            ..u64::try_from(range.end).map_err(|error| error.to_string())?,
    )
}

fn encode_aligned_layout(
    records: &[RowRecord],
    opaque_payload_bytes: usize,
) -> Result<EncodedAlignedLayout, String> {
    let groups = grouped_ranges(records)?;
    let group_contents = groups
        .iter()
        .map(|range| encode_group_contents(&records[range.clone()], opaque_payload_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let projection_contents = group_contents
        .iter()
        .map(|(projection, _)| projection.clone())
        .collect::<Vec<_>>();
    let payload_contents = group_contents
        .iter()
        .map(|(_, payload)| payload.clone())
        .collect::<Vec<_>>();
    let group_records = groups.iter().map(Range::len).collect::<Vec<_>>();
    let projection =
        encode_framed_object(FrameKind::Projection, &projection_contents, &group_records)?;
    let payload = encode_framed_object(FrameKind::Payload, &payload_contents, &group_records)?;

    let maximum_frame_pair_bytes = projection
        .frames
        .iter()
        .zip(&payload.frames)
        .map(|(left, right)| left.len().saturating_add(right.len()))
        .max()
        .ok_or_else(|| "columnar v2 layout has no frames".to_owned())?;
    if maximum_frame_pair_bytes > MAX_FRAME_PAIR_BYTES {
        return Err("columnar v2 frame pair exceeds the point byte ceiling".to_owned());
    }

    let mut projection_offset = 0_u64;
    let mut payload_offset = 0_u64;
    let mut entries = Vec::with_capacity(groups.len());
    for (ordinal, range) in groups.iter().enumerate() {
        entries.push(AlignedIndexEntry {
            first_key: key_u64(&records[range.start].key)?,
            projection_offset,
            payload_offset,
        });
        projection_offset = projection_offset
            .checked_add(u64::try_from(projection.frames[ordinal].len()).unwrap_or(u64::MAX))
            .ok_or_else(|| "columnar v2 projection offset overflow".to_owned())?;
        payload_offset = payload_offset
            .checked_add(u64::try_from(payload.frames[ordinal].len()).unwrap_or(u64::MAX))
            .ok_or_else(|| "columnar v2 payload offset overflow".to_owned())?;
    }
    let index = AlignedIndex {
        generation: FORMAT_GENERATION,
        group_target_rows: u32::try_from(GROUP_TARGET_ROWS).unwrap_or(u32::MAX),
        max_versions_per_key: u32::try_from(MAX_VERSIONS_PER_KEY).unwrap_or(u32::MAX),
        projection_length: u64::try_from(projection.bytes.len()).unwrap_or(u64::MAX),
        payload_length: u64::try_from(payload.bytes.len()).unwrap_or(u64::MAX),
        projection_root: projection.root,
        payload_root: payload.root,
        entries,
    };
    let index_bytes = index.encode()?;
    let covered_through = records
        .iter()
        .map(|record| record.version)
        .max()
        .unwrap_or(1);
    let manifest = AlignedManifest {
        format_version: FORMAT_VERSION,
        generation: FORMAT_GENERATION,
        covered_through,
        layout: "c5_columnar_main_aligned".to_owned(),
        projection_key: PROJECTION_KEY.to_owned(),
        projection_bytes: u64::try_from(projection.bytes.len()).unwrap_or(u64::MAX),
        projection_sha256: content_sha256(&projection.bytes),
        projection_merkle_root: digest_hex(&projection.root),
        payload_key: PAYLOAD_KEY.to_owned(),
        payload_bytes: u64::try_from(payload.bytes.len()).unwrap_or(u64::MAX),
        payload_sha256: content_sha256(&payload.bytes),
        payload_merkle_root: digest_hex(&payload.root),
        index_key: INDEX_KEY.to_owned(),
        index_bytes: u64::try_from(index_bytes.len()).unwrap_or(u64::MAX),
        index_sha256: content_sha256(&index_bytes),
        group_target_rows: u32::try_from(GROUP_TARGET_ROWS).unwrap_or(u32::MAX),
        max_versions_per_key: u32::try_from(MAX_VERSIONS_PER_KEY).unwrap_or(u32::MAX),
        opaque_payload_bytes: u32::try_from(opaque_payload_bytes).unwrap_or(u32::MAX),
        max_frame_pair_bytes: u32::try_from(MAX_FRAME_PAIR_BYTES).unwrap_or(u32::MAX),
        capabilities: vec![
            "indexed_mvcc_point".to_owned(),
            "concurrent_aligned_gather".to_owned(),
            "projection_only_scan".to_owned(),
            "merkle_range_proof".to_owned(),
            "disposable_range_engine_cache".to_owned(),
        ],
    };
    let manifest_bytes = manifest.encode()?;
    Ok(EncodedAlignedLayout {
        index,
        index_bytes,
        projection,
        payload,
        manifest,
        manifest_bytes,
        group_records,
        maximum_frame_pair_bytes,
    })
}

fn grouped_ranges(records: &[RowRecord]) -> Result<Vec<Range<usize>>, String> {
    if records.is_empty() {
        return Err("columnar v2 requires records".to_owned());
    }
    let mut groups = Vec::new();
    let mut group_start = 0_usize;
    let mut cursor = 0_usize;
    while cursor < records.len() {
        let chain_start = cursor;
        let key = records[cursor].key.as_slice();
        if chain_start > 0 && records[chain_start - 1].key.as_slice() > key {
            return Err("columnar v2 records are not key ordered".to_owned());
        }
        while cursor < records.len() && records[cursor].key.as_slice() == key {
            if cursor > chain_start && records[cursor - 1].version <= records[cursor].version {
                return Err("columnar v2 versions are not descending".to_owned());
            }
            cursor += 1;
        }
        let chain_len = cursor - chain_start;
        if chain_len == 0 || chain_len > MAX_VERSIONS_PER_KEY {
            return Err("columnar v2 version chain exceeds the format bound".to_owned());
        }
        if chain_start > group_start && cursor.saturating_sub(group_start) > GROUP_TARGET_ROWS {
            groups.push(group_start..chain_start);
            group_start = chain_start;
        }
    }
    groups.push(group_start..records.len());
    if groups
        .iter()
        .any(|range| range.is_empty() || range.len() > GROUP_TARGET_ROWS)
    {
        return Err("columnar v2 grouping exceeded its record bound".to_owned());
    }
    Ok(groups)
}

fn encode_group_contents(
    records: &[RowRecord],
    opaque_payload_bytes: usize,
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut projection = Vec::with_capacity(records.len().saturating_mul(PROJECTION_RECORD_BYTES));
    let mut payload = Vec::with_capacity(records.len().saturating_mul(opaque_payload_bytes));
    for record in records {
        let fields = record
            .value
            .as_ref()
            .map(|value| ValueFields::decode(value))
            .transpose()?;
        let (payload_offset, payload_length) = if let Some(fields) = &fields {
            if fields.payload.len() != opaque_payload_bytes {
                return Err("columnar v2 opaque payload width differs from the manifest".to_owned());
            }
            let offset = u32::try_from(payload.len()).map_err(|error| error.to_string())?;
            let length = u32::try_from(fields.payload.len()).map_err(|error| error.to_string())?;
            payload.extend_from_slice(&fields.payload);
            (offset, length)
        } else {
            (0, 0)
        };
        encode_projection_record(
            &mut projection,
            record,
            fields.as_ref(),
            payload_offset,
            payload_length,
        )?;
    }
    Ok((projection, payload))
}

fn encode_projection_record(
    encoded: &mut Vec<u8>,
    record: &RowRecord,
    fields: Option<&ValueFields>,
    payload_offset: u32,
    payload_length: u32,
) -> Result<(), String> {
    let start = encoded.len();
    encoded.extend_from_slice(&key_u64(&record.key)?.to_be_bytes());
    encoded.extend_from_slice(&record.version.to_be_bytes());
    encoded.push(u8::from(fields.is_some()));
    if let Some(fields) = fields {
        encoded.extend_from_slice(&fields.tenant.to_be_bytes());
        encoded.extend_from_slice(&fields.category.to_be_bytes());
        encoded.extend_from_slice(&fields.flags.to_be_bytes());
        encoded.extend_from_slice(&fields.quantity.to_be_bytes());
        encoded.extend_from_slice(&fields.updated_version.to_be_bytes());
        encoded.extend_from_slice(&fields.checksum.to_be_bytes());
    } else {
        encoded.extend_from_slice(&[0_u8; 32]);
    }
    encoded.extend_from_slice(&payload_offset.to_be_bytes());
    encoded.extend_from_slice(&payload_length.to_be_bytes());
    if encoded.len().saturating_sub(start) != PROJECTION_RECORD_BYTES {
        return Err("columnar v2 projection record width drifted".to_owned());
    }
    Ok(())
}

fn encode_framed_object(
    kind: FrameKind,
    contents: &[Vec<u8>],
    record_counts: &[usize],
) -> Result<FramedObject, String> {
    if contents.is_empty() || contents.len() != record_counts.len() {
        return Err("columnar v2 framed object input is invalid".to_owned());
    }
    let proof_count = proof_depth(contents.len());
    let mut bodies = Vec::with_capacity(contents.len());
    for (ordinal, content) in contents.iter().enumerate() {
        let mut bytes = Vec::with_capacity(FRAME_HEADER_BYTES.saturating_add(content.len()));
        bytes.extend_from_slice(kind.magic());
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(ordinal)
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(contents.len())
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(record_counts[ordinal])
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(content.len())
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u16::try_from(proof_count)
                .map_err(|error| error.to_string())?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(content);
        bodies.push(FrameBody { bytes });
    }
    let leaves = bodies
        .iter()
        .map(|body| digest_parts(&[kind.leaf_domain(), &body.bytes]))
        .collect::<Vec<_>>();
    let levels = merkle_levels(leaves);
    let root = levels
        .last()
        .and_then(|level| level.first())
        .copied()
        .ok_or_else(|| "columnar v2 Merkle root is absent".to_owned())?;
    let mut frames = Vec::with_capacity(bodies.len());
    for (ordinal, body) in bodies.into_iter().enumerate() {
        let proof = merkle_proof(&levels, ordinal)?;
        if proof.len() != proof_count {
            return Err("columnar v2 Merkle proof depth drifted".to_owned());
        }
        let mut frame = body.bytes;
        for node in proof {
            frame.extend_from_slice(&node);
        }
        frames.push(frame);
    }
    let bytes = frames.concat();
    Ok(FramedObject {
        bytes,
        frames,
        root,
    })
}

fn proof_depth(mut leaves: usize) -> usize {
    let mut depth = 0;
    while leaves > 1 {
        leaves = leaves.saturating_add(1) / 2;
        depth += 1;
    }
    depth
}

fn merkle_levels(mut current: Vec<[u8; DIGEST_BYTES]>) -> Vec<Vec<[u8; DIGEST_BYTES]>> {
    let mut levels = vec![current.clone()];
    while current.len() > 1 {
        let mut next = Vec::with_capacity(current.len().saturating_add(1) / 2);
        for pair in current.chunks(2) {
            let left = pair[0];
            let right = pair.get(1).copied().unwrap_or(left);
            next.push(digest_parts(&[NODE_DOMAIN, &left, &right]));
        }
        levels.push(next.clone());
        current = next;
    }
    levels
}

fn merkle_proof(
    levels: &[Vec<[u8; DIGEST_BYTES]>],
    mut ordinal: usize,
) -> Result<Vec<[u8; DIGEST_BYTES]>, String> {
    let mut proof = Vec::with_capacity(levels.len().saturating_sub(1));
    for level in levels.iter().take(levels.len().saturating_sub(1)) {
        let sibling = if ordinal % 2 == 0 {
            level.get(ordinal + 1).or_else(|| level.get(ordinal))
        } else {
            level.get(ordinal - 1)
        }
        .copied()
        .ok_or_else(|| "columnar v2 Merkle sibling is absent".to_owned())?;
        proof.push(sibling);
        ordinal /= 2;
    }
    Ok(proof)
}

fn decode_frame<'a>(
    kind: FrameKind,
    encoded: &'a [u8],
    expected_ordinal: usize,
    expected_group_count: usize,
    expected_root: &[u8; DIGEST_BYTES],
) -> Result<(usize, &'a [u8]), String> {
    let mut cursor = Cursor::new(encoded);
    if cursor.array::<4>()? != *kind.magic() || cursor.u16()? != FORMAT_VERSION {
        return Err("unsupported columnar v2 frame".to_owned());
    }
    if cursor.u16()? != 0 {
        return Err("columnar v2 frame has unknown flags".to_owned());
    }
    let ordinal = usize::try_from(cursor.u32()?).map_err(|error| error.to_string())?;
    let group_count = usize::try_from(cursor.u32()?).map_err(|error| error.to_string())?;
    let record_count = usize::try_from(cursor.u32()?).map_err(|error| error.to_string())?;
    let content_bytes = usize::try_from(cursor.u32()?).map_err(|error| error.to_string())?;
    let proof_count = usize::from(cursor.u16()?);
    if cursor.u16()? != 0
        || ordinal != expected_ordinal
        || group_count != expected_group_count
        || group_count == 0
        || ordinal >= group_count
        || record_count == 0
        || record_count > GROUP_TARGET_ROWS
        || proof_count != proof_depth(group_count)
    {
        return Err("invalid columnar v2 frame header".to_owned());
    }
    let content_start = cursor.position();
    let content_end = content_start
        .checked_add(content_bytes)
        .ok_or_else(|| "columnar v2 frame content overflow".to_owned())?;
    let proof_bytes = proof_count
        .checked_mul(DIGEST_BYTES)
        .ok_or_else(|| "columnar v2 proof length overflow".to_owned())?;
    if content_end
        .checked_add(proof_bytes)
        .ok_or_else(|| "columnar v2 frame length overflow".to_owned())?
        != encoded.len()
    {
        return Err("columnar v2 frame length mismatch".to_owned());
    }
    let body_end = content_end;
    let mut current = digest_parts(&[kind.leaf_domain(), &encoded[..body_end]]);
    let mut tree_ordinal = ordinal;
    let mut proof_cursor = Cursor::new(&encoded[content_end..]);
    for _ in 0..proof_count {
        let sibling = proof_cursor.array::<DIGEST_BYTES>()?;
        current = if tree_ordinal % 2 == 0 {
            digest_parts(&[NODE_DOMAIN, &current, &sibling])
        } else {
            digest_parts(&[NODE_DOMAIN, &sibling, &current])
        };
        tree_ordinal /= 2;
    }
    proof_cursor.finish()?;
    if &current != expected_root {
        return Err(match kind {
            FrameKind::Projection => "columnar v2 projection Merkle proof mismatch".to_owned(),
            FrameKind::Payload => "columnar v2 payload Merkle proof mismatch".to_owned(),
        });
    }
    Ok((record_count, &encoded[content_start..content_end]))
}

fn decode_projection_content(
    content: &[u8],
    record_count: usize,
) -> Result<Vec<AlignedProjectionRecord>, String> {
    if content.len()
        != record_count
            .checked_mul(PROJECTION_RECORD_BYTES)
            .ok_or_else(|| "columnar v2 projection length overflow".to_owned())?
    {
        return Err("columnar v2 projection content length mismatch".to_owned());
    }
    let mut cursor = Cursor::new(content);
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
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
        let payload_offset = cursor.u32()?;
        let payload_length = cursor.u32()?;
        if version == 0
            || operation > 1
            || (operation == 0
                && (fields.tenant != 0
                    || fields.category != 0
                    || fields.flags != 0
                    || fields.quantity != 0
                    || fields.updated_version != 0
                    || fields.checksum != 0
                    || payload_offset != 0
                    || payload_length != 0))
            || (operation == 1 && payload_length == 0)
        {
            return Err("invalid columnar v2 projection record".to_owned());
        }
        if let Some(previous) = records.last() {
            let previous: &AlignedProjectionRecord = previous;
            if previous.key > key || (previous.key == key && previous.version <= version) {
                return Err("columnar v2 projection order is invalid".to_owned());
            }
        }
        records.push(AlignedProjectionRecord {
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

fn point_from_media(
    index: &AlignedIndex,
    projection_object: &[u8],
    payload_object: &[u8],
    key: u64,
    read_version: u64,
) -> Result<PointReadOutcome, String> {
    let Some(ordinal) = index.locate(key) else {
        return Ok(PointReadOutcome::Absent);
    };
    let projection_range = index.projection_range(ordinal)?;
    let payload_range = index.payload_range(ordinal)?;
    let projection_frame = projection_object
        .get(projection_range)
        .ok_or_else(|| "columnar v2 projection range is outside the object".to_owned())?;
    let payload_frame = payload_object
        .get(payload_range)
        .ok_or_else(|| "columnar v2 payload range is outside the object".to_owned())?;
    let (projection_count, projection_content) = decode_frame(
        FrameKind::Projection,
        projection_frame,
        ordinal,
        index.entries.len(),
        &index.projection_root,
    )?;
    let (payload_count, payload_content) = decode_frame(
        FrameKind::Payload,
        payload_frame,
        ordinal,
        index.entries.len(),
        &index.payload_root,
    )?;
    if projection_count != payload_count {
        return Err("columnar v2 paired frame record counts differ".to_owned());
    }
    let records = decode_projection_content(projection_content, projection_count)?;
    let Some(record) = records
        .iter()
        .find(|record| record.key == key && record.version <= read_version)
    else {
        return Ok(PointReadOutcome::Absent);
    };
    let Some(fields) = record.fields.clone() else {
        return Ok(PointReadOutcome::Tombstone);
    };
    let start = usize::try_from(record.payload_offset).map_err(|error| error.to_string())?;
    let end = start
        .checked_add(usize::try_from(record.payload_length).map_err(|error| error.to_string())?)
        .ok_or_else(|| "columnar v2 payload slice overflow".to_owned())?;
    let payload = payload_content
        .get(start..end)
        .ok_or_else(|| "columnar v2 payload slice is outside its frame".to_owned())?;
    Ok(PointReadOutcome::Value(Bytes::from(
        ValueFields {
            payload: payload.to_vec(),
            ..fields
        }
        .encode(),
    )))
}

fn digest_parts(parts: &[&[u8]]) -> [u8; DIGEST_BYTES] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn digest_hex(digest: &[u8; DIGEST_BYTES]) -> String {
    let mut output = String::with_capacity(DIGEST_BYTES * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn position(&self) -> usize {
        self.offset
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| "columnar v2 cursor overflow".to_owned())?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "columnar v2 bytes are truncated".to_owned())?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "columnar v2 fixed field has the wrong width".to_owned())
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

    fn finish(self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("columnar v2 bytes have trailing content".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_layout::{LogicalHistory, StorageLayoutProfile};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct CompatibilityFixture {
        media: FixtureMedia,
        expected: FixtureExpected,
    }

    #[derive(Deserialize)]
    #[allow(clippy::struct_field_names)]
    struct FixtureMedia {
        index_hex: String,
        projection_hex: String,
        payload_hex: String,
        manifest_hex: String,
    }

    #[derive(Deserialize)]
    struct FixtureExpected {
        summary: FixtureSummary,
        points: Vec<FixturePoint>,
    }

    #[derive(Deserialize)]
    struct FixtureSummary {
        group_count: usize,
        index_sha256: String,
        projection_sha256: String,
        payload_sha256: String,
        manifest_sha256: String,
    }

    #[derive(Deserialize)]
    struct FixturePoint {
        key: u64,
        read_version: u64,
        outcome: FixtureOutcome,
    }

    #[derive(Deserialize)]
    struct FixtureOutcome {
        kind: String,
        value_hex: Option<String>,
    }

    #[derive(Deserialize)]
    struct PhysicalOracle {
        expected_media: ExpectedMedia,
    }

    #[derive(Deserialize)]
    struct ExpectedMedia {
        group_count: usize,
        record_count: usize,
        maximum_group_records: usize,
        index_bytes: usize,
        index_sha256: String,
        projection_bytes: usize,
        projection_sha256: String,
        projection_merkle_root: String,
        payload_bytes: usize,
        payload_sha256: String,
        payload_merkle_root: String,
        manifest_bytes: usize,
        manifest_sha256: String,
        total_media_bytes: usize,
        maximum_frame_pair_bytes: usize,
    }

    #[derive(Deserialize)]
    struct CorruptFixture {
        mutated_projection_hex: String,
        expected_error: String,
    }

    fn t28_profile() -> StorageLayoutProfile {
        StorageLayoutProfile {
            key_count: 16_384,
            canonical_live_row_bytes: 512,
            opaque_payload_bytes: 480,
            base_version: 1,
            delta_cycles: 4,
            update_fraction: 0.125,
            delete_fraction: 0.01,
            point_operations: 1_024,
            target_run_object_bytes: 8 * 1_024 * 1_024,
            row_block_bytes: 64 * 1_024,
            columnar_block_rows: 128,
            overlay_cache_bytes: 256 * 1_024,
            seeds: vec![5_699],
            repeats: 1,
        }
    }

    fn from_hex(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0);
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let text = std::str::from_utf8(pair).expect("hex utf8");
                u8::from_str_radix(text, 16).expect("hex byte")
            })
            .collect()
    }

    #[test]
    fn full_t28_encoder_matches_independent_physical_oracle() {
        let profile = t28_profile();
        let history = LogicalHistory::generate(&profile, 5_699).expect("history");
        let encoded =
            encode_aligned_layout(&history.records, profile.opaque_payload_bytes).expect("layout");
        let oracle: PhysicalOracle = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../evals/oracles/t28-aligned-columnar-v2-plan.json"
        )))
        .expect("physical oracle");
        let expected = oracle.expected_media;

        assert_eq!(history.records.len(), expected.record_count);
        assert_eq!(encoded.group_records.len(), expected.group_count);
        assert_eq!(
            encoded.group_records.iter().copied().max(),
            Some(expected.maximum_group_records)
        );
        assert_eq!(encoded.index_bytes.len(), expected.index_bytes);
        assert_eq!(content_sha256(&encoded.index_bytes), expected.index_sha256);
        assert_eq!(encoded.projection.bytes.len(), expected.projection_bytes);
        assert_eq!(
            content_sha256(&encoded.projection.bytes),
            expected.projection_sha256
        );
        assert_eq!(
            digest_hex(&encoded.projection.root),
            expected.projection_merkle_root
        );
        assert_eq!(encoded.payload.bytes.len(), expected.payload_bytes);
        assert_eq!(
            content_sha256(&encoded.payload.bytes),
            expected.payload_sha256
        );
        assert_eq!(
            digest_hex(&encoded.payload.root),
            expected.payload_merkle_root
        );
        assert_eq!(encoded.manifest_bytes.len(), expected.manifest_bytes);
        assert_eq!(
            content_sha256(&encoded.manifest_bytes),
            expected.manifest_sha256
        );
        assert_eq!(
            encoded.index_bytes.len()
                + encoded.projection.bytes.len()
                + encoded.payload.bytes.len()
                + encoded.manifest_bytes.len(),
            expected.total_media_bytes
        );
        assert_eq!(
            encoded.maximum_frame_pair_bytes,
            expected.maximum_frame_pair_bytes
        );
    }

    #[test]
    fn positive_compatibility_fixture_decodes_exact_points() {
        let fixture: CompatibilityFixture = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/columnar-overlay-v2.json"
        )))
        .expect("fixture");
        let index_bytes = from_hex(&fixture.media.index_hex);
        let projection = from_hex(&fixture.media.projection_hex);
        let payload = from_hex(&fixture.media.payload_hex);
        let manifest_bytes = from_hex(&fixture.media.manifest_hex);
        let index = AlignedIndex::decode(&index_bytes).expect("index");
        let manifest = AlignedManifest::decode(&manifest_bytes).expect("manifest");

        assert_eq!(index.entries.len(), fixture.expected.summary.group_count);
        assert_eq!(
            content_sha256(&index_bytes),
            fixture.expected.summary.index_sha256
        );
        assert_eq!(
            content_sha256(&projection),
            fixture.expected.summary.projection_sha256
        );
        assert_eq!(
            content_sha256(&payload),
            fixture.expected.summary.payload_sha256
        );
        assert_eq!(
            content_sha256(&manifest_bytes),
            fixture.expected.summary.manifest_sha256
        );
        assert_eq!(manifest.projection_sha256, content_sha256(&projection));
        assert_eq!(manifest.payload_sha256, content_sha256(&payload));
        assert_eq!(manifest.index_sha256, content_sha256(&index_bytes));

        for point in fixture.expected.points {
            let actual =
                point_from_media(&index, &projection, &payload, point.key, point.read_version)
                    .expect("point");
            match (point.outcome.kind.as_str(), actual) {
                ("absent", PointReadOutcome::Absent)
                | ("tombstone", PointReadOutcome::Tombstone) => {}
                ("value", PointReadOutcome::Value(value)) => {
                    assert_eq!(
                        from_hex(point.outcome.value_hex.as_deref().expect("value")),
                        value
                    );
                }
                (expected, actual) => panic!("expected {expected}, received {actual:?}"),
            }
        }
    }

    #[test]
    fn corrupted_projection_fixture_fails_merkle_verification() {
        let positive: CompatibilityFixture = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/columnar-overlay-v2.json"
        )))
        .expect("positive");
        let corrupt: CorruptFixture = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/columnar-overlay-v2-corrupt.json"
        )))
        .expect("corrupt");
        let index = AlignedIndex::decode(&from_hex(&positive.media.index_hex)).expect("index");
        let projection = from_hex(&corrupt.mutated_projection_hex);
        let range = index.projection_range(0).expect("range");
        let error = decode_frame(
            FrameKind::Projection,
            &projection[range],
            0,
            index.entries.len(),
            &index.projection_root,
        )
        .expect_err("corrupt frame must fail");
        assert_eq!(
            corrupt.expected_error,
            "columnar_v2_projection_merkle_proof_mismatch"
        );
        assert_eq!(error, "columnar v2 projection Merkle proof mismatch");
    }

    #[test]
    fn nonzero_tombstone_payload_reference_is_rejected() {
        let fixture: CompatibilityFixture = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/columnar-overlay-v2.json"
        )))
        .expect("fixture");
        let index = AlignedIndex::decode(&from_hex(&fixture.media.index_hex)).expect("index");
        let projection = from_hex(&fixture.media.projection_hex);
        let range = index.projection_range(0).expect("range");
        let (_, content) = decode_frame(
            FrameKind::Projection,
            &projection[range],
            0,
            index.entries.len(),
            &index.projection_root,
        )
        .expect("frame");
        let mut corrupt = content.to_vec();
        let tombstone_offset = 2 * PROJECTION_RECORD_BYTES;
        corrupt[tombstone_offset + 49..tombstone_offset + 53].copy_from_slice(&8_u32.to_be_bytes());
        assert_eq!(
            decode_projection_content(&corrupt, 32).expect_err("tombstone poison"),
            "invalid columnar v2 projection record"
        );
    }

    #[test]
    fn index_rejects_nonclosing_offsets() {
        let fixture: CompatibilityFixture = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/columnar-overlay-v2.json"
        )))
        .expect("fixture");
        let mut index = AlignedIndex::decode(&from_hex(&fixture.media.index_hex)).expect("index");
        index.entries[1].projection_offset = index.projection_length;
        assert_eq!(
            index.validate().expect_err("offset poison"),
            "columnar v2 offsets are empty or non-monotonic"
        );
    }
}
