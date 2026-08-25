//! Experimental aligned range-image format for RFC 0071.

use bytemuck::{Pod, Zeroable};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

const HEADER_MAGIC: &[u8; 8] = b"OKVRI003";
const FOOTER_MAGIC: &[u8; 8] = b"OKVRIF03";
const FORMAT_VERSION: u16 = 3;
pub const DIRECT_IO_ALIGNMENT_BYTES: usize = 4096;
pub const MAX_BLOCK_PAYLOAD_BYTES: usize = 65_536;
pub const MAX_DIRECT_EXTENT_BYTES: usize = 73_728;
const HEADER_BYTES: usize = DIRECT_IO_ALIGNMENT_BYTES;
const FOOTER_BYTES: usize = DIRECT_IO_ALIGNMENT_BYTES;
const CACHE_ENTRY_OVERHEAD: usize = 64;
type EncodedRow = (Vec<u8>, Vec<u8>);

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C, align(4096))]
struct AlignedReadPage {
    bytes: [u8; DIRECT_IO_ALIGNMENT_BYTES],
}

struct AlignedReadBuffer {
    pages: Vec<AlignedReadPage>,
}

impl AlignedReadBuffer {
    fn new(physical_length: usize) -> Self {
        Self {
            pages: vec![AlignedReadPage::zeroed(); physical_length / DIRECT_IO_ALIGNMENT_BYTES],
        }
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        bytemuck::cast_slice_mut(&mut self.pages)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NvmeIoMode {
    #[default]
    Buffered,
    Direct,
}

impl NvmeIoMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Direct => "direct",
        }
    }
}

#[derive(Clone, Debug)]
pub struct NvmeRangeImageIdentity<'a> {
    pub target_version: u64,
    pub range_begin: &'a [u8],
    pub range_end: &'a [u8],
    pub row_count: u64,
    pub root_identity_digest: [u8; 32],
    pub image_identity_sha256: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NvmeRangeImageConfig {
    pub block_payload_bytes: usize,
    pub reader_memory_budget_bytes: usize,
    pub maximum_concurrency: usize,
    pub io_mode: NvmeIoMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NvmeRangeImageWriteReceipt {
    pub image_identity_sha256: String,
    pub image_bytes: u64,
    pub index_logical_bytes: u64,
    pub index_physical_bytes: u64,
    pub block_count: u32,
    pub block_payload_bytes: u32,
    pub alignment_bytes: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NvmeFileIoSnapshot {
    pub operations: u64,
    pub bytes: u64,
}

impl NvmeFileIoSnapshot {
    #[must_use]
    pub const fn difference_since(self, before: Self) -> Self {
        Self {
            operations: self.operations.saturating_sub(before.operations),
            bytes: self.bytes.saturating_sub(before.bytes),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NvmeRangeImageOpenReceipt {
    pub image_identity_sha256: String,
    pub image_bytes: u64,
    pub index_logical_bytes: u64,
    pub index_physical_bytes: u64,
    pub block_count: u32,
    pub block_payload_bytes: u32,
    pub alignment_bytes: u32,
    pub open_file_io: NvmeFileIoSnapshot,
    pub base_resident_bytes: u64,
    pub maximum_cache_bytes: u64,
    pub maximum_inflight_buffer_bytes: u64,
    pub direct_io_active: bool,
    pub alignment_violations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NvmePointRead {
    pub value: Option<Vec<u8>>,
    pub file_operations: u64,
    pub physical_bytes: u64,
}

#[derive(Clone, Debug)]
struct BlockIndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    logical_length: u32,
    physical_length: u32,
    row_count: u32,
    sha256: [u8; 32],
}

#[derive(Debug)]
struct BlockCache {
    entries: BTreeMap<u32, Arc<[u8]>>,
    recency: VecDeque<u32>,
    resident_bytes: usize,
    maximum_bytes: usize,
}

impl BlockCache {
    fn new(maximum_bytes: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            recency: VecDeque::new(),
            resident_bytes: 0,
            maximum_bytes,
        }
    }

    fn get(&mut self, block: u32) -> Option<Arc<[u8]>> {
        let bytes = self.entries.get(&block)?.clone();
        self.recency.retain(|candidate| *candidate != block);
        self.recency.push_back(block);
        Some(bytes)
    }

    fn insert(&mut self, block: u32, bytes: Arc<[u8]>) {
        let entry_bytes = bytes.len().saturating_add(CACHE_ENTRY_OVERHEAD);
        if entry_bytes > self.maximum_bytes {
            return;
        }
        if let Some(prior) = self.entries.remove(&block) {
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(prior.len().saturating_add(CACHE_ENTRY_OVERHEAD));
            self.recency.retain(|candidate| *candidate != block);
        }
        while self.resident_bytes.saturating_add(entry_bytes) > self.maximum_bytes {
            let Some(oldest) = self.recency.pop_front() else {
                break;
            };
            if let Some(prior) = self.entries.remove(&oldest) {
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(prior.len().saturating_add(CACHE_ENTRY_OVERHEAD));
            }
        }
        self.resident_bytes = self.resident_bytes.saturating_add(entry_bytes);
        self.entries.insert(block, bytes);
        self.recency.push_back(block);
    }
}

#[derive(Debug)]
struct IoPermits {
    active: Mutex<usize>,
    available: Condvar,
    maximum: usize,
}

impl IoPermits {
    fn new(maximum: usize) -> Self {
        Self {
            active: Mutex::new(0),
            available: Condvar::new(),
            maximum,
        }
    }

    fn run<T>(&self, operation: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| "range-image I/O permit lock poisoned".to_owned())?;
        while *active >= self.maximum {
            active = self
                .available
                .wait(active)
                .map_err(|_| "range-image I/O permit lock poisoned".to_owned())?;
        }
        *active = active.saturating_add(1);
        drop(active);

        let result = operation();

        let mut active = self
            .active
            .lock()
            .map_err(|_| "range-image I/O permit lock poisoned".to_owned())?;
        *active = active.saturating_sub(1);
        self.available.notify_one();
        result
    }
}

pub struct NvmeRangeImageReader {
    file: File,
    io_mode: NvmeIoMode,
    range_begin: Vec<u8>,
    range_end: Vec<u8>,
    index: Vec<BlockIndexEntry>,
    cache: Mutex<BlockCache>,
    permits: IoPermits,
    file_read_operations: AtomicU64,
    file_read_bytes: AtomicU64,
    base_resident_bytes: usize,
    maximum_inflight_buffer_bytes: usize,
    peak_inflight_buffers: AtomicU64,
    image_identity_sha256: String,
}

impl NvmeRangeImageReader {
    /// Open a version-3 range image through buffered or Linux direct I/O.
    ///
    /// # Errors
    ///
    /// Returns an error when identity, layout, alignment, checksum, memory, or
    /// direct-I/O requirements are not satisfied.
    #[allow(clippy::too_many_lines)]
    pub fn open(
        path: &Path,
        expected: &NvmeRangeImageIdentity<'_>,
        config: NvmeRangeImageConfig,
    ) -> Result<(Self, NvmeRangeImageOpenReceipt), String> {
        validate_config(config)?;
        let file = open_reader_file(path, config.io_mode)?;
        let image_bytes = file.metadata().map_err(|error| error.to_string())?.len();
        if image_bytes < u64::try_from(HEADER_BYTES + FOOTER_BYTES).unwrap_or(u64::MAX)
            || image_bytes % DIRECT_IO_ALIGNMENT_BYTES as u64 != 0
        {
            return Err("aligned range image has invalid physical length".to_owned());
        }

        let mut open_operations = 0_u64;
        let mut open_bytes = 0_u64;
        let footer_offset = image_bytes.saturating_sub(FOOTER_BYTES as u64);
        let footer = read_aligned_extent(
            &file,
            config.io_mode,
            footer_offset,
            FOOTER_BYTES,
            FOOTER_BYTES,
        )?;
        open_operations = open_operations.saturating_add(1);
        open_bytes = open_bytes.saturating_add(FOOTER_BYTES as u64);
        let footer_fields = decode_footer(&footer)?;

        let header = read_aligned_extent(&file, config.io_mode, 0, HEADER_BYTES, HEADER_BYTES)?;
        open_operations = open_operations.saturating_add(1);
        open_bytes = open_bytes.saturating_add(HEADER_BYTES as u64);
        let header_fields = decode_header(&header)?;

        validate_header_and_footer(image_bytes, &header, &header_fields, &footer_fields)?;
        if header_fields.target_version != expected.target_version
            || header_fields.range_begin != expected.range_begin
            || header_fields.range_end != expected.range_end
            || header_fields.row_count != expected.row_count
            || header_fields.root_identity_digest != expected.root_identity_digest
            || header_fields.block_payload_bytes as usize != config.block_payload_bytes
            || header_fields.alignment_bytes as usize != DIRECT_IO_ALIGNMENT_BYTES
        {
            return Err("aligned range-image header does not match authorized root".to_owned());
        }

        let index_physical_bytes = usize::try_from(header_fields.index_physical_bytes)
            .map_err(|_| "aligned range-image index exceeds usize".to_owned())?;
        let index_logical_bytes = usize::try_from(header_fields.index_logical_bytes)
            .map_err(|_| "aligned range-image index exceeds usize".to_owned())?;
        let mut index_bytes = Vec::with_capacity(index_logical_bytes);
        let mut remaining_physical = index_physical_bytes;
        let mut remaining_logical = index_logical_bytes;
        let mut offset = header_fields.index_offset;
        while remaining_physical > 0 {
            let physical = remaining_physical.min(MAX_DIRECT_EXTENT_BYTES);
            if physical % DIRECT_IO_ALIGNMENT_BYTES != 0 {
                return Err("aligned range-image index chunk is not aligned".to_owned());
            }
            let logical = remaining_logical.min(physical);
            let chunk = read_aligned_extent(&file, config.io_mode, offset, logical, physical)?;
            index_bytes.extend_from_slice(&chunk);
            open_operations = open_operations.saturating_add(1);
            open_bytes = open_bytes.saturating_add(physical as u64);
            remaining_physical = remaining_physical.saturating_sub(physical);
            remaining_logical = remaining_logical.saturating_sub(logical);
            offset = offset.saturating_add(physical as u64);
        }
        let observed_index_sha256: [u8; 32] = Sha256::digest(&index_bytes).into();
        if remaining_logical != 0 || observed_index_sha256 != footer_fields.index_sha256 {
            return Err("aligned range-image index checksum mismatch".to_owned());
        }
        let index = decode_index(&index_bytes, &header_fields)?;
        let identity = image_identity(&header, &index_bytes, &footer);
        if expected
            .image_identity_sha256
            .is_some_and(|expected_identity| expected_identity != identity)
        {
            return Err("aligned range-image identity does not match ready receipt".to_owned());
        }

        let base_resident_bytes =
            resident_index_bytes(&index, &header_fields.range_begin, &header_fields.range_end);
        let maximum_inflight_buffer_bytes = config
            .maximum_concurrency
            .checked_mul(MAX_DIRECT_EXTENT_BYTES)
            .ok_or_else(|| "aligned range-image in-flight budget overflow".to_owned())?;
        let fixed_bytes = base_resident_bytes
            .checked_add(maximum_inflight_buffer_bytes)
            .ok_or_else(|| "aligned range-image reader budget overflow".to_owned())?;
        if fixed_bytes > config.reader_memory_budget_bytes {
            return Err("aligned range-image fixed reader state exceeds memory budget".to_owned());
        }
        let maximum_cache_bytes = config
            .reader_memory_budget_bytes
            .saturating_sub(fixed_bytes);
        let direct_io_active = config.io_mode == NvmeIoMode::Direct;
        let receipt = NvmeRangeImageOpenReceipt {
            image_identity_sha256: identity.clone(),
            image_bytes,
            index_logical_bytes: header_fields.index_logical_bytes,
            index_physical_bytes: header_fields.index_physical_bytes,
            block_count: header_fields.block_count,
            block_payload_bytes: header_fields.block_payload_bytes,
            alignment_bytes: header_fields.alignment_bytes,
            open_file_io: NvmeFileIoSnapshot {
                operations: open_operations,
                bytes: open_bytes,
            },
            base_resident_bytes: base_resident_bytes as u64,
            maximum_cache_bytes: maximum_cache_bytes as u64,
            maximum_inflight_buffer_bytes: maximum_inflight_buffer_bytes as u64,
            direct_io_active,
            alignment_violations: 0,
        };
        Ok((
            Self {
                file,
                io_mode: config.io_mode,
                range_begin: header_fields.range_begin,
                range_end: header_fields.range_end,
                index,
                cache: Mutex::new(BlockCache::new(maximum_cache_bytes)),
                permits: IoPermits::new(config.maximum_concurrency),
                file_read_operations: AtomicU64::new(open_operations),
                file_read_bytes: AtomicU64::new(open_bytes),
                base_resident_bytes,
                maximum_inflight_buffer_bytes,
                peak_inflight_buffers: AtomicU64::new(0),
                image_identity_sha256: identity,
            },
            receipt,
        ))
    }

    /// Return the exact value for one key inside the assigned range.
    ///
    /// # Errors
    ///
    /// Returns an error for out-of-range keys or invalid physical bytes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        self.get_with_io(key).map(|read| read.value)
    }

    /// Return one value and the explicit file I/O attributable to this call.
    ///
    /// # Errors
    ///
    /// Returns an error for out-of-range keys or invalid physical bytes.
    pub fn get_with_io(&self, key: &[u8]) -> Result<NvmePointRead, String> {
        if key < self.range_begin.as_slice() || key >= self.range_end.as_slice() {
            return Err("point is outside aligned placed range".to_owned());
        }
        let insertion = self
            .index
            .partition_point(|entry| entry.first_key.as_slice() <= key);
        if insertion == 0 {
            return Ok(NvmePointRead {
                value: None,
                file_operations: 0,
                physical_bytes: 0,
            });
        }
        let block_number = u32::try_from(insertion - 1)
            .map_err(|_| "aligned range-image block number exceeds u32".to_owned())?;
        let (bytes, file_operations, physical_bytes) = self.read_block(block_number)?;
        Ok(NvmePointRead {
            value: find_value_in_block(&bytes, key)?,
            file_operations,
            physical_bytes,
        })
    }

    /// Stream an exact ordered range through bounded batches.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, batches, or physical bytes.
    pub fn scan_batches<F>(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        batch_rows: usize,
        mut consume: F,
    ) -> Result<usize, String>
    where
        F: FnMut(&[(Vec<u8>, Vec<u8>)]) -> Result<(), String>,
    {
        if start < self.range_begin.as_slice()
            || end > self.range_end.as_slice()
            || start >= end
            || batch_rows == 0
        {
            return Err("scan is outside aligned placed range or has an empty batch".to_owned());
        }
        let mut emitted = 0_usize;
        let mut batch = Vec::with_capacity(batch_rows);
        for block_number in 0..self.index.len() {
            if emitted >= limit {
                break;
            }
            let (bytes, _, _) = self.read_block(
                u32::try_from(block_number)
                    .map_err(|_| "aligned range-image block number exceeds u32".to_owned())?,
            )?;
            for row in decode_block(&bytes)? {
                if row.0.as_slice() < start || row.0.as_slice() >= end {
                    continue;
                }
                batch.push(row);
                emitted = emitted.saturating_add(1);
                if batch.len() == batch_rows {
                    consume(&batch)?;
                    batch.clear();
                }
                if emitted >= limit {
                    break;
                }
            }
        }
        if !batch.is_empty() {
            consume(&batch)?;
        }
        Ok(emitted)
    }

    #[must_use]
    pub fn file_io(&self) -> NvmeFileIoSnapshot {
        NvmeFileIoSnapshot {
            operations: self.file_read_operations.load(Ordering::Relaxed),
            bytes: self.file_read_bytes.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn accounted_resident_bytes(&self) -> u64 {
        let cache_bytes = self
            .cache
            .lock()
            .map(|cache| cache.resident_bytes)
            .unwrap_or(usize::MAX);
        let peak_buffers = usize::try_from(self.peak_inflight_buffers.load(Ordering::Relaxed))
            .unwrap_or(usize::MAX)
            .min(self.maximum_inflight_buffer_bytes / MAX_DIRECT_EXTENT_BYTES);
        u64::try_from(
            self.base_resident_bytes
                .saturating_add(cache_bytes)
                .saturating_add(peak_buffers.saturating_mul(MAX_DIRECT_EXTENT_BYTES)),
        )
        .unwrap_or(u64::MAX)
    }

    #[must_use]
    pub fn image_identity_sha256(&self) -> &str {
        &self.image_identity_sha256
    }

    fn read_block(&self, block_number: u32) -> Result<(Arc<[u8]>, u64, u64), String> {
        if let Some(bytes) = self
            .cache
            .lock()
            .map_err(|_| "aligned range-image cache lock poisoned".to_owned())?
            .get(block_number)
        {
            return Ok((bytes, 0, 0));
        }
        let entry = self
            .index
            .get(block_number as usize)
            .ok_or_else(|| "aligned range-image block number is outside index".to_owned())?;
        let bytes = self.permits.run(|| {
            let active = self
                .permits
                .active
                .lock()
                .map_err(|_| "range-image I/O permit lock poisoned".to_owned())?;
            let active_u64 = u64::try_from(*active).unwrap_or(u64::MAX);
            drop(active);
            self.peak_inflight_buffers
                .fetch_max(active_u64, Ordering::Relaxed);
            read_aligned_extent(
                &self.file,
                self.io_mode,
                entry.offset,
                entry.logical_length as usize,
                entry.physical_length as usize,
            )
        })?;
        self.file_read_operations.fetch_add(1, Ordering::Relaxed);
        self.file_read_bytes
            .fetch_add(u64::from(entry.physical_length), Ordering::Relaxed);
        let observed: [u8; 32] = Sha256::digest(&bytes).into();
        if observed != entry.sha256 {
            return Err("aligned range-image block checksum mismatch".to_owned());
        }
        validate_block(&bytes, entry)?;
        let bytes: Arc<[u8]> = bytes.into();
        self.cache
            .lock()
            .map_err(|_| "aligned range-image cache lock poisoned".to_owned())?
            .insert(block_number, bytes.clone());
        Ok((bytes, 1, u64::from(entry.physical_length)))
    }
}

/// Write one root-bound, aligned, disposable version-3 range image.
///
/// # Errors
///
/// Returns an error when identity, rows, block geometry, or file persistence is invalid.
#[allow(clippy::too_many_lines)]
pub fn write_nvme_range_image(
    path: &Path,
    identity: &NvmeRangeImageIdentity<'_>,
    block_payload_bytes: usize,
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<NvmeRangeImageWriteReceipt, String> {
    if identity.row_count != u64::try_from(rows.len()).unwrap_or(u64::MAX) {
        return Err("aligned range-image row count does not match identity".to_owned());
    }
    write_nvme_range_image_stream(path, identity, block_payload_bytes, rows.iter().cloned())
}

/// Stream sorted rows into one root-bound, aligned version-3 range image.
///
/// # Errors
///
/// Returns an error when identity, rows, block geometry, or file persistence is invalid.
#[allow(clippy::too_many_lines)]
pub fn write_nvme_range_image_stream<I>(
    path: &Path,
    identity: &NvmeRangeImageIdentity<'_>,
    block_payload_bytes: usize,
    rows: I,
) -> Result<NvmeRangeImageWriteReceipt, String>
where
    I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
{
    if identity.image_identity_sha256.is_some()
        || identity.row_count == 0
        || identity.range_begin >= identity.range_end
        || !matches!(
            block_payload_bytes,
            8192 | 16384 | 32768 | MAX_BLOCK_PAYLOAD_BYTES
        )
    {
        return Err("aligned range-image writer configuration is invalid".to_owned());
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(&vec![0_u8; HEADER_BYTES])
        .map_err(|error| error.to_string())?;
    let mut offset = HEADER_BYTES as u64;
    let mut index = Vec::new();
    let mut block = Vec::with_capacity(block_payload_bytes.saturating_add(8192));
    block.extend_from_slice(&0_u32.to_be_bytes());
    let mut block_rows = 0_u32;
    let mut first_key = Vec::new();
    let mut previous_key: Option<Vec<u8>> = None;
    let mut observed_rows = 0_u64;
    for (key, value) in rows {
        if key.as_slice() < identity.range_begin
            || key.as_slice() >= identity.range_end
            || value.is_empty()
            || previous_key
                .as_ref()
                .is_some_and(|previous| previous.as_slice() >= key.as_slice())
        {
            return Err("aligned range-image rows are invalid".to_owned());
        }
        let row_bytes = 8_usize
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| "aligned range-image row size overflow".to_owned())?;
        if block_rows > 0 && block.len().saturating_add(row_bytes) > block_payload_bytes {
            flush_block(
                &mut file,
                &mut index,
                &mut offset,
                &mut block,
                &mut block_rows,
                &mut first_key,
            )?;
        }
        if block_rows == 0 {
            first_key.clone_from(&key);
        }
        append_length_prefixed(&mut block, &key)?;
        append_length_prefixed(&mut block, &value)?;
        block_rows = block_rows.saturating_add(1);
        previous_key = Some(key);
        observed_rows = observed_rows.saturating_add(1);
    }
    if observed_rows != identity.row_count {
        return Err("aligned range-image streamed row count does not match identity".to_owned());
    }
    if block_rows > 0 {
        flush_block(
            &mut file,
            &mut index,
            &mut offset,
            &mut block,
            &mut block_rows,
            &mut first_key,
        )?;
    }
    let block_count = u32::try_from(index.len())
        .map_err(|_| "aligned range-image block count exceeds u32".to_owned())?;
    let encoded_index = encode_index(&index)?;
    let index_logical_bytes = encoded_index.len();
    let index_physical_bytes = align_up(index_logical_bytes, DIRECT_IO_ALIGNMENT_BYTES)?;
    let index_offset = offset;
    write_padded(&mut file, &encoded_index, index_physical_bytes)?;
    offset = offset.saturating_add(index_physical_bytes as u64);
    let image_bytes = offset.saturating_add(FOOTER_BYTES as u64);
    let header = encode_header(&HeaderFields {
        target_version: identity.target_version,
        row_count: identity.row_count,
        block_count,
        block_payload_bytes: u32::try_from(block_payload_bytes)
            .map_err(|_| "aligned range-image payload exceeds u32".to_owned())?,
        alignment_bytes: u32::try_from(DIRECT_IO_ALIGNMENT_BYTES)
            .map_err(|_| "aligned range-image alignment exceeds u32".to_owned())?,
        index_offset,
        index_logical_bytes: index_logical_bytes as u64,
        index_physical_bytes: index_physical_bytes as u64,
        image_bytes,
        root_identity_digest: identity.root_identity_digest,
        range_begin: identity.range_begin.to_vec(),
        range_end: identity.range_end.to_vec(),
    })?;
    let footer = encode_footer(&FooterFields {
        header_sha256: Sha256::digest(&header).into(),
        index_sha256: Sha256::digest(&encoded_index).into(),
        index_offset,
        index_logical_bytes: index_logical_bytes as u64,
        index_physical_bytes: index_physical_bytes as u64,
        image_bytes,
    });
    file.write_all(&footer).map_err(|error| error.to_string())?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| error.to_string())?;
    file.write_all(&header).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    let identity_sha256 = image_identity(&header, &encoded_index, &footer);
    Ok(NvmeRangeImageWriteReceipt {
        image_identity_sha256: identity_sha256,
        image_bytes,
        index_logical_bytes: index_logical_bytes as u64,
        index_physical_bytes: index_physical_bytes as u64,
        block_count,
        block_payload_bytes: u32::try_from(block_payload_bytes)
            .map_err(|_| "aligned range-image payload exceeds u32".to_owned())?,
        alignment_bytes: u32::try_from(DIRECT_IO_ALIGNMENT_BYTES)
            .map_err(|_| "aligned range-image alignment exceeds u32".to_owned())?,
    })
}

#[derive(Clone, Debug)]
struct HeaderFields {
    target_version: u64,
    row_count: u64,
    block_count: u32,
    block_payload_bytes: u32,
    alignment_bytes: u32,
    index_offset: u64,
    index_logical_bytes: u64,
    index_physical_bytes: u64,
    image_bytes: u64,
    root_identity_digest: [u8; 32],
    range_begin: Vec<u8>,
    range_end: Vec<u8>,
}

#[derive(Clone, Debug)]
struct FooterFields {
    header_sha256: [u8; 32],
    index_sha256: [u8; 32],
    index_offset: u64,
    index_logical_bytes: u64,
    index_physical_bytes: u64,
    image_bytes: u64,
}

fn validate_config(config: NvmeRangeImageConfig) -> Result<(), String> {
    if !matches!(
        config.block_payload_bytes,
        8192 | 16384 | 32768 | MAX_BLOCK_PAYLOAD_BYTES
    ) || config.reader_memory_budget_bytes == 0
        || config.maximum_concurrency == 0
    {
        return Err("aligned range-image reader configuration is invalid".to_owned());
    }
    Ok(())
}

fn open_reader_file(path: &Path, io_mode: NvmeIoMode) -> Result<File, String> {
    match io_mode {
        NvmeIoMode::Buffered => File::open(path).map_err(|error| error.to_string()),
        NvmeIoMode::Direct => open_direct_file(path),
    }
}

#[cfg(target_os = "linux")]
fn open_direct_file(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .map_err(|error| format!("open aligned range image with O_DIRECT: {error}"))
}

#[cfg(not(target_os = "linux"))]
fn open_direct_file(_path: &Path) -> Result<File, String> {
    Err("aligned range-image direct I/O requires Linux".to_owned())
}

fn read_aligned_extent(
    file: &File,
    io_mode: NvmeIoMode,
    offset: u64,
    logical_length: usize,
    physical_length: usize,
) -> Result<Vec<u8>, String> {
    if offset % DIRECT_IO_ALIGNMENT_BYTES as u64 != 0
        || physical_length == 0
        || physical_length % DIRECT_IO_ALIGNMENT_BYTES != 0
        || physical_length > MAX_DIRECT_EXTENT_BYTES
        || logical_length > physical_length
    {
        return Err("aligned range-image extent violates direct-I/O geometry".to_owned());
    }
    let mut buffer = AlignedReadBuffer::new(physical_length);
    let destination = buffer.bytes_mut();
    let pointer = destination.as_ptr() as usize;
    if pointer % DIRECT_IO_ALIGNMENT_BYTES != 0 {
        return Err("aligned range-image destination buffer is not aligned".to_owned());
    }
    let read = read_at(file, destination, offset)?;
    if read != physical_length {
        return Err("aligned range-image direct extent was short".to_owned());
    }
    if io_mode == NvmeIoMode::Direct && pointer % DIRECT_IO_ALIGNMENT_BYTES != 0 {
        return Err("aligned range-image direct destination is not aligned".to_owned());
    }
    if destination[logical_length..].iter().any(|byte| *byte != 0) {
        return Err("aligned range-image extent padding is not zero".to_owned());
    }
    Ok(destination[..logical_length].to_vec())
}

#[cfg(unix)]
fn read_at(file: &File, destination: &mut [u8], offset: u64) -> Result<usize, String> {
    use std::os::unix::fs::FileExt;
    file.read_at(destination, offset)
        .map_err(|error| error.to_string())
}

#[cfg(windows)]
fn read_at(file: &File, destination: &mut [u8], offset: u64) -> Result<usize, String> {
    use std::os::windows::fs::FileExt;
    file.seek_read(destination, offset)
        .map_err(|error| error.to_string())
}

fn flush_block(
    file: &mut File,
    index: &mut Vec<BlockIndexEntry>,
    offset: &mut u64,
    block: &mut Vec<u8>,
    row_count: &mut u32,
    first_key: &mut Vec<u8>,
) -> Result<(), String> {
    block[..4].copy_from_slice(&row_count.to_be_bytes());
    let logical_length = block.len();
    let physical_length = align_up(logical_length, DIRECT_IO_ALIGNMENT_BYTES)?;
    if physical_length > MAX_DIRECT_EXTENT_BYTES {
        return Err("aligned range-image block exceeds maximum direct extent".to_owned());
    }
    write_padded(file, block, physical_length)?;
    index.push(BlockIndexEntry {
        first_key: std::mem::take(first_key),
        offset: *offset,
        logical_length: u32::try_from(logical_length)
            .map_err(|_| "aligned range-image logical block exceeds u32".to_owned())?,
        physical_length: u32::try_from(physical_length)
            .map_err(|_| "aligned range-image physical block exceeds u32".to_owned())?,
        row_count: *row_count,
        sha256: Sha256::digest(block.as_slice()).into(),
    });
    *offset = offset.saturating_add(physical_length as u64);
    block.clear();
    block.extend_from_slice(&0_u32.to_be_bytes());
    *row_count = 0;
    Ok(())
}

fn write_padded(file: &mut File, logical: &[u8], physical_length: usize) -> Result<(), String> {
    if logical.len() > physical_length || physical_length % DIRECT_IO_ALIGNMENT_BYTES != 0 {
        return Err("aligned range-image padded write is invalid".to_owned());
    }
    file.write_all(logical).map_err(|error| error.to_string())?;
    let padding = physical_length.saturating_sub(logical.len());
    if padding > 0 {
        file.write_all(&vec![0_u8; padding])
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn encode_header(fields: &HeaderFields) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::with_capacity(HEADER_BYTES);
    bytes.extend_from_slice(HEADER_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&fields.alignment_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.block_payload_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.target_version.to_be_bytes());
    bytes.extend_from_slice(&fields.row_count.to_be_bytes());
    bytes.extend_from_slice(&fields.block_count.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.extend_from_slice(&fields.index_offset.to_be_bytes());
    bytes.extend_from_slice(&fields.index_logical_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.index_physical_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.image_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.root_identity_digest);
    append_length_prefixed(&mut bytes, &fields.range_begin)?;
    append_length_prefixed(&mut bytes, &fields.range_end)?;
    if bytes.len() > HEADER_BYTES {
        return Err("aligned range-image header exceeds one alignment page".to_owned());
    }
    bytes.resize(HEADER_BYTES, 0);
    Ok(bytes)
}

fn decode_header(bytes: &[u8]) -> Result<HeaderFields, String> {
    if bytes.len() != HEADER_BYTES || bytes.get(..8) != Some(HEADER_MAGIC.as_slice()) {
        return Err("aligned range-image header magic is invalid".to_owned());
    }
    let mut cursor = 8_usize;
    let version = take_u16(bytes, &mut cursor)?;
    if version != FORMAT_VERSION {
        return Err("aligned range-image version is unsupported".to_owned());
    }
    let alignment_bytes = take_u32(bytes, &mut cursor)?;
    let block_payload_bytes = take_u32(bytes, &mut cursor)?;
    let target_version = take_u64(bytes, &mut cursor)?;
    let row_count = take_u64(bytes, &mut cursor)?;
    let block_count = take_u32(bytes, &mut cursor)?;
    let _reserved = take_u32(bytes, &mut cursor)?;
    let index_offset = take_u64(bytes, &mut cursor)?;
    let index_logical_bytes = take_u64(bytes, &mut cursor)?;
    let index_physical_bytes = take_u64(bytes, &mut cursor)?;
    let image_bytes = take_u64(bytes, &mut cursor)?;
    let root_identity_digest = take_array_32(bytes, &mut cursor)?;
    let range_begin = take_length_prefixed(bytes, &mut cursor)?;
    let range_end = take_length_prefixed(bytes, &mut cursor)?;
    Ok(HeaderFields {
        target_version,
        row_count,
        block_count,
        block_payload_bytes,
        alignment_bytes,
        index_offset,
        index_logical_bytes,
        index_physical_bytes,
        image_bytes,
        root_identity_digest,
        range_begin,
        range_end,
    })
}

fn encode_footer(fields: &FooterFields) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FOOTER_BYTES);
    bytes.extend_from_slice(FOOTER_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&fields.header_sha256);
    bytes.extend_from_slice(&fields.index_sha256);
    bytes.extend_from_slice(&fields.index_offset.to_be_bytes());
    bytes.extend_from_slice(&fields.index_logical_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.index_physical_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.image_bytes.to_be_bytes());
    bytes.resize(FOOTER_BYTES, 0);
    bytes
}

fn decode_footer(bytes: &[u8]) -> Result<FooterFields, String> {
    if bytes.len() != FOOTER_BYTES || bytes.get(..8) != Some(FOOTER_MAGIC.as_slice()) {
        return Err("aligned range-image footer magic is invalid".to_owned());
    }
    let mut cursor = 8_usize;
    let version = take_u16(bytes, &mut cursor)?;
    if version != FORMAT_VERSION {
        return Err("aligned range-image footer version is unsupported".to_owned());
    }
    Ok(FooterFields {
        header_sha256: take_array_32(bytes, &mut cursor)?,
        index_sha256: take_array_32(bytes, &mut cursor)?,
        index_offset: take_u64(bytes, &mut cursor)?,
        index_logical_bytes: take_u64(bytes, &mut cursor)?,
        index_physical_bytes: take_u64(bytes, &mut cursor)?,
        image_bytes: take_u64(bytes, &mut cursor)?,
    })
}

fn validate_header_and_footer(
    image_bytes: u64,
    header: &[u8],
    header_fields: &HeaderFields,
    footer_fields: &FooterFields,
) -> Result<(), String> {
    let header_sha256: [u8; 32] = Sha256::digest(header).into();
    if header_sha256 != footer_fields.header_sha256
        || image_bytes != header_fields.image_bytes
        || image_bytes != footer_fields.image_bytes
        || header_fields.index_offset != footer_fields.index_offset
        || header_fields.index_logical_bytes != footer_fields.index_logical_bytes
        || header_fields.index_physical_bytes != footer_fields.index_physical_bytes
        || header_fields.alignment_bytes as usize != DIRECT_IO_ALIGNMENT_BYTES
        || header_fields.index_offset % DIRECT_IO_ALIGNMENT_BYTES as u64 != 0
        || header_fields.index_physical_bytes % DIRECT_IO_ALIGNMENT_BYTES as u64 != 0
        || header_fields
            .index_offset
            .saturating_add(header_fields.index_physical_bytes)
            .saturating_add(FOOTER_BYTES as u64)
            != image_bytes
        || header_fields.index_logical_bytes > header_fields.index_physical_bytes
    {
        return Err("aligned range-image header and footer disagree".to_owned());
    }
    Ok(())
}

fn encode_index(index: &[BlockIndexEntry]) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u32::try_from(index.len())
            .map_err(|_| "aligned range-image index count exceeds u32".to_owned())?
            .to_be_bytes(),
    );
    for entry in index {
        append_length_prefixed(&mut bytes, &entry.first_key)?;
        bytes.extend_from_slice(&entry.offset.to_be_bytes());
        bytes.extend_from_slice(&entry.logical_length.to_be_bytes());
        bytes.extend_from_slice(&entry.physical_length.to_be_bytes());
        bytes.extend_from_slice(&entry.row_count.to_be_bytes());
        bytes.extend_from_slice(&entry.sha256);
    }
    Ok(bytes)
}

fn decode_index(bytes: &[u8], header: &HeaderFields) -> Result<Vec<BlockIndexEntry>, String> {
    let mut cursor = 0_usize;
    let count = take_u32(bytes, &mut cursor)?;
    if count != header.block_count {
        return Err("aligned range-image index count mismatch".to_owned());
    }
    let mut index = Vec::with_capacity(count as usize);
    let mut total_rows = 0_u64;
    for _ in 0..count {
        let entry = BlockIndexEntry {
            first_key: take_length_prefixed(bytes, &mut cursor)?,
            offset: take_u64(bytes, &mut cursor)?,
            logical_length: take_u32(bytes, &mut cursor)?,
            physical_length: take_u32(bytes, &mut cursor)?,
            row_count: take_u32(bytes, &mut cursor)?,
            sha256: take_array_32(bytes, &mut cursor)?,
        };
        if entry.offset < HEADER_BYTES as u64
            || entry.offset % DIRECT_IO_ALIGNMENT_BYTES as u64 != 0
            || entry.physical_length == 0
            || entry.physical_length as usize % DIRECT_IO_ALIGNMENT_BYTES != 0
            || entry.physical_length as usize > MAX_DIRECT_EXTENT_BYTES
            || entry.logical_length > entry.physical_length
            || entry
                .offset
                .saturating_add(u64::from(entry.physical_length))
                > header.index_offset
            || index
                .last()
                .is_some_and(|prior: &BlockIndexEntry| prior.first_key >= entry.first_key)
        {
            return Err("aligned range-image index entry is invalid".to_owned());
        }
        total_rows = total_rows.saturating_add(u64::from(entry.row_count));
        index.push(entry);
    }
    if cursor != bytes.len() || total_rows != header.row_count {
        return Err("aligned range-image index coverage mismatch".to_owned());
    }
    Ok(index)
}

fn validate_block(bytes: &[u8], entry: &BlockIndexEntry) -> Result<(), String> {
    let rows = decode_block(bytes)?;
    if rows.len() != entry.row_count as usize
        || rows.first().map(|row| row.0.as_slice()) != Some(entry.first_key.as_slice())
    {
        return Err("aligned range-image block does not match sparse index".to_owned());
    }
    Ok(())
}

fn decode_block(bytes: &[u8]) -> Result<Vec<EncodedRow>, String> {
    let mut cursor = 0_usize;
    let row_count = take_u32(bytes, &mut cursor)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let key = take_length_prefixed(bytes, &mut cursor)?;
        let value = take_length_prefixed(bytes, &mut cursor)?;
        if rows
            .last()
            .is_some_and(|prior: &(Vec<u8>, Vec<u8>)| prior.0 >= key)
        {
            return Err("aligned range-image block keys are not ordered".to_owned());
        }
        rows.push((key, value));
    }
    if cursor != bytes.len() {
        return Err("aligned range-image block has trailing bytes".to_owned());
    }
    Ok(rows)
}

fn find_value_in_block(bytes: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
    for (candidate, value) in decode_block(bytes)? {
        match candidate.as_slice().cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some(value)),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
}

fn resident_index_bytes(index: &[BlockIndexEntry], range_begin: &[u8], range_end: &[u8]) -> usize {
    index.iter().fold(
        size_of::<Vec<BlockIndexEntry>>()
            .saturating_add(index.len().saturating_mul(size_of::<BlockIndexEntry>()))
            .saturating_add(range_begin.len())
            .saturating_add(range_end.len()),
        |total, entry| total.saturating_add(entry.first_key.capacity()),
    )
}

fn image_identity(header: &[u8], index: &[u8], footer: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-aligned-range-image-v3");
    hasher.update(header);
    hasher.update(index);
    hasher.update(footer);
    format!("{:x}", hasher.finalize())
}

fn align_up(value: usize, alignment: usize) -> Result<usize, String> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err("aligned range-image alignment is invalid".to_owned());
    }
    value
        .checked_add(alignment.saturating_sub(1))
        .map(|candidate| candidate & !(alignment - 1))
        .ok_or_else(|| "aligned range-image length overflow".to_owned())
}

fn append_length_prefixed(target: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    target.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| "aligned range-image field exceeds u32".to_owned())?
            .to_be_bytes(),
    );
    target.extend_from_slice(value);
    Ok(())
}

fn take_length_prefixed(source: &[u8], cursor: &mut usize) -> Result<Vec<u8>, String> {
    let length = take_u32(source, cursor)? as usize;
    Ok(take(source, cursor, length)?.to_vec())
}

fn take_u16(source: &[u8], cursor: &mut usize) -> Result<u16, String> {
    let bytes: [u8; 2] = take(source, cursor, 2)?
        .try_into()
        .map_err(|_| "aligned range-image u16 is truncated".to_owned())?;
    Ok(u16::from_be_bytes(bytes))
}

fn take_u32(source: &[u8], cursor: &mut usize) -> Result<u32, String> {
    let bytes: [u8; 4] = take(source, cursor, 4)?
        .try_into()
        .map_err(|_| "aligned range-image u32 is truncated".to_owned())?;
    Ok(u32::from_be_bytes(bytes))
}

fn take_u64(source: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let bytes: [u8; 8] = take(source, cursor, 8)?
        .try_into()
        .map_err(|_| "aligned range-image u64 is truncated".to_owned())?;
    Ok(u64::from_be_bytes(bytes))
}

fn take_array_32(source: &[u8], cursor: &mut usize) -> Result<[u8; 32], String> {
    take(source, cursor, 32)?
        .try_into()
        .map_err(|_| "aligned range-image digest is truncated".to_owned())
}

fn take<'a>(source: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8], String> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| "aligned range-image cursor overflow".to_owned())?;
    let value = source
        .get(*cursor..end)
        .ok_or_else(|| "aligned range-image bytes are truncated".to_owned())?;
    *cursor = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn rows(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|ordinal| {
                (
                    format!("key-{ordinal:08}").into_bytes(),
                    vec![u8::try_from(ordinal % 251).unwrap_or(0); 8192],
                )
            })
            .collect()
    }

    #[test]
    fn aligned_buffer_has_direct_io_alignment() {
        let mut buffer = AlignedReadBuffer::new(MAX_DIRECT_EXTENT_BYTES);
        let bytes = buffer.bytes_mut();
        assert_eq!(bytes.as_ptr() as usize % DIRECT_IO_ALIGNMENT_BYTES, 0);
    }

    #[test]
    fn buffered_reader_preserves_points_scan_and_budget_for_every_class() {
        let directory = tempdir().expect("tempdir");
        let rows = rows(96);
        for block_payload_bytes in [8192, 16384, 32768, MAX_BLOCK_PAYLOAD_BYTES] {
            let path = directory
                .path()
                .join(format!("range-{block_payload_bytes}.okv"));
            let identity = NvmeRangeImageIdentity {
                target_version: 7,
                range_begin: b"key-00000000",
                range_end: b"key-99999999",
                row_count: u64::try_from(rows.len()).expect("row count fits u64"),
                root_identity_digest: [23; 32],
                image_identity_sha256: None,
            };
            let write = write_nvme_range_image(&path, &identity, block_payload_bytes, &rows)
                .expect("write aligned image");
            assert_eq!(write.image_bytes % DIRECT_IO_ALIGNMENT_BYTES as u64, 0);
            let (reader, open) = NvmeRangeImageReader::open(
                &path,
                &NvmeRangeImageIdentity {
                    image_identity_sha256: Some(&write.image_identity_sha256),
                    ..identity
                },
                NvmeRangeImageConfig {
                    block_payload_bytes,
                    reader_memory_budget_bytes: 4 * 1024 * 1024,
                    maximum_concurrency: 8,
                    io_mode: NvmeIoMode::Buffered,
                },
            )
            .expect("open aligned image");
            assert_eq!(open.alignment_violations, 0);
            assert_eq!(
                reader.get(&rows[47].0).expect("get"),
                Some(rows[47].1.clone())
            );
            let mut observed = Vec::new();
            assert_eq!(
                reader
                    .scan_batches(b"key-00000000", b"key-99999999", rows.len(), 11, |batch| {
                        observed.extend_from_slice(batch);
                        Ok(())
                    })
                    .expect("scan"),
                rows.len()
            );
            assert_eq!(observed, rows);
            assert!(reader.accounted_resident_bytes() <= 4 * 1024 * 1024);
        }
    }
}
