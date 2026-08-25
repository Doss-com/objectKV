//! Experimental bounded-memory assigned-range image.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const HEADER_MAGIC: &[u8; 8] = b"OKVRI002";
const INDEX_MAGIC: &[u8; 8] = b"OKVRIDX2";
const FOOTER_MAGIC: &[u8; 8] = b"OKVRIF02";
const FORMAT_VERSION: u16 = 2;
const FOOTER_BYTES: usize = 72;
pub(crate) const MAX_BLOCK_BYTES: usize = 65_536;
const CACHE_ENTRY_OVERHEAD: usize = 64;

pub(crate) type RangeRow = (Vec<u8>, Vec<u8>);
pub(crate) type RangeRows = Vec<RangeRow>;

#[derive(Clone, Debug)]
pub(crate) struct RangeImageIdentity<'a> {
    pub target_version: u64,
    pub range_begin: &'a [u8],
    pub range_end: &'a [u8],
    pub row_count: u64,
    pub root_identity_digest: [u8; 32],
    pub image_identity_sha256: Option<&'a str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RangeImageWriteReceipt {
    pub image_identity_sha256: String,
    pub image_bytes: u64,
    pub index_bytes: u64,
    pub block_count: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FileIoSnapshot {
    pub operations: u64,
    pub bytes: u64,
}

impl FileIoSnapshot {
    pub fn difference_since(self, before: Self) -> Self {
        Self {
            operations: self.operations.saturating_sub(before.operations),
            bytes: self.bytes.saturating_sub(before.bytes),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RangeImageOpenReceipt {
    pub image_identity_sha256: String,
    pub image_bytes: u64,
    pub index_bytes: u64,
    pub block_count: u32,
    pub open_file_io: FileIoSnapshot,
    pub accounted_resident_bytes: u64,
}

#[derive(Clone, Debug)]
struct BlockIndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    length: u32,
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

pub(crate) struct RangeImageReader {
    file: File,
    range_begin: Vec<u8>,
    range_end: Vec<u8>,
    index: Vec<BlockIndexEntry>,
    cache: Mutex<BlockCache>,
    file_read_operations: AtomicU64,
    file_read_bytes: AtomicU64,
    base_resident_bytes: usize,
    image_bytes: u64,
    image_identity_sha256: String,
    index_bytes: u64,
}

impl RangeImageReader {
    pub fn open(
        path: &Path,
        expected: &RangeImageIdentity<'_>,
        memory_budget_bytes: usize,
    ) -> Result<(Self, RangeImageOpenReceipt), String> {
        let file = File::open(path).map_err(|error| error.to_string())?;
        let image_bytes = file.metadata().map_err(|error| error.to_string())?.len();
        if image_bytes < u64::try_from(FOOTER_BYTES).unwrap_or(u64::MAX) {
            return Err("range image is shorter than its footer".to_owned());
        }
        let counters = (AtomicU64::new(0), AtomicU64::new(0));
        let counter_refs = (&counters.0, &counters.1);
        let footer_offset = image_bytes.saturating_sub(FOOTER_BYTES as u64);
        let footer = read_exact_at_counted(&file, footer_offset, FOOTER_BYTES, &counter_refs)?;
        let footer_fields = decode_footer(&footer)?;
        if footer_fields.image_bytes != image_bytes
            || footer_fields.header_bytes == 0
            || footer_fields.index_bytes == 0
            || footer_fields.index_offset < footer_fields.header_bytes
            || footer_fields
                .index_offset
                .saturating_add(footer_fields.index_bytes)
                != footer_offset
        {
            return Err("range image footer layout is invalid".to_owned());
        }
        let header_length = usize::try_from(footer_fields.header_bytes)
            .map_err(|_| "range image header exceeds usize".to_owned())?;
        let index_length = usize::try_from(footer_fields.index_bytes)
            .map_err(|_| "range image index exceeds usize".to_owned())?;
        let header = read_exact_at_counted(&file, 0, header_length, &counter_refs)?;
        let index_bytes = read_exact_at_counted(
            &file,
            footer_fields.index_offset,
            index_length,
            &counter_refs,
        )?;
        let index_sha256: [u8; 32] = Sha256::digest(&index_bytes).into();
        if index_sha256 != footer_fields.index_sha256 {
            return Err("range image index checksum mismatch".to_owned());
        }
        let header_fields = decode_header(&header)?;
        if header_fields.target_version != expected.target_version
            || header_fields.range_begin != expected.range_begin
            || header_fields.range_end != expected.range_end
            || header_fields.row_count != expected.row_count
            || header_fields.root_identity_digest != expected.root_identity_digest
            || header_fields.maximum_block_bytes as usize != MAX_BLOCK_BYTES
        {
            return Err("range image header does not match authorized root".to_owned());
        }
        let index = decode_index(
            &index_bytes,
            header_fields.block_count,
            footer_fields.header_bytes,
            footer_fields.index_offset,
        )?;
        let indexed_rows = index.iter().fold(0_u64, |total, entry| {
            total.saturating_add(u64::from(entry.row_count))
        });
        if indexed_rows != expected.row_count {
            return Err("range image index row count mismatch".to_owned());
        }
        let image_identity_sha256 = image_identity(&header, &index_bytes, &footer);
        if expected
            .image_identity_sha256
            .is_some_and(|identity| identity != image_identity_sha256)
        {
            return Err("range image identity does not match ready receipt".to_owned());
        }
        let base_resident_bytes =
            resident_index_bytes(&index, &header_fields.range_begin, &header_fields.range_end);
        if base_resident_bytes > memory_budget_bytes {
            return Err("range image sparse index exceeds reader memory budget".to_owned());
        }
        let cache_bytes = memory_budget_bytes.saturating_sub(base_resident_bytes);
        let open_file_io = FileIoSnapshot {
            operations: counters.0.load(Ordering::Relaxed),
            bytes: counters.1.load(Ordering::Relaxed),
        };
        let receipt = RangeImageOpenReceipt {
            image_identity_sha256: image_identity_sha256.clone(),
            image_bytes,
            index_bytes: footer_fields.index_bytes,
            block_count: header_fields.block_count,
            open_file_io,
            accounted_resident_bytes: base_resident_bytes as u64,
        };
        Ok((
            Self {
                file,
                range_begin: header_fields.range_begin,
                range_end: header_fields.range_end,
                index,
                cache: Mutex::new(BlockCache::new(cache_bytes)),
                file_read_operations: AtomicU64::new(open_file_io.operations),
                file_read_bytes: AtomicU64::new(open_file_io.bytes),
                base_resident_bytes,
                image_bytes,
                image_identity_sha256,
                index_bytes: footer_fields.index_bytes,
            },
            receipt,
        ))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if key < self.range_begin.as_slice() || key >= self.range_end.as_slice() {
            return Err("point is outside placed range".to_owned());
        }
        let insertion = self
            .index
            .partition_point(|entry| entry.first_key.as_slice() <= key);
        if insertion == 0 {
            return Ok(None);
        }
        let block_number = u32::try_from(insertion - 1)
            .map_err(|_| "range image block number exceeds u32".to_owned())?;
        let bytes = self.read_block(block_number)?;
        find_value_in_block(&bytes, key)
    }

    pub fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<RangeRows, String> {
        let mut rows = Vec::new();
        self.scan_batches(start, end, limit, 32, |batch| {
            rows.extend(batch.iter().cloned());
            Ok(())
        })?;
        Ok(rows)
    }

    pub fn scan_batches<F>(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        batch_rows: usize,
        mut consume: F,
    ) -> Result<usize, String>
    where
        F: FnMut(&[RangeRow]) -> Result<(), String>,
    {
        if start < self.range_begin.as_slice()
            || end > self.range_end.as_slice()
            || start >= end
            || batch_rows == 0
        {
            return Err("scan is outside placed range or has an empty batch".to_owned());
        }
        let mut emitted = 0_usize;
        let mut batch = Vec::with_capacity(batch_rows);
        for block_number in 0..self.index.len() {
            if emitted >= limit {
                break;
            }
            let bytes = self.read_block(
                u32::try_from(block_number)
                    .map_err(|_| "range image block number exceeds u32".to_owned())?,
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

    pub fn file_io(&self) -> FileIoSnapshot {
        FileIoSnapshot {
            operations: self.file_read_operations.load(Ordering::Relaxed),
            bytes: self.file_read_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn accounted_resident_bytes(&self) -> u64 {
        let cache_bytes = self
            .cache
            .lock()
            .map(|cache| cache.resident_bytes)
            .unwrap_or(usize::MAX);
        u64::try_from(self.base_resident_bytes.saturating_add(cache_bytes)).unwrap_or(u64::MAX)
    }

    pub fn image_bytes(&self) -> u64 {
        self.image_bytes
    }

    pub fn image_identity_sha256(&self) -> &str {
        &self.image_identity_sha256
    }

    pub fn index_bytes(&self) -> u64 {
        self.index_bytes
    }

    fn read_block(&self, block_number: u32) -> Result<Arc<[u8]>, String> {
        if let Some(bytes) = self
            .cache
            .lock()
            .map_err(|_| "range image block cache lock poisoned".to_owned())?
            .get(block_number)
        {
            return Ok(bytes);
        }
        let entry = self
            .index
            .get(block_number as usize)
            .ok_or_else(|| "range image block number is outside index".to_owned())?;
        let counters = (&self.file_read_operations, &self.file_read_bytes);
        let bytes =
            read_exact_at_counted(&self.file, entry.offset, entry.length as usize, &counters)?;
        let observed: [u8; 32] = Sha256::digest(&bytes).into();
        if observed != entry.sha256 {
            return Err("range image data-block checksum mismatch".to_owned());
        }
        validate_block(&bytes, entry)?;
        let bytes: Arc<[u8]> = bytes.into();
        self.cache
            .lock()
            .map_err(|_| "range image block cache lock poisoned".to_owned())?
            .insert(block_number, bytes.clone());
        Ok(bytes)
    }
}

pub(crate) fn write_range_image(
    path: &Path,
    identity: &RangeImageIdentity<'_>,
    rows: &[RangeRow],
) -> Result<RangeImageWriteReceipt, String> {
    if identity.image_identity_sha256.is_some()
        || identity.row_count != u64::try_from(rows.len()).unwrap_or(u64::MAX)
        || identity.range_begin >= identity.range_end
    {
        return Err("range image writer identity is invalid".to_owned());
    }
    validate_rows(identity, rows)?;
    let block_count = count_blocks(rows)?;
    let header = encode_header(identity, block_count)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(&header).map_err(|error| error.to_string())?;
    let mut offset = u64::try_from(header.len()).unwrap_or(u64::MAX);
    let mut index = Vec::with_capacity(block_count as usize);
    let mut block = Vec::with_capacity(MAX_BLOCK_BYTES);
    block.extend_from_slice(&0_u32.to_be_bytes());
    let mut block_rows = 0_u32;
    let mut block_first_key = Vec::new();
    for (key, value) in rows {
        let encoded_bytes = 8_usize
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or_else(|| "range image row size overflow".to_owned())?;
        if block_rows > 0 && block.len().saturating_add(encoded_bytes) > MAX_BLOCK_BYTES {
            flush_block(
                &mut file,
                &mut index,
                &mut offset,
                &mut block,
                &mut block_rows,
                &mut block_first_key,
            )?;
        }
        if block_rows == 0 {
            block_first_key.clone_from(key);
        }
        append_length_prefixed(&mut block, key)?;
        append_length_prefixed(&mut block, value)?;
        block_rows = block_rows.saturating_add(1);
    }
    if block_rows > 0 {
        flush_block(
            &mut file,
            &mut index,
            &mut offset,
            &mut block,
            &mut block_rows,
            &mut block_first_key,
        )?;
    }
    if index.len() != block_count as usize {
        return Err("range image block count changed while writing".to_owned());
    }
    let encoded_index = encode_index(&index)?;
    let index_offset = offset;
    file.write_all(&encoded_index)
        .map_err(|error| error.to_string())?;
    offset = offset.saturating_add(u64::try_from(encoded_index.len()).unwrap_or(u64::MAX));
    let index_sha256: [u8; 32] = Sha256::digest(&encoded_index).into();
    let image_bytes = offset.saturating_add(FOOTER_BYTES as u64);
    let footer = encode_footer(FooterFields {
        header_bytes: u64::try_from(header.len()).unwrap_or(u64::MAX),
        index_offset,
        index_bytes: u64::try_from(encoded_index.len()).unwrap_or(u64::MAX),
        image_bytes,
        index_sha256,
    });
    file.write_all(&footer).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    Ok(RangeImageWriteReceipt {
        image_identity_sha256: image_identity(&header, &encoded_index, &footer),
        image_bytes,
        index_bytes: u64::try_from(encoded_index.len()).unwrap_or(u64::MAX),
        block_count,
    })
}

pub(crate) fn root_identity_digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-range-image-root-identity-v2");
    for part in parts {
        hasher.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn validate_rows(identity: &RangeImageIdentity<'_>, rows: &[RangeRow]) -> Result<(), String> {
    for (index, (key, value)) in rows.iter().enumerate() {
        if key.as_slice() < identity.range_begin || key.as_slice() >= identity.range_end {
            return Err("range image contains an out-of-range key".to_owned());
        }
        if index > 0 && rows[index - 1].0 >= *key {
            return Err("range image keys are not strictly ordered".to_owned());
        }
        if 12_usize
            .saturating_add(key.len())
            .saturating_add(value.len())
            > MAX_BLOCK_BYTES
        {
            return Err("range image row exceeds maximum block bytes".to_owned());
        }
    }
    Ok(())
}

fn count_blocks(rows: &[RangeRow]) -> Result<u32, String> {
    let mut blocks = 0_u32;
    let mut bytes = 4_usize;
    let mut row_count = 0_usize;
    for (key, value) in rows {
        let row_bytes = 8_usize
            .checked_add(key.len())
            .and_then(|length| length.checked_add(value.len()))
            .ok_or_else(|| "range image row size overflow".to_owned())?;
        if row_count > 0 && bytes.saturating_add(row_bytes) > MAX_BLOCK_BYTES {
            blocks = blocks.saturating_add(1);
            bytes = 4;
            row_count = 0;
        }
        bytes = bytes.saturating_add(row_bytes);
        row_count = row_count.saturating_add(1);
    }
    if row_count > 0 {
        blocks = blocks.saturating_add(1);
    }
    Ok(blocks)
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
    let length =
        u32::try_from(block.len()).map_err(|_| "range image block exceeds u32".to_owned())?;
    let sha256: [u8; 32] = Sha256::digest(block.as_slice()).into();
    file.write_all(block).map_err(|error| error.to_string())?;
    index.push(BlockIndexEntry {
        first_key: std::mem::take(first_key),
        offset: *offset,
        length,
        row_count: *row_count,
        sha256,
    });
    *offset = offset.saturating_add(u64::from(length));
    block.clear();
    block.extend_from_slice(&0_u32.to_be_bytes());
    *row_count = 0;
    Ok(())
}

#[derive(Debug)]
struct HeaderFields {
    target_version: u64,
    row_count: u64,
    block_count: u32,
    maximum_block_bytes: u32,
    range_begin: Vec<u8>,
    range_end: Vec<u8>,
    root_identity_digest: [u8; 32],
}

fn encode_header(identity: &RangeImageIdentity<'_>, block_count: u32) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::with_capacity(96);
    bytes.extend_from_slice(HEADER_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&identity.target_version.to_be_bytes());
    bytes.extend_from_slice(&identity.row_count.to_be_bytes());
    bytes.extend_from_slice(&block_count.to_be_bytes());
    bytes.extend_from_slice(&(MAX_BLOCK_BYTES as u32).to_be_bytes());
    append_length_prefixed(&mut bytes, identity.range_begin)?;
    append_length_prefixed(&mut bytes, identity.range_end)?;
    bytes.extend_from_slice(&identity.root_identity_digest);
    Ok(bytes)
}

fn decode_header(bytes: &[u8]) -> Result<HeaderFields, String> {
    let mut input = bytes;
    if take_bytes(&mut input, 8)? != HEADER_MAGIC {
        return Err("range image header magic mismatch".to_owned());
    }
    if read_u16(&mut input)? != FORMAT_VERSION || read_u16(&mut input)? != 0 {
        return Err("range image format version mismatch".to_owned());
    }
    let target_version = read_u64(&mut input)?;
    let row_count = read_u64(&mut input)?;
    let block_count = read_u32(&mut input)?;
    let maximum_block_bytes = read_u32(&mut input)?;
    let range_begin = read_length_prefixed(&mut input)?;
    let range_end = read_length_prefixed(&mut input)?;
    let root_identity_digest = take_bytes(&mut input, 32)?
        .try_into()
        .map_err(|_| "range image root identity is truncated".to_owned())?;
    if !input.is_empty() || range_begin >= range_end || block_count == 0 {
        return Err("range image header fields are invalid".to_owned());
    }
    Ok(HeaderFields {
        target_version,
        row_count,
        block_count,
        maximum_block_bytes,
        range_begin,
        range_end,
        root_identity_digest,
    })
}

fn encode_index(entries: &[BlockIndexEntry]) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::with_capacity(entries.len().saturating_mul(70));
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(entries.len())
            .map_err(|_| "range image index count exceeds u32".to_owned())?
            .to_be_bytes(),
    );
    for entry in entries {
        append_length_prefixed(&mut bytes, &entry.first_key)?;
        bytes.extend_from_slice(&entry.offset.to_be_bytes());
        bytes.extend_from_slice(&entry.length.to_be_bytes());
        bytes.extend_from_slice(&entry.row_count.to_be_bytes());
        bytes.extend_from_slice(&entry.sha256);
    }
    Ok(bytes)
}

fn decode_index(
    bytes: &[u8],
    expected_count: u32,
    header_bytes: u64,
    index_offset: u64,
) -> Result<Vec<BlockIndexEntry>, String> {
    let mut input = bytes;
    if take_bytes(&mut input, 8)? != INDEX_MAGIC || read_u32(&mut input)? != expected_count {
        return Err("range image sparse-index header mismatch".to_owned());
    }
    let mut entries = Vec::with_capacity(expected_count as usize);
    let mut expected_offset = header_bytes;
    for _ in 0..expected_count {
        let first_key = read_length_prefixed(&mut input)?;
        let offset = read_u64(&mut input)?;
        let length = read_u32(&mut input)?;
        let row_count = read_u32(&mut input)?;
        let sha256 = take_bytes(&mut input, 32)?
            .try_into()
            .map_err(|_| "range image block digest is truncated".to_owned())?;
        if length == 0
            || length as usize > MAX_BLOCK_BYTES
            || row_count == 0
            || offset != expected_offset
            || entries
                .last()
                .is_some_and(|prior: &BlockIndexEntry| prior.first_key >= first_key)
        {
            return Err("range image sparse-index entry is invalid".to_owned());
        }
        expected_offset = expected_offset.saturating_add(u64::from(length));
        entries.push(BlockIndexEntry {
            first_key,
            offset,
            length,
            row_count,
            sha256,
        });
    }
    if !input.is_empty() || expected_offset != index_offset {
        return Err("range image sparse index has trailing or missing bytes".to_owned());
    }
    Ok(entries)
}

#[derive(Clone, Copy, Debug)]
struct FooterFields {
    header_bytes: u64,
    index_offset: u64,
    index_bytes: u64,
    image_bytes: u64,
    index_sha256: [u8; 32],
}

fn encode_footer(fields: FooterFields) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FOOTER_BYTES);
    bytes.extend_from_slice(FOOTER_MAGIC);
    bytes.extend_from_slice(&fields.header_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.index_offset.to_be_bytes());
    bytes.extend_from_slice(&fields.index_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.image_bytes.to_be_bytes());
    bytes.extend_from_slice(&fields.index_sha256);
    bytes
}

fn decode_footer(bytes: &[u8]) -> Result<FooterFields, String> {
    let mut input = bytes;
    if take_bytes(&mut input, 8)? != FOOTER_MAGIC {
        return Err("range image footer magic mismatch".to_owned());
    }
    let fields = FooterFields {
        header_bytes: read_u64(&mut input)?,
        index_offset: read_u64(&mut input)?,
        index_bytes: read_u64(&mut input)?,
        image_bytes: read_u64(&mut input)?,
        index_sha256: take_bytes(&mut input, 32)?
            .try_into()
            .map_err(|_| "range image footer checksum is truncated".to_owned())?,
    };
    if !input.is_empty() {
        return Err("range image footer has trailing bytes".to_owned());
    }
    Ok(fields)
}

fn image_identity(header: &[u8], index: &[u8], footer: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-range-image-identity-v2");
    hasher.update(header);
    hasher.update(index);
    hasher.update(footer);
    format!("{:x}", hasher.finalize())
}

fn resident_index_bytes(entries: &Vec<BlockIndexEntry>, begin: &[u8], end: &[u8]) -> usize {
    std::mem::size_of::<RangeImageReader>()
        .saturating_add(
            entries
                .capacity()
                .saturating_mul(std::mem::size_of::<BlockIndexEntry>()),
        )
        .saturating_add(
            entries
                .iter()
                .map(|entry| entry.first_key.capacity())
                .sum::<usize>(),
        )
        .saturating_add(begin.len())
        .saturating_add(end.len())
}

fn validate_block(bytes: &[u8], entry: &BlockIndexEntry) -> Result<(), String> {
    let mut input = bytes;
    let row_count = read_u32(&mut input)?;
    let mut prior: Option<&[u8]> = None;
    for row in 0..row_count {
        let key = read_length_prefixed_ref(&mut input)?;
        let _value = read_length_prefixed_ref(&mut input)?;
        if prior.is_some_and(|prior| prior >= key)
            || (row == 0 && key != entry.first_key.as_slice())
        {
            return Err("range image data-block keys or first key mismatch".to_owned());
        }
        prior = Some(key);
    }
    if row_count != entry.row_count || !input.is_empty() {
        return Err("range image data-block metadata mismatch".to_owned());
    }
    Ok(())
}

fn decode_block(bytes: &[u8]) -> Result<RangeRows, String> {
    let mut input = bytes;
    let row_count = read_u32(&mut input)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let key = read_length_prefixed(&mut input)?;
        let value = read_length_prefixed(&mut input)?;
        if rows.last().is_some_and(|prior: &RangeRow| prior.0 >= key) {
            return Err("range image block keys are not strictly ordered".to_owned());
        }
        rows.push((key, value));
    }
    if !input.is_empty() {
        return Err("range image data block has trailing bytes".to_owned());
    }
    Ok(rows)
}

fn find_value_in_block(bytes: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let mut input = bytes;
    let row_count = read_u32(&mut input)?;
    let mut prior: Option<&[u8]> = None;
    for _ in 0..row_count {
        let candidate = read_length_prefixed_ref(&mut input)?;
        let value = read_length_prefixed_ref(&mut input)?;
        if prior.is_some_and(|prior| prior >= candidate) {
            return Err("range image block keys are not strictly ordered".to_owned());
        }
        match candidate.cmp(key) {
            std::cmp::Ordering::Equal => return Ok(Some(value.to_vec())),
            std::cmp::Ordering::Greater => return Ok(None),
            std::cmp::Ordering::Less => prior = Some(candidate),
        }
    }
    if !input.is_empty() {
        return Err("range image data block has trailing bytes".to_owned());
    }
    Ok(None)
}

fn append_length_prefixed(output: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| "range image field exceeds u32 length".to_owned())?
            .to_be_bytes(),
    );
    output.extend_from_slice(value);
    Ok(())
}

fn read_u16(input: &mut &[u8]) -> Result<u16, String> {
    Ok(u16::from_be_bytes(
        take_bytes(input, 2)?
            .try_into()
            .map_err(|_| "range image u16 is truncated".to_owned())?,
    ))
}

fn read_u32(input: &mut &[u8]) -> Result<u32, String> {
    Ok(u32::from_be_bytes(
        take_bytes(input, 4)?
            .try_into()
            .map_err(|_| "range image u32 is truncated".to_owned())?,
    ))
}

fn read_u64(input: &mut &[u8]) -> Result<u64, String> {
    Ok(u64::from_be_bytes(
        take_bytes(input, 8)?
            .try_into()
            .map_err(|_| "range image u64 is truncated".to_owned())?,
    ))
}

fn read_length_prefixed(input: &mut &[u8]) -> Result<Vec<u8>, String> {
    Ok(read_length_prefixed_ref(input)?.to_vec())
}

fn read_length_prefixed_ref<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let length = read_u32(input)? as usize;
    take_bytes(input, length)
}

fn take_bytes<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8], String> {
    if input.len() < length {
        return Err("range image is truncated".to_owned());
    }
    let (head, tail) = input.split_at(length);
    *input = tail;
    Ok(head)
}

fn read_exact_at_counted(
    file: &File,
    offset: u64,
    length: usize,
    counters: &(&AtomicU64, &AtomicU64),
) -> Result<Vec<u8>, String> {
    let mut bytes = vec![0_u8; length];
    let operations = read_exact_at(file, &mut bytes, offset)?;
    counters.0.fetch_add(operations, Ordering::Relaxed);
    counters
        .1
        .fetch_add(u64::try_from(length).unwrap_or(u64::MAX), Ordering::Relaxed);
    Ok(bytes)
}

#[cfg(unix)]
fn read_exact_at(file: &File, bytes: &mut [u8], mut offset: u64) -> Result<u64, String> {
    use std::os::unix::fs::FileExt;

    let mut remaining = bytes;
    let mut operations = 0_u64;
    while !remaining.is_empty() {
        let read = file
            .read_at(remaining, offset)
            .map_err(|error| error.to_string())?;
        operations = operations.saturating_add(1);
        if read == 0 {
            return Err("range image positional read reached end of file".to_owned());
        }
        offset = offset.saturating_add(read as u64);
        remaining = &mut remaining[read..];
    }
    Ok(operations)
}

#[cfg(windows)]
fn read_exact_at(file: &File, bytes: &mut [u8], offset: u64) -> Result<u64, String> {
    use std::os::windows::fs::FileExt;

    let mut completed = 0_usize;
    let mut operations = 0_u64;
    while completed < bytes.len() {
        let read = file
            .seek_read(
                &mut bytes[completed..],
                offset.saturating_add(completed as u64),
            )
            .map_err(|error| error.to_string())?;
        operations = operations.saturating_add(1);
        if read == 0 {
            return Err("range image positional read reached end of file".to_owned());
        }
        completed = completed.saturating_add(read);
    }
    Ok(operations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Instant;

    fn rows(count: usize, value_bytes: usize) -> RangeRows {
        (0..count)
            .map(|ordinal| {
                (
                    format!("k/{ordinal:016x}").into_bytes(),
                    vec![u8::try_from(ordinal % 251).unwrap_or(0); value_bytes],
                )
            })
            .collect()
    }

    fn identity<'a>(rows: &'a [RangeRow]) -> RangeImageIdentity<'a> {
        RangeImageIdentity {
            target_version: 7,
            range_begin: &rows[0].0,
            range_end: b"k0",
            row_count: rows.len() as u64,
            root_identity_digest: root_identity_digest(&[b"cell", b"root", b"txlog"]),
            image_identity_sha256: None,
        }
    }

    #[test]
    fn sparse_image_bounds_point_io_and_memory() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("range-image.okv");
        let expected_rows = rows(512, 8_192);
        let write = write_range_image(&path, &identity(&expected_rows), &expected_rows).unwrap();
        let mut expected = identity(&expected_rows);
        expected.image_identity_sha256 = Some(&write.image_identity_sha256);
        let (reader, open) = RangeImageReader::open(&path, &expected, 524_288).unwrap();
        assert_eq!(open.open_file_io.operations, 3);
        assert!(open.open_file_io.bytes <= 524_288);
        let before = reader.file_io();
        assert_eq!(
            reader.get(&expected_rows[400].0).unwrap(),
            Some(expected_rows[400].1.clone())
        );
        let point = reader.file_io().difference_since(before);
        assert_eq!(point.operations, 1);
        assert!(point.bytes <= MAX_BLOCK_BYTES as u64);
        assert!(reader.accounted_resident_bytes() <= 524_288);
        assert_eq!(
            reader.scan(&expected_rows[0].0, b"k0", 512).unwrap(),
            expected_rows
        );
    }

    #[test]
    fn sparse_image_rejects_corrupt_index_and_block() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("range-image.okv");
        let expected_rows = rows(64, 1_024);
        let write = write_range_image(&path, &identity(&expected_rows), &expected_rows).unwrap();
        let mut expected = identity(&expected_rows);
        expected.image_identity_sha256 = Some(&write.image_identity_sha256);
        let mut bytes = fs::read(&path).unwrap();
        let index_offset = bytes.len() - FOOTER_BYTES - write.index_bytes as usize;
        bytes[index_offset + 8] ^= 0x01;
        fs::write(&path, &bytes).unwrap();
        assert!(RangeImageReader::open(&path, &expected, 524_288).is_err());

        let path = root.path().join("range-image-block.okv");
        let write = write_range_image(&path, &identity(&expected_rows), &expected_rows).unwrap();
        let mut expected = identity(&expected_rows);
        expected.image_identity_sha256 = Some(&write.image_identity_sha256);
        let mut bytes = fs::read(&path).unwrap();
        bytes[96] ^= 0x01;
        fs::write(&path, &bytes).unwrap();
        let (reader, _) = RangeImageReader::open(&path, &expected, 524_288).unwrap();
        assert!(reader.scan(&expected_rows[0].0, b"k0", 64).is_err());
    }

    #[test]
    fn frozen_full_range_curve_meets_bounded_io_contract() {
        const MEMORY_BUDGET: usize = 4_194_304;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("range-image.okv");
        let expected_rows = rows(4_096, 8_192);
        let write = write_range_image(&path, &identity(&expected_rows), &expected_rows).unwrap();
        let mut expected = identity(&expected_rows);
        expected.image_identity_sha256 = Some(&write.image_identity_sha256);
        let open_started = Instant::now();
        let (reader, open) = RangeImageReader::open(&path, &expected, MEMORY_BUDGET).unwrap();
        let open_seconds = open_started.elapsed().as_secs_f64();

        let mut state = 724_851_u64;
        let mut next_ordinal = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            usize::try_from(state % 4_096).unwrap_or(0)
        };
        for _ in 0..256 {
            let ordinal = next_ordinal();
            assert_eq!(
                reader.get(&expected_rows[ordinal].0).unwrap(),
                Some(expected_rows[ordinal].1.clone())
            );
        }
        let mut durations = Vec::with_capacity(4_096);
        let mut operations = Vec::with_capacity(4_096);
        let mut bytes = Vec::with_capacity(4_096);
        let mut hits = 0_u64;
        for _ in 0..4_096 {
            let ordinal = next_ordinal();
            let before = reader.file_io();
            let started = Instant::now();
            let observed = reader.get(&expected_rows[ordinal].0).unwrap();
            durations.push(started.elapsed().as_secs_f64());
            let point = reader.file_io().difference_since(before);
            operations.push(point.operations);
            bytes.push(point.bytes);
            hits = hits.saturating_add(u64::from(point.operations == 0));
            assert_eq!(observed, Some(expected_rows[ordinal].1.clone()));
        }
        durations.sort_by(f64::total_cmp);
        operations.sort_unstable();
        bytes.sort_unstable();
        let p99 = |length: usize| length.saturating_sub(1).saturating_mul(99) / 100;
        let duration_p99 = durations[p99(durations.len())];
        let operations_p99 = operations[p99(operations.len())];
        let bytes_p99 = bytes[p99(bytes.len())];
        let accounted = reader.accounted_resident_bytes();
        let image_to_memory = write.image_bytes as f64 / MEMORY_BUDGET as f64;
        let hit_ratio = hits as f64 / 4_096.0;
        eprintln!(
            "range_image_full image={} ratio={image_to_memory:.3} accounted={} index={} blocks={} open_ops={} open_bytes={} open_us={:.3} point_ops_p99={} point_bytes_p99={} point_us_p99={:.3} hit_ratio={hit_ratio:.3}",
            write.image_bytes,
            accounted,
            write.index_bytes,
            write.block_count,
            open.open_file_io.operations,
            open.open_file_io.bytes,
            open_seconds * 1_000_000.0,
            operations_p99,
            bytes_p99,
            duration_p99 * 1_000_000.0,
        );
        assert!(image_to_memory >= 8.0);
        assert!(accounted <= MEMORY_BUDGET as u64);
        assert!(open.open_file_io.operations <= 4);
        assert!(open.open_file_io.bytes <= 524_288);
        assert!(operations_p99 <= 2);
        assert!(bytes_p99 <= MAX_BLOCK_BYTES as u64);
        assert!(duration_p99 <= 0.001);
    }
}
