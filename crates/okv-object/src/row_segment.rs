//! Portable immutable row-object blocks with a warm sparse index.

use crate::{Backend, RevisionToken};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;

const DATA_MAGIC: &[u8; 4] = b"OKVB";
const INDEX_MAGIC: &[u8; 4] = b"OKVI";
const FORMAT_VERSION: u16 = 1;
const DIGEST_BYTES: usize = 32;
const MAX_BLOCKS: usize = 1_000_000;

/// One versioned row-object record. `None` is a tombstone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowRecord {
    pub key: Vec<u8>,
    pub version: u64,
    pub value: Option<Vec<u8>>,
}

impl RowRecord {
    #[must_use]
    pub fn value(key: impl AsRef<[u8]>, version: u64, value: impl AsRef<[u8]>) -> Self {
        Self {
            key: key.as_ref().to_vec(),
            version,
            value: Some(value.as_ref().to_vec()),
        }
    }

    #[must_use]
    pub fn tombstone(key: impl AsRef<[u8]>, version: u64) -> Self {
        Self {
            key: key.as_ref().to_vec(),
            version,
            value: None,
        }
    }
}

/// Encoded immutable data and its separately cacheable sparse index.
#[derive(Clone, Debug)]
pub struct EncodedRowSegment {
    pub data: Bytes,
    pub index: Bytes,
    pub block_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BlockIndex {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    offset: u64,
    length: u64,
    min_version: u64,
    max_version: u64,
    digest: [u8; DIGEST_BYTES],
}

/// Validated sparse index for one immutable row object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowSegmentIndex {
    generation: u64,
    min_version: u64,
    max_version: u64,
    object_length: u64,
    data_digest: [u8; DIGEST_BYTES],
    blocks: Vec<BlockIndex>,
}

impl RowSegmentIndex {
    /// Decode and fully validate one cached index object.
    ///
    /// # Errors
    ///
    /// Returns an error for checksum, version, bounds, ordering, or framing
    /// violations.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < DIGEST_BYTES {
            return Err("row index is shorter than its checksum".to_owned());
        }
        let payload_length = bytes.len() - DIGEST_BYTES;
        if digest(&bytes[..payload_length]).as_slice() != &bytes[payload_length..] {
            return Err("row index checksum mismatch".to_owned());
        }
        let mut cursor = Cursor::new(&bytes[..payload_length]);
        if cursor.take_array::<4>()? != *INDEX_MAGIC {
            return Err("row index magic mismatch".to_owned());
        }
        if cursor.u16()? != FORMAT_VERSION {
            return Err("unsupported row index format version".to_owned());
        }
        let generation = cursor.u64()?;
        let min_version = cursor.u64()?;
        let max_version = cursor.u64()?;
        let object_length = cursor.u64()?;
        let data_digest = cursor.take_array::<DIGEST_BYTES>()?;
        let block_count = usize::try_from(cursor.u32()?)
            .map_err(|error| format!("invalid row index block count: {error}"))?;
        if generation == 0
            || min_version == 0
            || max_version < min_version
            || object_length == 0
            || block_count == 0
            || block_count > MAX_BLOCKS
        {
            return Err("invalid row index header".to_owned());
        }
        let mut blocks = Vec::with_capacity(block_count);
        let mut expected_offset = 0_u64;
        for _ in 0..block_count {
            let first_key = cursor.length_prefixed()?;
            let last_key = cursor.length_prefixed()?;
            let offset = cursor.u64()?;
            let length = cursor.u64()?;
            let block_min_version = cursor.u64()?;
            let block_max_version = cursor.u64()?;
            let block_digest = cursor.take_array::<DIGEST_BYTES>()?;
            if first_key.is_empty()
                || first_key > last_key
                || offset != expected_offset
                || length == 0
                || block_min_version == 0
                || block_max_version < block_min_version
                || block_min_version < min_version
                || block_max_version > max_version
            {
                return Err("invalid row index block".to_owned());
            }
            if let Some(previous) = blocks.last() {
                let previous: &BlockIndex = previous;
                if previous.last_key >= first_key {
                    return Err("row index key ranges overlap or regress".to_owned());
                }
            }
            expected_offset = offset
                .checked_add(length)
                .ok_or_else(|| "row index block offset overflow".to_owned())?;
            blocks.push(BlockIndex {
                first_key,
                last_key,
                offset,
                length,
                min_version: block_min_version,
                max_version: block_max_version,
                digest: block_digest,
            });
        }
        cursor.finish()?;
        if expected_offset != object_length {
            return Err("row index does not cover the complete data object".to_owned());
        }
        Ok(Self {
            generation,
            min_version,
            max_version,
            object_length,
            data_digest,
            blocks,
        })
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn min_version(&self) -> u64 {
        self.min_version
    }

    #[must_use]
    pub fn max_version(&self) -> u64 {
        self.max_version
    }

    #[must_use]
    pub fn object_length(&self) -> u64 {
        self.object_length
    }

    #[must_use]
    pub fn data_sha256(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(DIGEST_BYTES * 2);
        for byte in self.data_digest {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }

    #[must_use]
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    #[must_use]
    pub fn max_block_bytes(&self) -> u64 {
        self.blocks
            .iter()
            .map(|block| block.length)
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn first_key(&self) -> &[u8] {
        &self.blocks[0].first_key
    }

    #[must_use]
    pub fn last_key(&self) -> &[u8] {
        &self.blocks[self.blocks.len() - 1].last_key
    }

    fn locate(&self, key: &[u8]) -> Option<&BlockIndex> {
        let mut lower = 0_usize;
        let mut upper = self.blocks.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.blocks[middle].first_key.as_slice() <= key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let candidate = lower
            .checked_sub(1)
            .and_then(|index| self.blocks.get(index))?;
        (key <= candidate.last_key.as_slice()).then_some(candidate)
    }
}

/// Logical result of a versioned point lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PointReadOutcome {
    Value(Bytes),
    Tombstone,
    Absent,
}

/// Result plus physical bytes fetched from the row data object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointRead {
    pub outcome: PointReadOutcome,
    pub data_bytes: u64,
}

/// Encode sorted MVCC records into independently checksummed row blocks and a
/// checksummed sparse index. Records are ordered by key ascending, then version
/// descending. Versions for one key are never split across blocks.
///
/// # Errors
///
/// Returns an error for empty input, invalid versions or ordering, values too
/// large for the format, or arithmetic overflow.
pub fn encode_row_segment(
    generation: u64,
    records: &[RowRecord],
    target_block_bytes: usize,
) -> Result<EncodedRowSegment, String> {
    validate_records(generation, records, target_block_bytes)?;
    let mut encoded_blocks = Vec::new();
    let mut block_start = 0_usize;
    let mut block_estimate = 0_usize;
    let mut cursor = 0_usize;
    while cursor < records.len() {
        let group_start = cursor;
        let key = records[cursor].key.as_slice();
        let mut group_estimate = 0_usize;
        while cursor < records.len() && records[cursor].key.as_slice() == key {
            group_estimate = group_estimate
                .checked_add(record_encoded_length(&records[cursor])?)
                .ok_or_else(|| "row block length overflow".to_owned())?;
            cursor += 1;
        }
        if group_start > block_start
            && block_estimate.saturating_add(group_estimate) > target_block_bytes
        {
            encoded_blocks.push(encode_block(&records[block_start..group_start])?);
            block_start = group_start;
            block_estimate = 0;
        }
        block_estimate = block_estimate
            .checked_add(group_estimate)
            .ok_or_else(|| "row block length overflow".to_owned())?;
    }
    encoded_blocks.push(encode_block(&records[block_start..])?);

    let mut data = Vec::new();
    let mut blocks = Vec::with_capacity(encoded_blocks.len());
    for encoded in encoded_blocks {
        let offset = u64::try_from(data.len())
            .map_err(|error| format!("row object offset overflow: {error}"))?;
        let length = u64::try_from(encoded.bytes.len())
            .map_err(|error| format!("row block length overflow: {error}"))?;
        data.extend_from_slice(&encoded.bytes);
        blocks.push(BlockIndex {
            first_key: encoded.first_key,
            last_key: encoded.last_key,
            offset,
            length,
            min_version: encoded.min_version,
            max_version: encoded.max_version,
            digest: digest(&encoded.bytes),
        });
    }
    let min_version = records
        .iter()
        .map(|record| record.version)
        .min()
        .unwrap_or(0);
    let max_version = records
        .iter()
        .map(|record| record.version)
        .max()
        .unwrap_or(0);
    let object_length = u64::try_from(data.len())
        .map_err(|error| format!("row object length overflow: {error}"))?;
    let block_count = blocks.len();
    let index = encode_index(&RowSegmentIndex {
        generation,
        min_version,
        max_version,
        object_length,
        data_digest: digest(&data),
        blocks,
    })?;
    Ok(EncodedRowSegment {
        data: Bytes::from(data),
        index: Bytes::from(index),
        block_count,
    })
}

/// Read one point with at most one data range GET after the index is cached.
///
/// # Errors
///
/// Returns an error for an invalid read version, object identity mismatch,
/// short or corrupt range response, or malformed block.
pub async fn read_indexed_point(
    backend: &dyn Backend,
    data_key: &str,
    expected: Option<&RevisionToken>,
    index: &RowSegmentIndex,
    key: &[u8],
    read_version: u64,
) -> Result<PointRead, String> {
    if read_version == 0 {
        return Err("row point read version must be non-zero".to_owned());
    }
    let Some(block) = index.locate(key) else {
        return Ok(PointRead {
            outcome: PointReadOutcome::Absent,
            data_bytes: 0,
        });
    };
    let end = block
        .offset
        .checked_add(block.length)
        .ok_or_else(|| "row block range overflow".to_owned())?;
    let read = backend
        .get(data_key, Some(block.offset..end), expected)
        .await
        .map_err(|error| error.to_string())?;
    if read.object_length != index.object_length
        || read.returned_range != (block.offset..end)
        || u64::try_from(read.bytes.len()).unwrap_or(u64::MAX) != block.length
    {
        return Err("row block response does not match its index".to_owned());
    }
    let records = decode_block(&read.bytes, block)?;
    Ok(PointRead {
        outcome: select_version(&records, key, read_version),
        data_bytes: block.length,
    })
}

/// Deliberately scan the complete object for one point. This exists as the
/// negative-control implementation for the cold-point evaluation gate.
///
/// # Errors
///
/// Returns an error for object identity, checksum, framing, or block failures.
pub async fn scan_full_object_for_point(
    backend: &dyn Backend,
    data_key: &str,
    expected: Option<&RevisionToken>,
    index: &RowSegmentIndex,
    key: &[u8],
    read_version: u64,
) -> Result<PointRead, String> {
    let read = backend
        .get(data_key, None, expected)
        .await
        .map_err(|error| error.to_string())?;
    if read.object_length != index.object_length {
        return Err("complete row object does not match its index".to_owned());
    }
    read_point_from_full_object(&read.bytes, index, key, read_version)
}

/// Decode one point from already fetched complete object bytes.
///
/// # Errors
///
/// Returns an error for an invalid read version, object digest, framing, or
/// block checksum.
pub fn read_point_from_full_object(
    data: &[u8],
    index: &RowSegmentIndex,
    key: &[u8],
    read_version: u64,
) -> Result<PointRead, String> {
    if read_version == 0 {
        return Err("row point read version must be non-zero".to_owned());
    }
    if u64::try_from(data.len()).unwrap_or(u64::MAX) != index.object_length
        || digest(data) != index.data_digest
    {
        return Err("complete row object does not match its index".to_owned());
    }
    let mut outcome = PointReadOutcome::Absent;
    for block in &index.blocks {
        if key < block.first_key.as_slice() || key > block.last_key.as_slice() {
            continue;
        }
        let start = usize::try_from(block.offset)
            .map_err(|error| format!("row block offset overflow: {error}"))?;
        let end = usize::try_from(block.offset.saturating_add(block.length))
            .map_err(|error| format!("row block end overflow: {error}"))?;
        let records = decode_block(&data[start..end], block)?;
        outcome = select_version(&records, key, read_version);
        break;
    }
    Ok(PointRead {
        outcome,
        data_bytes: index.object_length,
    })
}

/// Verify the complete immutable data object against its sparse index and
/// decode every checksummed block before the object can become a recovery
/// frontier.
///
/// # Errors
///
/// Returns an error for an object identity mismatch, malformed block, checksum
/// failure, or key ordering regression within the encoded object.
pub fn validate_full_row_object(data: &[u8], index: &RowSegmentIndex) -> Result<(), String> {
    if u64::try_from(data.len()).unwrap_or(u64::MAX) != index.object_length
        || digest(data) != index.data_digest
    {
        return Err("complete row object does not match its index".to_owned());
    }
    let mut previous_key: Option<Vec<u8>> = None;
    for block in &index.blocks {
        let start = usize::try_from(block.offset)
            .map_err(|error| format!("row block offset overflow: {error}"))?;
        let end = usize::try_from(block.offset.saturating_add(block.length))
            .map_err(|error| format!("row block end overflow: {error}"))?;
        let records = decode_block(&data[start..end], block)?;
        for record in records {
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous.as_slice() > record.key.as_slice())
            {
                return Err("row object keys regress across block boundaries".to_owned());
            }
            previous_key = Some(record.key);
        }
    }
    Ok(())
}

/// Decode every record from a complete immutable row object after validating
/// its object digest, block checksums, framing, and cross-block ordering.
///
/// # Errors
///
/// Returns an error for an object identity mismatch, malformed block, checksum
/// failure, or key ordering regression.
pub fn decode_full_row_object(
    data: &[u8],
    index: &RowSegmentIndex,
) -> Result<Vec<RowRecord>, String> {
    if u64::try_from(data.len()).unwrap_or(u64::MAX) != index.object_length
        || digest(data) != index.data_digest
    {
        return Err("complete row object does not match its index".to_owned());
    }
    let mut decoded = Vec::new();
    for block in &index.blocks {
        let start = usize::try_from(block.offset)
            .map_err(|error| format!("row block offset overflow: {error}"))?;
        let end = usize::try_from(block.offset.saturating_add(block.length))
            .map_err(|error| format!("row block end overflow: {error}"))?;
        decoded.extend(decode_block(&data[start..end], block)?);
    }
    validate_records(1, &decoded, 4_096)?;
    Ok(decoded)
}

fn validate_records(
    generation: u64,
    records: &[RowRecord],
    target_block_bytes: usize,
) -> Result<(), String> {
    if generation == 0 || records.is_empty() || target_block_bytes < 4_096 {
        return Err("invalid row segment build parameters".to_owned());
    }
    for (index, record) in records.iter().enumerate() {
        if record.key.is_empty() || record.version == 0 {
            return Err("row records require non-empty keys and non-zero versions".to_owned());
        }
        let _ = record_encoded_length(record)?;
        if let Some(previous) = index.checked_sub(1).and_then(|prior| records.get(prior)) {
            match previous.key.cmp(&record.key) {
                Ordering::Greater => return Err("row records are not key sorted".to_owned()),
                Ordering::Equal if previous.version <= record.version => {
                    return Err("row record versions are not strictly descending".to_owned());
                }
                Ordering::Equal | Ordering::Less => {}
            }
        }
    }
    Ok(())
}

struct EncodedBlock {
    bytes: Vec<u8>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    min_version: u64,
    max_version: u64,
}

fn encode_block(records: &[RowRecord]) -> Result<EncodedBlock, String> {
    let count = u32::try_from(records.len())
        .map_err(|error| format!("too many row records in one block: {error}"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(DATA_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&count.to_be_bytes());
    for record in records {
        put_length_prefixed(&mut bytes, &record.key)?;
        bytes.extend_from_slice(&record.version.to_be_bytes());
        if let Some(value) = &record.value {
            bytes.push(1);
            put_length_prefixed(&mut bytes, value)?;
        } else {
            bytes.push(0);
            bytes.extend_from_slice(&0_u32.to_be_bytes());
        }
    }
    Ok(EncodedBlock {
        bytes,
        first_key: records[0].key.clone(),
        last_key: records[records.len() - 1].key.clone(),
        min_version: records
            .iter()
            .map(|record| record.version)
            .min()
            .unwrap_or(0),
        max_version: records
            .iter()
            .map(|record| record.version)
            .max()
            .unwrap_or(0),
    })
}

fn decode_block(bytes: &[u8], index: &BlockIndex) -> Result<Vec<RowRecord>, String> {
    if digest(bytes) != index.digest {
        return Err("row block checksum mismatch".to_owned());
    }
    let mut cursor = Cursor::new(bytes);
    if cursor.take_array::<4>()? != *DATA_MAGIC || cursor.u16()? != FORMAT_VERSION {
        return Err("row block format mismatch".to_owned());
    }
    let count = usize::try_from(cursor.u32()?)
        .map_err(|error| format!("invalid row block record count: {error}"))?;
    if count == 0 {
        return Err("row block is empty".to_owned());
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let key = cursor.length_prefixed()?;
        let version = cursor.u64()?;
        let kind = cursor.u8()?;
        let value = cursor.length_prefixed()?;
        let value = match kind {
            0 if value.is_empty() => None,
            1 => Some(value),
            _ => return Err("invalid row record kind".to_owned()),
        };
        records.push(RowRecord {
            key,
            version,
            value,
        });
    }
    cursor.finish()?;
    validate_records(1, &records, 4_096)?;
    if records[0].key != index.first_key
        || records[records.len() - 1].key != index.last_key
        || records.iter().map(|record| record.version).min() != Some(index.min_version)
        || records.iter().map(|record| record.version).max() != Some(index.max_version)
    {
        return Err("row block metadata does not match its index".to_owned());
    }
    Ok(records)
}

fn select_version(records: &[RowRecord], key: &[u8], read_version: u64) -> PointReadOutcome {
    records
        .iter()
        .find(|record| record.key.as_slice() == key && record.version <= read_version)
        .map_or(PointReadOutcome::Absent, |record| {
            record
                .value
                .as_ref()
                .map_or(PointReadOutcome::Tombstone, |value| {
                    PointReadOutcome::Value(Bytes::copy_from_slice(value))
                })
        })
}

fn encode_index(index: &RowSegmentIndex) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&index.generation.to_be_bytes());
    bytes.extend_from_slice(&index.min_version.to_be_bytes());
    bytes.extend_from_slice(&index.max_version.to_be_bytes());
    bytes.extend_from_slice(&index.object_length.to_be_bytes());
    bytes.extend_from_slice(&index.data_digest);
    let count = u32::try_from(index.blocks.len())
        .map_err(|error| format!("too many row index blocks: {error}"))?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for block in &index.blocks {
        put_length_prefixed(&mut bytes, &block.first_key)?;
        put_length_prefixed(&mut bytes, &block.last_key)?;
        bytes.extend_from_slice(&block.offset.to_be_bytes());
        bytes.extend_from_slice(&block.length.to_be_bytes());
        bytes.extend_from_slice(&block.min_version.to_be_bytes());
        bytes.extend_from_slice(&block.max_version.to_be_bytes());
        bytes.extend_from_slice(&block.digest);
    }
    let checksum = digest(&bytes);
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn record_encoded_length(record: &RowRecord) -> Result<usize, String> {
    let value_length = record.value.as_ref().map_or(0, Vec::len);
    let _ = u32::try_from(record.key.len())
        .map_err(|error| format!("row key is too large: {error}"))?;
    let _ =
        u32::try_from(value_length).map_err(|error| format!("row value is too large: {error}"))?;
    4_usize
        .checked_add(record.key.len())
        .and_then(|length| length.checked_add(8 + 1 + 4))
        .and_then(|length| length.checked_add(value_length))
        .ok_or_else(|| "row record length overflow".to_owned())
}

fn put_length_prefixed(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    let length =
        u32::try_from(value.len()).map_err(|error| format!("row field is too large: {error}"))?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn digest(bytes: &[u8]) -> [u8; DIGEST_BYTES] {
    Sha256::digest(bytes).into()
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let value = self.take(N)?;
        value
            .try_into()
            .map_err(|_| "row segment field length mismatch".to_owned())
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| "row segment cursor overflow".to_owned())?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| "truncated row segment".to_owned())?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take_array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(self.take_array()?))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.take_array()?))
    }

    fn length_prefixed(&mut self) -> Result<Vec<u8>, String> {
        let length = usize::try_from(self.u32()?)
            .map_err(|error| format!("invalid row field length: {error}"))?;
        Ok(self.take(length)?.to_vec())
    }

    fn finish(self) -> Result<(), String> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing bytes in row segment".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_full_row_object, encode_row_segment, read_indexed_point, scan_full_object_for_point,
        PointReadOutcome, RowRecord, RowSegmentIndex,
    };
    use crate::{memory_backend, Backend, ObservedBackend, WriteCondition};
    use bytes::Bytes;
    use std::sync::Arc;

    fn records() -> Vec<RowRecord> {
        vec![
            RowRecord::value(b"a", 5, b"a5"),
            RowRecord::value(b"a", 3, b"a3"),
            RowRecord::tombstone(b"b", 4),
            RowRecord::value(b"b", 2, b"b2"),
            RowRecord::value(b"c", 1, b"c1"),
        ]
    }

    #[tokio::test]
    async fn indexed_point_read_uses_one_range_get() {
        let encoded = encode_row_segment(7, &records(), 4_096).expect("encode row segment");
        let index = RowSegmentIndex::decode(&encoded.index).expect("decode row index");
        let observed = Arc::new(ObservedBackend::new(memory_backend()));
        let revision = observed
            .put("rows/data", encoded.data, WriteCondition::Create)
            .await
            .expect("put row data");
        observed.clear_stats();

        let read = read_indexed_point(
            observed.as_ref(),
            "rows/data",
            Some(&revision),
            &index,
            b"a",
            4,
        )
        .await
        .expect("indexed point read");
        assert_eq!(
            read.outcome,
            PointReadOutcome::Value(Bytes::from_static(b"a3"))
        );
        let stats = observed.stats();
        assert_eq!(stats.requests.len(), 1);
        assert_eq!(stats.requests[0].api, "get.range");
        assert_eq!(stats.requests[0].count, 1);
    }

    #[tokio::test]
    async fn tombstones_and_full_scan_have_exact_semantics() {
        let encoded = encode_row_segment(7, &records(), 4_096).expect("encode row segment");
        let index = RowSegmentIndex::decode(&encoded.index).expect("decode row index");
        let backend = memory_backend();
        let revision = backend
            .put("rows/data", encoded.data, WriteCondition::Create)
            .await
            .expect("put row data");
        let indexed = read_indexed_point(
            backend.as_ref(),
            "rows/data",
            Some(&revision),
            &index,
            b"b",
            4,
        )
        .await
        .expect("indexed tombstone");
        let scanned = scan_full_object_for_point(
            backend.as_ref(),
            "rows/data",
            Some(&revision),
            &index,
            b"b",
            3,
        )
        .await
        .expect("scanned historical value");
        assert_eq!(indexed.outcome, PointReadOutcome::Tombstone);
        assert_eq!(
            scanned.outcome,
            PointReadOutcome::Value(Bytes::from_static(b"b2"))
        );
    }

    #[tokio::test]
    async fn corrupt_block_is_rejected() {
        let encoded = encode_row_segment(7, &records(), 4_096).expect("encode row segment");
        let index = RowSegmentIndex::decode(&encoded.index).expect("decode row index");
        let mut corrupt = encoded.data.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x80;
        let backend = memory_backend();
        let revision = backend
            .put("rows/data", Bytes::from(corrupt), WriteCondition::Create)
            .await
            .expect("put corrupt row data");
        let error = read_indexed_point(
            backend.as_ref(),
            "rows/data",
            Some(&revision),
            &index,
            b"c",
            1,
        )
        .await
        .expect_err("corrupt block must fail");
        assert!(error.contains("checksum"));
    }

    #[test]
    fn malformed_order_and_index_checksum_are_rejected() {
        let mut unsorted = records();
        unsorted.swap(0, 2);
        assert!(encode_row_segment(7, &unsorted, 4_096).is_err());

        let encoded = encode_row_segment(7, &records(), 4_096).expect("encode row segment");
        let mut corrupt_index = encoded.index.to_vec();
        corrupt_index[0] ^= 0x80;
        assert_eq!(
            RowSegmentIndex::decode(&corrupt_index),
            Err("row index checksum mismatch".to_owned())
        );
    }

    #[test]
    fn complete_object_decode_preserves_the_mvcc_stream() {
        let records = records();
        let encoded = encode_row_segment(7, &records, 4_096).expect("encode row segment");
        let index = RowSegmentIndex::decode(&encoded.index).expect("decode row index");
        assert_eq!(
            decode_full_row_object(&encoded.data, &index).expect("decode complete row object"),
            records
        );

        let mut corrupt = encoded.data.to_vec();
        corrupt[0] ^= 0x80;
        assert!(decode_full_row_object(&corrupt, &index).is_err());
    }
}
