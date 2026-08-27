//! Checksummed manifest for a sorted set of immutable row objects.

use crate::{EncodedRowSegment, RowRecord, RowSegmentIndex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const MANIFEST_MAGIC: &[u8; 4] = b"OKVM";
const MANIFEST_FORMAT_VERSION: u16 = 1;
const MANIFEST_SCHEMA_VERSION: u16 = 1;
const DIGEST_BYTES: usize = 32;
const MAX_SEGMENTS: usize = 1_000_000;
const ROW_BLOCK_HEADER_BYTES: usize = 4 + 2 + 4;

/// Exact immutable object identities and bounds for one row segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowObjectReference {
    pub data_key: String,
    pub index_key: String,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub generation: u64,
    pub min_version: u64,
    pub max_version: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub data_sha256: String,
    pub index_sha256: String,
}

impl RowObjectReference {
    /// Build a content-addressed reference from one encoded segment.
    ///
    /// # Errors
    ///
    /// Returns an error when the index is invalid or lengths overflow.
    pub fn from_encoded(prefix: &str, encoded: &EncodedRowSegment) -> Result<Self, String> {
        if prefix.is_empty() {
            return Err("row object prefix must be non-empty".to_owned());
        }
        let index = RowSegmentIndex::decode(&encoded.index)?;
        let data_sha256 = content_sha256(&encoded.data);
        let index_sha256 = content_sha256(&encoded.index);
        let prefix = prefix.trim_end_matches('/');
        let reference = Self {
            data_key: format!("{prefix}/data/sha256/{data_sha256}"),
            index_key: format!("{prefix}/index/sha256/{index_sha256}"),
            first_key: index.first_key().to_vec(),
            last_key: index.last_key().to_vec(),
            generation: index.generation(),
            min_version: index.min_version(),
            max_version: index.max_version(),
            data_bytes: u64::try_from(encoded.data.len()).unwrap_or(u64::MAX),
            index_bytes: u64::try_from(encoded.index.len()).unwrap_or(u64::MAX),
            data_sha256,
            index_sha256,
        };
        reference.validate()?;
        Ok(reference)
    }

    /// Verify a warmed index against this manifest reference.
    ///
    /// # Errors
    ///
    /// Returns an error when identity, bounds, generation, versions, or lengths
    /// do not match.
    pub fn validate_index(
        &self,
        index_bytes: &[u8],
        index: &RowSegmentIndex,
    ) -> Result<(), String> {
        if content_sha256(index_bytes) != self.index_sha256
            || u64::try_from(index_bytes.len()).unwrap_or(u64::MAX) != self.index_bytes
            || index.data_sha256() != self.data_sha256
            || index.object_length() != self.data_bytes
            || index.first_key() != self.first_key
            || index.last_key() != self.last_key
            || index.generation() != self.generation
            || index.min_version() != self.min_version
            || index.max_version() != self.max_version
        {
            return Err("row object index does not match manifest reference".to_owned());
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), String> {
        if self.data_key.is_empty()
            || self.index_key.is_empty()
            || self.data_key == self.index_key
            || self.first_key.is_empty()
            || self.first_key > self.last_key
            || self.generation == 0
            || self.min_version == 0
            || self.max_version < self.min_version
            || self.data_bytes == 0
            || self.index_bytes == 0
            || !valid_digest(&self.data_sha256)
            || !valid_digest(&self.index_sha256)
            || !self.data_key.ends_with(&self.data_sha256)
            || !self.index_key.ends_with(&self.index_sha256)
        {
            return Err("invalid row object reference".to_owned());
        }
        Ok(())
    }
}

/// One complete sorted row-object closure for a range and covered version.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowObjectManifestV1 {
    pub schema_version: u16,
    pub generation: u64,
    pub covered_through: u64,
    pub segments: Vec<RowObjectReference>,
}

impl RowObjectManifestV1 {
    /// Create and validate one exact row-object closure.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, overlapping, unordered, duplicated, or
    /// generation-inconsistent references.
    pub fn new(
        generation: u64,
        covered_through: u64,
        segments: Vec<RowObjectReference>,
    ) -> Result<Self, String> {
        let manifest = Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            generation,
            covered_through,
            segments,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Encode a deterministic JSON payload in a checksummed `OKVM` envelope.
    ///
    /// # Errors
    ///
    /// Returns an error when validation, JSON encoding, or length conversion
    /// fails.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let payload = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let payload_length = u32::try_from(payload.len())
            .map_err(|error| format!("row manifest is too large: {error}"))?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&MANIFEST_FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&payload_length.to_be_bytes());
        bytes.extend_from_slice(&payload);
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }

    /// Decode and validate one checksummed manifest envelope.
    ///
    /// # Errors
    ///
    /// Returns an error for truncation, checksum, version, JSON, or closure
    /// violations.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        const HEADER_BYTES: usize = 4 + 2 + 4;
        if bytes.len() < HEADER_BYTES + DIGEST_BYTES {
            return Err("row manifest is truncated".to_owned());
        }
        let checksum_offset = bytes.len() - DIGEST_BYTES;
        if Sha256::digest(&bytes[..checksum_offset]).as_slice() != &bytes[checksum_offset..] {
            return Err("row manifest checksum mismatch".to_owned());
        }
        if &bytes[..4] != MANIFEST_MAGIC {
            return Err("row manifest magic mismatch".to_owned());
        }
        let format_version = u16::from_be_bytes(
            bytes[4..6]
                .try_into()
                .map_err(|_| "row manifest version is truncated".to_owned())?,
        );
        if format_version != MANIFEST_FORMAT_VERSION {
            return Err("unsupported row manifest format version".to_owned());
        }
        let payload_length = usize::try_from(u32::from_be_bytes(
            bytes[6..10]
                .try_into()
                .map_err(|_| "row manifest length is truncated".to_owned())?,
        ))
        .map_err(|error| format!("invalid row manifest length: {error}"))?;
        if HEADER_BYTES.saturating_add(payload_length) != checksum_offset {
            return Err("row manifest payload length mismatch".to_owned());
        }
        let manifest: Self = serde_json::from_slice(&bytes[HEADER_BYTES..checksum_offset])
            .map_err(|error| error.to_string())?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Select the only segment whose key bounds can contain `key`.
    #[must_use]
    pub fn locate(&self, key: &[u8]) -> Option<&RowObjectReference> {
        let mut lower = 0_usize;
        let mut upper = self.segments.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.segments[middle].first_key.as_slice() <= key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let candidate = lower
            .checked_sub(1)
            .and_then(|index| self.segments.get(index))?;
        (key <= candidate.last_key.as_slice()).then_some(candidate)
    }

    fn validate(&self) -> Result<(), String> {
        if self.schema_version != MANIFEST_SCHEMA_VERSION
            || self.generation == 0
            || self.covered_through == 0
            || self.segments.is_empty()
            || self.segments.len() > MAX_SEGMENTS
        {
            return Err("invalid row object manifest header".to_owned());
        }
        let mut object_keys = BTreeSet::new();
        for (position, segment) in self.segments.iter().enumerate() {
            segment.validate()?;
            if segment.generation != self.generation
                || segment.max_version > self.covered_through
                || !object_keys.insert(segment.data_key.as_str())
                || !object_keys.insert(segment.index_key.as_str())
            {
                return Err("invalid row object manifest segment".to_owned());
            }
            if let Some(previous) = position
                .checked_sub(1)
                .and_then(|index| self.segments.get(index))
            {
                if previous.last_key >= segment.first_key {
                    return Err("row object manifest key ranges overlap or regress".to_owned());
                }
            }
        }
        Ok(())
    }
}

/// Split one sorted record stream into bounded row objects without splitting
/// the versions of any key across objects.
///
/// # Errors
///
/// Returns an error for invalid targets, records, encoding, or arithmetic
/// overflow.
pub fn encode_row_object_set(
    generation: u64,
    records: &[RowRecord],
    target_object_bytes: usize,
    target_block_bytes: usize,
) -> Result<Vec<EncodedRowSegment>, String> {
    if target_object_bytes < target_block_bytes || records.is_empty() {
        return Err("invalid row object-set build parameters".to_owned());
    }
    let mut segments = Vec::new();
    let mut segment_start = 0_usize;
    let mut segment_data_bytes = 0_usize;
    let mut block_payload_bytes = 0_usize;
    let mut cursor = 0_usize;
    while cursor < records.len() {
        let group_start = cursor;
        let key = records[cursor].key.as_slice();
        let mut group_bytes = 0_usize;
        while cursor < records.len() && records[cursor].key.as_slice() == key {
            group_bytes = group_bytes
                .checked_add(estimated_record_bytes(&records[cursor])?)
                .ok_or_else(|| "row object length overflow".to_owned())?;
            cursor += 1;
        }
        let starts_new_block = block_payload_bytes == 0
            || block_payload_bytes.saturating_add(group_bytes) > target_block_bytes;
        let additional_bytes = group_bytes
            .checked_add(if starts_new_block {
                ROW_BLOCK_HEADER_BYTES
            } else {
                0
            })
            .ok_or_else(|| "row object length overflow".to_owned())?;
        if group_start > segment_start
            && segment_data_bytes.saturating_add(additional_bytes) > target_object_bytes
        {
            segments.push(crate::encode_row_segment(
                generation,
                &records[segment_start..group_start],
                target_block_bytes,
            )?);
            segment_start = group_start;
            segment_data_bytes = 0;
            block_payload_bytes = 0;
        }
        let starts_new_block = block_payload_bytes == 0
            || block_payload_bytes.saturating_add(group_bytes) > target_block_bytes;
        if starts_new_block {
            segment_data_bytes = segment_data_bytes
                .checked_add(ROW_BLOCK_HEADER_BYTES)
                .ok_or_else(|| "row object length overflow".to_owned())?;
            block_payload_bytes = 0;
        }
        segment_data_bytes = segment_data_bytes
            .checked_add(group_bytes)
            .ok_or_else(|| "row object length overflow".to_owned())?;
        block_payload_bytes = block_payload_bytes
            .checked_add(group_bytes)
            .ok_or_else(|| "row block length overflow".to_owned())?;
        if segment_data_bytes > target_object_bytes {
            return Err("one row-key version group exceeds the row-object target".to_owned());
        }
    }
    segments.push(crate::encode_row_segment(
        generation,
        &records[segment_start..],
        target_block_bytes,
    )?);
    Ok(segments)
}

/// Return the lowercase SHA-256 identity of exact object bytes.
#[must_use]
pub fn content_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn estimated_record_bytes(record: &RowRecord) -> Result<usize, String> {
    let value_bytes = record.value.as_ref().map_or(0, Vec::len);
    let _ = u32::try_from(record.key.len())
        .map_err(|error| format!("row key is too large: {error}"))?;
    let _ =
        u32::try_from(value_bytes).map_err(|error| format!("row value is too large: {error}"))?;
    4_usize
        .checked_add(record.key.len())
        .and_then(|bytes| bytes.checked_add(8 + 1 + 4))
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .ok_or_else(|| "row record length overflow".to_owned())
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::{content_sha256, encode_row_object_set, RowObjectManifestV1, RowObjectReference};
    use crate::{RowRecord, RowSegmentIndex};

    fn records(count: u64) -> Vec<RowRecord> {
        (0..count)
            .map(|key| RowRecord::value(key.to_be_bytes(), 9, vec![0x5a; 256]))
            .collect()
    }

    #[test]
    fn object_set_manifest_round_trips_and_locates() {
        let encoded =
            encode_row_object_set(7, &records(100), 8_192, 4_096).expect("encode row object set");
        assert!(encoded.len() > 1);
        assert!(encoded.iter().all(|segment| segment.data.len() <= 8_192));
        let references = encoded
            .iter()
            .map(|segment| RowObjectReference::from_encoded("rows", segment).expect("reference"))
            .collect::<Vec<_>>();
        let manifest = RowObjectManifestV1::new(7, 9, references).expect("manifest");
        let bytes = manifest.encode().expect("encode manifest");
        let decoded = RowObjectManifestV1::decode(&bytes).expect("decode manifest");
        assert_eq!(manifest, decoded);
        assert_eq!(
            decoded
                .locate(&50_u64.to_be_bytes())
                .map(|segment| segment.generation),
            Some(7)
        );
        assert_eq!(content_sha256(&bytes).len(), 64);
    }

    #[test]
    fn manifest_rejects_corruption_overlap_and_index_mismatch() {
        let encoded =
            encode_row_object_set(7, &records(100), 8_192, 4_096).expect("encode row object set");
        let mut references = encoded
            .iter()
            .map(|segment| RowObjectReference::from_encoded("rows", segment).expect("reference"))
            .collect::<Vec<_>>();
        references[1].first_key = references[0].last_key.clone();
        assert!(RowObjectManifestV1::new(7, 9, references).is_err());

        let reference = RowObjectReference::from_encoded("rows", &encoded[0]).expect("reference");
        let mut corrupt = encoded[0].index.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x80;
        assert!(RowSegmentIndex::decode(&corrupt).is_err());
        let index = RowSegmentIndex::decode(&encoded[1].index).expect("other index");
        assert!(reference.validate_index(&encoded[1].index, &index).is_err());

        let manifest = RowObjectManifestV1::new(
            7,
            9,
            encoded
                .iter()
                .map(|segment| {
                    RowObjectReference::from_encoded("rows", segment).expect("reference")
                })
                .collect(),
        )
        .expect("manifest");
        let mut manifest_bytes = manifest.encode().expect("encode manifest");
        manifest_bytes[0] ^= 0x80;
        assert_eq!(
            RowObjectManifestV1::decode(&manifest_bytes),
            Err("row manifest checksum mismatch".to_owned())
        );
    }
}
