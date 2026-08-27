//! Experimental application-history records above objectKV primitives.
//!
//! This crate does not own recovery-log durability, consensus, object
//! publication, or a public reducer ABI. It provides the shared playground
//! record identities used to test those layers without duplicating them in
//! every application.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt::{self, Display};

const CHECKPOINT_MAGIC: &[u8; 4] = b"OKCP";
const CHECKPOINT_FORMAT_VERSION: u8 = 1;
const APPLICATION_RECORD_MAGIC: &[u8; 4] = b"OKAR";
const APPLICATION_RECORD_FORMAT_VERSION: u8 = 1;
const HISTORY_SEGMENT_MAGIC: &[u8; 4] = b"OKHS";
const HISTORY_SEGMENT_FORMAT_VERSION: u8 = 1;
const CHECKSUM_BYTES: usize = 16;
const CHECKPOINT_HEADER_BYTES: usize = 4 + 1 + 1 + 1 + 8 + 2;
const APPLICATION_RECORD_HEADER_BYTES: usize = 4 + 1 + 1 + 1 + 8 + 4;

/// Bytes added around one encoded state in the playground checkpoint format.
pub const CHECKPOINT_OVERHEAD_BYTES: usize = CHECKPOINT_HEADER_BYTES + CHECKSUM_BYTES;

/// Bytes added around one application delta before WAL framing or replication.
pub const APPLICATION_RECORD_OVERHEAD_BYTES: usize =
    APPLICATION_RECORD_HEADER_BYTES + CHECKSUM_BYTES;

/// Stable reducer registry tag within the unpublished playground format.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum ReducerId {
    Tetris = 1,
    Chess = 2,
}

/// State recovered from one identity-bound checkpoint record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedCheckpoint {
    pub position: u64,
    pub state: Vec<u8>,
}

/// Application delta recovered from one identity-bound history record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedApplicationRecord {
    pub position: u64,
    pub payload: Vec<u8>,
}

/// Exact immutable object reference stored by one game-history manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HistoryObjectRef {
    pub key: String,
    pub length: u64,
    pub sha256: String,
}

/// One contiguous application-history segment reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HistorySegmentRef {
    pub object: HistoryObjectRef,
    pub first_position: u64,
    pub last_position: u64,
}

/// Versioned root manifest for one application-history line.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GameHistoryManifestV1 {
    pub format_version: u8,
    pub reducer: ReducerId,
    pub reducer_schema: u8,
    pub checkpoint: HistoryObjectRef,
    pub checkpoint_position: u64,
    pub segments: Vec<HistorySegmentRef>,
    pub covered_through: u64,
    pub parent_manifest: Option<HistoryObjectRef>,
    pub fork_position: Option<u64>,
    pub expected_fingerprint: String,
}

impl GameHistoryManifestV1 {
    /// Validate identity, reachability, and contiguous-position invariants.
    ///
    /// # Errors
    ///
    /// Returns a stable error string when the manifest is malformed or its
    /// segment positions contain a gap, overlap, or false coverage claim.
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != 1
            || self.reducer_schema == 0
            || !valid_history_ref(&self.checkpoint)
            || self.expected_fingerprint.len() != 16
        {
            return Err("invalid game-history manifest identity".to_owned());
        }
        if self.parent_manifest.is_some() != self.fork_position.is_some() {
            return Err("parent manifest and fork position must appear together".to_owned());
        }
        if self
            .parent_manifest
            .as_ref()
            .is_some_and(|reference| !valid_history_ref(reference))
        {
            return Err("invalid parent manifest reference".to_owned());
        }
        let base_position = self.fork_position.unwrap_or(self.checkpoint_position);
        if self
            .fork_position
            .is_some_and(|position| position < self.checkpoint_position)
        {
            return Err("fork position precedes checkpoint".to_owned());
        }
        let mut expected_first = base_position.saturating_add(1);
        for segment in &self.segments {
            if !valid_history_ref(&segment.object)
                || segment.first_position != expected_first
                || segment.last_position < segment.first_position
            {
                return Err("history segments are not exact and contiguous".to_owned());
            }
            expected_first = segment.last_position.saturating_add(1);
        }
        let actual_coverage = self
            .segments
            .last()
            .map_or(base_position, |segment| segment.last_position);
        if actual_coverage != self.covered_through {
            return Err("manifest coverage does not match its segment tail".to_owned());
        }
        Ok(())
    }

    /// Serialize one validated manifest to deterministic JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid manifest or JSON serialization failure.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| error.to_string())
    }

    /// Decode and validate one manifest.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed JSON or violated manifest invariants.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        manifest.validate()?;
        Ok(manifest)
    }
}

fn valid_history_ref(reference: &HistoryObjectRef) -> bool {
    !reference.key.is_empty()
        && reference.length != 0
        && reference.sha256.len() == 64
        && reference
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Checkpoint format or identity failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointError {
    StateTooLarge(usize),
    Truncated,
    UnsupportedFormat(u8),
    ReducerMismatch,
    SchemaMismatch,
    PositionMismatch,
    LengthMismatch,
    ChecksumMismatch,
}

/// Application-record format or identity failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationRecordError {
    PayloadTooLarge(usize),
    Truncated,
    UnsupportedFormat(u8),
    ReducerMismatch,
    SchemaMismatch,
    PositionMismatch,
    LengthMismatch,
    ChecksumMismatch,
}

impl Display for CheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StateTooLarge(length) => {
                write!(formatter, "checkpoint state is too large: {length}")
            }
            Self::Truncated => formatter.write_str("checkpoint record is truncated"),
            Self::UnsupportedFormat(version) => {
                write!(formatter, "unsupported checkpoint format {version}")
            }
            Self::ReducerMismatch => formatter.write_str("checkpoint reducer identity mismatch"),
            Self::SchemaMismatch => formatter.write_str("checkpoint reducer schema mismatch"),
            Self::PositionMismatch => formatter.write_str("checkpoint position mismatch"),
            Self::LengthMismatch => formatter.write_str("checkpoint state length mismatch"),
            Self::ChecksumMismatch => formatter.write_str("checkpoint checksum mismatch"),
        }
    }
}

impl Error for CheckpointError {}

impl Display for ApplicationRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge(length) => {
                write!(
                    formatter,
                    "application record payload is too large: {length}"
                )
            }
            Self::Truncated => formatter.write_str("application record is truncated"),
            Self::UnsupportedFormat(version) => {
                write!(formatter, "unsupported application record format {version}")
            }
            Self::ReducerMismatch => {
                formatter.write_str("application record reducer identity mismatch")
            }
            Self::SchemaMismatch => {
                formatter.write_str("application record reducer schema mismatch")
            }
            Self::PositionMismatch => formatter.write_str("application record position mismatch"),
            Self::LengthMismatch => {
                formatter.write_str("application record payload length mismatch")
            }
            Self::ChecksumMismatch => formatter.write_str("application record checksum mismatch"),
        }
    }
}

impl Error for ApplicationRecordError {}

/// Encode one contiguous immutable segment of reducer payloads.
///
/// # Errors
///
/// Returns an error for an empty segment, zero first position, count overflow,
/// or an application record that exceeds the bounded format.
pub fn encode_history_segment(
    reducer: ReducerId,
    reducer_schema: u8,
    first_position: u64,
    payloads: &[Vec<u8>],
) -> Result<Vec<u8>, ApplicationRecordError> {
    if first_position == 0 || payloads.is_empty() {
        return Err(ApplicationRecordError::PositionMismatch);
    }
    let count = u32::try_from(payloads.len())
        .map_err(|_| ApplicationRecordError::PayloadTooLarge(payloads.len()))?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(HISTORY_SEGMENT_MAGIC);
    encoded.push(HISTORY_SEGMENT_FORMAT_VERSION);
    encoded.push(reducer as u8);
    encoded.push(reducer_schema);
    encoded.extend_from_slice(&first_position.to_be_bytes());
    encoded.extend_from_slice(&count.to_be_bytes());
    for (offset, payload) in payloads.iter().enumerate() {
        let position = first_position.saturating_add(offset as u64);
        let record = encode_application_record(reducer, reducer_schema, position, payload)?;
        let length = u32::try_from(record.len())
            .map_err(|_| ApplicationRecordError::PayloadTooLarge(record.len()))?;
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(&record);
    }
    let checksum = Sha256::digest(&encoded);
    encoded.extend_from_slice(&checksum[..CHECKSUM_BYTES]);
    Ok(encoded)
}

/// Decode and validate one immutable contiguous application-history segment.
///
/// # Errors
///
/// Returns an error for corrupt bytes, false identity, gaps, or trailing data.
pub fn decode_history_segment(
    expected_reducer: ReducerId,
    expected_schema: u8,
    expected_first_position: u64,
    encoded: &[u8],
) -> Result<Vec<DecodedApplicationRecord>, ApplicationRecordError> {
    const SEGMENT_HEADER_BYTES: usize = 4 + 1 + 1 + 1 + 8 + 4;
    if encoded.len() < SEGMENT_HEADER_BYTES + CHECKSUM_BYTES {
        return Err(ApplicationRecordError::Truncated);
    }
    if &encoded[..4] != HISTORY_SEGMENT_MAGIC || encoded[4] != HISTORY_SEGMENT_FORMAT_VERSION {
        return Err(ApplicationRecordError::UnsupportedFormat(encoded[4]));
    }
    if encoded[5] != expected_reducer as u8 {
        return Err(ApplicationRecordError::ReducerMismatch);
    }
    if encoded[6] != expected_schema {
        return Err(ApplicationRecordError::SchemaMismatch);
    }
    let mut first = [0_u8; 8];
    first.copy_from_slice(&encoded[7..15]);
    if u64::from_be_bytes(first) != expected_first_position {
        return Err(ApplicationRecordError::PositionMismatch);
    }
    let checksum_offset = encoded.len() - CHECKSUM_BYTES;
    let expected_checksum = Sha256::digest(&encoded[..checksum_offset]);
    if encoded[checksum_offset..] != expected_checksum[..CHECKSUM_BYTES] {
        return Err(ApplicationRecordError::ChecksumMismatch);
    }
    let count = u32::from_be_bytes([encoded[15], encoded[16], encoded[17], encoded[18]]) as usize;
    let mut cursor = SEGMENT_HEADER_BYTES;
    let mut records = Vec::with_capacity(count);
    for offset in 0..count {
        let length_end = cursor
            .checked_add(4)
            .ok_or(ApplicationRecordError::LengthMismatch)?;
        let length_bytes = encoded
            .get(cursor..length_end)
            .ok_or(ApplicationRecordError::Truncated)?;
        let length = u32::from_be_bytes([
            length_bytes[0],
            length_bytes[1],
            length_bytes[2],
            length_bytes[3],
        ]) as usize;
        cursor = length_end;
        let record_end = cursor
            .checked_add(length)
            .ok_or(ApplicationRecordError::LengthMismatch)?;
        if record_end > checksum_offset {
            return Err(ApplicationRecordError::Truncated);
        }
        let position = expected_first_position.saturating_add(offset as u64);
        records.push(decode_application_record(
            expected_reducer,
            expected_schema,
            position,
            &encoded[cursor..record_end],
        )?);
        cursor = record_end;
    }
    if cursor != checksum_offset {
        return Err(ApplicationRecordError::LengthMismatch);
    }
    Ok(records)
}

/// Encode one application delta at an exact ordered history position.
///
/// # Errors
///
/// Returns an error when the payload exceeds the format's `u32` bound.
pub fn encode_application_record(
    reducer: ReducerId,
    reducer_schema: u8,
    position: u64,
    payload: &[u8],
) -> Result<Vec<u8>, ApplicationRecordError> {
    let payload_length = u32::try_from(payload.len())
        .map_err(|_| ApplicationRecordError::PayloadTooLarge(payload.len()))?;
    let mut encoded = Vec::with_capacity(APPLICATION_RECORD_OVERHEAD_BYTES + payload.len());
    encoded.extend_from_slice(APPLICATION_RECORD_MAGIC);
    encoded.push(APPLICATION_RECORD_FORMAT_VERSION);
    encoded.push(reducer as u8);
    encoded.push(reducer_schema);
    encoded.extend_from_slice(&position.to_be_bytes());
    encoded.extend_from_slice(&payload_length.to_be_bytes());
    encoded.extend_from_slice(payload);
    let checksum = Sha256::digest(&encoded);
    encoded.extend_from_slice(&checksum[..CHECKSUM_BYTES]);
    Ok(encoded)
}

/// Decode and validate an application delta against its expected identity and
/// ordered history position.
///
/// # Errors
///
/// Returns a precise error for malformed, corrupt, or misbound records.
pub fn decode_application_record(
    expected_reducer: ReducerId,
    expected_schema: u8,
    expected_position: u64,
    encoded: &[u8],
) -> Result<DecodedApplicationRecord, ApplicationRecordError> {
    if encoded.len() < APPLICATION_RECORD_OVERHEAD_BYTES {
        return Err(ApplicationRecordError::Truncated);
    }
    if &encoded[..4] != APPLICATION_RECORD_MAGIC {
        return Err(ApplicationRecordError::UnsupportedFormat(0));
    }
    if encoded[4] != APPLICATION_RECORD_FORMAT_VERSION {
        return Err(ApplicationRecordError::UnsupportedFormat(encoded[4]));
    }
    if encoded[5] != expected_reducer as u8 {
        return Err(ApplicationRecordError::ReducerMismatch);
    }
    if encoded[6] != expected_schema {
        return Err(ApplicationRecordError::SchemaMismatch);
    }
    let mut position = [0_u8; 8];
    position.copy_from_slice(&encoded[7..15]);
    let position = u64::from_be_bytes(position);
    if position != expected_position {
        return Err(ApplicationRecordError::PositionMismatch);
    }
    let payload_length = usize::try_from(u32::from_be_bytes([
        encoded[15],
        encoded[16],
        encoded[17],
        encoded[18],
    ]))
    .map_err(|_| ApplicationRecordError::LengthMismatch)?;
    let expected_length = APPLICATION_RECORD_HEADER_BYTES
        .checked_add(payload_length)
        .and_then(|length| length.checked_add(CHECKSUM_BYTES))
        .ok_or(ApplicationRecordError::LengthMismatch)?;
    if encoded.len() != expected_length {
        return Err(ApplicationRecordError::LengthMismatch);
    }
    let checksum_offset = expected_length - CHECKSUM_BYTES;
    let expected_checksum = Sha256::digest(&encoded[..checksum_offset]);
    if encoded[checksum_offset..] != expected_checksum[..CHECKSUM_BYTES] {
        return Err(ApplicationRecordError::ChecksumMismatch);
    }
    Ok(DecodedApplicationRecord {
        position,
        payload: encoded[APPLICATION_RECORD_HEADER_BYTES..checksum_offset].to_vec(),
    })
}

/// Encode one reducer state at an exact application-log position.
///
/// # Errors
///
/// Returns an error when the playground state exceeds the format's `u16`
/// bound.
pub fn encode_checkpoint(
    reducer: ReducerId,
    reducer_schema: u8,
    position: u64,
    state: &[u8],
) -> Result<Vec<u8>, CheckpointError> {
    let state_length =
        u16::try_from(state.len()).map_err(|_| CheckpointError::StateTooLarge(state.len()))?;
    let mut encoded = Vec::with_capacity(CHECKPOINT_OVERHEAD_BYTES + state.len());
    encoded.extend_from_slice(CHECKPOINT_MAGIC);
    encoded.push(CHECKPOINT_FORMAT_VERSION);
    encoded.push(reducer as u8);
    encoded.push(reducer_schema);
    encoded.extend_from_slice(&position.to_be_bytes());
    encoded.extend_from_slice(&state_length.to_be_bytes());
    encoded.extend_from_slice(state);
    let checksum = Sha256::digest(&encoded);
    encoded.extend_from_slice(&checksum[..CHECKSUM_BYTES]);
    Ok(encoded)
}

/// Decode and validate a checkpoint against the expected reducer identity,
/// schema, and application-log position.
///
/// # Errors
///
/// Returns a precise error for malformed, corrupt, or misbound records.
pub fn decode_checkpoint(
    expected_reducer: ReducerId,
    expected_schema: u8,
    expected_position: u64,
    encoded: &[u8],
) -> Result<DecodedCheckpoint, CheckpointError> {
    if encoded.len() < CHECKPOINT_OVERHEAD_BYTES {
        return Err(CheckpointError::Truncated);
    }
    if &encoded[..4] != CHECKPOINT_MAGIC {
        return Err(CheckpointError::UnsupportedFormat(0));
    }
    if encoded[4] != CHECKPOINT_FORMAT_VERSION {
        return Err(CheckpointError::UnsupportedFormat(encoded[4]));
    }
    if encoded[5] != expected_reducer as u8 {
        return Err(CheckpointError::ReducerMismatch);
    }
    if encoded[6] != expected_schema {
        return Err(CheckpointError::SchemaMismatch);
    }
    let mut position = [0_u8; 8];
    position.copy_from_slice(&encoded[7..15]);
    let position = u64::from_be_bytes(position);
    if position != expected_position {
        return Err(CheckpointError::PositionMismatch);
    }
    let state_length = usize::from(u16::from_be_bytes([encoded[15], encoded[16]]));
    let expected_length = CHECKPOINT_HEADER_BYTES
        .checked_add(state_length)
        .and_then(|length| length.checked_add(CHECKSUM_BYTES))
        .ok_or(CheckpointError::LengthMismatch)?;
    if encoded.len() != expected_length {
        return Err(CheckpointError::LengthMismatch);
    }
    let checksum_offset = expected_length - CHECKSUM_BYTES;
    let expected_checksum = Sha256::digest(&encoded[..checksum_offset]);
    if encoded[checksum_offset..] != expected_checksum[..CHECKSUM_BYTES] {
        return Err(CheckpointError::ChecksumMismatch);
    }
    Ok(DecodedCheckpoint {
        position,
        state: encoded[CHECKPOINT_HEADER_BYTES..checksum_offset].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_round_trips_with_exact_identity() {
        let encoded = encode_checkpoint(ReducerId::Chess, 3, 41, b"state").expect("encode");
        let decoded = decode_checkpoint(ReducerId::Chess, 3, 41, &encoded).expect("decode");
        assert_eq!(decoded.position, 41);
        assert_eq!(decoded.state, b"state");
        assert_eq!(encoded.len(), CHECKPOINT_OVERHEAD_BYTES + 5);
    }

    #[test]
    fn checkpoint_rejects_corruption_and_false_identity() {
        let encoded = encode_checkpoint(ReducerId::Tetris, 1, 9, b"state").expect("encode");
        assert_eq!(
            decode_checkpoint(ReducerId::Chess, 1, 9, &encoded),
            Err(CheckpointError::ReducerMismatch)
        );
        assert_eq!(
            decode_checkpoint(ReducerId::Tetris, 2, 9, &encoded),
            Err(CheckpointError::SchemaMismatch)
        );
        assert_eq!(
            decode_checkpoint(ReducerId::Tetris, 1, 10, &encoded),
            Err(CheckpointError::PositionMismatch)
        );
        let mut corrupt = encoded;
        corrupt[CHECKPOINT_HEADER_BYTES] ^= 0x01;
        assert_eq!(
            decode_checkpoint(ReducerId::Tetris, 1, 9, &corrupt),
            Err(CheckpointError::ChecksumMismatch)
        );
    }

    #[test]
    fn application_record_round_trips_with_exact_identity() {
        let encoded =
            encode_application_record(ReducerId::Tetris, 2, 41, b"delta").expect("encode");
        let decoded =
            decode_application_record(ReducerId::Tetris, 2, 41, &encoded).expect("decode");
        assert_eq!(decoded.position, 41);
        assert_eq!(decoded.payload, b"delta");
        assert_eq!(encoded.len(), APPLICATION_RECORD_OVERHEAD_BYTES + 5);
    }

    #[test]
    fn application_record_rejects_corruption_and_false_identity() {
        let encoded = encode_application_record(ReducerId::Chess, 1, 9, b"move").expect("encode");
        assert_eq!(
            decode_application_record(ReducerId::Tetris, 1, 9, &encoded),
            Err(ApplicationRecordError::ReducerMismatch)
        );
        assert_eq!(
            decode_application_record(ReducerId::Chess, 2, 9, &encoded),
            Err(ApplicationRecordError::SchemaMismatch)
        );
        assert_eq!(
            decode_application_record(ReducerId::Chess, 1, 10, &encoded),
            Err(ApplicationRecordError::PositionMismatch)
        );
        let mut corrupt = encoded;
        corrupt[APPLICATION_RECORD_HEADER_BYTES] ^= 0x01;
        assert_eq!(
            decode_application_record(ReducerId::Chess, 1, 9, &corrupt),
            Err(ApplicationRecordError::ChecksumMismatch)
        );
    }

    #[test]
    fn history_segment_round_trips_and_rejects_corruption() {
        let payloads = vec![b"a".to_vec(), b"b".to_vec()];
        let encoded = encode_history_segment(ReducerId::Tetris, 1, 7, &payloads).expect("encode");
        let decoded = decode_history_segment(ReducerId::Tetris, 1, 7, &encoded).expect("decode");
        assert_eq!(decoded[0].position, 7);
        assert_eq!(decoded[1].payload, b"b");
        let mut corrupt = encoded;
        corrupt[20] ^= 1;
        assert_eq!(
            decode_history_segment(ReducerId::Tetris, 1, 7, &corrupt),
            Err(ApplicationRecordError::ChecksumMismatch)
        );
    }

    #[test]
    fn manifest_rejects_segment_gaps_and_false_coverage() {
        let object = |key: &str| HistoryObjectRef {
            key: key.to_owned(),
            length: 1,
            sha256: "a".repeat(64),
        };
        let mut manifest = GameHistoryManifestV1 {
            format_version: 1,
            reducer: ReducerId::Chess,
            reducer_schema: 1,
            checkpoint: object("checkpoint"),
            checkpoint_position: 0,
            segments: vec![HistorySegmentRef {
                object: object("segment"),
                first_position: 2,
                last_position: 3,
            }],
            covered_through: 3,
            parent_manifest: None,
            fork_position: None,
            expected_fingerprint: "0".repeat(16),
        };
        assert!(manifest.validate().is_err());
        manifest.segments[0].first_position = 1;
        manifest.covered_through = 4;
        assert!(manifest.validate().is_err());
    }
}
