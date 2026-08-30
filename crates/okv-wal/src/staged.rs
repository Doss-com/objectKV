//! Stable node journal and immutable segment codec for a staged transaction log.
//!
//! This module owns only one log node's durable bytes and deterministic segment
//! construction. Quorum collection, network transport, writer assignment, and
//! transaction commit remain above it.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const JOURNAL_MAGIC: &[u8; 4] = b"OKVT";
const JOURNAL_VERSION: u16 = 1;
const JOURNAL_HEADER_BYTES: usize = 4 + 2 + 1 + 1 + 4;
const JOURNAL_CHECKSUM_BYTES: usize = 32;
const JOURNAL_KIND_EPOCH: u8 = 1;
const JOURNAL_KIND_APPEND: u8 = 2;
const JOURNAL_FLAGS: u8 = 0;

const SEGMENT_MAGIC: &[u8; 4] = b"OKVL";
const SEGMENT_VERSION: u16 = 1;
const SEGMENT_FLAGS: u16 = 0;
const SEGMENT_HEADER_BYTES: usize = 4 + 2 + 2 + 32 + 8 + 8 + 8 + 4 + 8;
const SEGMENT_CHECKSUM_BYTES: usize = 32;

const LOG_IDENTITY_BYTES: usize = 32;
const REQUEST_IDENTITY_BYTES: usize = 32;
const MAX_RECORD_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_PAYLOAD_BYTES: usize =
    MAX_RECORD_BODY_BYTES - LOG_IDENTITY_BYTES - 8 - 8 - REQUEST_IDENTITY_BYTES - 4;
const JOURNAL_FILE_NAME: &str = "txlog.journal";

/// Digest-sized identity for one staged log stream.
pub type StagedLogIdentity = [u8; LOG_IDENTITY_BYTES];

/// Stable identity for one append request and its retry outcome.
pub type StagedRequestIdentity = [u8; REQUEST_IDENTITY_BYTES];

/// One durable staged-log record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedLogRecord {
    pub writer_epoch: u64,
    pub position: u64,
    pub request_identity: StagedRequestIdentity,
    pub payload: Vec<u8>,
}

/// Result returned only after the node journal has synchronized the append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedAppendOutcome {
    pub position: u64,
    pub frame_bytes: u64,
    pub physical_bytes: u64,
    pub replayed: bool,
    pub synchronized: bool,
}

/// Result of installing a writer epoch on one node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedEpochOutcome {
    pub writer_epoch: u64,
    pub physical_bytes: u64,
    pub replayed: bool,
    pub synchronized: bool,
}

/// Decoded immutable staged-log segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedLogSegment {
    pub log_identity: StagedLogIdentity,
    pub first_position: u64,
    pub last_position: u64,
    pub committed_through: u64,
    pub records: Vec<StagedLogRecord>,
}

/// Staged node-journal or segment-codec failure.
#[derive(Debug)]
pub enum StagedLogError {
    Io(io::Error),
    InvalidEpoch(u64),
    StaleWriter { current: u64, proposed: u64 },
    WriterNotOpen,
    WriterEpochMismatch { current: u64, proposed: u64 },
    InvalidPosition(u64),
    NonConsecutive { expected: u64, actual: u64 },
    ConflictingRetry(u64),
    PayloadTooLarge(usize),
    CorruptFrame { offset: u64 },
    LogIdentityMismatch,
    MissingRecord(u64),
    InvalidSegment(&'static str),
}

impl Display for StagedLogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::InvalidEpoch(epoch) => write!(formatter, "invalid writer epoch {epoch}"),
            Self::StaleWriter { current, proposed } => write!(
                formatter,
                "stale writer epoch {proposed}; current epoch is {current}"
            ),
            Self::WriterNotOpen => write!(formatter, "no writer epoch is installed"),
            Self::WriterEpochMismatch { current, proposed } => write!(
                formatter,
                "writer epoch {proposed} does not match current epoch {current}"
            ),
            Self::InvalidPosition(position) => {
                write!(formatter, "invalid staged-log position {position}")
            }
            Self::NonConsecutive { expected, actual } => write!(
                formatter,
                "non-consecutive staged-log append: expected {expected}, received {actual}"
            ),
            Self::ConflictingRetry(position) => {
                write!(formatter, "conflicting retry at position {position}")
            }
            Self::PayloadTooLarge(length) => {
                write!(formatter, "staged-log payload too large: {length}")
            }
            Self::CorruptFrame { offset } => {
                write!(formatter, "corrupt staged-log frame at byte {offset}")
            }
            Self::LogIdentityMismatch => write!(formatter, "staged-log identity mismatch"),
            Self::MissingRecord(position) => {
                write!(
                    formatter,
                    "missing staged-log record at position {position}"
                )
            }
            Self::InvalidSegment(reason) => {
                write!(formatter, "invalid staged-log segment: {reason}")
            }
        }
    }
}

impl Error for StagedLogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for StagedLogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StagedNodeState {
    writer_epoch: Option<u64>,
    records: BTreeMap<u64, StagedLogRecord>,
}

impl StagedNodeState {
    fn new() -> Self {
        Self {
            writer_epoch: None,
            records: BTreeMap::new(),
        }
    }

    fn next_position(&self) -> u64 {
        self.records
            .last_key_value()
            .map_or(1, |(position, _)| position.saturating_add(1))
    }
}

/// One staged log node backed by a synchronized append-only journal.
#[derive(Debug)]
pub struct StagedLogNode {
    path: PathBuf,
    log_identity: StagedLogIdentity,
    state: StagedNodeState,
    recovered_torn_tail: bool,
}

impl StagedLogNode {
    /// Create or recover one staged log node.
    ///
    /// An incomplete final frame is truncated and synchronized before the node
    /// accepts another request. Complete corruption fails closed.
    ///
    /// # Errors
    ///
    /// Returns an error for filesystem, checksum, identity, or state-machine
    /// recovery failure.
    pub fn open(
        root: impl AsRef<Path>,
        log_identity: StagedLogIdentity,
    ) -> Result<Self, StagedLogError> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        sync_directory(root)?;
        let path = root.join(JOURNAL_FILE_NAME);
        if !path.exists() {
            File::create(&path)?.sync_all()?;
            sync_directory(root)?;
        }

        let mut bytes = Vec::new();
        File::open(&path)?.read_to_end(&mut bytes)?;
        let (state, valid_bytes, recovered_torn_tail) = replay_journal(&bytes, log_identity)?;
        if recovered_torn_tail {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(to_u64(valid_bytes))?;
            file.sync_all()?;
        }
        Ok(Self {
            path,
            log_identity,
            state,
            recovered_torn_tail,
        })
    }

    /// Exact journal path for bounded crash and torn-write probes.
    #[must_use]
    pub fn journal_path(&self) -> &Path {
        &self.path
    }

    /// Current installed writer epoch.
    #[must_use]
    pub const fn writer_epoch(&self) -> Option<u64> {
        self.state.writer_epoch
    }

    /// Next append position after the recovered consecutive prefix.
    #[must_use]
    pub fn next_position(&self) -> u64 {
        self.state.next_position()
    }

    /// Whether open removed an incomplete final frame.
    #[must_use]
    pub const fn recovered_torn_tail(&self) -> bool {
        self.recovered_torn_tail
    }

    /// Current synchronized journal length.
    ///
    /// # Errors
    ///
    /// Returns an error when file metadata cannot be read.
    pub fn physical_bytes(&self) -> Result<u64, StagedLogError> {
        Ok(fs::metadata(&self.path)?.len())
    }

    /// Install a strictly newer writer epoch, or replay the current epoch.
    ///
    /// # Errors
    ///
    /// Rejects zero and stale epochs before physical mutation.
    pub fn install_writer_epoch(
        &mut self,
        writer_epoch: u64,
    ) -> Result<StagedEpochOutcome, StagedLogError> {
        if writer_epoch == 0 {
            return Err(StagedLogError::InvalidEpoch(writer_epoch));
        }
        if let Some(current) = self.state.writer_epoch {
            if writer_epoch < current {
                return Err(StagedLogError::StaleWriter {
                    current,
                    proposed: writer_epoch,
                });
            }
            if writer_epoch == current {
                return Ok(StagedEpochOutcome {
                    writer_epoch,
                    physical_bytes: self.physical_bytes()?,
                    replayed: true,
                    synchronized: true,
                });
            }
        }

        let frame = encode_epoch_frame(self.log_identity, writer_epoch);
        self.persist_frame(&frame)?;
        self.state.writer_epoch = Some(writer_epoch);
        Ok(StagedEpochOutcome {
            writer_epoch,
            physical_bytes: self.physical_bytes()?,
            replayed: false,
            synchronized: true,
        })
    }

    /// Append one consecutive record and synchronize its journal frame.
    ///
    /// An exact retry returns the retained outcome without writing another
    /// frame. A conflicting retry, stale epoch, or gap fails before mutation.
    ///
    /// # Errors
    ///
    /// Returns a semantic, encoding, or filesystem error.
    pub fn append(
        &mut self,
        writer_epoch: u64,
        position: u64,
        request_identity: StagedRequestIdentity,
        payload: &[u8],
    ) -> Result<StagedAppendOutcome, StagedLogError> {
        let record = StagedLogRecord {
            writer_epoch,
            position,
            request_identity,
            payload: payload.to_vec(),
        };
        self.append_batch(std::slice::from_ref(&record))?
            .into_iter()
            .next()
            .ok_or(StagedLogError::InvalidSegment(
                "single append produced no outcome",
            ))
    }

    /// Validate and append one consecutive record batch with one journal sync.
    ///
    /// Exact retries may be mixed with new consecutive records. The complete
    /// batch is validated before the journal is changed. New frames are written
    /// together and become visible in memory only after the shared sync
    /// succeeds.
    ///
    /// # Errors
    ///
    /// Returns a semantic, encoding, or filesystem error without advancing the
    /// in-memory state.
    pub fn append_batch(
        &mut self,
        records: &[StagedLogRecord],
    ) -> Result<Vec<StagedAppendOutcome>, StagedLogError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let current = self
            .state
            .writer_epoch
            .ok_or(StagedLogError::WriterNotOpen)?;
        let mut expected = self.state.next_position();
        let mut planned_records = BTreeMap::<u64, StagedLogRecord>::new();
        let mut planned_frames = Vec::<Vec<u8>>::new();
        let mut outcome_plan = Vec::<(u64, u64, bool)>::with_capacity(records.len());

        for record in records {
            if record.position == 0 {
                return Err(StagedLogError::InvalidPosition(record.position));
            }
            if record.payload.len() > MAX_PAYLOAD_BYTES
                || u32::try_from(record.payload.len()).is_err()
            {
                return Err(StagedLogError::PayloadTooLarge(record.payload.len()));
            }
            if record.writer_epoch != current {
                return Err(if record.writer_epoch < current {
                    StagedLogError::StaleWriter {
                        current,
                        proposed: record.writer_epoch,
                    }
                } else {
                    StagedLogError::WriterEpochMismatch {
                        current,
                        proposed: record.writer_epoch,
                    }
                });
            }

            if let Some(existing) = self
                .state
                .records
                .get(&record.position)
                .or_else(|| planned_records.get(&record.position))
            {
                if existing.request_identity == record.request_identity
                    && existing.payload == record.payload
                    && existing.writer_epoch == record.writer_epoch
                {
                    outcome_plan.push((record.position, 0, true));
                    continue;
                }
                return Err(StagedLogError::ConflictingRetry(record.position));
            }

            if record.position != expected {
                return Err(StagedLogError::NonConsecutive {
                    expected,
                    actual: record.position,
                });
            }
            let frame = encode_append_frame(self.log_identity, record)?;
            outcome_plan.push((record.position, to_u64(frame.len()), false));
            planned_frames.push(frame);
            planned_records.insert(record.position, record.clone());
            expected = expected.saturating_add(1);
        }

        if !planned_frames.is_empty() {
            let mut file = OpenOptions::new().append(true).open(&self.path)?;
            for frame in &planned_frames {
                file.write_all(frame)?;
            }
            file.sync_all()?;
            self.state.records.extend(planned_records);
            self.recovered_torn_tail = false;
        }
        let physical_bytes = self.physical_bytes()?;
        Ok(outcome_plan
            .into_iter()
            .map(|(position, frame_bytes, replayed)| StagedAppendOutcome {
                position,
                frame_bytes,
                physical_bytes,
                replayed,
                synchronized: true,
            })
            .collect())
    }

    /// Return one recovered record.
    #[must_use]
    pub fn read(&self, position: u64) -> Option<&StagedLogRecord> {
        self.state.records.get(&position)
    }

    /// Return the complete recovered consecutive prefix.
    #[must_use]
    pub fn records(&self) -> Vec<StagedLogRecord> {
        self.state.records.values().cloned().collect()
    }

    /// Construct one deterministic immutable segment preview.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid interval, missing record, or oversized
    /// segment body.
    pub fn encode_segment(
        &self,
        first_position: u64,
        last_position: u64,
        committed_through: u64,
    ) -> Result<Vec<u8>, StagedLogError> {
        if first_position == 0 || first_position > last_position {
            return Err(StagedLogError::InvalidSegment("invalid position interval"));
        }
        if last_position > committed_through {
            return Err(StagedLogError::InvalidSegment(
                "segment exceeds committed frontier",
            ));
        }
        let mut records = Vec::new();
        for position in first_position..=last_position {
            records.push(
                self.state
                    .records
                    .get(&position)
                    .cloned()
                    .ok_or(StagedLogError::MissingRecord(position))?,
            );
        }
        encode_staged_segment(
            self.log_identity,
            first_position,
            last_position,
            committed_through,
            &records,
        )
    }

    fn persist_frame(&self, frame: &[u8]) -> Result<(), StagedLogError> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(frame)?;
        file.sync_all()?;
        Ok(())
    }
}

/// Decode and verify one complete `OKVL` segment.
///
/// # Errors
///
/// Returns an error for checksum, length, identity, ordering, or frontier
/// violations.
pub fn decode_staged_segment(bytes: &[u8]) -> Result<StagedLogSegment, StagedLogError> {
    if bytes.len() < SEGMENT_HEADER_BYTES + SEGMENT_CHECKSUM_BYTES {
        return Err(StagedLogError::InvalidSegment("truncated segment"));
    }
    let header = &bytes[..SEGMENT_HEADER_BYTES];
    if &header[..4] != SEGMENT_MAGIC {
        return Err(StagedLogError::InvalidSegment("bad magic"));
    }
    let version = u16::from_be_bytes(
        header[4..6]
            .try_into()
            .map_err(|_| StagedLogError::InvalidSegment("bad version"))?,
    );
    let flags = u16::from_be_bytes(
        header[6..8]
            .try_into()
            .map_err(|_| StagedLogError::InvalidSegment("bad flags"))?,
    );
    if version != SEGMENT_VERSION || flags != SEGMENT_FLAGS {
        return Err(StagedLogError::InvalidSegment(
            "unsupported version or flags",
        ));
    }
    let log_identity = header[8..40]
        .try_into()
        .map_err(|_| StagedLogError::InvalidSegment("bad log identity"))?;
    let first_position = read_u64(&header[40..48], "bad first position")?;
    let last_position = read_u64(&header[48..56], "bad last position")?;
    let committed_through = read_u64(&header[56..64], "bad committed frontier")?;
    let record_count = usize::try_from(read_u32(&header[64..68], "bad record count")?)
        .map_err(|_| StagedLogError::InvalidSegment("record count overflow"))?;
    let body_len = usize::try_from(read_u64(&header[68..76], "bad body length")?)
        .map_err(|_| StagedLogError::InvalidSegment("body length overflow"))?;
    let expected_len = SEGMENT_HEADER_BYTES
        .checked_add(body_len)
        .and_then(|value| value.checked_add(SEGMENT_CHECKSUM_BYTES))
        .ok_or(StagedLogError::InvalidSegment("segment length overflow"))?;
    if bytes.len() != expected_len {
        return Err(StagedLogError::InvalidSegment("segment length mismatch"));
    }
    let checksum_offset = expected_len - SEGMENT_CHECKSUM_BYTES;
    if digest(&bytes[..checksum_offset]).as_slice() != &bytes[checksum_offset..] {
        return Err(StagedLogError::InvalidSegment("checksum mismatch"));
    }
    if first_position == 0 || first_position > last_position || last_position > committed_through {
        return Err(StagedLogError::InvalidSegment("invalid frontier interval"));
    }

    let mut body = &bytes[SEGMENT_HEADER_BYTES..checksum_offset];
    let mut records = Vec::with_capacity(record_count);
    for expected_position in first_position..=last_position {
        let writer_epoch = take_u64(&mut body, "truncated record epoch")?;
        let position = take_u64(&mut body, "truncated record position")?;
        let request_identity =
            take_array::<REQUEST_IDENTITY_BYTES>(&mut body, "truncated request identity")?;
        let payload_len = usize::try_from(take_u32(&mut body, "truncated payload length")?)
            .map_err(|_| StagedLogError::InvalidSegment("payload length overflow"))?;
        let payload = take_bytes(&mut body, payload_len, "truncated payload")?.to_vec();
        if position != expected_position {
            return Err(StagedLogError::InvalidSegment("non-consecutive record"));
        }
        records.push(StagedLogRecord {
            writer_epoch,
            position,
            request_identity,
            payload,
        });
    }
    if !body.is_empty() || records.len() != record_count {
        return Err(StagedLogError::InvalidSegment("record count mismatch"));
    }
    Ok(StagedLogSegment {
        log_identity,
        first_position,
        last_position,
        committed_through,
        records,
    })
}

fn encode_staged_segment(
    log_identity: StagedLogIdentity,
    first_position: u64,
    last_position: u64,
    committed_through: u64,
    records: &[StagedLogRecord],
) -> Result<Vec<u8>, StagedLogError> {
    let mut body = Vec::new();
    for record in records {
        if record.payload.len() > MAX_PAYLOAD_BYTES || u32::try_from(record.payload.len()).is_err()
        {
            return Err(StagedLogError::PayloadTooLarge(record.payload.len()));
        }
        body.extend_from_slice(&record.writer_epoch.to_be_bytes());
        body.extend_from_slice(&record.position.to_be_bytes());
        body.extend_from_slice(&record.request_identity);
        body.extend_from_slice(
            &u32::try_from(record.payload.len())
                .map_err(|_| StagedLogError::PayloadTooLarge(record.payload.len()))?
                .to_be_bytes(),
        );
        body.extend_from_slice(&record.payload);
    }
    let record_count = u32::try_from(records.len())
        .map_err(|_| StagedLogError::InvalidSegment("too many records"))?;
    let mut segment = Vec::with_capacity(
        SEGMENT_HEADER_BYTES
            .saturating_add(body.len())
            .saturating_add(SEGMENT_CHECKSUM_BYTES),
    );
    segment.extend_from_slice(SEGMENT_MAGIC);
    segment.extend_from_slice(&SEGMENT_VERSION.to_be_bytes());
    segment.extend_from_slice(&SEGMENT_FLAGS.to_be_bytes());
    segment.extend_from_slice(&log_identity);
    segment.extend_from_slice(&first_position.to_be_bytes());
    segment.extend_from_slice(&last_position.to_be_bytes());
    segment.extend_from_slice(&committed_through.to_be_bytes());
    segment.extend_from_slice(&record_count.to_be_bytes());
    segment.extend_from_slice(&to_u64(body.len()).to_be_bytes());
    segment.extend_from_slice(&body);
    segment.extend_from_slice(&digest(&segment));
    Ok(segment)
}

fn replay_journal(
    bytes: &[u8],
    log_identity: StagedLogIdentity,
) -> Result<(StagedNodeState, usize, bool), StagedLogError> {
    let mut state = StagedNodeState::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < JOURNAL_HEADER_BYTES {
            return Ok((state, offset, true));
        }
        let header = &bytes[offset..offset + JOURNAL_HEADER_BYTES];
        if &header[..4] != JOURNAL_MAGIC
            || u16::from_be_bytes(header[4..6].try_into().map_err(|_| {
                StagedLogError::CorruptFrame {
                    offset: to_u64(offset),
                }
            })?) != JOURNAL_VERSION
            || header[7] != JOURNAL_FLAGS
        {
            return Err(StagedLogError::CorruptFrame {
                offset: to_u64(offset),
            });
        }
        let body_len = usize::try_from(u32::from_be_bytes(header[8..12].try_into().map_err(
            |_| StagedLogError::CorruptFrame {
                offset: to_u64(offset),
            },
        )?))
        .map_err(|_| StagedLogError::CorruptFrame {
            offset: to_u64(offset),
        })?;
        if body_len > MAX_RECORD_BODY_BYTES {
            return Err(StagedLogError::CorruptFrame {
                offset: to_u64(offset),
            });
        }
        let frame_len = JOURNAL_HEADER_BYTES
            .checked_add(body_len)
            .and_then(|value| value.checked_add(JOURNAL_CHECKSUM_BYTES))
            .ok_or(StagedLogError::CorruptFrame {
                offset: to_u64(offset),
            })?;
        if remaining < frame_len {
            return Ok((state, offset, true));
        }
        let frame = &bytes[offset..offset + frame_len];
        let checksum_offset = frame_len - JOURNAL_CHECKSUM_BYTES;
        if digest(&frame[..checksum_offset]).as_slice() != &frame[checksum_offset..] {
            return Err(StagedLogError::CorruptFrame {
                offset: to_u64(offset),
            });
        }
        apply_journal_body(
            &mut state,
            log_identity,
            header[6],
            &frame[JOURNAL_HEADER_BYTES..checksum_offset],
            offset,
        )?;
        offset += frame_len;
    }
    Ok((state, offset, false))
}

fn apply_journal_body(
    state: &mut StagedNodeState,
    expected_log_identity: StagedLogIdentity,
    kind: u8,
    mut body: &[u8],
    offset: usize,
) -> Result<(), StagedLogError> {
    let log_identity = take_array::<LOG_IDENTITY_BYTES>(&mut body, "truncated log identity")
        .map_err(|_| StagedLogError::CorruptFrame {
            offset: to_u64(offset),
        })?;
    if log_identity != expected_log_identity {
        return Err(StagedLogError::LogIdentityMismatch);
    }
    match kind {
        JOURNAL_KIND_EPOCH => {
            let writer_epoch = take_u64(&mut body, "truncated writer epoch").map_err(|_| {
                StagedLogError::CorruptFrame {
                    offset: to_u64(offset),
                }
            })?;
            if !body.is_empty()
                || writer_epoch == 0
                || state
                    .writer_epoch
                    .is_some_and(|current| writer_epoch <= current)
            {
                return Err(StagedLogError::CorruptFrame {
                    offset: to_u64(offset),
                });
            }
            state.writer_epoch = Some(writer_epoch);
        }
        JOURNAL_KIND_APPEND => {
            let writer_epoch = take_u64(&mut body, "truncated writer epoch").map_err(|_| {
                StagedLogError::CorruptFrame {
                    offset: to_u64(offset),
                }
            })?;
            let position = take_u64(&mut body, "truncated position").map_err(|_| {
                StagedLogError::CorruptFrame {
                    offset: to_u64(offset),
                }
            })?;
            let request_identity =
                take_array::<REQUEST_IDENTITY_BYTES>(&mut body, "truncated request identity")
                    .map_err(|_| StagedLogError::CorruptFrame {
                        offset: to_u64(offset),
                    })?;
            let payload_len = usize::try_from(
                take_u32(&mut body, "truncated payload length").map_err(|_| {
                    StagedLogError::CorruptFrame {
                        offset: to_u64(offset),
                    }
                })?,
            )
            .map_err(|_| StagedLogError::CorruptFrame {
                offset: to_u64(offset),
            })?;
            let payload = take_bytes(&mut body, payload_len, "truncated payload")
                .map_err(|_| StagedLogError::CorruptFrame {
                    offset: to_u64(offset),
                })?
                .to_vec();
            if !body.is_empty()
                || position != state.next_position()
                || state.writer_epoch != Some(writer_epoch)
            {
                return Err(StagedLogError::CorruptFrame {
                    offset: to_u64(offset),
                });
            }
            state.records.insert(
                position,
                StagedLogRecord {
                    writer_epoch,
                    position,
                    request_identity,
                    payload,
                },
            );
        }
        _ => {
            return Err(StagedLogError::CorruptFrame {
                offset: to_u64(offset),
            });
        }
    }
    Ok(())
}

fn encode_epoch_frame(log_identity: StagedLogIdentity, writer_epoch: u64) -> Vec<u8> {
    let mut body = Vec::with_capacity(LOG_IDENTITY_BYTES + 8);
    body.extend_from_slice(&log_identity);
    body.extend_from_slice(&writer_epoch.to_be_bytes());
    encode_journal_frame(JOURNAL_KIND_EPOCH, &body)
}

fn encode_append_frame(
    log_identity: StagedLogIdentity,
    record: &StagedLogRecord,
) -> Result<Vec<u8>, StagedLogError> {
    let mut body = Vec::with_capacity(
        LOG_IDENTITY_BYTES + 8 + 8 + REQUEST_IDENTITY_BYTES + 4 + record.payload.len(),
    );
    body.extend_from_slice(&log_identity);
    body.extend_from_slice(&record.writer_epoch.to_be_bytes());
    body.extend_from_slice(&record.position.to_be_bytes());
    body.extend_from_slice(&record.request_identity);
    body.extend_from_slice(
        &u32::try_from(record.payload.len())
            .map_err(|_| StagedLogError::PayloadTooLarge(record.payload.len()))?
            .to_be_bytes(),
    );
    body.extend_from_slice(&record.payload);
    Ok(encode_journal_frame(JOURNAL_KIND_APPEND, &body))
}

fn encode_journal_frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(JOURNAL_HEADER_BYTES + body.len() + JOURNAL_CHECKSUM_BYTES);
    frame.extend_from_slice(JOURNAL_MAGIC);
    frame.extend_from_slice(&JOURNAL_VERSION.to_be_bytes());
    frame.push(kind);
    frame.push(JOURNAL_FLAGS);
    frame.extend_from_slice(
        &u32::try_from(body.len())
            .expect("journal body length is bounded before encoding")
            .to_be_bytes(),
    );
    frame.extend_from_slice(body);
    frame.extend_from_slice(&digest(&frame));
    frame
}

fn read_u32(bytes: &[u8], reason: &'static str) -> Result<u32, StagedLogError> {
    Ok(u32::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| StagedLogError::InvalidSegment(reason))?,
    ))
}

fn read_u64(bytes: &[u8], reason: &'static str) -> Result<u64, StagedLogError> {
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| StagedLogError::InvalidSegment(reason))?,
    ))
}

fn take_u32(bytes: &mut &[u8], reason: &'static str) -> Result<u32, StagedLogError> {
    Ok(u32::from_be_bytes(take_array::<4>(bytes, reason)?))
}

fn take_u64(bytes: &mut &[u8], reason: &'static str) -> Result<u64, StagedLogError> {
    Ok(u64::from_be_bytes(take_array::<8>(bytes, reason)?))
}

fn take_array<const N: usize>(
    bytes: &mut &[u8],
    reason: &'static str,
) -> Result<[u8; N], StagedLogError> {
    let selected = take_bytes(bytes, N, reason)?;
    selected
        .try_into()
        .map_err(|_| StagedLogError::InvalidSegment(reason))
}

fn take_bytes<'a>(
    bytes: &mut &'a [u8],
    length: usize,
    reason: &'static str,
) -> Result<&'a [u8], StagedLogError> {
    if bytes.len() < length {
        return Err(StagedLogError::InvalidSegment(reason));
    }
    let (selected, remaining) = bytes.split_at(length);
    *bytes = remaining;
    Ok(selected)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "okv-staged-log-{label}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        let value = value.trim();
        assert_eq!(value.len() % 2, 0);
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                u8::try_from((high << 4) | low).unwrap()
            })
            .collect()
    }

    #[test]
    fn node_journal_v1_matches_frozen_fixture() {
        let root = TempDir::new("journal-fixture");
        let log_identity = [0x11; LOG_IDENTITY_BYTES];
        let mut node = StagedLogNode::open(&root.0, log_identity).unwrap();
        node.install_writer_epoch(7).unwrap();
        node.append(7, 1, [0x22; REQUEST_IDENTITY_BYTES], b"abc")
            .unwrap();

        let actual = fs::read(node.journal_path()).unwrap();
        let expected = decode_hex(include_str!("../fixtures/staged-node-journal-v1.hex"));
        assert_eq!(actual, expected);

        drop(node);
        let recovered = StagedLogNode::open(&root.0, log_identity).unwrap();
        assert_eq!(recovered.writer_epoch(), Some(7));
        assert_eq!(recovered.records().len(), 1);
        assert_eq!(recovered.read(1).unwrap().payload, b"abc");
    }

    #[test]
    fn log_segment_v1_matches_frozen_fixture() {
        let root = TempDir::new("segment-fixture");
        let log_identity = [0x11; LOG_IDENTITY_BYTES];
        let mut node = StagedLogNode::open(&root.0, log_identity).unwrap();
        node.install_writer_epoch(7).unwrap();
        node.append(7, 1, [0x22; REQUEST_IDENTITY_BYTES], b"abc")
            .unwrap();

        let actual = node.encode_segment(1, 1, 1).unwrap();
        let expected = decode_hex(include_str!("../fixtures/staged-log-segment-v1.hex"));
        assert_eq!(actual, expected);

        let decoded = decode_staged_segment(&expected).unwrap();
        assert_eq!(decoded.log_identity, log_identity);
        assert_eq!(decoded.first_position, 1);
        assert_eq!(decoded.last_position, 1);
        assert_eq!(decoded.committed_through, 1);
        assert_eq!(decoded.records, node.records());
    }

    #[test]
    fn exact_retry_restart_epoch_fence_and_torn_repair_are_physical() {
        let root = TempDir::new("restart");
        let log_identity = [0x11; LOG_IDENTITY_BYTES];
        let request_identity = [0x22; REQUEST_IDENTITY_BYTES];
        let mut node = StagedLogNode::open(&root.0, log_identity).unwrap();
        node.install_writer_epoch(7).unwrap();
        let first = node.append(7, 1, request_identity, b"payload").unwrap();
        let before_retry = node.physical_bytes().unwrap();
        let retry = node.append(7, 1, request_identity, b"payload").unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.frame_bytes, 0);
        assert_eq!(node.physical_bytes().unwrap(), before_retry);
        assert!(first.synchronized);
        let path = node.journal_path().to_path_buf();
        drop(node);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"OKVT\0").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut reopened = StagedLogNode::open(&root.0, log_identity).unwrap();
        assert!(reopened.recovered_torn_tail());
        assert_eq!(reopened.records().len(), 1);
        reopened.install_writer_epoch(8).unwrap();
        assert!(matches!(
            reopened.append(7, 2, [0x33; 32], b"stale"),
            Err(StagedLogError::StaleWriter {
                current: 8,
                proposed: 7
            })
        ));
        reopened.append(8, 2, [0x44; 32], b"next").unwrap();
        assert_eq!(reopened.next_position(), 3);
    }

    #[test]
    fn batch_append_recovers_exactly_and_retries_without_growth() {
        let root = TempDir::new("batch");
        let log_identity = [0x31; LOG_IDENTITY_BYTES];
        let mut node = StagedLogNode::open(&root.0, log_identity).unwrap();
        node.install_writer_epoch(7).unwrap();
        let records = (1_u64..=256)
            .map(|position| StagedLogRecord {
                writer_epoch: 7,
                position,
                request_identity: [u8::try_from(position % 251).unwrap(); 32],
                payload: vec![u8::try_from(position % 239).unwrap(); 128],
            })
            .collect::<Vec<_>>();

        let outcomes = node.append_batch(&records).unwrap();
        assert_eq!(outcomes.len(), records.len());
        assert!(outcomes.iter().all(|outcome| {
            outcome.synchronized && !outcome.replayed && outcome.frame_bytes > 0
        }));
        let after_first = node.physical_bytes().unwrap();
        let retries = node.append_batch(&records).unwrap();
        assert!(retries
            .iter()
            .all(|outcome| outcome.synchronized && outcome.replayed && outcome.frame_bytes == 0));
        assert_eq!(node.physical_bytes().unwrap(), after_first);

        drop(node);
        let recovered = StagedLogNode::open(&root.0, log_identity).unwrap();
        assert_eq!(recovered.records(), records);
        assert_eq!(recovered.next_position(), 257);
    }

    #[test]
    fn invalid_batch_is_rejected_before_physical_mutation() {
        let root = TempDir::new("invalid-batch");
        let log_identity = [0x41; LOG_IDENTITY_BYTES];
        let mut node = StagedLogNode::open(&root.0, log_identity).unwrap();
        node.install_writer_epoch(7).unwrap();
        let before = node.physical_bytes().unwrap();
        let records = [
            StagedLogRecord {
                writer_epoch: 7,
                position: 1,
                request_identity: [0x51; 32],
                payload: b"first".to_vec(),
            },
            StagedLogRecord {
                writer_epoch: 7,
                position: 3,
                request_identity: [0x53; 32],
                payload: b"gap".to_vec(),
            },
        ];
        assert!(matches!(
            node.append_batch(&records),
            Err(StagedLogError::NonConsecutive {
                expected: 2,
                actual: 3
            })
        ));
        assert_eq!(node.physical_bytes().unwrap(), before);
        assert!(node.records().is_empty());
    }

    #[test]
    fn segment_preview_is_deterministic_and_frontier_checked() {
        let first_root = TempDir::new("segment-a");
        let second_root = TempDir::new("segment-b");
        let log_identity = [0x51; LOG_IDENTITY_BYTES];
        let mut first = StagedLogNode::open(&first_root.0, log_identity).unwrap();
        let mut second = StagedLogNode::open(&second_root.0, log_identity).unwrap();
        for node in [&mut first, &mut second] {
            node.install_writer_epoch(9).unwrap();
            node.append(9, 1, [0x61; 32], b"alpha").unwrap();
            node.append(9, 2, [0x62; 32], b"beta").unwrap();
        }
        let first_bytes = first.encode_segment(1, 2, 2).unwrap();
        let second_bytes = second.encode_segment(1, 2, 2).unwrap();
        assert_eq!(first_bytes, second_bytes);
        let decoded = decode_staged_segment(&first_bytes).unwrap();
        assert_eq!(decoded.log_identity, log_identity);
        assert_eq!(decoded.records, first.records());
        assert!(matches!(
            first.encode_segment(1, 2, 1),
            Err(StagedLogError::InvalidSegment(
                "segment exceeds committed frontier"
            ))
        ));
    }
}
