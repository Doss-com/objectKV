//! Per-node stable journal for a consensus log adapter.
//!
//! The journal is append-only at the file layer. Logical truncation and purge
//! are themselves durable records, so restart reconstructs the exact Raft log
//! state without editing an acknowledged history in place.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};

const JOURNAL_MAGIC: &[u8; 4] = b"OKVR";
const JOURNAL_VERSION: u16 = 1;
const JOURNAL_HEADER_BYTES: usize = 4 + 2 + 1 + 1 + 4;
const JOURNAL_CHECKSUM_BYTES: usize = 32;
const MAX_RECORD_BODY_BYTES: usize = 64 * 1024 * 1024;

const KIND_VOTE: u8 = 1;
const KIND_COMMITTED: u8 = 2;
const KIND_APPEND: u8 = 3;
const KIND_TRUNCATE: u8 = 4;
const KIND_PURGE: u8 = 5;

const FLAG_NONE: u8 = 0;
const FLAG_SOME: u8 = 1;

/// A durable log identifier retained after its entry bytes are purged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalMarker {
    pub index: u64,
    pub payload: Vec<u8>,
}

/// Reconstructed state owned by one consensus node.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JournalState {
    vote: Option<Vec<u8>>,
    committed: Option<Vec<u8>>,
    last_purged: Option<JournalMarker>,
    entries: BTreeMap<u64, Vec<u8>>,
}

impl JournalState {
    /// Last durably granted vote, encoded by the consensus adapter.
    #[must_use]
    pub fn vote(&self) -> Option<&[u8]> {
        self.vote.as_deref()
    }

    /// Last durably committed log identifier, encoded by the adapter.
    #[must_use]
    pub fn committed(&self) -> Option<&[u8]> {
        self.committed.as_deref()
    }

    /// Greatest durably purged log identifier.
    #[must_use]
    pub fn last_purged(&self) -> Option<&JournalMarker> {
        self.last_purged.as_ref()
    }

    /// Last retained entry, if any.
    #[must_use]
    pub fn last_entry(&self) -> Option<(u64, &[u8])> {
        self.entries
            .last_key_value()
            .map(|(index, payload)| (*index, payload.as_slice()))
    }

    /// Copy retained entries in the requested half-open or inclusive range.
    #[must_use]
    pub fn entries<R>(&self, range: R) -> Vec<(u64, Vec<u8>)>
    where
        R: RangeBounds<u64>,
    {
        self.entries
            .range(range)
            .map(|(index, payload)| (*index, payload.clone()))
            .collect()
    }
}

/// Stable journal or recovery failure.
#[derive(Debug)]
pub enum JournalError {
    Io(io::Error),
    PayloadTooLarge(usize),
    CorruptFrame { offset: u64 },
    InvalidRecord(&'static str),
    NonConsecutive { expected: u64, actual: u64 },
    TruncatePurged { from: u64, purged: u64 },
    PurgeRegression { current: u64, proposed: u64 },
}

impl Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::PayloadTooLarge(length) => {
                write!(formatter, "journal record body too large: {length}")
            }
            Self::CorruptFrame { offset } => {
                write!(
                    formatter,
                    "complete journal frame corruption at byte {offset}"
                )
            }
            Self::InvalidRecord(reason) => write!(formatter, "invalid journal record: {reason}"),
            Self::NonConsecutive { expected, actual } => write!(
                formatter,
                "non-consecutive journal append: expected index {expected}, received {actual}"
            ),
            Self::TruncatePurged { from, purged } => write!(
                formatter,
                "cannot truncate from {from} through already purged index {purged}"
            ),
            Self::PurgeRegression { current, proposed } => write!(
                formatter,
                "purge index regressed from {current} to {proposed}"
            ),
        }
    }
}

impl Error for JournalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for JournalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// One node's durable vote, committed index, and Raft entries.
#[derive(Debug)]
pub struct NodeJournal {
    path: PathBuf,
    state: JournalState,
    recovered_torn_tail: bool,
}

impl NodeJournal {
    /// Create or recover one node journal.
    ///
    /// An incomplete final frame is truncated before new writes are accepted.
    /// Any complete invalid frame fails closed.
    ///
    /// # Errors
    ///
    /// Returns an error for filesystem failure, corruption, or a semantically
    /// invalid recovered operation sequence.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, JournalError> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        sync_directory(root)?;
        let path = root.join("raft.journal");
        if !path.exists() {
            File::create(&path)?.sync_all()?;
            sync_directory(root)?;
        }

        let mut bytes = Vec::new();
        File::open(&path)?.read_to_end(&mut bytes)?;
        let (state, valid_bytes, recovered_torn_tail) = replay(&bytes)?;
        if recovered_torn_tail {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(u64::try_from(valid_bytes).unwrap_or(u64::MAX))?;
            file.sync_all()?;
        }
        Ok(Self {
            path,
            state,
            recovered_torn_tail,
        })
    }

    /// Reconstructed durable state.
    #[must_use]
    pub const fn state(&self) -> &JournalState {
        &self.state
    }

    /// Exact journal path for bounded crash and corruption probes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether open repaired an incomplete final frame.
    #[must_use]
    pub const fn recovered_torn_tail(&self) -> bool {
        self.recovered_torn_tail
    }

    /// Current physical journal bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when file metadata cannot be read.
    pub fn physical_bytes(&self) -> Result<u64, JournalError> {
        Ok(fs::metadata(&self.path)?.len())
    }

    /// Persist a newly granted vote before returning.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be written and synchronized.
    pub fn save_vote(&mut self, payload: &[u8]) -> Result<(), JournalError> {
        self.persist(&Record::Vote(payload.to_vec()))
    }

    /// Persist the most recent committed log identifier before returning.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be written and synchronized.
    pub fn save_committed(&mut self, payload: Option<&[u8]>) -> Result<(), JournalError> {
        self.persist(&Record::Committed(payload.map(<[u8]>::to_vec)))
    }

    /// Persist one consecutive batch of encoded entries.
    ///
    /// The implementation is intentionally synchronous for the bootstrap
    /// correctness gate. A later IO engine may batch and complete the
    /// consensus callback asynchronously without changing this journal format.
    ///
    /// # Errors
    ///
    /// Returns an error for gaps, oversized entries, or failed durable IO.
    pub fn append(&mut self, entries: &[(u64, Vec<u8>)]) -> Result<(), JournalError> {
        if entries.is_empty() {
            return Ok(());
        }
        let records = plan_append(&self.state, entries)?;
        self.persist_batch(&records)
    }

    /// Durably remove entries at `from` and above.
    ///
    /// # Errors
    ///
    /// Returns an error when truncation crosses the purged prefix or IO fails.
    pub fn truncate(&mut self, from: u64) -> Result<(), JournalError> {
        self.persist(&Record::Truncate { from })
    }

    /// Durably purge entries through the supplied exact log marker.
    ///
    /// # Errors
    ///
    /// Returns an error for a regressing marker or failed durable IO.
    pub fn purge(&mut self, marker: JournalMarker) -> Result<(), JournalError> {
        self.persist(&Record::Purge(marker))
    }

    fn persist(&mut self, record: &Record) -> Result<(), JournalError> {
        self.persist_batch(std::slice::from_ref(record))
    }

    fn persist_batch(&mut self, records: &[Record]) -> Result<(), JournalError> {
        let mut next = self.state.clone();
        for record in records {
            apply_record(&mut next, record.clone())?;
        }
        let frames = records
            .iter()
            .map(encode_record)
            .collect::<Result<Vec<_>, _>>()?;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        for frame in frames {
            file.write_all(&frame)?;
        }
        file.sync_all()?;
        self.state = next;
        self.recovered_torn_tail = false;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Record {
    Vote(Vec<u8>),
    Committed(Option<Vec<u8>>),
    Append { index: u64, payload: Vec<u8> },
    Truncate { from: u64 },
    Purge(JournalMarker),
}

fn replay(bytes: &[u8]) -> Result<(JournalState, usize, bool), JournalError> {
    let mut state = JournalState::default();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < JOURNAL_HEADER_BYTES {
            return Ok((state, offset, true));
        }
        let header = &bytes[offset..offset + JOURNAL_HEADER_BYTES];
        if &header[..4] != JOURNAL_MAGIC
            || u16::from_be_bytes(header[4..6].try_into().map_err(|_| {
                JournalError::CorruptFrame {
                    offset: to_u64(offset),
                }
            })?) != JOURNAL_VERSION
        {
            return Err(JournalError::CorruptFrame {
                offset: to_u64(offset),
            });
        }
        let body_len = usize::try_from(u32::from_be_bytes(header[8..12].try_into().map_err(
            |_| JournalError::CorruptFrame {
                offset: to_u64(offset),
            },
        )?))
        .map_err(|_| JournalError::CorruptFrame {
            offset: to_u64(offset),
        })?;
        if body_len > MAX_RECORD_BODY_BYTES {
            return Err(JournalError::CorruptFrame {
                offset: to_u64(offset),
            });
        }
        let frame_len = JOURNAL_HEADER_BYTES
            .checked_add(body_len)
            .and_then(|value| value.checked_add(JOURNAL_CHECKSUM_BYTES))
            .ok_or(JournalError::CorruptFrame {
                offset: to_u64(offset),
            })?;
        if remaining < frame_len {
            return Ok((state, offset, true));
        }
        let frame = &bytes[offset..offset + frame_len];
        let checksum_offset = frame_len - JOURNAL_CHECKSUM_BYTES;
        if digest(&frame[..checksum_offset]).as_slice() != &frame[checksum_offset..] {
            return Err(JournalError::CorruptFrame {
                offset: to_u64(offset),
            });
        }
        let body = &frame[JOURNAL_HEADER_BYTES..checksum_offset];
        let record =
            decode_record(header[6], header[7], body).map_err(|()| JournalError::CorruptFrame {
                offset: to_u64(offset),
            })?;
        apply_record(&mut state, record)?;
        offset += frame_len;
    }
    Ok((state, offset, false))
}

fn validate_append(state: &JournalState, entries: &[(u64, Vec<u8>)]) -> Result<(), JournalError> {
    let mut expected = state
        .last_entry()
        .map(|(index, _)| index.saturating_add(1))
        .or_else(|| {
            state
                .last_purged()
                .map(|marker| marker.index.saturating_add(1))
        });
    for (index, payload) in entries {
        if payload.len().saturating_add(8) > MAX_RECORD_BODY_BYTES {
            return Err(JournalError::PayloadTooLarge(payload.len()));
        }
        if let Some(want) = expected {
            if *index != want {
                return Err(JournalError::NonConsecutive {
                    expected: want,
                    actual: *index,
                });
            }
        }
        expected = Some(index.saturating_add(1));
    }
    Ok(())
}

fn plan_append(
    state: &JournalState,
    entries: &[(u64, Vec<u8>)],
) -> Result<Vec<Record>, JournalError> {
    let entries = if let Some(purged) = state.last_purged() {
        let first_live = entries.partition_point(|(index, _)| *index <= purged.index);
        &entries[first_live..]
    } else {
        entries
    };
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let first = entries
        .first()
        .map(|(index, _)| *index)
        .ok_or(JournalError::InvalidRecord("empty append batch"))?;
    let mut expected = first;
    for (index, payload) in entries {
        if *index != expected {
            return Err(JournalError::NonConsecutive {
                expected,
                actual: *index,
            });
        }
        if payload.len().saturating_add(8) > MAX_RECORD_BODY_BYTES {
            return Err(JournalError::PayloadTooLarge(payload.len()));
        }
        expected = expected.saturating_add(1);
    }

    let mut records = Vec::with_capacity(entries.len().saturating_add(1));
    if let Some((last, _)) = state.last_entry() {
        if first <= last {
            records.push(Record::Truncate { from: first });
        } else {
            let wanted = last.saturating_add(1);
            if first != wanted {
                return Err(JournalError::NonConsecutive {
                    expected: wanted,
                    actual: first,
                });
            }
        }
    } else if let Some(purged) = state.last_purged() {
        let wanted = purged.index.saturating_add(1);
        if first != wanted {
            return Err(JournalError::NonConsecutive {
                expected: wanted,
                actual: first,
            });
        }
    }

    records.extend(entries.iter().map(|(index, payload)| Record::Append {
        index: *index,
        payload: payload.clone(),
    }));
    Ok(records)
}

fn apply_record(state: &mut JournalState, record: Record) -> Result<(), JournalError> {
    match record {
        Record::Vote(payload) => state.vote = Some(payload),
        Record::Committed(payload) => state.committed = payload,
        Record::Append { index, payload } => {
            validate_append(state, &[(index, payload.clone())])?;
            state.entries.insert(index, payload);
        }
        Record::Truncate { from } => {
            if let Some(purged) = state.last_purged.as_ref() {
                if from <= purged.index {
                    return Err(JournalError::TruncatePurged {
                        from,
                        purged: purged.index,
                    });
                }
            }
            let removed = state
                .entries
                .range(from..)
                .map(|(index, _)| *index)
                .collect::<Vec<_>>();
            for index in removed {
                state.entries.remove(&index);
            }
        }
        Record::Purge(marker) => {
            if let Some(current) = state.last_purged.as_ref() {
                if marker.index < current.index {
                    return Err(JournalError::PurgeRegression {
                        current: current.index,
                        proposed: marker.index,
                    });
                }
                if marker.index == current.index && marker.payload != current.payload {
                    return Err(JournalError::InvalidRecord(
                        "purge marker changed at the same index",
                    ));
                }
            }
            let removed = state
                .entries
                .range(..=marker.index)
                .map(|(index, _)| *index)
                .collect::<Vec<_>>();
            for index in removed {
                state.entries.remove(&index);
            }
            state.last_purged = Some(marker);
        }
    }
    Ok(())
}

fn encode_record(record: &Record) -> Result<Vec<u8>, JournalError> {
    let (kind, flags, body) = match record {
        Record::Vote(payload) => (KIND_VOTE, FLAG_SOME, payload.clone()),
        Record::Committed(Some(payload)) => (KIND_COMMITTED, FLAG_SOME, payload.clone()),
        Record::Committed(None) => (KIND_COMMITTED, FLAG_NONE, Vec::new()),
        Record::Append { index, payload } => {
            let mut body = Vec::with_capacity(8 + payload.len());
            body.extend_from_slice(&index.to_be_bytes());
            body.extend_from_slice(payload);
            (KIND_APPEND, FLAG_NONE, body)
        }
        Record::Truncate { from } => (KIND_TRUNCATE, FLAG_NONE, from.to_be_bytes().to_vec()),
        Record::Purge(marker) => {
            let mut body = Vec::with_capacity(8 + marker.payload.len());
            body.extend_from_slice(&marker.index.to_be_bytes());
            body.extend_from_slice(&marker.payload);
            (KIND_PURGE, FLAG_NONE, body)
        }
    };
    if body.len() > MAX_RECORD_BODY_BYTES || u32::try_from(body.len()).is_err() {
        return Err(JournalError::PayloadTooLarge(body.len()));
    }
    let mut frame = Vec::with_capacity(JOURNAL_HEADER_BYTES + body.len() + JOURNAL_CHECKSUM_BYTES);
    frame.extend_from_slice(JOURNAL_MAGIC);
    frame.extend_from_slice(&JOURNAL_VERSION.to_be_bytes());
    frame.push(kind);
    frame.push(flags);
    frame.extend_from_slice(
        &u32::try_from(body.len())
            .map_err(|_| JournalError::PayloadTooLarge(body.len()))?
            .to_be_bytes(),
    );
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&digest(&frame));
    Ok(frame)
}

fn decode_record(kind: u8, flags: u8, body: &[u8]) -> Result<Record, ()> {
    match (kind, flags) {
        (KIND_VOTE, FLAG_SOME) if !body.is_empty() => Ok(Record::Vote(body.to_vec())),
        (KIND_COMMITTED, FLAG_NONE) if body.is_empty() => Ok(Record::Committed(None)),
        (KIND_COMMITTED, FLAG_SOME) if !body.is_empty() => {
            Ok(Record::Committed(Some(body.to_vec())))
        }
        (KIND_APPEND, FLAG_NONE) if body.len() >= 8 => Ok(Record::Append {
            index: u64::from_be_bytes(body[..8].try_into().map_err(|_| ())?),
            payload: body[8..].to_vec(),
        }),
        (KIND_TRUNCATE, FLAG_NONE) if body.len() == 8 => Ok(Record::Truncate {
            from: u64::from_be_bytes(body.try_into().map_err(|_| ())?),
        }),
        (KIND_PURGE, FLAG_NONE) if body.len() > 8 => Ok(Record::Purge(JournalMarker {
            index: u64::from_be_bytes(body[..8].try_into().map_err(|_| ())?),
            payload: body[8..].to_vec(),
        })),
        _ => Err(()),
    }
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
                "okv-node-journal-{label}-{}-{sequence}",
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

    #[test]
    fn vote_commit_log_truncate_and_purge_survive_reopen() {
        let root = TempDir::new("state");
        let mut journal = NodeJournal::open(&root.0).unwrap();
        journal.save_vote(b"vote-3-node-1").unwrap();
        journal
            .append(&[
                (0, b"entry-0".to_vec()),
                (1, b"entry-1-old".to_vec()),
                (2, b"entry-2-old".to_vec()),
            ])
            .unwrap();
        journal.save_committed(Some(b"log-1")).unwrap();
        journal.truncate(1).unwrap();
        journal
            .append(&[(1, b"entry-1-new".to_vec()), (2, b"entry-2-new".to_vec())])
            .unwrap();
        journal
            .purge(JournalMarker {
                index: 1,
                payload: b"log-1-new".to_vec(),
            })
            .unwrap();
        drop(journal);

        let reopened = NodeJournal::open(&root.0).unwrap();
        assert_eq!(reopened.state().vote(), Some(b"vote-3-node-1".as_slice()));
        assert_eq!(reopened.state().committed(), Some(b"log-1".as_slice()));
        assert_eq!(
            reopened.state().last_purged(),
            Some(&JournalMarker {
                index: 1,
                payload: b"log-1-new".to_vec()
            })
        );
        assert_eq!(
            reopened.state().entries(..),
            vec![(2, b"entry-2-new".to_vec())]
        );
    }

    #[test]
    fn torn_tail_is_removed_before_a_new_durable_record() {
        let root = TempDir::new("torn");
        let mut journal = NodeJournal::open(&root.0).unwrap();
        journal.save_vote(b"vote-1").unwrap();
        let path = journal.path().to_path_buf();
        drop(journal);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"OKR")
            .unwrap();

        let mut repaired = NodeJournal::open(&root.0).unwrap();
        assert!(repaired.recovered_torn_tail());
        repaired.save_committed(Some(b"log-0")).unwrap();
        drop(repaired);

        let reopened = NodeJournal::open(&root.0).unwrap();
        assert_eq!(reopened.state().vote(), Some(b"vote-1".as_slice()));
        assert_eq!(reopened.state().committed(), Some(b"log-0".as_slice()));
        assert!(!reopened.recovered_torn_tail());
    }

    #[test]
    fn complete_corruption_fails_closed() {
        let root = TempDir::new("corrupt");
        let mut journal = NodeJournal::open(&root.0).unwrap();
        journal.save_vote(b"vote-1").unwrap();
        let path = journal.path().to_path_buf();
        drop(journal);
        let mut bytes = fs::read(&path).unwrap();
        bytes[JOURNAL_HEADER_BYTES] ^= 0xff;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            NodeJournal::open(&root.0),
            Err(JournalError::CorruptFrame { offset: 0 })
        ));
    }

    #[test]
    fn journal_v1_compatibility_fixture_is_dual_readable() {
        let fixture = decode_hex(include_str!("../fixtures/node-journal-v1.hex"));
        let encoded = encode_record(&Record::Append {
            index: 1,
            payload: b"entry".to_vec(),
        })
        .unwrap();
        assert_eq!(fixture, encoded);

        let root = TempDir::new("fixture");
        let path = root.0.join("raft.journal");
        fs::write(&path, fixture).unwrap();
        let journal = NodeJournal::open(&root.0).unwrap();
        assert_eq!(journal.state().entries(..), vec![(1, b"entry".to_vec())]);
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        let value = value.trim().as_bytes();
        assert_eq!(value.len() % 2, 0);
        value
            .chunks_exact(2)
            .map(|digits| (nibble(digits[0]) << 4) | nibble(digits[1]))
            .collect()
    }

    const fn nibble(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("invalid fixture hex"),
        }
    }
}
