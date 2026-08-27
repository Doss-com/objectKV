//! Per-node stable journal for a consensus log adapter.
//!
//! The journal is append-only between compactions. Logical truncation and purge
//! are themselves durable records. A later canonical compaction rewrites the
//! current vote, committed marker, purge marker, and retained suffix through an
//! atomic same-directory replacement.

pub use okv_log::PurgeMarker as JournalMarker;
use okv_log::{LogCommand, LogEntry, LogError, LogState};
use sha2::{Digest, Sha256};
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
const COMPACTION_FILE_NAME: &str = "raft.journal.compact";

const KIND_VOTE: u8 = 1;
const KIND_COMMITTED: u8 = 2;
const KIND_APPEND: u8 = 3;
const KIND_TRUNCATE: u8 = 4;
const KIND_PURGE: u8 = 5;

const FLAG_NONE: u8 = 0;
const FLAG_SOME: u8 = 1;

/// Reconstructed state owned by one consensus node.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JournalState {
    vote: Option<Vec<u8>>,
    committed: Option<Vec<u8>>,
    log: LogState,
}

/// Physical result of one canonical node-journal compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalCompaction {
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub reclaimed_bytes: u64,
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
        self.log.last_purged()
    }

    /// Last retained entry, if any.
    #[must_use]
    pub fn last_entry(&self) -> Option<(u64, &[u8])> {
        self.log.last_entry()
    }

    /// Copy retained entries in the requested half-open or inclusive range.
    #[must_use]
    pub fn entries<R>(&self, range: R) -> Vec<(u64, Vec<u8>)>
    where
        R: RangeBounds<u64>,
    {
        self.log
            .entries_clamped(range)
            .into_iter()
            .map(|entry| (entry.index, entry.payload))
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
        let compaction_path = root.join(COMPACTION_FILE_NAME);
        if compaction_path.exists() {
            fs::remove_file(&compaction_path)?;
            sync_directory(root)?;
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
        if payload.is_empty() {
            return Err(JournalError::InvalidRecord("empty vote"));
        }
        self.persist(&Record::Vote(payload.to_vec()))
    }

    /// Persist the most recent committed log identifier before returning.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be written and synchronized.
    pub fn save_committed(&mut self, payload: Option<&[u8]>) -> Result<(), JournalError> {
        if payload.is_some_and(<[u8]>::is_empty) {
            return Err(JournalError::InvalidRecord("empty committed identity"));
        }
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
        if marker.payload.is_empty() {
            return Err(JournalError::InvalidRecord("empty purge identity"));
        }
        self.persist(&Record::Purge(marker))
    }

    /// Replace obsolete journal history with one canonical encoding of the
    /// current state.
    ///
    /// The existing journal remains authoritative until the replacement file
    /// is fully written and synchronized. The same-directory rename is then
    /// synchronized through the parent directory. A crash before rename leaves
    /// an ignorable temporary file; a crash after rename leaves the same
    /// reconstructed state under a smaller physical history.
    ///
    /// # Errors
    ///
    /// Returns an error when canonical encoding, durable IO, or replacement
    /// fails. The in-memory state is never advanced by compaction.
    pub fn compact(&mut self) -> Result<JournalCompaction, JournalError> {
        let before_bytes = self.physical_bytes()?;
        let records = canonical_records(&self.state);
        let mut bytes = Vec::new();
        for record in &records {
            bytes.extend_from_slice(&encode_record(record)?);
        }
        let (replayed, valid_bytes, torn) = replay(&bytes)?;
        if torn || valid_bytes != bytes.len() || replayed != self.state {
            return Err(JournalError::InvalidRecord(
                "canonical compaction changed reconstructed state",
            ));
        }

        let root = self.path.parent().ok_or(JournalError::InvalidRecord(
            "journal path has no parent directory",
        ))?;
        let compaction_path = root.join(COMPACTION_FILE_NAME);
        if compaction_path.exists() {
            fs::remove_file(&compaction_path)?;
            sync_directory(root)?;
        }
        let mut replacement = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&compaction_path)?;
        replacement.write_all(&bytes)?;
        replacement.sync_all()?;
        drop(replacement);
        fs::rename(&compaction_path, &self.path)?;
        sync_directory(root)?;

        let after_bytes = self.physical_bytes()?;
        self.recovered_torn_tail = false;
        Ok(JournalCompaction {
            before_bytes,
            after_bytes,
            reclaimed_bytes: before_bytes.saturating_sub(after_bytes),
        })
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

fn canonical_records(state: &JournalState) -> Vec<Record> {
    let mut records = Vec::new();
    if let Some(vote) = &state.vote {
        records.push(Record::Vote(vote.clone()));
    }
    if let Some(committed) = &state.committed {
        records.push(Record::Committed(Some(committed.clone())));
    }
    if let Some(marker) = state.log.last_purged() {
        records.push(Record::Purge(marker.clone()));
    }
    records.extend(
        state
            .log
            .entries_clamped(..)
            .into_iter()
            .map(|entry| Record::Append {
                index: entry.index,
                payload: entry.payload,
            }),
    );
    records
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

fn plan_append(
    state: &JournalState,
    entries: &[(u64, Vec<u8>)],
) -> Result<Vec<Record>, JournalError> {
    let proposed = entries
        .iter()
        .map(|(index, payload)| LogEntry {
            index: *index,
            payload: payload.clone(),
        })
        .collect::<Vec<_>>();
    state
        .log
        .plan_suffix_append(&proposed)
        .map_err(|error| map_log_error(&error))?
        .into_iter()
        .map(|command| match command {
            LogCommand::Append(entry) => {
                if entry.payload.len().saturating_add(8) > MAX_RECORD_BODY_BYTES {
                    return Err(JournalError::PayloadTooLarge(entry.payload.len()));
                }
                Ok(Record::Append {
                    index: entry.index,
                    payload: entry.payload,
                })
            }
            LogCommand::TruncateSuffix { from } => Ok(Record::Truncate { from }),
            LogCommand::PurgePrefix(_) => Err(JournalError::InvalidRecord(
                "append planner returned a purge command",
            )),
        })
        .collect()
}

fn apply_record(state: &mut JournalState, record: Record) -> Result<(), JournalError> {
    match record {
        Record::Vote(payload) => state.vote = Some(payload),
        Record::Committed(payload) => state.committed = payload,
        Record::Append { index, payload } => {
            state
                .log
                .apply_all(&[LogCommand::Append(LogEntry { index, payload })])
                .map_err(|error| map_log_error(&error))?;
        }
        Record::Truncate { from } => {
            state
                .log
                .apply_all(&[LogCommand::TruncateSuffix { from }])
                .map_err(|error| map_log_error(&error))?;
        }
        Record::Purge(marker) => {
            state
                .log
                .apply_all(&[LogCommand::PurgePrefix(marker)])
                .map_err(|error| map_log_error(&error))?;
        }
    }
    Ok(())
}

fn map_log_error(error: &LogError) -> JournalError {
    match error {
        LogError::NonConsecutive { expected, actual } => JournalError::NonConsecutive {
            expected: *expected,
            actual: *actual,
        },
        LogError::TruncatePurged { from, purged } => JournalError::TruncatePurged {
            from: *from,
            purged: *purged,
        },
        LogError::PurgeRegression { current, proposed } => JournalError::PurgeRegression {
            current: *current,
            proposed: *proposed,
        },
        LogError::ConflictingPurge { .. } => {
            JournalError::InvalidRecord("purge marker changed at the same index")
        }
        LogError::IndexExhausted { .. } => JournalError::InvalidRecord("log index exhausted"),
        LogError::InvalidRange { .. } | LogError::PositionExpired { .. } => {
            JournalError::InvalidRecord("ordered-log read error during journal mutation")
        }
    }
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
    fn empty_vote_is_rejected_before_any_bytes_are_written() {
        let root = TempDir::new("empty-vote");
        let mut journal = NodeJournal::open(&root.0).unwrap();

        assert!(matches!(
            journal.save_vote(&[]),
            Err(JournalError::InvalidRecord("empty vote"))
        ));
        assert_eq!(journal.physical_bytes().unwrap(), 0);
    }

    #[test]
    fn empty_committed_identity_is_rejected_before_write() {
        let root = TempDir::new("empty-committed");
        let mut journal = NodeJournal::open(&root.0).unwrap();

        assert!(matches!(
            journal.save_committed(Some(&[])),
            Err(JournalError::InvalidRecord("empty committed identity"))
        ));
        assert_eq!(journal.physical_bytes().unwrap(), 0);
    }

    #[test]
    fn empty_purge_identity_is_rejected_before_write() {
        let root = TempDir::new("empty-purge");
        let mut journal = NodeJournal::open(&root.0).unwrap();

        assert!(matches!(
            journal.purge(JournalMarker {
                index: 1,
                payload: Vec::new(),
            }),
            Err(JournalError::InvalidRecord("empty purge identity"))
        ));
        assert_eq!(journal.physical_bytes().unwrap(), 0);
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

    #[test]
    fn every_writable_record_kind_round_trips_through_the_decoder() {
        let records = [
            Record::Vote(b"vote".to_vec()),
            Record::Committed(None),
            Record::Committed(Some(b"committed".to_vec())),
            Record::Append {
                index: 7,
                payload: b"entry".to_vec(),
            },
            Record::Truncate { from: 7 },
            Record::Purge(JournalMarker {
                index: 6,
                payload: b"purged".to_vec(),
            }),
        ];

        for record in records {
            let frame = encode_record(&record).unwrap();
            let checksum_offset = frame.len() - JOURNAL_CHECKSUM_BYTES;
            assert_eq!(
                decode_record(
                    frame[6],
                    frame[7],
                    &frame[JOURNAL_HEADER_BYTES..checksum_offset]
                ),
                Ok(record)
            );
        }
    }

    #[test]
    fn raw_history_corpus_freezes_pre_refactor_replay_behavior() {
        let accepted = decode_hex(include_str!(
            "../fixtures/node-journal-accepted-history-v1.hex"
        ));
        let (state, valid_bytes, torn) = replay(&accepted).unwrap();
        assert_eq!(valid_bytes, accepted.len());
        assert!(!torn);
        assert_eq!(state.vote(), Some(b"vote-3-node-1".as_slice()));
        assert_eq!(state.committed(), Some(b"log-1".as_slice()));
        assert_eq!(
            state.last_purged(),
            Some(&JournalMarker {
                index: 1,
                payload: b"log-1-new".to_vec(),
            })
        );
        assert_eq!(state.entries(..), vec![(2, b"entry-2-new".to_vec())]);

        let gap = decode_hex(include_str!("../fixtures/node-journal-reject-gap-v1.hex"));
        assert!(matches!(
            replay(&gap),
            Err(JournalError::NonConsecutive {
                expected: 1,
                actual: 2
            })
        ));

        let truncate_purged = decode_hex(include_str!(
            "../fixtures/node-journal-reject-truncate-purged-v1.hex"
        ));
        assert!(matches!(
            replay(&truncate_purged),
            Err(JournalError::TruncatePurged { from: 1, purged: 1 })
        ));

        let purge_regression = decode_hex(include_str!(
            "../fixtures/node-journal-reject-purge-regression-v1.hex"
        ));
        assert!(matches!(
            replay(&purge_regression),
            Err(JournalError::PurgeRegression {
                current: 2,
                proposed: 1
            })
        ));

        let purge_conflict = decode_hex(include_str!(
            "../fixtures/node-journal-reject-purge-conflict-v1.hex"
        ));
        assert!(matches!(
            replay(&purge_conflict),
            Err(JournalError::InvalidRecord(
                "purge marker changed at the same index"
            ))
        ));
    }

    #[test]
    fn expired_append_batch_writes_nothing_and_straddling_batch_writes_only_live_entries() {
        let root = TempDir::new("purge-filter");
        let mut journal = NodeJournal::open(&root.0).unwrap();
        journal
            .purge(JournalMarker {
                index: 1,
                payload: b"marker-one".to_vec(),
            })
            .unwrap();
        let after_purge = journal.physical_bytes().unwrap();

        journal
            .append(&[(0, b"zero".to_vec()), (1, b"one".to_vec())])
            .unwrap();
        assert_eq!(journal.physical_bytes().unwrap(), after_purge);

        let expected_live_frame = encode_record(&Record::Append {
            index: 2,
            payload: b"two".to_vec(),
        })
        .unwrap();
        journal
            .append(&[
                (0, b"zero".to_vec()),
                (1, b"one".to_vec()),
                (2, b"two".to_vec()),
            ])
            .unwrap();
        assert_eq!(
            journal.physical_bytes().unwrap(),
            after_purge + u64::try_from(expected_live_frame.len()).unwrap()
        );
        assert_eq!(journal.state().entries(..), vec![(2, b"two".to_vec())]);
    }

    #[test]
    fn durable_prefix_of_suffix_replacement_is_replayable() {
        let root = TempDir::new("replacement-prefix");
        let mut journal = NodeJournal::open(&root.0).unwrap();
        journal
            .append(&[
                (0, b"zero".to_vec()),
                (1, b"one-old".to_vec()),
                (2, b"two-old".to_vec()),
            ])
            .unwrap();
        let path = journal.path().to_path_buf();
        let replacement = plan_append(
            journal.state(),
            &[(1, b"one-new".to_vec()), (2, b"two-new".to_vec())],
        )
        .unwrap();
        assert!(matches!(replacement[0], Record::Truncate { from: 1 }));
        let durable_prefix = encode_record(&replacement[0]).unwrap();
        drop(journal);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&durable_prefix).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut reopened = NodeJournal::open(&root.0).unwrap();
        assert_eq!(reopened.state().entries(..), vec![(0, b"zero".to_vec())]);
        reopened
            .append(&[(1, b"one-new".to_vec()), (2, b"two-new".to_vec())])
            .unwrap();
        assert_eq!(
            reopened.state().entries(..),
            vec![
                (0, b"zero".to_vec()),
                (1, b"one-new".to_vec()),
                (2, b"two-new".to_vec()),
            ]
        );
    }

    #[test]
    fn canonical_compaction_reclaims_obsolete_history_and_reopens_exactly() {
        let root = TempDir::new("compact");
        let mut journal = NodeJournal::open(&root.0).unwrap();
        journal.save_vote(b"vote-7-node-1").unwrap();
        journal.save_committed(Some(b"log-255")).unwrap();
        let original = (0_u64..256)
            .map(|index| (index, vec![b'o'; 1_024]))
            .collect::<Vec<_>>();
        journal.append(&original).unwrap();
        journal.truncate(128).unwrap();
        let replacement = (128_u64..256)
            .map(|index| (index, vec![b'n'; 1_024]))
            .collect::<Vec<_>>();
        journal.append(&replacement).unwrap();
        journal
            .purge(JournalMarker {
                index: 223,
                payload: b"log-223".to_vec(),
            })
            .unwrap();
        let expected = journal.state().clone();

        let outcome = journal.compact().unwrap();

        assert_eq!(
            outcome.before_bytes,
            outcome.after_bytes + outcome.reclaimed_bytes
        );
        assert!(outcome.reclaimed_bytes > 300_000);
        assert_eq!(journal.state(), &expected);
        drop(journal);

        let reopened = NodeJournal::open(&root.0).unwrap();
        assert_eq!(reopened.state(), &expected);
        assert_eq!(reopened.physical_bytes().unwrap(), outcome.after_bytes);
    }

    #[test]
    fn stale_compaction_file_is_ignored_after_authoritative_replay() {
        let root = TempDir::new("stale-compaction");
        let mut journal = NodeJournal::open(&root.0).unwrap();
        journal.save_vote(b"authoritative-vote").unwrap();
        drop(journal);
        let compaction_path = root.0.join(COMPACTION_FILE_NAME);
        fs::write(&compaction_path, b"uncommitted replacement").unwrap();

        let reopened = NodeJournal::open(&root.0).unwrap();

        assert_eq!(
            reopened.state().vote(),
            Some(b"authoritative-vote".as_slice())
        );
        assert!(!compaction_path.exists());
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
