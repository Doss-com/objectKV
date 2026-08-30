//! Prototype stable-storage seam for the objectKV replicated WAL.
//!
//! This crate proves checksummed framing, local `fsync`, quorum reconstruction,
//! and torn-suffix handling on ordinary files. It does not implement consensus,
//! leader election, replication transport, or independent failure domains.

mod node_journal;
mod staged;

pub use node_journal::{JournalCompaction, JournalError, JournalMarker, JournalState, NodeJournal};
pub use staged::{
    decode_staged_segment, StagedAppendOutcome, StagedEpochOutcome, StagedLogError,
    StagedLogIdentity, StagedLogNode, StagedLogRecord, StagedLogSegment, StagedRequestIdentity,
};

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const FRAME_MAGIC: &[u8; 4] = b"OKVW";
const FRAME_VERSION: u16 = 1;
/// Bytes before the opaque payload in one prototype frame.
pub const FRAME_HEADER_BYTES: usize = 4 + 2 + 8 + 4;
const FRAME_CHECKSUM_LEN: usize = 32;
const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// One local replica identifier in the prototype topology.
pub type ReplicaId = u8;

/// Result of appending and synchronizing one frame to selected replicas.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendOutcome {
    pub log_index: u64,
    pub frame_bytes: u64,
    pub synced_replicas: Vec<ReplicaId>,
    pub quorum_durable: bool,
}

/// One record reconstructed from matching checksum-valid replica frames.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredRecord {
    pub log_index: u64,
    pub payload: Vec<u8>,
    pub replica_ids: Vec<ReplicaId>,
}

/// Quorum reconstruction result after opening replica files from scratch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Recovery {
    pub records: Vec<RecoveredRecord>,
    pub torn_tail_replicas: Vec<ReplicaId>,
    pub ignored_uncommitted_records: u64,
    pub physical_bytes: u64,
}

impl Recovery {
    /// Highest contiguous quorum-reconstructed log index.
    #[must_use]
    pub fn last_index(&self) -> u64 {
        self.records.last().map_or(0, |record| record.log_index)
    }
}

/// Stable-storage or quorum reconstruction failure.
#[derive(Debug)]
pub enum WalError {
    InvalidTopology,
    InvalidLogIndex(u64),
    PayloadTooLarge(usize),
    Io(io::Error),
    CompleteFrameCorruption {
        replica_id: ReplicaId,
        log_index: u64,
    },
    MissingContiguousQuorum(u64),
    ConflictingQuorum(u64),
}

impl Display for WalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopology => write!(formatter, "invalid WAL topology"),
            Self::InvalidLogIndex(index) => write!(formatter, "invalid log index {index}"),
            Self::PayloadTooLarge(length) => write!(formatter, "WAL payload too large: {length}"),
            Self::Io(error) => Display::fmt(error, formatter),
            Self::CompleteFrameCorruption {
                replica_id,
                log_index,
            } => write!(
                formatter,
                "complete WAL frame corruption on replica {replica_id} at index {log_index}"
            ),
            Self::MissingContiguousQuorum(index) => {
                write!(formatter, "missing contiguous WAL quorum at index {index}")
            }
            Self::ConflictingQuorum(index) => {
                write!(formatter, "conflicting WAL quorums at index {index}")
            }
        }
    }
}

impl Error for WalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// A three-file prototype of a replicated WAL stable-storage boundary.
///
/// Every selected replica is written and synchronized independently. The
/// caller may acknowledge only when [`AppendOutcome::quorum_durable`] is true.
#[derive(Debug)]
pub struct LocalReplicatedWal {
    root: PathBuf,
    replica_count: u8,
    quorum: usize,
}

impl LocalReplicatedWal {
    /// Create or open a fixed local replica topology.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid quorum or a filesystem failure.
    pub fn open(
        root: impl AsRef<Path>,
        replica_count: u8,
        quorum: usize,
    ) -> Result<Self, WalError> {
        if replica_count == 0 || quorum == 0 || quorum > usize::from(replica_count) {
            return Err(WalError::InvalidTopology);
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        sync_directory(&root)?;
        for replica_id in 0..replica_count {
            let directory = replica_directory(&root, replica_id);
            fs::create_dir_all(&directory)?;
            let path = replica_path(&root, replica_id);
            if !path.exists() {
                File::create(&path)?.sync_all()?;
                sync_directory(&directory)?;
            }
        }
        Ok(Self {
            root,
            replica_count,
            quorum,
        })
    }

    /// Append one framed payload to selected replicas and `fsync` each file.
    ///
    /// # Errors
    ///
    /// Returns an error for index zero, oversized payloads, unknown replicas,
    /// duplicate replica identifiers, or any selected replica write failure.
    pub fn append(
        &self,
        log_index: u64,
        payload: &[u8],
        selected_replicas: &[ReplicaId],
    ) -> Result<AppendOutcome, WalError> {
        if log_index == 0 {
            return Err(WalError::InvalidLogIndex(log_index));
        }
        if payload.len() > MAX_PAYLOAD_LEN || u32::try_from(payload.len()).is_err() {
            return Err(WalError::PayloadTooLarge(payload.len()));
        }
        let mut unique = BTreeSet::new();
        for replica_id in selected_replicas {
            if *replica_id >= self.replica_count || !unique.insert(*replica_id) {
                return Err(WalError::InvalidTopology);
            }
        }

        let frame = encode_frame(log_index, payload);
        let mut synced_replicas = Vec::with_capacity(selected_replicas.len());
        for replica_id in selected_replicas {
            let mut file = OpenOptions::new()
                .append(true)
                .open(replica_path(&self.root, *replica_id))?;
            file.write_all(&frame)?;
            file.sync_all()?;
            synced_replicas.push(*replica_id);
        }
        synced_replicas.sort_unstable();
        Ok(AppendOutcome {
            log_index,
            frame_bytes: u64::try_from(frame.len()).unwrap_or(u64::MAX),
            quorum_durable: synced_replicas.len() >= self.quorum,
            synced_replicas,
        })
    }

    /// Scan all local replica files and reconstruct the contiguous quorum log.
    ///
    /// A short final frame is ignored on that replica. A complete corrupt frame
    /// is tolerated only when matching valid copies still form a quorum at the
    /// same index. Recovery stops before a final non-quorum suffix and fails if
    /// later records prove that the missing quorum is in the middle of history.
    ///
    /// # Errors
    ///
    /// Returns an error for complete corruption without a valid quorum,
    /// conflicting quorums, a missing middle quorum, or filesystem failures.
    pub fn recover(&self) -> Result<Recovery, WalError> {
        let mut scans = Vec::with_capacity(usize::from(self.replica_count));
        let mut physical_bytes = 0_u64;
        for replica_id in 0..self.replica_count {
            let scan = scan_replica(replica_id, &replica_path(&self.root, replica_id))?;
            physical_bytes = physical_bytes.saturating_add(scan.physical_bytes);
            scans.push(scan);
        }

        let maximum_index = scans
            .iter()
            .filter_map(|scan| scan.records.keys().next_back().copied())
            .max()
            .unwrap_or(0);
        let mut records = Vec::new();
        let mut ignored_uncommitted_records = 0_u64;
        for log_index in 1..=maximum_index {
            let mut candidates: BTreeMap<[u8; 32], (Vec<u8>, Vec<ReplicaId>)> = BTreeMap::new();
            for scan in &scans {
                if let Some(payload) = scan.records.get(&log_index) {
                    let candidate = candidates
                        .entry(digest(payload))
                        .or_insert_with(|| (payload.clone(), Vec::new()));
                    candidate.1.push(scan.replica_id);
                }
            }
            let mut quorum_candidates = candidates
                .into_values()
                .filter(|(_, replicas)| replicas.len() >= self.quorum);
            let Some((payload, mut replica_ids)) = quorum_candidates.next() else {
                if let Some(replica_id) = scans
                    .iter()
                    .find(|scan| scan.corrupt_index == Some(log_index))
                    .map(|scan| scan.replica_id)
                {
                    return Err(WalError::CompleteFrameCorruption {
                        replica_id,
                        log_index,
                    });
                }
                let later_quorum_exists = (log_index + 1..=maximum_index).any(|later| {
                    let mut counts = BTreeMap::<[u8; 32], usize>::new();
                    for scan in &scans {
                        if let Some(payload) = scan.records.get(&later) {
                            *counts.entry(digest(payload)).or_default() += 1;
                        }
                    }
                    counts.into_values().any(|count| count >= self.quorum)
                });
                if later_quorum_exists {
                    return Err(WalError::MissingContiguousQuorum(log_index));
                }
                ignored_uncommitted_records = scans
                    .iter()
                    .map(|scan| {
                        u64::try_from(scan.records.range(log_index..).count()).unwrap_or(u64::MAX)
                    })
                    .sum();
                break;
            };
            if quorum_candidates.next().is_some() {
                return Err(WalError::ConflictingQuorum(log_index));
            }
            replica_ids.sort_unstable();
            records.push(RecoveredRecord {
                log_index,
                payload,
                replica_ids,
            });
        }

        Ok(Recovery {
            records,
            torn_tail_replicas: scans
                .iter()
                .filter(|scan| scan.torn_tail)
                .map(|scan| scan.replica_id)
                .collect(),
            ignored_uncommitted_records,
            physical_bytes,
        })
    }

    /// Exact path for one replica file, exposed for bounded fault fixtures.
    #[must_use]
    pub fn replica_path(&self, replica_id: ReplicaId) -> Option<PathBuf> {
        (replica_id < self.replica_count).then(|| replica_path(&self.root, replica_id))
    }
}

#[derive(Debug)]
struct ReplicaScan {
    replica_id: ReplicaId,
    records: BTreeMap<u64, Vec<u8>>,
    torn_tail: bool,
    corrupt_index: Option<u64>,
    physical_bytes: u64,
}

fn scan_replica(replica_id: ReplicaId, path: &Path) -> Result<ReplicaScan, WalError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    let physical_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let mut offset = 0_usize;
    let mut records = BTreeMap::new();
    let mut torn_tail = false;
    let mut corrupt_index = None;
    let mut expected_index = 1_u64;

    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < FRAME_HEADER_BYTES {
            torn_tail = true;
            break;
        }
        let header = &bytes[offset..offset + FRAME_HEADER_BYTES];
        let parsed = decode_header(header);
        let Ok((log_index, payload_len)) = parsed else {
            corrupt_index = Some(expected_index);
            break;
        };
        let frame_len = FRAME_HEADER_BYTES
            .checked_add(payload_len)
            .and_then(|length| length.checked_add(FRAME_CHECKSUM_LEN))
            .ok_or(WalError::PayloadTooLarge(payload_len))?;
        if payload_len > MAX_PAYLOAD_LEN {
            corrupt_index = Some(log_index);
            break;
        }
        if remaining < frame_len {
            torn_tail = true;
            break;
        }
        let frame = &bytes[offset..offset + frame_len];
        let checksum_offset = frame_len - FRAME_CHECKSUM_LEN;
        if digest(&frame[..checksum_offset]).as_slice() != &frame[checksum_offset..] {
            corrupt_index = Some(log_index);
            break;
        }
        if log_index != expected_index {
            corrupt_index = Some(expected_index);
            break;
        }
        records.insert(
            log_index,
            frame[FRAME_HEADER_BYTES..FRAME_HEADER_BYTES + payload_len].to_vec(),
        );
        expected_index = expected_index.saturating_add(1);
        offset += frame_len;
    }

    Ok(ReplicaScan {
        replica_id,
        records,
        torn_tail,
        corrupt_index,
        physical_bytes,
    })
}

fn encode_frame(log_index: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len() + FRAME_CHECKSUM_LEN);
    frame.extend_from_slice(FRAME_MAGIC);
    frame.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    frame.extend_from_slice(&log_index.to_be_bytes());
    frame.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("payload length validated before encoding")
            .to_be_bytes(),
    );
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&digest(&frame));
    frame
}

fn decode_header(header: &[u8]) -> Result<(u64, usize), ()> {
    if header.len() != FRAME_HEADER_BYTES || &header[..4] != FRAME_MAGIC {
        return Err(());
    }
    let version = u16::from_be_bytes(header[4..6].try_into().map_err(|_| ())?);
    if version != FRAME_VERSION {
        return Err(());
    }
    let log_index = u64::from_be_bytes(header[6..14].try_into().map_err(|_| ())?);
    let payload_len = usize::try_from(u32::from_be_bytes(
        header[14..18].try_into().map_err(|_| ())?,
    ))
    .map_err(|_| ())?;
    if log_index == 0 {
        return Err(());
    }
    Ok((log_index, payload_len))
}

fn replica_directory(root: &Path, replica_id: ReplicaId) -> PathBuf {
    root.join(format!("replica-{replica_id}"))
}

fn replica_path(root: &Path, replica_id: ReplicaId) -> PathBuf {
    replica_directory(root, replica_id).join("wal.log")
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
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
            let path = std::env::temp_dir()
                .join(format!("okv-wal-{label}-{}-{sequence}", std::process::id()));
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
    fn quorum_frames_survive_reopen_and_single_replica_loss() {
        let root = TempDir::new("quorum");
        let wal = LocalReplicatedWal::open(&root.0, 3, 2).unwrap();
        let append = wal.append(1, b"commit-envelope", &[0, 1, 2]).unwrap();
        assert!(append.quorum_durable);
        fs::remove_file(wal.replica_path(2).unwrap()).unwrap();
        File::create(wal.replica_path(2).unwrap()).unwrap();
        drop(wal);

        let reopened = LocalReplicatedWal::open(&root.0, 3, 2).unwrap();
        let recovery = reopened.recover().unwrap();
        assert_eq!(recovery.last_index(), 1);
        assert_eq!(recovery.records[0].payload, b"commit-envelope");
        assert_eq!(recovery.records[0].replica_ids, vec![0, 1]);
    }

    #[test]
    fn leader_only_suffix_is_not_recovered() {
        let root = TempDir::new("leader-only");
        let wal = LocalReplicatedWal::open(&root.0, 3, 2).unwrap();
        assert!(
            wal.append(1, b"committed", &[0, 1, 2])
                .unwrap()
                .quorum_durable
        );
        assert!(!wal.append(2, b"leader-only", &[0]).unwrap().quorum_durable);
        drop(wal);

        let recovery = LocalReplicatedWal::open(&root.0, 3, 2)
            .unwrap()
            .recover()
            .unwrap();
        assert_eq!(recovery.last_index(), 1);
        assert_eq!(recovery.ignored_uncommitted_records, 1);
    }

    #[test]
    fn torn_suffix_is_ignored_but_complete_corruption_without_quorum_fails() {
        let root = TempDir::new("damage");
        let wal = LocalReplicatedWal::open(&root.0, 3, 2).unwrap();
        wal.append(1, b"committed", &[0, 1, 2]).unwrap();

        let replica_zero = wal.replica_path(0).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&replica_zero)
            .unwrap()
            .write_all(b"OKV")
            .unwrap();
        let recovery = wal.recover().unwrap();
        assert_eq!(recovery.last_index(), 1);
        assert_eq!(recovery.torn_tail_replicas, vec![0]);

        let replica_one = wal.replica_path(1).unwrap();
        let replica_two = wal.replica_path(2).unwrap();
        let mut bytes = fs::read(&replica_one).unwrap();
        bytes[FRAME_HEADER_BYTES] ^= 0xff;
        fs::write(&replica_one, bytes).unwrap();
        fs::write(&replica_two, []).unwrap();
        assert!(matches!(
            wal.recover(),
            Err(WalError::CompleteFrameCorruption {
                replica_id: 1,
                log_index: 1
            })
        ));
    }

    #[test]
    fn frame_v1_compatibility_fixture_is_dual_readable() {
        let fixture = decode_hex(include_str!("../fixtures/frame-v1.hex"));
        assert_eq!(fixture, encode_frame(1, b"commit-envelope"));

        let root = TempDir::new("fixture-v1");
        let wal = LocalReplicatedWal::open(&root.0, 3, 2).unwrap();
        for replica_id in [0, 1] {
            fs::write(wal.replica_path(replica_id).unwrap(), &fixture).unwrap();
        }
        let recovery = wal.recover().unwrap();
        assert_eq!(recovery.last_index(), 1);
        assert_eq!(recovery.records[0].payload, b"commit-envelope");
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        let value = value.trim().as_bytes();
        assert_eq!(value.len() % 2, 0);
        value
            .chunks_exact(2)
            .map(|digits| (nibble(digits[0]) << 4) | nibble(digits[1]))
            .collect()
    }

    fn nibble(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("invalid fixture hex"),
        }
    }
}
