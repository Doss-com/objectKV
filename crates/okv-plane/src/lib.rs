//! Incumbent transaction-plane adapter contract.
//!
//! This crate does not implement a distributed transaction system. It freezes
//! the boundary that a selected incumbent must satisfy before objectKV adds
//! objectification, reconstruction, history, and branching around it.

use async_trait::async_trait;
use okv_transaction::TransactionCommand;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

/// `FoundationDB` release pinned for the first provider preflight.
pub const FOUNDATIONDB_7_4_6_REVISION: &str = "e77b64d4c5d01d240931c08c5384a834cae27337";

/// `TiKV` release pinned for the first provider preflight.
pub const TIKV_8_5_7_REVISION: &str = "3f446cfa9eb1d5c653031d261e185911495d0359";

/// `TiKV` Rust client revision pinned for the first provider preflight.
pub const TIKV_CLIENT_REVISION: &str = "88688d6eb3a55a864885d7bccc8abf428dce076c";

/// One provider-local commit identity.
///
/// `FoundationDB` exposes the same shape as its ten-byte transaction
/// versionstamp: an eight-byte commit version followed by a two-byte batch
/// order. Stamps are comparable only inside one objectKV generation.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ProviderStamp {
    pub commit_version: u64,
    pub batch_order: u16,
}

impl ProviderStamp {
    /// Encode the stamp in the ordering used by retained-change keys.
    #[must_use]
    pub fn to_ordered_bytes(self) -> [u8; 10] {
        let mut bytes = [0_u8; 10];
        bytes[..8].copy_from_slice(&self.commit_version.to_be_bytes());
        bytes[8..].copy_from_slice(&self.batch_order.to_be_bytes());
        bytes
    }

    /// Decode one complete ordered stamp.
    #[must_use]
    pub fn from_ordered_bytes(bytes: [u8; 10]) -> Self {
        let mut commit_version = [0_u8; 8];
        commit_version.copy_from_slice(&bytes[..8]);
        let mut batch_order = [0_u8; 2];
        batch_order.copy_from_slice(&bytes[8..]);
        Self {
            commit_version: u64::from_be_bytes(commit_version),
            batch_order: u16::from_be_bytes(batch_order),
        }
    }
}

/// An objectKV version whose provider stamp is scoped to one generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LogicalVersion {
    pub generation: u64,
    pub stamp: ProviderStamp,
}

/// Stable identity and content fingerprint for an exactly retryable request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestIdentity {
    pub id: Vec<u8>,
    pub fingerprint: [u8; 32],
}

/// One bounded point or ordered-range result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ReadOutcome {
    Value(Vec<u8>),
    Tombstone,
    Absent,
}

/// One ordered key and its visible point result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScanEntry {
    pub key: Vec<u8>,
    pub outcome: ReadOutcome,
}

/// One bounded ordered range page at an exact logical version.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScanPage {
    pub at: LogicalVersion,
    pub entries: Vec<ScanEntry>,
    pub continuation: Option<Vec<u8>>,
}

/// Input to one incumbent transaction attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitRequest {
    pub generation: u64,
    pub identity: RequestIdentity,
    pub command: TransactionCommand,
}

/// Exact commit outcome retained by the provider transaction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CommitOutcome {
    Committed {
        version: LogicalVersion,
        exact_replay: bool,
    },
    Conflict,
    Rejected {
        reason: String,
    },
    Unknown,
}

/// One transactionally emitted change, ordered by its provider stamp.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetainedChange {
    pub version: LogicalVersion,
    pub identity: RequestIdentity,
    pub command: TransactionCommand,
}

/// One bounded retained-change page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangePage {
    pub after: ProviderStamp,
    pub through: ProviderStamp,
    pub records: Vec<RetainedChange>,
    pub next: ProviderStamp,
    pub complete: bool,
}

/// Authoritative immutable closure selected by the transaction plane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectFrontier {
    pub through: LogicalVersion,
    pub manifest_key: String,
    pub manifest_sha256: [u8; 32],
}

/// One deterministic restore record from an authenticated object closure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestoreRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// One idempotent restore chunk for a fenced destination generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestoreChunk {
    pub destination_generation: u64,
    pub chunk_id: [u8; 32],
    pub source_through: LogicalVersion,
    pub records: Vec<RestoreRecord>,
}

/// Stable adapter failure category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    InvalidRequest,
    StaleGeneration,
    VersionTooOld,
    Conflict,
    OutcomeUnknown,
    RetentionGap,
    FrontierConflict,
    IncompleteClosure,
    RestoreConflict,
    Unavailable,
    Unsupported,
}

/// Classified provider-adapter failure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Error {
    pub kind: ErrorKind,
    pub detail: String,
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.detail)
    }
}

impl std::error::Error for Error {}

/// Narrow hot and lifecycle boundary implemented by the selected incumbent.
///
/// Provider-specific clients, Regions, system keys, backup files, and storage
/// engine formats remain behind this trait.
#[async_trait]
pub trait TransactionPlaneAdapter: std::fmt::Debug + Send + Sync {
    /// Return a causally current read version for one tenant keyspace.
    async fn read_version(&self, tenant: Vec<u8>) -> Result<LogicalVersion, Error>;

    /// Read one point at an exact logical version.
    async fn get(
        &self,
        tenant: Vec<u8>,
        at: LogicalVersion,
        key: Vec<u8>,
    ) -> Result<ReadOutcome, Error>;

    /// Read one ordered, half-open range page at an exact logical version.
    async fn scan(
        &self,
        tenant: Vec<u8>,
        at: LogicalVersion,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
    ) -> Result<ScanPage, Error>;

    /// Apply user mutations, retained change, and exact request outcome in one
    /// strict-serializable provider transaction.
    async fn commit(&self, tenant: Vec<u8>, request: CommitRequest)
        -> Result<CommitOutcome, Error>;

    /// Read a complete, ordered retained-change page after one cursor.
    async fn changes(
        &self,
        tenant: Vec<u8>,
        generation: u64,
        after: ProviderStamp,
        through: ProviderStamp,
        limit: u32,
    ) -> Result<ChangePage, Error>;

    /// Read the current object frontier.
    async fn object_frontier(&self, tenant: Vec<u8>) -> Result<ObjectFrontier, Error>;

    /// Atomically replace the frontier only when the expected value still
    /// matches and the named immutable closure has already been verified.
    async fn compare_and_advance_frontier(
        &self,
        tenant: Vec<u8>,
        expected: ObjectFrontier,
        replacement: ObjectFrontier,
    ) -> Result<bool, Error>;

    /// Apply one deterministic restore chunk exactly once.
    async fn restore_chunk(&self, tenant: Vec<u8>, chunk: RestoreChunk) -> Result<bool, Error>;

    /// Activate a complete destination generation after exact reconstruction.
    async fn finish_restore(
        &self,
        tenant: Vec<u8>,
        destination_generation: u64,
        through: LogicalVersion,
        state_sha256: [u8; 32],
    ) -> Result<(), Error>;
}

/// Isolation behavior exercised by the frozen semantic preflight.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationModel {
    StrictSerializable,
    SnapshotIsolation,
}

/// Required provider behavior from RFC-0041.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    StrictSerializableConflicts,
    OrderedVersionedReads,
    AtomicOrderedMutations,
    TotalOrderedCommitStamp,
    AtomicRetainedChangeAndOutcome,
    ExactUnknownResultRetry,
    BoundedRetainedChangeScan,
    CompareAndAdvanceObjectFrontier,
    EmptyGenerationRestore,
    TypedLimitsAndErrors,
}

/// Source-pinned provider mapping evaluated before integration work.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderProfile {
    pub id: String,
    pub server_revision: String,
    pub client_revision: Option<String>,
    pub isolation: IsolationModel,
    pub advertised_capabilities: BTreeSet<Capability>,
}

impl ProviderProfile {
    /// `FoundationDB` mapping proposed by RFC-0041.
    #[must_use]
    pub fn foundationdb_7_4_6() -> Self {
        Self {
            id: "foundationdb-7.4.6-explicit-retained-keys".to_owned(),
            server_revision: FOUNDATIONDB_7_4_6_REVISION.to_owned(),
            client_revision: Some(FOUNDATIONDB_7_4_6_REVISION.to_owned()),
            isolation: IsolationModel::StrictSerializable,
            advertised_capabilities: Capability::required().into_iter().collect(),
        }
    }

    /// `TiKV` mapping proposed by RFC-0041.
    #[must_use]
    pub fn tikv_8_5_7() -> Self {
        let mut capabilities: BTreeSet<_> = Capability::required().into_iter().collect();
        capabilities.remove(&Capability::StrictSerializableConflicts);
        capabilities.remove(&Capability::AtomicRetainedChangeAndOutcome);
        Self {
            id: "tikv-8.5.7-transaction-client".to_owned(),
            server_revision: TIKV_8_5_7_REVISION.to_owned(),
            client_revision: Some(TIKV_CLIENT_REVISION.to_owned()),
            isolation: IsolationModel::SnapshotIsolation,
            advertised_capabilities: capabilities,
        }
    }
}

impl Capability {
    /// Complete hard-gate set from RFC-0041.
    #[must_use]
    pub const fn required() -> [Self; 10] {
        [
            Self::StrictSerializableConflicts,
            Self::OrderedVersionedReads,
            Self::AtomicOrderedMutations,
            Self::TotalOrderedCommitStamp,
            Self::AtomicRetainedChangeAndOutcome,
            Self::ExactUnknownResultRetry,
            Self::BoundedRetainedChangeScan,
            Self::CompareAndAdvanceObjectFrontier,
            Self::EmptyGenerationRestore,
            Self::TypedLimitsAndErrors,
        ]
    }
}

/// Result of the source-pinned semantic mapping. This is not a live provider
/// receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreflightResult {
    pub provider: String,
    pub required_capabilities: u64,
    pub unsupported_capabilities: Vec<Capability>,
    pub write_skew_commits: u64,
    pub correctness_anomalies: u64,
    pub eligible_for_live_spike: bool,
}

/// Evaluate the frozen write-skew history plus the RFC-0041 capability set.
#[must_use]
pub fn preflight(profile: &ProviderProfile) -> PreflightResult {
    let required = Capability::required();
    let unsupported_capabilities = required
        .into_iter()
        .filter(|capability| !profile.advertised_capabilities.contains(capability))
        .collect::<Vec<_>>();
    let write_skew_commits = execute_write_skew(profile.isolation);
    let correctness_anomalies = u64::from(write_skew_commits > 1)
        + u64::try_from(unsupported_capabilities.len()).unwrap_or(u64::MAX);
    PreflightResult {
        provider: profile.id.clone(),
        required_capabilities: u64::try_from(required.len()).unwrap_or(u64::MAX),
        unsupported_capabilities,
        write_skew_commits,
        correctness_anomalies,
        eligible_for_live_spike: correctness_anomalies == 0,
    }
}

#[derive(Clone, Copy)]
struct SkewAttempt {
    read_version: u64,
    read_keys: [u8; 2],
    write_key: u8,
}

/// Execute the two-transaction RFC-0041 write-skew history.
///
/// Snapshot isolation checks overlapping writes only. Strict serializability
/// also checks whether a committed write intersects a later attempt's reads.
#[must_use]
pub fn execute_write_skew(isolation: IsolationModel) -> u64 {
    let attempts = [
        SkewAttempt {
            read_version: 0,
            read_keys: [b'l', b'r'],
            write_key: b'l',
        },
        SkewAttempt {
            read_version: 0,
            read_keys: [b'l', b'r'],
            write_key: b'r',
        },
    ];
    let mut committed: Vec<(u64, u8)> = Vec::new();
    for attempt in attempts {
        let conflicts = committed.iter().any(|(version, write_key)| {
            if *version <= attempt.read_version {
                return false;
            }
            match isolation {
                IsolationModel::SnapshotIsolation => *write_key == attempt.write_key,
                IsolationModel::StrictSerializable => attempt.read_keys.contains(write_key),
            }
        });
        if !conflicts {
            let version = u64::try_from(committed.len()).unwrap_or(u64::MAX) + 1;
            committed.push((version, attempt.write_key));
        }
    }
    u64::try_from(committed.len()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_stamp_round_trips_and_sorts() {
        let earlier = ProviderStamp {
            commit_version: 41,
            batch_order: u16::MAX,
        };
        let later = ProviderStamp {
            commit_version: 42,
            batch_order: 0,
        };
        assert!(earlier.to_ordered_bytes() < later.to_ordered_bytes());
        assert_eq!(
            ProviderStamp::from_ordered_bytes(earlier.to_ordered_bytes()),
            earlier
        );
    }

    #[test]
    fn strict_serializability_rejects_one_write_skew_attempt() {
        assert_eq!(execute_write_skew(IsolationModel::StrictSerializable), 1);
    }

    #[test]
    fn snapshot_isolation_admits_both_write_skew_attempts() {
        assert_eq!(execute_write_skew(IsolationModel::SnapshotIsolation), 2);
    }

    #[test]
    fn foundationdb_mapping_advances_to_live_spike() {
        let result = preflight(&ProviderProfile::foundationdb_7_4_6());
        assert_eq!(result.required_capabilities, 10);
        assert!(result.unsupported_capabilities.is_empty());
        assert_eq!(result.write_skew_commits, 1);
        assert_eq!(result.correctness_anomalies, 0);
        assert!(result.eligible_for_live_spike);
    }

    #[test]
    fn tikv_mapping_fails_closed_before_lifecycle_work() {
        let result = preflight(&ProviderProfile::tikv_8_5_7());
        assert!(result
            .unsupported_capabilities
            .contains(&Capability::StrictSerializableConflicts));
        assert!(result
            .unsupported_capabilities
            .contains(&Capability::AtomicRetainedChangeAndOutcome));
        assert_eq!(result.write_skew_commits, 2);
        assert_eq!(result.correctness_anomalies, 3);
        assert!(!result.eligible_for_live_spike);
    }

    #[test]
    fn false_serializable_label_does_not_hide_snapshot_isolation() {
        let mut unsafe_profile = ProviderProfile::tikv_8_5_7();
        unsafe_profile
            .advertised_capabilities
            .insert(Capability::StrictSerializableConflicts);
        unsafe_profile
            .advertised_capabilities
            .insert(Capability::AtomicRetainedChangeAndOutcome);
        let result = preflight(&unsafe_profile);
        assert!(result.unsupported_capabilities.is_empty());
        assert_eq!(result.write_skew_commits, 2);
        assert_eq!(result.correctness_anomalies, 1);
        assert!(!result.eligible_for_live_spike);
    }
}
