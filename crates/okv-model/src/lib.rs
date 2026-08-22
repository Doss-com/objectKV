//! Single-threaded specification model for objectKV.
//!
//! This crate is deliberately independent of a storage engine. Implementations
//! are compared against it; optimization experiments must not modify it.

mod history;
mod htap;

pub use history::{run_differential_history, DifferentialMode, DifferentialReport};
pub use htap::{run_htap_contract, HtapContractMode, HtapContractReport};

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

type KeyHistory = Vec<(Version, Option<Vec<u8>>)>;

/// An owned key/value row returned by a scan.
pub type Row = (Vec<u8>, Vec<u8>);

/// A generation-aware, cell-scoped commit identifier. It is not wall time.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Version {
    generation: u64,
    sequence: u64,
}

impl Version {
    /// Reserved origin before any commit has been applied.
    pub const ZERO: Self = Self::from_parts(0, 0);

    /// Construct a generation-zero version for adapters with one epoch.
    #[must_use]
    pub const fn new(sequence: u64) -> Self {
        Self::from_parts(0, sequence)
    }

    /// Construct a version ordered first by generation, then by sequence.
    #[must_use]
    pub const fn from_parts(generation: u64, sequence: u64) -> Self {
        Self {
            generation,
            sequence,
        }
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Compatibility accessor for generation-zero engine APIs.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.sequence
    }

    /// Encode so lexicographic byte order equals version order.
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 16] {
        let mut encoded = [0_u8; 16];
        encoded[..8].copy_from_slice(&self.generation.to_be_bytes());
        encoded[8..].copy_from_slice(&self.sequence.to_be_bytes());
        encoded
    }

    /// Decode the stable 16-byte representation.
    #[must_use]
    pub fn from_be_bytes(encoded: [u8; 16]) -> Self {
        let mut generation = [0_u8; 8];
        let mut sequence = [0_u8; 8];
        generation.copy_from_slice(&encoded[..8]);
        sequence.copy_from_slice(&encoded[8..]);
        Self::from_parts(u64::from_be_bytes(generation), u64::from_be_bytes(sequence))
    }
}

impl Display for Version {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.generation, self.sequence)
    }
}

/// Stable identity supplied by a client for an idempotent commit attempt.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommitIdentity {
    pub client_id: [u8; 16],
    pub request_id: u64,
}

impl CommitIdentity {
    #[must_use]
    pub const fn new(client_id: [u8; 16], request_id: u64) -> Self {
        Self {
            client_id,
            request_id,
        }
    }

    #[must_use]
    pub const fn for_test(request_id: u64) -> Self {
        Self::new([0_u8; 16], request_id)
    }
}

/// A non-empty half-open key range `[start, end)`.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl KeyRange {
    /// Construct a validated half-open range.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidRange`] when `start >= end`.
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Result<Self, ModelError> {
        if start >= end {
            return Err(ModelError::InvalidRange { start, end });
        }
        Ok(Self { start, end })
    }

    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }
}

/// A committed point or range mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Clear { key: Vec<u8> },
    ClearRange { range: KeyRange },
}

/// Mutations sharing one externally assigned commit version and replay identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitBatch {
    pub version: Version,
    pub identity: CommitIdentity,
    pub mutations: Vec<Mutation>,
}

impl CommitBatch {
    /// Return the canonical SHA-256 replay fingerprint.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::AmbiguousPointMutations`] when two distinct point
    /// mutations target one key in the same commit.
    pub fn fingerprint(&self) -> Result<[u8; 32], ModelError> {
        let canonical = canonical_mutations(&self.mutations)?;
        let mut digest = Sha256::new();
        digest.update(b"okv-commit-v2");
        digest.update(self.version.to_be_bytes());
        digest.update(self.identity.client_id);
        digest.update(self.identity.request_id.to_be_bytes());
        digest.update((canonical.len() as u64).to_be_bytes());
        for mutation in canonical {
            match mutation {
                Mutation::Set { key, value } => {
                    digest.update([1]);
                    hash_bytes(&mut digest, &key);
                    hash_bytes(&mut digest, &value);
                }
                Mutation::Clear { key } => {
                    digest.update([2]);
                    hash_bytes(&mut digest, &key);
                }
                Mutation::ClearRange { range } => {
                    digest.update([3]);
                    hash_bytes(&mut digest, &range.start);
                    hash_bytes(&mut digest, &range.end);
                }
            }
        }
        Ok(digest.finalize().into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelError {
    AmbiguousPointMutations { key: Vec<u8> },
    ConflictingReplay { version: Version },
    InvalidRange { start: Vec<u8>, end: Vec<u8> },
    NonMonotonicVersion { latest: Version, attempted: Version },
    VersionNotApplied { latest: Version, requested: Version },
    VersionTooOld { oldest: Version, requested: Version },
    ZeroVersion,
}

impl Display for ModelError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AmbiguousPointMutations { key } => {
                write!(formatter, "multiple point mutations target key {key:?}")
            }
            Self::ConflictingReplay { version } => {
                write!(formatter, "version {version} has a conflicting replay")
            }
            Self::InvalidRange { start, end } => {
                write!(formatter, "invalid half-open range {start:?}..{end:?}")
            }
            Self::NonMonotonicVersion { latest, attempted } => {
                write!(formatter, "version {attempted} cannot follow {latest}")
            }
            Self::VersionNotApplied { latest, requested } => {
                write!(
                    formatter,
                    "version {requested} is newer than applied version {latest}"
                )
            }
            Self::VersionTooOld { oldest, requested } => {
                write!(
                    formatter,
                    "version {requested} is older than retained version {oldest}"
                )
            }
            Self::ZeroVersion => formatter.write_str("version zero is reserved"),
        }
    }
}

impl Error for ModelError {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommitRecord {
    identity: CommitIdentity,
    fingerprint: [u8; 32],
}

/// A deterministic reference implementation for MVCC visibility and replay.
#[derive(Debug, Default)]
pub struct Model {
    commits: BTreeMap<Version, CommitRecord>,
    keys: BTreeMap<Vec<u8>, KeyHistory>,
    range_clears: Vec<(Version, KeyRange)>,
    latest: Version,
    oldest_readable: Version,
}

impl Model {
    #[must_use]
    pub fn latest_version(&self) -> Version {
        self.latest
    }

    #[must_use]
    pub fn oldest_readable_version(&self) -> Version {
        self.oldest_readable
    }

    /// Advance the retention boundary without changing visible state.
    ///
    /// # Errors
    ///
    /// Returns an availability error if the boundary was not applied or moves
    /// backwards.
    pub fn retain_from(&mut self, version: Version) -> Result<(), ModelError> {
        self.check_read_version(version)?;
        if version < self.oldest_readable {
            return Err(ModelError::VersionTooOld {
                oldest: self.oldest_readable,
                requested: version,
            });
        }
        self.oldest_readable = version;
        Ok(())
    }

    /// Apply a commit, or accept an exact canonical replay idempotently.
    ///
    /// # Errors
    ///
    /// Rejects zero/non-monotonic versions, conflicting replays, and ambiguous
    /// point mutations.
    #[allow(clippy::needless_pass_by_value)]
    pub fn apply(&mut self, batch: CommitBatch) -> Result<ApplyOutcome, ModelError> {
        if batch.version == Version::ZERO {
            return Err(ModelError::ZeroVersion);
        }
        let fingerprint = batch.fingerprint()?;
        if let Some(existing) = self.commits.get(&batch.version) {
            return if existing.identity == batch.identity && existing.fingerprint == fingerprint {
                Ok(ApplyOutcome::AlreadyApplied)
            } else {
                Err(ModelError::ConflictingReplay {
                    version: batch.version,
                })
            };
        }
        if batch.version <= self.latest {
            return Err(ModelError::NonMonotonicVersion {
                latest: self.latest,
                attempted: batch.version,
            });
        }

        for mutation in canonical_mutations(&batch.mutations)? {
            match mutation {
                Mutation::Set { key, value } => self
                    .keys
                    .entry(key)
                    .or_default()
                    .push((batch.version, Some(value))),
                Mutation::Clear { key } => self
                    .keys
                    .entry(key)
                    .or_default()
                    .push((batch.version, None)),
                Mutation::ClearRange { range } => self.range_clears.push((batch.version, range)),
            }
        }
        self.commits.insert(
            batch.version,
            CommitRecord {
                identity: batch.identity,
                fingerprint,
            },
        );
        self.latest = batch.version;
        Ok(ApplyOutcome::Applied)
    }

    /// Return the newest visible value at or before `read_version`.
    ///
    /// A point mutation wins over a covering range clear at the same version.
    ///
    /// # Errors
    ///
    /// Returns distinct errors for a future or expired version.
    pub fn get(&self, key: &[u8], read_version: Version) -> Result<Option<&[u8]>, ModelError> {
        self.check_read_version(read_version)?;
        let point = self.keys.get(key).and_then(|history| {
            history
                .iter()
                .rev()
                .find(|(version, _)| *version <= read_version)
        });
        let cleared_at = self
            .range_clears
            .iter()
            .rev()
            .find(|(version, range)| *version <= read_version && range.contains(key))
            .map(|(version, _)| *version);
        Ok(match (point, cleared_at) {
            (Some((point_version, _)), Some(clear_version)) if *point_version < clear_version => {
                None
            }
            (Some((_, value)), _) => value.as_deref(),
            (None, _) => None,
        })
    }

    /// Scan visible point keys in a half-open range.
    ///
    /// # Errors
    ///
    /// Returns distinct errors for a future or expired version.
    pub fn scan(&self, range: &KeyRange, read_version: Version) -> Result<Vec<Row>, ModelError> {
        self.check_read_version(read_version)?;
        let mut rows = Vec::new();
        for (key, _) in self.keys.range(range.start.clone()..range.end.clone()) {
            if let Some(value) = self.get(key, read_version)? {
                rows.push((key.clone(), value.to_vec()));
            }
        }
        Ok(rows)
    }

    /// Create a read-your-writes view over one retained snapshot.
    ///
    /// # Errors
    ///
    /// Returns distinct errors for a future or expired version.
    pub fn transaction(&self, read_version: Version) -> Result<TransactionView<'_>, ModelError> {
        self.check_read_version(read_version)?;
        Ok(TransactionView {
            model: self,
            read_version,
            mutations: Vec::new(),
        })
    }

    fn check_read_version(&self, requested: Version) -> Result<(), ModelError> {
        if requested > self.latest {
            return Err(ModelError::VersionNotApplied {
                latest: self.latest,
                requested,
            });
        }
        if requested < self.oldest_readable {
            return Err(ModelError::VersionTooOld {
                oldest: self.oldest_readable,
                requested,
            });
        }
        Ok(())
    }
}

/// An ordered, uncommitted read-your-writes overlay.
pub struct TransactionView<'a> {
    model: &'a Model,
    read_version: Version,
    mutations: Vec<Mutation>,
}

impl TransactionView<'_> {
    pub fn mutate(&mut self, mutation: Mutation) {
        self.mutations.push(mutation);
    }

    /// Read from the snapshot, then apply pending mutations in call order.
    ///
    /// # Errors
    ///
    /// Propagates snapshot availability errors.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ModelError> {
        let mut value = self.model.get(key, self.read_version)?.map(<[u8]>::to_vec);
        for mutation in &self.mutations {
            match mutation {
                Mutation::Set {
                    key: target,
                    value: new,
                } if target == key => {
                    value = Some(new.clone());
                }
                Mutation::Clear { key: target } if target == key => value = None,
                Mutation::ClearRange { range } if range.contains(key) => value = None,
                _ => {}
            }
        }
        Ok(value)
    }

    /// Scan the snapshot plus pending point keys.
    ///
    /// # Errors
    ///
    /// Propagates snapshot availability errors.
    pub fn scan(&self, range: &KeyRange) -> Result<Vec<Row>, ModelError> {
        let mut keys: BTreeSet<Vec<u8>> = self
            .model
            .scan(range, self.read_version)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        for mutation in &self.mutations {
            if let Mutation::Set { key, .. } = mutation {
                if range.contains(key) {
                    keys.insert(key.clone());
                }
            }
        }
        let mut rows = Vec::new();
        for key in keys {
            if let Some(value) = self.get(&key)? {
                rows.push((key, value));
            }
        }
        Ok(rows)
    }
}

fn canonical_mutations(mutations: &[Mutation]) -> Result<Vec<Mutation>, ModelError> {
    let mut canonical = mutations.to_vec();
    canonical.sort_by(|left, right| mutation_key(left).cmp(&mutation_key(right)));
    canonical.dedup();
    let mut point_keys = BTreeSet::new();
    for mutation in &canonical {
        let key = match mutation {
            Mutation::Set { key, .. } | Mutation::Clear { key } => Some(key),
            Mutation::ClearRange { .. } => None,
        };
        if let Some(key) = key {
            if !point_keys.insert(key.clone()) {
                return Err(ModelError::AmbiguousPointMutations { key: key.clone() });
            }
        }
    }
    Ok(canonical)
}

fn mutation_key(mutation: &Mutation) -> (u8, &[u8], &[u8]) {
    match mutation {
        Mutation::ClearRange { range } => (0, &range.start, &range.end),
        Mutation::Clear { key } => (1, key, &[]),
        Mutation::Set { key, value } => (2, key, value),
    }
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(version: Version, key: &[u8], value: &[u8]) -> CommitBatch {
        CommitBatch {
            version,
            identity: CommitIdentity::for_test(version.sequence()),
            mutations: vec![Mutation::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        }
    }

    #[test]
    fn version_encoding_preserves_generation_order() {
        let older = Version::from_parts(7, u64::MAX);
        let newer = Version::from_parts(8, 0);
        assert!(older < newer);
        assert!(older.to_be_bytes() < newer.to_be_bytes());
        assert_eq!(Version::from_be_bytes(newer.to_be_bytes()), newer);
    }

    #[test]
    fn range_clear_obeys_half_open_bounds_and_same_version_point_precedence() {
        let mut model = Model::default();
        model.apply(set(Version::new(1), b"a", b"a1")).expect("a");
        model.apply(set(Version::new(2), b"b", b"b1")).expect("b");
        model.apply(set(Version::new(3), b"c", b"c1")).expect("c");
        model
            .apply(CommitBatch {
                version: Version::new(4),
                identity: CommitIdentity::for_test(4),
                mutations: vec![
                    Mutation::ClearRange {
                        range: KeyRange::new(b"a".to_vec(), b"c".to_vec()).expect("range"),
                    },
                    Mutation::Set {
                        key: b"b".to_vec(),
                        value: b"b2".to_vec(),
                    },
                ],
            })
            .expect("range clear");
        assert_eq!(model.get(b"a", Version::new(4)), Ok(None));
        assert_eq!(model.get(b"b", Version::new(4)), Ok(Some(&b"b2"[..])));
        assert_eq!(model.get(b"c", Version::new(4)), Ok(Some(&b"c1"[..])));
    }

    #[test]
    fn replay_is_canonical_but_identity_bound() {
        let mut model = Model::default();
        let range = Mutation::ClearRange {
            range: KeyRange::new(b"a".to_vec(), b"z".to_vec()).expect("range"),
        };
        let point = Mutation::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        model
            .apply(CommitBatch {
                version: Version::new(1),
                identity: CommitIdentity::for_test(9),
                mutations: vec![range.clone(), point.clone()],
            })
            .expect("first");
        assert_eq!(
            model.apply(CommitBatch {
                version: Version::new(1),
                identity: CommitIdentity::for_test(9),
                mutations: vec![point.clone(), range],
            }),
            Ok(ApplyOutcome::AlreadyApplied)
        );
        assert_eq!(
            model.apply(CommitBatch {
                version: Version::new(1),
                identity: CommitIdentity::for_test(10),
                mutations: vec![point],
            }),
            Err(ModelError::ConflictingReplay {
                version: Version::new(1)
            })
        );
    }

    #[test]
    fn availability_errors_are_distinct() {
        let mut model = Model::default();
        model
            .apply(set(Version::new(1), b"k", b"one"))
            .expect("one");
        model
            .apply(set(Version::new(2), b"k", b"two"))
            .expect("two");
        model.retain_from(Version::new(2)).expect("retain");
        assert_eq!(model.get(b"k", Version::new(2)), Ok(Some(&b"two"[..])));
        assert_eq!(
            model.get(b"k", Version::new(1)),
            Err(ModelError::VersionTooOld {
                oldest: Version::new(2),
                requested: Version::new(1)
            })
        );
        assert_eq!(
            model.get(b"k", Version::new(3)),
            Err(ModelError::VersionNotApplied {
                latest: Version::new(2),
                requested: Version::new(3)
            })
        );
    }

    #[test]
    fn transaction_reads_writes_in_operation_order() {
        let mut model = Model::default();
        model
            .apply(set(Version::new(1), b"k", b"base"))
            .expect("base");
        let mut transaction = model.transaction(Version::new(1)).expect("transaction");
        transaction.mutate(Mutation::Set {
            key: b"k".to_vec(),
            value: b"pending".to_vec(),
        });
        assert_eq!(transaction.get(b"k"), Ok(Some(b"pending".to_vec())));
        transaction.mutate(Mutation::ClearRange {
            range: KeyRange::new(b"a".to_vec(), b"z".to_vec()).expect("range"),
        });
        assert_eq!(transaction.get(b"k"), Ok(None));
        transaction.mutate(Mutation::Set {
            key: b"k".to_vec(),
            value: b"again".to_vec(),
        });
        assert_eq!(transaction.get(b"k"), Ok(Some(b"again".to_vec())));
    }

    #[test]
    fn ambiguous_point_mutations_are_rejected() {
        let mut model = Model::default();
        let result = model.apply(CommitBatch {
            version: Version::new(1),
            identity: CommitIdentity::for_test(1),
            mutations: vec![
                Mutation::Clear { key: b"k".to_vec() },
                Mutation::Set {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            ],
        });
        assert_eq!(
            result,
            Err(ModelError::AmbiguousPointMutations { key: b"k".to_vec() })
        );
    }

    #[test]
    fn stale_generation_is_non_monotonic_even_with_larger_sequence() {
        let mut model = Model::default();
        model
            .apply(set(Version::from_parts(2, 1), b"k", b"new"))
            .expect("new generation");
        assert_eq!(
            model.apply(set(Version::from_parts(1, u64::MAX), b"k", b"stale")),
            Err(ModelError::NonMonotonicVersion {
                latest: Version::from_parts(2, 1),
                attempted: Version::from_parts(1, u64::MAX),
            })
        );
    }
}
