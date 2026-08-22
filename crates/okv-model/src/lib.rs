//! Single-threaded specification model for objectKV.
//!
//! This crate is deliberately small. Distributed implementations are compared
//! against it; optimization experiments must not modify it.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

type KeyHistory = Vec<(Version, Option<Vec<u8>>)>;

/// A totally ordered commit identifier. It is not wall-clock time.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Version(u64);

impl Version {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A committed change to one user key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Clear { key: Vec<u8> },
}

/// Mutations sharing one externally assigned commit version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitBatch {
    pub version: Version,
    pub mutations: Vec<Mutation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied,
    AlreadyApplied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelError {
    ConflictingReplay { version: Version },
    NonMonotonicVersion { latest: Version, attempted: Version },
    ReadVersionUnavailable { latest: Version, requested: Version },
}

impl Display for ModelError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConflictingReplay { version } => write!(
                formatter,
                "version {} was replayed with different mutations",
                version.get()
            ),
            Self::NonMonotonicVersion { latest, attempted } => write!(
                formatter,
                "version {} cannot follow version {}",
                attempted.get(),
                latest.get()
            ),
            Self::ReadVersionUnavailable { latest, requested } => write!(
                formatter,
                "read version {} is newer than applied version {}",
                requested.get(),
                latest.get()
            ),
        }
    }
}

impl Error for ModelError {}

/// A deterministic reference implementation for version visibility and replay.
#[derive(Debug, Default)]
pub struct Model {
    commits: BTreeMap<Version, Vec<Mutation>>,
    keys: BTreeMap<Vec<u8>, KeyHistory>,
    latest: Version,
}

impl Model {
    #[must_use]
    pub fn latest_version(&self) -> Version {
        self.latest
    }

    /// Applies a new commit, or accepts an exact replay idempotently.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::ConflictingReplay`] when an existing version is
    /// replayed with different mutations, or
    /// [`ModelError::NonMonotonicVersion`] when a new batch does not advance
    /// the applied version.
    pub fn apply(&mut self, batch: CommitBatch) -> Result<ApplyOutcome, ModelError> {
        if let Some(existing) = self.commits.get(&batch.version) {
            return if existing == &batch.mutations {
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

        for mutation in &batch.mutations {
            match mutation {
                Mutation::Set { key, value } => self
                    .keys
                    .entry(key.clone())
                    .or_default()
                    .push((batch.version, Some(value.clone()))),
                Mutation::Clear { key } => self
                    .keys
                    .entry(key.clone())
                    .or_default()
                    .push((batch.version, None)),
            }
        }

        self.latest = batch.version;
        self.commits.insert(batch.version, batch.mutations);
        Ok(ApplyOutcome::Applied)
    }

    /// Returns the newest visible value at or before `read_version`.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::ReadVersionUnavailable`] when `read_version` is
    /// newer than the model's applied version.
    pub fn get(&self, key: &[u8], read_version: Version) -> Result<Option<&[u8]>, ModelError> {
        if read_version > self.latest {
            return Err(ModelError::ReadVersionUnavailable {
                latest: self.latest,
                requested: read_version,
            });
        }

        Ok(self.keys.get(key).and_then(|history| {
            history
                .iter()
                .rev()
                .find(|(version, _)| *version <= read_version)
                .and_then(|(_, value)| value.as_deref())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{ApplyOutcome, CommitBatch, Model, ModelError, Mutation, Version};

    fn set(version: u64, key: &[u8], value: &[u8]) -> CommitBatch {
        CommitBatch {
            version: Version::new(version),
            mutations: vec![Mutation::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        }
    }

    #[test]
    fn reads_historical_versions() {
        let mut model = Model::default();
        model.apply(set(1, b"k", b"one")).expect("version 1");
        model.apply(set(2, b"k", b"two")).expect("version 2");

        assert_eq!(model.get(b"k", Version::new(1)), Ok(Some(&b"one"[..])));
        assert_eq!(model.get(b"k", Version::new(2)), Ok(Some(&b"two"[..])));
    }

    #[test]
    fn clear_hides_only_newer_snapshots() {
        let mut model = Model::default();
        model.apply(set(1, b"k", b"one")).expect("version 1");
        model
            .apply(CommitBatch {
                version: Version::new(2),
                mutations: vec![Mutation::Clear { key: b"k".to_vec() }],
            })
            .expect("version 2");

        assert_eq!(model.get(b"k", Version::new(1)), Ok(Some(&b"one"[..])));
        assert_eq!(model.get(b"k", Version::new(2)), Ok(None));
    }

    #[test]
    fn exact_replay_is_idempotent() {
        let mut model = Model::default();
        let batch = set(1, b"k", b"one");

        assert_eq!(model.apply(batch.clone()), Ok(ApplyOutcome::Applied));
        assert_eq!(model.apply(batch), Ok(ApplyOutcome::AlreadyApplied));
    }

    #[test]
    fn conflicting_replay_is_rejected() {
        let mut model = Model::default();
        model.apply(set(1, b"k", b"one")).expect("version 1");

        assert_eq!(
            model.apply(set(1, b"k", b"different")),
            Err(ModelError::ConflictingReplay {
                version: Version::new(1)
            })
        );
    }

    #[test]
    fn out_of_order_version_is_rejected() {
        let mut model = Model::default();
        model.apply(set(2, b"k", b"two")).expect("version 2");

        assert_eq!(
            model.apply(set(1, b"k", b"one")),
            Err(ModelError::NonMonotonicVersion {
                latest: Version::new(2),
                attempted: Version::new(1)
            })
        );
    }

    #[test]
    fn future_read_is_rejected() {
        let mut model = Model::default();
        model.apply(set(1, b"k", b"one")).expect("version 1");

        assert_eq!(
            model.get(b"k", Version::new(2)),
            Err(ModelError::ReadVersionUnavailable {
                latest: Version::new(1),
                requested: Version::new(2)
            })
        );
    }
}
