//! RFC-0059 minimum-readable floor and compaction-filter mechanism.

use crate::{decode_user_version_key, AdapterError, USER_KEY_PREFIX};
use async_trait::async_trait;
use okv_model::Version;
use serde::{Deserialize, Serialize};
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry, ValueDeletable,
};
use std::io::{Error as IoError, ErrorKind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Monotonic, generation-zero minimum-readable version consumed by a KV
/// Runtime and frozen into each compaction job.
#[derive(Debug)]
pub struct MvccRetentionFloor {
    sequence: AtomicU64,
}

impl MvccRetentionFloor {
    /// Construct one generation-zero floor.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError::UnsupportedGeneration`] for a nonzero logical
    /// generation, which the current `SlateDB` adapter cannot encode.
    pub fn new(initial: Version) -> Result<Self, AdapterError> {
        require_generation_zero(initial)?;
        Ok(Self {
            sequence: AtomicU64::new(initial.sequence()),
        })
    }

    /// Return the current minimum-readable version.
    #[must_use]
    pub fn current(&self) -> Version {
        Version::new(self.sequence.load(Ordering::Acquire))
    }

    /// Advance the floor, returning `true` only when it changed.
    ///
    /// # Errors
    ///
    /// Returns an adapter error for a nonzero generation or a regression.
    pub fn advance(&self, attempted: Version) -> Result<bool, AdapterError> {
        require_generation_zero(attempted)?;
        let attempted_sequence = attempted.sequence();
        let mut current = self.sequence.load(Ordering::Acquire);
        loop {
            if attempted_sequence < current {
                return Err(AdapterError::RetentionFloorRegression {
                    current: Version::new(current),
                    attempted,
                });
            }
            if attempted_sequence == current {
                return Ok(false);
            }
            match self.sequence.compare_exchange_weak(
                current,
                attempted_sequence,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(true),
                Err(observed) => current = observed,
            }
        }
    }
}

fn require_generation_zero(version: Version) -> Result<(), AdapterError> {
    if version.generation() == 0 {
        Ok(())
    } else {
        Err(AdapterError::UnsupportedGeneration {
            generation: version.generation(),
        })
    }
}

#[derive(Debug, Default)]
struct MvccHistoryFilterStats {
    inspected_user_entries: AtomicU64,
    kept_newer_entries: AtomicU64,
    kept_floor_anchors: AtomicU64,
    dropped_older_entries: AtomicU64,
    kept_metadata_entries: AtomicU64,
    malformed_entries: AtomicU64,
}

/// Stable receipt snapshot for one or more compaction-filter jobs.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvccHistoryFilterStatsSnapshot {
    pub inspected_user_entries: u64,
    pub kept_newer_entries: u64,
    pub kept_floor_anchors: u64,
    pub dropped_older_entries: u64,
    pub kept_metadata_entries: u64,
    pub malformed_entries: u64,
}

/// Correct filter behavior or one bounded falsifier used by RFC-0059.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MvccHistoryFilterMode {
    #[default]
    Correct,
    DropFloorAnchor,
    DropTombstoneAnchor,
    ReloadFloorDuringJob,
}

impl MvccHistoryFilterStats {
    fn snapshot(&self) -> MvccHistoryFilterStatsSnapshot {
        MvccHistoryFilterStatsSnapshot {
            inspected_user_entries: self.inspected_user_entries.load(Ordering::Acquire),
            kept_newer_entries: self.kept_newer_entries.load(Ordering::Acquire),
            kept_floor_anchors: self.kept_floor_anchors.load(Ordering::Acquire),
            dropped_older_entries: self.dropped_older_entries.load(Ordering::Acquire),
            kept_metadata_entries: self.kept_metadata_entries.load(Ordering::Acquire),
            malformed_entries: self.malformed_entries.load(Ordering::Acquire),
        }
    }
}

/// `SlateDB` supplier that captures the objectKV floor exactly once per
/// compaction job.
#[derive(Clone, Debug)]
pub struct MvccHistoryFilterSupplier {
    floor: Arc<MvccRetentionFloor>,
    stats: Arc<MvccHistoryFilterStats>,
    mode: MvccHistoryFilterMode,
}

impl MvccHistoryFilterSupplier {
    #[must_use]
    pub fn new(floor: Arc<MvccRetentionFloor>) -> Self {
        Self {
            floor,
            stats: Arc::new(MvccHistoryFilterStats::default()),
            mode: MvccHistoryFilterMode::Correct,
        }
    }

    /// Construct a deliberately selectable subject for an evaluation lane.
    #[must_use]
    pub fn with_mode(floor: Arc<MvccRetentionFloor>, mode: MvccHistoryFilterMode) -> Self {
        Self {
            floor,
            stats: Arc::new(MvccHistoryFilterStats::default()),
            mode,
        }
    }

    #[must_use]
    pub fn stats(&self) -> MvccHistoryFilterStatsSnapshot {
        self.stats.snapshot()
    }
}

#[async_trait]
impl CompactionFilterSupplier for MvccHistoryFilterSupplier {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        let objectkv_floor = self.floor.current().sequence();
        let effective_floor = context
            .retention_min_seq
            .map_or(objectkv_floor, |internal_floor| {
                objectkv_floor.min(internal_floor)
            });
        Ok(Box::new(MvccHistoryFilter::new(
            Version::new(effective_floor),
            Arc::clone(&self.stats),
            self.mode,
            (self.mode == MvccHistoryFilterMode::ReloadFloorDuringJob)
                .then(|| Arc::clone(&self.floor)),
            context.retention_min_seq,
        )))
    }
}

struct MvccHistoryFilter {
    floor: Version,
    current_user_key: Option<Vec<u8>>,
    previous_version: Option<Version>,
    floor_anchor_seen: bool,
    stats: Arc<MvccHistoryFilterStats>,
    mode: MvccHistoryFilterMode,
    live_floor: Option<Arc<MvccRetentionFloor>>,
    internal_floor: Option<u64>,
}

impl MvccHistoryFilter {
    fn new(
        floor: Version,
        stats: Arc<MvccHistoryFilterStats>,
        mode: MvccHistoryFilterMode,
        live_floor: Option<Arc<MvccRetentionFloor>>,
        internal_floor: Option<u64>,
    ) -> Self {
        Self {
            floor,
            current_user_key: None,
            previous_version: None,
            floor_anchor_seen: false,
            stats,
            mode,
            live_floor,
            internal_floor,
        }
    }

    fn effective_floor(&self) -> Version {
        let floor = self
            .live_floor
            .as_ref()
            .map_or(self.floor, |live| live.current());
        self.internal_floor.map_or(floor, |internal| {
            Version::new(floor.sequence().min(internal))
        })
    }

    fn malformed(&self, message: impl Into<String>) -> CompactionFilterError {
        self.stats.malformed_entries.fetch_add(1, Ordering::AcqRel);
        CompactionFilterError::FilterError(Box::new(IoError::new(
            ErrorKind::InvalidData,
            message.into(),
        )))
    }
}

#[async_trait]
impl CompactionFilter for MvccHistoryFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        if entry.key.first() != Some(&USER_KEY_PREFIX) {
            self.stats
                .kept_metadata_entries
                .fetch_add(1, Ordering::AcqRel);
            return Ok(CompactionFilterDecision::Keep);
        }

        let (user_key, version) = decode_user_version_key(entry.key.as_ref())
            .map_err(|error| self.malformed(error.to_string()))?;
        self.stats
            .inspected_user_entries
            .fetch_add(1, Ordering::AcqRel);

        if self.current_user_key.as_deref() != Some(user_key.as_slice()) {
            if self
                .current_user_key
                .as_ref()
                .is_some_and(|previous| previous >= &user_key)
            {
                return Err(self.malformed("compaction user keys are not strictly ascending"));
            }
            self.current_user_key = Some(user_key);
            self.previous_version = None;
            self.floor_anchor_seen = false;
        }

        if self
            .previous_version
            .is_some_and(|previous| version > previous)
        {
            return Err(
                self.malformed("compaction versions are not descending inside one user key")
            );
        }
        self.previous_version = Some(version);

        let effective_floor = self.effective_floor();
        if version > effective_floor {
            self.stats.kept_newer_entries.fetch_add(1, Ordering::AcqRel);
            return Ok(CompactionFilterDecision::Keep);
        }
        if !self.floor_anchor_seen {
            if self.mode == MvccHistoryFilterMode::DropFloorAnchor {
                self.floor_anchor_seen = true;
                self.stats
                    .dropped_older_entries
                    .fetch_add(1, Ordering::AcqRel);
                return Ok(CompactionFilterDecision::Drop);
            }
            if self.mode == MvccHistoryFilterMode::DropTombstoneAnchor
                && matches!(&entry.value, ValueDeletable::Value(value) if value.as_ref() == [0])
            {
                self.stats
                    .dropped_older_entries
                    .fetch_add(1, Ordering::AcqRel);
                return Ok(CompactionFilterDecision::Drop);
            }
            self.floor_anchor_seen = true;
            self.stats.kept_floor_anchors.fetch_add(1, Ordering::AcqRel);
            return Ok(CompactionFilterDecision::Keep);
        }

        self.stats
            .dropped_older_entries
            .fetch_add(1, Ordering::AcqRel);
        Ok(CompactionFilterDecision::Drop)
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MvccHistoryFilter, MvccHistoryFilterMode, MvccHistoryFilterStats,
        MvccHistoryFilterSupplier, MvccRetentionFloor,
    };
    use crate::{user_version_key, AdapterError};
    use bytes::Bytes;
    use okv_model::Version;
    use slatedb::{
        CompactionFilter, CompactionFilterDecision, CompactionFilterSupplier, CompactionJobContext,
        RowEntry, ValueDeletable,
    };
    use std::sync::Arc;

    fn entry(key: &[u8], version: u64, value: &[u8]) -> RowEntry {
        RowEntry {
            key: Bytes::from(user_version_key(key, Version::new(version))),
            value: ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq: version,
            create_ts: None,
            expire_ts: None,
        }
    }

    fn context(retention_min_seq: Option<u64>) -> CompactionJobContext {
        CompactionJobContext {
            destination: 1,
            is_dest_last_run: true,
            compaction_clock_tick: 0,
            retention_min_seq,
        }
    }

    #[test]
    fn floor_advances_but_never_retreats() {
        let floor = MvccRetentionFloor::new(Version::new(10)).expect("create floor");
        assert_eq!(floor.current(), Version::new(10));
        assert_eq!(floor.advance(Version::new(10)), Ok(false));
        assert_eq!(floor.advance(Version::new(20)), Ok(true));
        assert_eq!(floor.current(), Version::new(20));
        assert_eq!(
            floor.advance(Version::new(19)),
            Err(AdapterError::RetentionFloorRegression {
                current: Version::new(20),
                attempted: Version::new(19),
            })
        );
    }

    #[tokio::test]
    async fn keeps_newer_versions_and_one_floor_anchor_per_key() {
        let stats = Arc::new(MvccHistoryFilterStats::default());
        let mut filter = MvccHistoryFilter::new(
            Version::new(10),
            Arc::clone(&stats),
            MvccHistoryFilterMode::Correct,
            None,
            None,
        );
        let cases = [
            (entry(b"a", 20, &[1, 20]), CompactionFilterDecision::Keep),
            (entry(b"a", 10, &[1, 10]), CompactionFilterDecision::Keep),
            (entry(b"a", 9, &[1, 9]), CompactionFilterDecision::Drop),
            (entry(b"a", 1, &[1, 1]), CompactionFilterDecision::Drop),
            (entry(b"b", 12, &[1, 12]), CompactionFilterDecision::Keep),
            (entry(b"b", 8, &[0]), CompactionFilterDecision::Keep),
            (entry(b"b", 4, &[1, 4]), CompactionFilterDecision::Drop),
        ];
        for (entry, expected) in cases {
            assert_eq!(filter.filter(&entry).await.expect("filter entry"), expected);
        }
        let receipt = stats.snapshot();
        assert_eq!(receipt.kept_newer_entries, 2);
        assert_eq!(receipt.kept_floor_anchors, 2);
        assert_eq!(receipt.dropped_older_entries, 3);
        assert_eq!(receipt.malformed_entries, 0);
    }

    #[tokio::test]
    async fn supplier_freezes_the_lower_internal_snapshot_floor() {
        let floor = Arc::new(MvccRetentionFloor::new(Version::new(20)).expect("create floor"));
        let supplier = MvccHistoryFilterSupplier::new(floor);
        let mut filter = supplier
            .create_compaction_filter(&context(Some(10)))
            .await
            .expect("create filter");
        assert_eq!(
            filter
                .filter(&entry(b"a", 15, &[1, 15]))
                .await
                .expect("keep internal-snapshot version"),
            CompactionFilterDecision::Keep
        );
        assert_eq!(
            filter
                .filter(&entry(b"a", 10, &[1, 10]))
                .await
                .expect("keep internal-snapshot anchor"),
            CompactionFilterDecision::Keep
        );
        assert_eq!(
            filter
                .filter(&entry(b"a", 9, &[1, 9]))
                .await
                .expect("drop older version"),
            CompactionFilterDecision::Drop
        );
    }

    #[tokio::test]
    async fn running_job_does_not_reload_an_advanced_floor() {
        let floor = Arc::new(MvccRetentionFloor::new(Version::new(10)).expect("create floor"));
        let supplier = MvccHistoryFilterSupplier::new(Arc::clone(&floor));
        let mut frozen = supplier
            .create_compaction_filter(&context(None))
            .await
            .expect("create frozen filter");
        floor
            .advance(Version::new(20))
            .expect("advance authority floor");

        assert_eq!(
            frozen
                .filter(&entry(b"a", 15, &[1, 15]))
                .await
                .expect("old job keeps version newer than frozen floor"),
            CompactionFilterDecision::Keep
        );
        assert_eq!(
            frozen
                .filter(&entry(b"a", 10, &[1, 10]))
                .await
                .expect("old job keeps frozen anchor"),
            CompactionFilterDecision::Keep
        );

        let mut next = supplier
            .create_compaction_filter(&context(None))
            .await
            .expect("create next filter");
        assert_eq!(
            next.filter(&entry(b"a", 15, &[1, 15]))
                .await
                .expect("new job uses advanced floor"),
            CompactionFilterDecision::Keep
        );
        assert_eq!(
            next.filter(&entry(b"a", 10, &[1, 10]))
                .await
                .expect("new job drops below advanced anchor"),
            CompactionFilterDecision::Drop
        );
    }

    #[tokio::test]
    async fn malformed_or_out_of_order_physical_keys_abort_collection() {
        let stats = Arc::new(MvccHistoryFilterStats::default());
        let mut filter = MvccHistoryFilter::new(
            Version::new(10),
            Arc::clone(&stats),
            MvccHistoryFilterMode::Correct,
            None,
            None,
        );
        let malformed = RowEntry {
            key: Bytes::from_static(&[1, b'a', 0]),
            value: ValueDeletable::Value(Bytes::from_static(&[1, 1])),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        };
        assert!(filter.filter(&malformed).await.is_err());

        let mut ordered = MvccHistoryFilter::new(
            Version::new(10),
            Arc::clone(&stats),
            MvccHistoryFilterMode::Correct,
            None,
            None,
        );
        ordered
            .filter(&entry(b"a", 5, &[1, 5]))
            .await
            .expect("first entry");
        assert!(ordered.filter(&entry(b"a", 6, &[1, 6])).await.is_err());
        assert_eq!(stats.snapshot().malformed_entries, 2);
    }
}
