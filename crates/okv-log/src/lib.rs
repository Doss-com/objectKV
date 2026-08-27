//! Ordered opaque-record semantics shared by objectKV durability adapters.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::ops::RangeBounds;

/// One opaque record at a partition-local logical index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEntry {
    pub index: u64,
    pub payload: Vec<u8>,
}

impl LogEntry {
    /// Construct an entry without interpreting its payload.
    #[must_use]
    pub fn new(index: u64, payload: impl AsRef<[u8]>) -> Self {
        Self {
            index,
            payload: payload.as_ref().to_vec(),
        }
    }
}

/// Durable identity for the greatest purged prefix position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgeMarker {
    pub index: u64,
    pub payload: Vec<u8>,
}

impl PurgeMarker {
    /// Construct a marker without interpreting its payload.
    #[must_use]
    pub fn new(index: u64, payload: impl AsRef<[u8]>) -> Self {
        Self {
            index,
            payload: payload.as_ref().to_vec(),
        }
    }
}

/// One deterministic ordered-log transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogCommand {
    Append(LogEntry),
    TruncateSuffix { from: u64 },
    PurgePrefix(PurgeMarker),
}

/// Invalid ordered-log history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogError {
    NonConsecutive { expected: u64, actual: u64 },
    IndexExhausted { at: u64 },
    InvalidRange { from: u64, to: u64 },
    PositionExpired { requested: u64, oldest: u64 },
    TruncatePurged { from: u64, purged: u64 },
    PurgeRegression { current: u64, proposed: u64 },
    ConflictingPurge { index: u64 },
}

impl Display for LogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonConsecutive { expected, actual } => write!(
                formatter,
                "non-consecutive log append: expected index {expected}, received {actual}"
            ),
            Self::IndexExhausted { at } => write!(formatter, "log index exhausted at {at}"),
            Self::InvalidRange { from, to } => {
                write!(formatter, "invalid log range [{from}, {to})")
            }
            Self::PositionExpired { requested, oldest } => write!(
                formatter,
                "log position {requested} expired; oldest retained position is {oldest}"
            ),
            Self::TruncatePurged { from, purged } => write!(
                formatter,
                "cannot truncate from {from} through already purged index {purged}"
            ),
            Self::PurgeRegression { current, proposed } => {
                write!(
                    formatter,
                    "purge index regressed from {current} to {proposed}"
                )
            }
            Self::ConflictingPurge { index } => {
                write!(formatter, "purge marker changed at index {index}")
            }
        }
    }
}

impl Error for LogError {}

/// Pure retained state for one ordered-log partition.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LogState {
    last_purged: Option<PurgeMarker>,
    entries: BTreeMap<u64, Vec<u8>>,
}

impl LogState {
    /// Greatest purged position and its consumer-defined identity.
    #[must_use]
    pub const fn last_purged(&self) -> Option<&PurgeMarker> {
        self.last_purged.as_ref()
    }

    /// Last retained entry, if any.
    #[must_use]
    pub fn last_entry(&self) -> Option<(u64, &[u8])> {
        self.entries
            .last_key_value()
            .map(|(index, payload)| (*index, payload.as_slice()))
    }

    /// Plan a consecutive append against the current retained suffix.
    ///
    /// # Errors
    ///
    /// Returns an error when the proposed batch contains or creates a gap.
    pub fn plan_suffix_append(&self, entries: &[LogEntry]) -> Result<Vec<LogCommand>, LogError> {
        let entries = if let Some(marker) = self.last_purged.as_ref() {
            let first_live = entries
                .iter()
                .position(|entry| entry.index > marker.index)
                .unwrap_or(entries.len());
            &entries[first_live..]
        } else {
            entries
        };
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let first = entries[0].index;
        let mut expected = first;
        for (position, entry) in entries.iter().enumerate() {
            if entry.index != expected {
                return Err(LogError::NonConsecutive {
                    expected,
                    actual: entry.index,
                });
            }
            if position + 1 < entries.len() {
                expected = expected
                    .checked_add(1)
                    .ok_or(LogError::IndexExhausted { at: entry.index })?;
            }
        }

        let mut commands = Vec::with_capacity(entries.len().saturating_add(1));
        if let Some(last) = self.entries.last_key_value().map(|(index, _)| *index) {
            if first <= last {
                commands.push(LogCommand::TruncateSuffix { from: first });
            } else {
                let expected = last
                    .checked_add(1)
                    .ok_or(LogError::IndexExhausted { at: last })?;
                if first != expected {
                    return Err(LogError::NonConsecutive {
                        expected,
                        actual: first,
                    });
                }
            }
        } else if let Some(marker) = self.last_purged.as_ref() {
            let expected = marker
                .index
                .checked_add(1)
                .ok_or(LogError::IndexExhausted { at: marker.index })?;
            if first != expected {
                return Err(LogError::NonConsecutive {
                    expected,
                    actual: first,
                });
            }
        }
        commands.extend(entries.iter().cloned().map(LogCommand::Append));
        Ok(commands)
    }

    /// Apply a command sequence atomically to this in-memory state.
    ///
    /// # Errors
    ///
    /// Returns an error without changing this state when any command is
    /// invalid.
    pub fn apply_all(&mut self, commands: &[LogCommand]) -> Result<(), LogError> {
        let mut next = self.clone();
        for command in commands {
            next.apply(command.clone())?;
        }
        *self = next;
        Ok(())
    }

    /// Copy retained entries in a requested compatibility range.
    #[must_use]
    pub fn entries_clamped<R>(&self, range: R) -> Vec<LogEntry>
    where
        R: RangeBounds<u64>,
    {
        self.entries
            .range(range)
            .map(|(index, payload)| LogEntry::new(*index, payload))
            .collect()
    }

    /// Read one exact half-open retained range.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::PositionExpired`] instead of silently clamping a
    /// request that begins in the purged prefix.
    pub fn entries_exact(&self, from: u64, to: u64) -> Result<Vec<LogEntry>, LogError> {
        if from > to {
            return Err(LogError::InvalidRange { from, to });
        }
        if let Some(marker) = self.last_purged.as_ref() {
            if from <= marker.index {
                return Err(LogError::PositionExpired {
                    requested: from,
                    oldest: marker.index.saturating_add(1),
                });
            }
        }
        Ok(self.entries_clamped(from..to))
    }

    fn apply(&mut self, command: LogCommand) -> Result<(), LogError> {
        match command {
            LogCommand::Append(entry) => {
                let frontier = self
                    .entries
                    .last_key_value()
                    .map(|(index, _)| *index)
                    .or_else(|| self.last_purged.as_ref().map(|marker| marker.index));
                if let Some(frontier) = frontier {
                    let expected = frontier
                        .checked_add(1)
                        .ok_or(LogError::IndexExhausted { at: frontier })?;
                    if entry.index != expected {
                        return Err(LogError::NonConsecutive {
                            expected,
                            actual: entry.index,
                        });
                    }
                }
                self.entries.insert(entry.index, entry.payload);
            }
            LogCommand::TruncateSuffix { from } => {
                if let Some(marker) = self.last_purged.as_ref() {
                    if from <= marker.index {
                        return Err(LogError::TruncatePurged {
                            from,
                            purged: marker.index,
                        });
                    }
                }
                self.entries.split_off(&from);
            }
            LogCommand::PurgePrefix(marker) => {
                if let Some(current) = self.last_purged.as_ref() {
                    if marker.index < current.index {
                        return Err(LogError::PurgeRegression {
                            current: current.index,
                            proposed: marker.index,
                        });
                    }
                    if marker.index == current.index && marker.payload != current.payload {
                        return Err(LogError::ConflictingPurge {
                            index: marker.index,
                        });
                    }
                }
                self.entries.retain(|index, _| *index > marker.index);
                self.last_purged = Some(marker);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_append_establishes_an_arbitrary_base() {
        let state = LogState::default();
        let entries = vec![LogEntry::new(7, b"seven"), LogEntry::new(8, b"eight")];

        let commands = state.plan_suffix_append(&entries).unwrap();
        let mut applied = state;
        applied.apply_all(&commands).unwrap();

        assert_eq!(applied.entries_clamped(..), entries);
    }

    #[test]
    fn overlapping_append_plans_prefix_closed_suffix_replacement() {
        let mut state = LogState::default();
        state
            .apply_all(&[
                LogCommand::Append(LogEntry::new(7, b"seven")),
                LogCommand::Append(LogEntry::new(8, b"eight-old")),
                LogCommand::Append(LogEntry::new(9, b"nine-old")),
            ])
            .unwrap();
        let replacement = vec![
            LogEntry::new(8, b"eight-new"),
            LogEntry::new(9, b"nine-new"),
            LogEntry::new(10, b"ten"),
        ];

        let commands = state.plan_suffix_append(&replacement).unwrap();
        assert_eq!(commands[0], LogCommand::TruncateSuffix { from: 8 });

        for prefix_len in 1..=commands.len() {
            let mut prefix_state = state.clone();
            prefix_state.apply_all(&commands[..prefix_len]).unwrap();
        }
        state.apply_all(&commands).unwrap();
        assert_eq!(
            state.entries_clamped(..),
            vec![LogEntry::new(7, b"seven")]
                .into_iter()
                .chain(replacement)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn purge_supports_clamped_and_fail_closed_reads() {
        let mut state = LogState::default();
        state
            .apply_all(&[
                LogCommand::Append(LogEntry::new(0, b"zero")),
                LogCommand::Append(LogEntry::new(1, b"one")),
                LogCommand::Append(LogEntry::new(2, b"two")),
                LogCommand::PurgePrefix(PurgeMarker::new(1, b"marker-one")),
            ])
            .unwrap();

        assert_eq!(
            state.last_purged(),
            Some(&PurgeMarker::new(1, b"marker-one"))
        );
        assert_eq!(state.entries_clamped(0..3), vec![LogEntry::new(2, b"two")]);
        assert_eq!(
            state.entries_exact(0, 3),
            Err(LogError::PositionExpired {
                requested: 0,
                oldest: 2,
            })
        );
        assert_eq!(
            state.entries_exact(2, 3).unwrap(),
            vec![LogEntry::new(2, b"two")]
        );
    }

    #[test]
    fn append_planner_filters_purged_prefix_before_replacing_live_suffix() {
        let mut state = LogState::default();
        state
            .apply_all(&[
                LogCommand::PurgePrefix(PurgeMarker::new(1, b"marker-one")),
                LogCommand::Append(LogEntry::new(2, b"two-old")),
            ])
            .unwrap();

        assert!(state
            .plan_suffix_append(&[LogEntry::new(0, b"zero"), LogEntry::new(1, b"one")])
            .unwrap()
            .is_empty());

        let commands = state
            .plan_suffix_append(&[
                LogEntry::new(0, b"zero"),
                LogEntry::new(1, b"one"),
                LogEntry::new(2, b"two-new"),
                LogEntry::new(3, b"three"),
            ])
            .unwrap();
        assert_eq!(
            commands,
            vec![
                LogCommand::TruncateSuffix { from: 2 },
                LogCommand::Append(LogEntry::new(2, b"two-new")),
                LogCommand::Append(LogEntry::new(3, b"three")),
            ]
        );
    }

    #[test]
    fn truncate_and_purge_edges_fail_closed_without_partial_state() {
        let marker = PurgeMarker::new(1, b"marker-one");
        let mut state = LogState::default();
        state
            .apply_all(&[
                LogCommand::PurgePrefix(marker.clone()),
                LogCommand::Append(LogEntry::new(2, b"two")),
            ])
            .unwrap();
        let before_error = state.clone();

        assert_eq!(
            state.apply_all(&[LogCommand::TruncateSuffix { from: 1 }]),
            Err(LogError::TruncatePurged { from: 1, purged: 1 })
        );
        assert_eq!(state, before_error);
        state
            .apply_all(&[LogCommand::TruncateSuffix { from: 99 }])
            .unwrap();
        state
            .apply_all(&[LogCommand::PurgePrefix(marker.clone())])
            .unwrap();
        assert_eq!(
            state.apply_all(&[LogCommand::PurgePrefix(PurgeMarker::new(1, b"different"))]),
            Err(LogError::ConflictingPurge { index: 1 })
        );
        assert_eq!(
            state.apply_all(&[LogCommand::PurgePrefix(PurgeMarker::new(0, b"zero"))]),
            Err(LogError::PurgeRegression {
                current: 1,
                proposed: 0,
            })
        );

        state
            .apply_all(&[LogCommand::PurgePrefix(PurgeMarker::new(10, b"ten"))])
            .unwrap();
        assert_eq!(state.last_purged(), Some(&PurgeMarker::new(10, b"ten")));
        assert!(state.entries_clamped(..).is_empty());
    }

    #[test]
    fn fresh_base_is_consumer_selected_but_following_indexes_are_consecutive() {
        for base in [0, 1, 7] {
            let mut state = LogState::default();
            let commands = state
                .plan_suffix_append(&[LogEntry::new(base, b"base")])
                .unwrap();
            state.apply_all(&commands).unwrap();

            assert_eq!(
                state.plan_suffix_append(&[LogEntry::new(base + 2, b"gap")]),
                Err(LogError::NonConsecutive {
                    expected: base + 1,
                    actual: base + 2,
                })
            );
        }
    }
}
