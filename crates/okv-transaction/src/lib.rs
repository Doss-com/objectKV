//! Deterministic Cell v0 transaction authority.
//!
//! Consensus orders encoded commands. This state machine assigns an accepted
//! commit version from the applied log index, checks OCC conflict ranges, and
//! applies opaque key/value mutations atomically.

use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io;

const COMMAND_MAGIC_V1: &[u8] = b"OKVT1";
const COMMAND_MAGIC_V2: &[u8] = b"OKVT2";

#[derive(Deserialize, Serialize)]
struct WireKeyRangeV2 {
    start: String,
    end: String,
}

impl From<&KeyRange> for WireKeyRangeV2 {
    fn from(range: &KeyRange) -> Self {
        Self {
            start: STANDARD_NO_PAD.encode(&range.start),
            end: STANDARD_NO_PAD.encode(&range.end),
        }
    }
}

impl TryFrom<WireKeyRangeV2> for KeyRange {
    type Error = serde_json::Error;

    fn try_from(range: WireKeyRangeV2) -> Result<Self, Self::Error> {
        Ok(Self {
            start: decode_wire_bytes(&range.start)?,
            end: decode_wire_bytes(&range.end)?,
        })
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WireMutationV2 {
    Set { key: String, value: String },
    Clear { key: String },
    ClearRange { range: WireKeyRangeV2 },
}

impl From<&Mutation> for WireMutationV2 {
    fn from(mutation: &Mutation) -> Self {
        match mutation {
            Mutation::Set { key, value } => Self::Set {
                key: STANDARD_NO_PAD.encode(key),
                value: STANDARD_NO_PAD.encode(value),
            },
            Mutation::Clear { key } => Self::Clear {
                key: STANDARD_NO_PAD.encode(key),
            },
            Mutation::ClearRange { range } => Self::ClearRange {
                range: range.into(),
            },
        }
    }
}

impl TryFrom<WireMutationV2> for Mutation {
    type Error = serde_json::Error;

    fn try_from(mutation: WireMutationV2) -> Result<Self, Self::Error> {
        match mutation {
            WireMutationV2::Set { key, value } => Ok(Self::Set {
                key: decode_wire_bytes(&key)?,
                value: decode_wire_bytes(&value)?,
            }),
            WireMutationV2::Clear { key } => Ok(Self::Clear {
                key: decode_wire_bytes(&key)?,
            }),
            WireMutationV2::ClearRange { range } => Ok(Self::ClearRange {
                range: range.try_into()?,
            }),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct WireTransactionCommandV2 {
    read_version: u64,
    read_conflicts: Vec<WireKeyRangeV2>,
    write_conflicts: Vec<WireKeyRangeV2>,
    mutations: Vec<WireMutationV2>,
}

impl From<&TransactionCommand> for WireTransactionCommandV2 {
    fn from(command: &TransactionCommand) -> Self {
        Self {
            read_version: command.read_version,
            read_conflicts: command.read_conflicts.iter().map(Into::into).collect(),
            write_conflicts: command.write_conflicts.iter().map(Into::into).collect(),
            mutations: command.mutations.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<WireTransactionCommandV2> for TransactionCommand {
    type Error = serde_json::Error;

    fn try_from(command: WireTransactionCommandV2) -> Result<Self, Self::Error> {
        Ok(Self {
            read_version: command.read_version,
            read_conflicts: command
                .read_conflicts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            write_conflicts: command
                .write_conflicts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            mutations: command
                .mutations
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

fn decode_wire_bytes(encoded: &str) -> Result<Vec<u8>, serde_json::Error> {
    STANDARD_NO_PAD.decode(encoded).map_err(|error| {
        serde_json::Error::io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid base64 transaction bytes: {error}"),
        ))
    })
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl KeyRange {
    #[must_use]
    pub fn point(key: &[u8]) -> Self {
        let mut end = key.to_vec();
        end.push(0);
        Self {
            start: key.to_vec(),
            end,
        }
    }

    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    #[must_use]
    pub fn contains_range(&self, other: &Self) -> bool {
        self.start <= other.start && other.end <= self.end
    }

    #[must_use]
    pub fn intersects(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
    }

    fn valid(&self) -> bool {
        self.start < self.end
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Mutation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Clear { key: Vec<u8> },
    ClearRange { range: KeyRange },
}

impl Mutation {
    fn range(&self) -> KeyRange {
        match self {
            Self::Set { key, .. } | Self::Clear { key } => KeyRange::point(key),
            Self::ClearRange { range } => range.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionCommand {
    pub read_version: u64,
    pub read_conflicts: Vec<KeyRange>,
    pub write_conflicts: Vec<KeyRange>,
    pub mutations: Vec<Mutation>,
}

impl TransactionCommand {
    /// Encode one versioned command for carriage inside a replicated client
    /// command.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = COMMAND_MAGIC_V2.to_vec();
        encoded.extend(serde_json::to_vec(&WireTransactionCommandV2::from(self))?);
        Ok(encoded)
    }

    /// Reconstruct the legacy v1 bytes for retry-fingerprint compatibility.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    #[doc(hidden)]
    pub fn encode_v1_for_compatibility(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = COMMAND_MAGIC_V1.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    /// Decode this command version, or return `None` for another payload type.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the magic matches but JSON is
    /// malformed.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        if let Some(encoded) = bytes.strip_prefix(COMMAND_MAGIC_V2) {
            return serde_json::from_slice::<WireTransactionCommandV2>(encoded)
                .and_then(TryInto::try_into)
                .map(Some);
        }
        bytes
            .strip_prefix(COMMAND_MAGIC_V1)
            .map(serde_json::from_slice)
            .transpose()
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionAuthorityFaults {
    pub accept_conflicts: bool,
    pub partial_apply: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionRejectReason {
    ConflictCoverage,
    FutureReadVersion,
    InvalidConflictRange,
    InvalidLogPosition,
    InvalidMutationOrder,
    ReadVersionExpired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TransactionStatus {
    Committed { commit_version: u64 },
    Conflict { conflicting_version: u64 },
    Rejected { reason: TransactionRejectReason },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionApplyResponse {
    pub applied_log_index: u64,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub batch_order: u16,
    pub status: TransactionStatus,
    pub applied_mutation_count: u64,
}

/// One accepted transaction retained for recovery consumers.
///
/// Versionstamps `(commit_version, batch_order)` are strictly increasing in a
/// stream but commit versions are not required to be numerically contiguous.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetainedTransactionRecord {
    pub commit_version: u64,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub batch_order: u16,
    pub command: TransactionCommand,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VersionedValue {
    pub version: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedConflict {
    pub version: u64,
    pub write_conflicts: Vec<KeyRange>,
}

/// Latest ordered values needed to serve the current database image.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionServingState {
    #[serde(with = "versioned_values")]
    values: BTreeMap<Vec<u8>, VersionedValue>,
}

/// Commit-version and OCC history needed to resolve admitted transactions.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionResolverState {
    current_version: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    conflict_retention_floor: u64,
    committed_conflicts: Vec<CommittedConflict>,
}

/// Cell v0 composition of independently retained serving and resolver state.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionAuthority {
    #[serde(flatten)]
    resolver: TransactionResolverState,
    #[serde(flatten)]
    serving: TransactionServingState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionAuthorityView {
    pub current_version: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub conflict_retention_floor: u64,
    #[serde(with = "versioned_values")]
    pub values: BTreeMap<Vec<u8>, VersionedValue>,
    pub retained_conflict_versions: u64,
}

/// Rejection produced while advancing the minimum admitted read version.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictRetentionError {
    FutureFloor,
    FloorRegression,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero_u16(value: &u16) -> bool {
    *value == 0
}

mod versioned_values {
    use super::VersionedValue;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    #[derive(Deserialize)]
    struct Entry {
        key: Vec<u8>,
        value: VersionedValue,
    }

    #[derive(Serialize)]
    struct EntryRef<'a> {
        key: &'a [u8],
        value: &'a VersionedValue,
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Representation {
        Ordered(Vec<Entry>),
        LegacyEmpty(BTreeMap<String, VersionedValue>),
    }

    pub fn serialize<S>(
        values: &BTreeMap<Vec<u8>, VersionedValue>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        values
            .iter()
            .map(|(key, value)| EntryRef { key, value })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<Vec<u8>, VersionedValue>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Representation::deserialize(deserializer)? {
            Representation::Ordered(entries) => {
                let mut values = BTreeMap::new();
                for entry in entries {
                    if values
                        .last_key_value()
                        .is_some_and(|(prior, _)| prior >= &entry.key)
                    {
                        return Err(serde::de::Error::custom(
                            "transaction value keys must be strictly increasing",
                        ));
                    }
                    values.insert(entry.key, entry.value);
                }
                Ok(values)
            }
            Representation::LegacyEmpty(values) if values.is_empty() => Ok(BTreeMap::new()),
            Representation::LegacyEmpty(_) => Err(serde::de::Error::custom(
                "legacy transaction value map must be empty",
            )),
        }
    }
}

impl TransactionAuthority {
    #[must_use]
    pub const fn current_version(&self) -> u64 {
        self.resolver.current_version
    }

    #[must_use]
    pub const fn conflict_retention_floor(&self) -> u64 {
        self.resolver.conflict_retention_floor
    }

    #[must_use]
    pub const fn serving(&self) -> &TransactionServingState {
        &self.serving
    }

    #[must_use]
    pub const fn resolver(&self) -> &TransactionResolverState {
        &self.resolver
    }

    #[must_use]
    pub fn view(&self) -> TransactionAuthorityView {
        TransactionAuthorityView {
            current_version: self.resolver.current_version,
            conflict_retention_floor: self.resolver.conflict_retention_floor,
            values: self.serving.values.clone(),
            retained_conflict_versions: u64::try_from(self.resolver.committed_conflicts.len())
                .unwrap_or(u64::MAX),
        }
    }

    /// Validate a new minimum admitted read version without changing state.
    ///
    /// # Errors
    ///
    /// Returns a typed rejection when the floor regresses or exceeds the
    /// latest committed transaction version.
    pub const fn validate_conflict_retention_floor(
        &self,
        new_floor: u64,
    ) -> Result<(), ConflictRetentionError> {
        if new_floor < self.resolver.conflict_retention_floor {
            Err(ConflictRetentionError::FloorRegression)
        } else if new_floor > self.resolver.current_version {
            Err(ConflictRetentionError::FutureFloor)
        } else {
            Ok(())
        }
    }

    /// Advance the minimum admitted read version and reclaim obsolete OCC
    /// history.
    ///
    /// # Errors
    ///
    /// Returns a typed rejection when the floor is not a valid monotonic
    /// transition.
    pub fn advance_conflict_retention_floor(
        &mut self,
        new_floor: u64,
    ) -> Result<u64, ConflictRetentionError> {
        self.validate_conflict_retention_floor(new_floor)?;
        let before = self.resolver.committed_conflicts.len();
        self.resolver
            .committed_conflicts
            .retain(|committed| committed.version > new_floor);
        self.resolver.conflict_retention_floor = new_floor;
        Ok(
            u64::try_from(before.saturating_sub(self.resolver.committed_conflicts.len()))
                .unwrap_or(u64::MAX),
        )
    }

    #[must_use]
    pub fn apply(
        &mut self,
        applied_log_index: u64,
        command: &TransactionCommand,
        faults: TransactionAuthorityFaults,
    ) -> TransactionApplyResponse {
        self.apply_ordered(applied_log_index, 0, false, command, faults)
    }

    /// Apply one transaction inside an ordered transaction batch.
    ///
    /// Transactions in a batch share `applied_log_index` as their snapshot
    /// commit version. `batch_order` makes their versionstamps unique and lets
    /// later items conflict with earlier accepted items.
    #[must_use]
    pub fn apply_in_batch(
        &mut self,
        applied_log_index: u64,
        batch_order: u16,
        command: &TransactionCommand,
        faults: TransactionAuthorityFaults,
    ) -> TransactionApplyResponse {
        self.apply_ordered(applied_log_index, batch_order, true, command, faults)
    }

    fn apply_ordered(
        &mut self,
        applied_log_index: u64,
        batch_order: u16,
        allow_current_version: bool,
        command: &TransactionCommand,
        faults: TransactionAuthorityFaults,
    ) -> TransactionApplyResponse {
        if applied_log_index == 0
            || applied_log_index < self.resolver.current_version
            || (applied_log_index == self.resolver.current_version && !allow_current_version)
        {
            return rejected(
                applied_log_index,
                batch_order,
                TransactionRejectReason::InvalidLogPosition,
            );
        }
        if command.read_version < self.resolver.conflict_retention_floor {
            return rejected(
                applied_log_index,
                batch_order,
                TransactionRejectReason::ReadVersionExpired,
            );
        }
        if command.read_version > self.resolver.current_version {
            return rejected(
                applied_log_index,
                batch_order,
                TransactionRejectReason::FutureReadVersion,
            );
        }
        if !canonical_ranges(&command.read_conflicts) || !canonical_ranges(&command.write_conflicts)
        {
            return rejected(
                applied_log_index,
                batch_order,
                TransactionRejectReason::InvalidConflictRange,
            );
        }
        if !canonical_mutations(&command.mutations) {
            return rejected(
                applied_log_index,
                batch_order,
                TransactionRejectReason::InvalidMutationOrder,
            );
        }
        if !mutations_covered(&command.mutations, &command.write_conflicts) {
            return rejected(
                applied_log_index,
                batch_order,
                TransactionRejectReason::ConflictCoverage,
            );
        }

        if let Some(conflicting_version) = self.first_conflict(command) {
            if !faults.accept_conflicts {
                return TransactionApplyResponse {
                    applied_log_index,
                    batch_order,
                    status: TransactionStatus::Conflict {
                        conflicting_version,
                    },
                    applied_mutation_count: 0,
                };
            }
        }

        let apply_count = if faults.partial_apply && command.mutations.len() > 1 {
            1
        } else {
            command.mutations.len()
        };
        for mutation in command.mutations.iter().take(apply_count) {
            self.serving.apply_mutation(applied_log_index, mutation);
        }
        self.resolver.current_version = applied_log_index;
        self.resolver.committed_conflicts.push(CommittedConflict {
            version: applied_log_index,
            write_conflicts: command.write_conflicts.clone(),
        });
        TransactionApplyResponse {
            applied_log_index,
            batch_order,
            status: TransactionStatus::Committed {
                commit_version: applied_log_index,
            },
            applied_mutation_count: u64::try_from(apply_count).unwrap_or(u64::MAX),
        }
    }

    fn first_conflict(&self, command: &TransactionCommand) -> Option<u64> {
        if command.read_conflicts.is_empty() {
            return None;
        }
        self.resolver
            .committed_conflicts
            .iter()
            .filter(|committed| committed.version > command.read_version)
            .find(|committed| {
                committed.write_conflicts.iter().any(|write| {
                    command
                        .read_conflicts
                        .iter()
                        .any(|read| read.intersects(write))
                })
            })
            .map(|committed| committed.version)
    }
}

impl TransactionServingState {
    fn apply_mutation(&mut self, version: u64, mutation: &Mutation) {
        match mutation {
            Mutation::Set { key, value } => {
                self.values.insert(
                    key.clone(),
                    VersionedValue {
                        version,
                        value: value.clone(),
                    },
                );
            }
            Mutation::Clear { key } => {
                self.values.remove(key);
            }
            Mutation::ClearRange { range } => {
                let keys: Vec<Vec<u8>> = self
                    .values
                    .range(range.start.clone()..range.end.clone())
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in keys {
                    self.values.remove(&key);
                }
            }
        }
    }
}

fn rejected(
    applied_log_index: u64,
    batch_order: u16,
    reason: TransactionRejectReason,
) -> TransactionApplyResponse {
    TransactionApplyResponse {
        applied_log_index,
        batch_order,
        status: TransactionStatus::Rejected { reason },
        applied_mutation_count: 0,
    }
}

fn canonical_ranges(ranges: &[KeyRange]) -> bool {
    ranges.iter().all(KeyRange::valid)
        && ranges
            .windows(2)
            .all(|window| window[0].end <= window[1].start)
}

fn canonical_mutations(mutations: &[Mutation]) -> bool {
    let ranges: Vec<KeyRange> = mutations.iter().map(Mutation::range).collect();
    let mut starts = BTreeSet::new();
    ranges
        .iter()
        .all(|range| range.valid() && starts.insert(&range.start))
        && ranges
            .windows(2)
            .all(|window| window[0].end <= window[1].start)
}

fn mutations_covered(mutations: &[Mutation], write_conflicts: &[KeyRange]) -> bool {
    mutations.iter().all(|mutation| {
        let required = mutation.range();
        write_conflicts
            .iter()
            .any(|declared| declared.contains_range(&required))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> Vec<u8> {
        name.as_bytes().to_vec()
    }

    fn set(name: &str, value: u8) -> Mutation {
        Mutation::Set {
            key: key(name),
            value: vec![value],
        }
    }

    fn command(read_version: u64, names: &[&str]) -> TransactionCommand {
        let mut conflicts: Vec<KeyRange> = names
            .iter()
            .map(|name| KeyRange::point(name.as_bytes()))
            .collect();
        conflicts.sort();
        let mut mutations: Vec<Mutation> = names
            .iter()
            .enumerate()
            .map(|(index, name)| set(name, u8::try_from(index + 1).unwrap_or(u8::MAX)))
            .collect();
        mutations.sort_by_key(Mutation::range);
        TransactionCommand {
            read_version,
            read_conflicts: conflicts.clone(),
            write_conflicts: conflicts,
            mutations,
        }
    }

    #[test]
    fn assigns_log_index_and_applies_multi_range_atomically() {
        let mut authority = TransactionAuthority::default();
        let response = authority.apply(
            7,
            &command(0, &["a/account", "z/account"]),
            TransactionAuthorityFaults::default(),
        );
        assert_eq!(
            response.status,
            TransactionStatus::Committed { commit_version: 7 }
        );
        assert_eq!(response.applied_mutation_count, 2);
        assert_eq!(authority.current_version(), 7);
        assert_eq!(authority.view().values.len(), 2);
    }

    #[test]
    fn non_empty_authority_and_view_round_trip_through_json() {
        let mut authority = TransactionAuthority::default();
        let _ = authority.apply(
            7,
            &command(0, &["a/account", "z/account"]),
            TransactionAuthorityFaults::default(),
        );

        let encoded_authority = serde_json::to_vec(&authority).expect("encode authority");
        assert_eq!(
            encoded_authority,
            include_bytes!("../fixtures/transaction-authority-v2.json")
                .strip_suffix(b"\n")
                .unwrap_or(include_bytes!("../fixtures/transaction-authority-v2.json"))
        );
        let decoded_authority: TransactionAuthority =
            serde_json::from_slice(&encoded_authority).expect("decode authority");
        assert_eq!(decoded_authority, authority);
        assert_eq!(
            serde_json::from_slice::<TransactionAuthority>(include_bytes!(
                "../fixtures/transaction-authority-v1.json"
            ))
            .expect("decode pre-split authority"),
            authority
        );

        let view = authority.view();
        let encoded_view = serde_json::to_vec(&view).expect("encode authority view");
        let decoded_view: TransactionAuthorityView =
            serde_json::from_slice(&encoded_view).expect("decode authority view");
        assert_eq!(decoded_view, view);

        let legacy_empty = br#"{"current_version":0,"values":{},"committed_conflicts":[]}"#;
        assert_eq!(
            serde_json::from_slice::<TransactionAuthority>(legacy_empty)
                .expect("decode initial empty-map authority"),
            TransactionAuthority::default()
        );
        let unordered = br#"{"current_version":1,"values":[{"key":[98],"value":{"version":1,"value":[]}},{"key":[97],"value":{"version":1,"value":[]}}],"committed_conflicts":[]}"#;
        assert!(serde_json::from_slice::<TransactionAuthority>(unordered).is_err());
    }

    #[test]
    fn conflict_frontier_reclaims_history_and_rejects_stale_reads() {
        let mut authority = TransactionAuthority::default();
        let first = command(0, &["a"]);
        assert!(matches!(
            authority
                .apply(7, &first, TransactionAuthorityFaults::default())
                .status,
            TransactionStatus::Committed { .. }
        ));
        assert_eq!(authority.advance_conflict_retention_floor(7), Ok(1));
        assert_eq!(authority.conflict_retention_floor(), 7);
        assert_eq!(authority.view().retained_conflict_versions, 0);
        assert_eq!(
            authority
                .apply(8, &first, TransactionAuthorityFaults::default())
                .status,
            TransactionStatus::Rejected {
                reason: TransactionRejectReason::ReadVersionExpired
            }
        );
        assert_eq!(authority.current_version(), 7);
        assert_eq!(
            authority
                .view()
                .values
                .get(b"a".as_slice())
                .map(|v| v.version),
            Some(7)
        );
        assert_eq!(
            authority.validate_conflict_retention_floor(8),
            Err(ConflictRetentionError::FutureFloor)
        );
        assert_eq!(
            authority.validate_conflict_retention_floor(6),
            Err(ConflictRetentionError::FloorRegression)
        );
    }

    #[test]
    fn rejects_point_and_range_conflicts() {
        let mut authority = TransactionAuthority::default();
        let first = command(0, &["a/account", "z/account"]);
        assert!(matches!(
            authority
                .apply(5, &first, TransactionAuthorityFaults::default())
                .status,
            TransactionStatus::Committed { .. }
        ));
        assert_eq!(
            authority
                .apply(6, &first, TransactionAuthorityFaults::default())
                .status,
            TransactionStatus::Conflict {
                conflicting_version: 5
            }
        );

        let range = KeyRange {
            start: key("m/unique/"),
            end: key("m/unique0"),
        };
        let phantom = TransactionCommand {
            read_version: 5,
            read_conflicts: vec![range.clone()],
            write_conflicts: vec![KeyRange::point(b"m/unique/one")],
            mutations: vec![set("m/unique/one", 1)],
        };
        assert!(matches!(
            authority
                .apply(7, &phantom, TransactionAuthorityFaults::default())
                .status,
            TransactionStatus::Committed { .. }
        ));
        let second = TransactionCommand {
            read_version: 5,
            read_conflicts: vec![range],
            write_conflicts: vec![KeyRange::point(b"m/unique/two")],
            mutations: vec![set("m/unique/two", 2)],
        };
        assert_eq!(
            authority
                .apply(8, &second, TransactionAuthorityFaults::default())
                .status,
            TransactionStatus::Conflict {
                conflicting_version: 7
            }
        );
    }

    #[test]
    fn rejects_future_versions_and_incomplete_write_coverage() {
        let mut authority = TransactionAuthority::default();
        assert_eq!(
            authority
                .apply(
                    1,
                    &command(9, &["a"]),
                    TransactionAuthorityFaults::default(),
                )
                .status,
            TransactionStatus::Rejected {
                reason: TransactionRejectReason::FutureReadVersion
            }
        );
        let mut uncovered = command(0, &["a"]);
        uncovered.write_conflicts.clear();
        assert_eq!(
            authority
                .apply(2, &uncovered, TransactionAuthorityFaults::default())
                .status,
            TransactionStatus::Rejected {
                reason: TransactionRejectReason::ConflictCoverage
            }
        );
    }

    #[test]
    fn poison_faults_are_visible_in_state_and_response() {
        let mut authority = TransactionAuthority::default();
        let transaction = command(0, &["a", "z"]);
        let _ = authority.apply(1, &transaction, TransactionAuthorityFaults::default());
        let conflict = authority.apply(
            2,
            &transaction,
            TransactionAuthorityFaults {
                accept_conflicts: true,
                partial_apply: true,
            },
        );
        assert!(matches!(
            conflict.status,
            TransactionStatus::Committed { commit_version: 2 }
        ));
        assert_eq!(conflict.applied_mutation_count, 1);
    }

    #[test]
    fn ordered_batch_items_share_version_and_conflict_in_batch_order() {
        let mut authority = TransactionAuthority::default();
        let first = command(0, &["a"]);
        let second = TransactionCommand {
            read_version: 0,
            read_conflicts: vec![KeyRange::point(b"a")],
            write_conflicts: vec![KeyRange::point(b"b")],
            mutations: vec![Mutation::Set {
                key: b"b".to_vec(),
                value: b"b".to_vec(),
            }],
        };
        let first_response =
            authority.apply_in_batch(7, 0, &first, TransactionAuthorityFaults::default());
        let second_response =
            authority.apply_in_batch(7, 1, &second, TransactionAuthorityFaults::default());
        assert_eq!(first_response.batch_order, 0);
        assert_eq!(
            first_response.status,
            TransactionStatus::Committed { commit_version: 7 }
        );
        assert_eq!(second_response.batch_order, 1);
        assert_eq!(
            second_response.status,
            TransactionStatus::Conflict {
                conflicting_version: 7
            }
        );
    }

    #[test]
    fn command_codec_rejects_other_payloads_and_round_trips() {
        let command = command(7, &["a", "z"]);
        let encoded = command.encode().expect("encode");
        assert_eq!(
            TransactionCommand::decode(&encoded).expect("decode"),
            Some(command)
        );
        assert_eq!(TransactionCommand::decode(b"opaque").expect("decode"), None);
    }

    #[test]
    fn frozen_v1_command_fixture_remains_readable() {
        let fixture = decode_hex(include_str!("../fixtures/transaction-command-v1.hex"));
        let expected = command(7, &["a", "z"]);
        assert_eq!(
            TransactionCommand::decode(&fixture).expect("decode fixture"),
            Some(expected.clone())
        );
        assert!(expected.encode().expect("encode v2").starts_with(b"OKVT2"));
    }

    #[test]
    fn frozen_v2_command_fixture_is_byte_exact() {
        let fixture = decode_hex(include_str!("../fixtures/transaction-command-v2.hex"));
        let expected = command(7, &["a", "z"]);
        assert_eq!(
            TransactionCommand::decode(&fixture).expect("decode fixture"),
            Some(expected.clone())
        );
        assert_eq!(expected.encode().expect("encode fixture"), fixture);
    }

    #[test]
    fn malformed_v2_bytes_fail_closed() {
        let malformed = br#"OKVT2{"read_version":0,"read_conflicts":[],"write_conflicts":[],"mutations":[{"kind":"clear","key":"!"}]}"#;
        assert!(TransactionCommand::decode(malformed).is_err());
    }

    #[test]
    fn frozen_v1_retained_record_fixture_is_byte_exact() {
        let expected = RetainedTransactionRecord {
            commit_version: 7,
            batch_order: 0,
            command: command(0, &["a", "z"]),
        };
        let fixture = include_bytes!("../fixtures/retained-transaction-record-v1.json")
            .strip_suffix(b"\n")
            .unwrap_or(include_bytes!(
                "../fixtures/retained-transaction-record-v1.json"
            ));
        assert_eq!(
            serde_json::to_vec(&expected).expect("encode record"),
            fixture
        );
        assert_eq!(
            serde_json::from_slice::<RetainedTransactionRecord>(fixture)
                .expect("decode retained record"),
            expected
        );
    }

    fn decode_hex(encoded: &str) -> Vec<u8> {
        let encoded = encoded.trim();
        assert_eq!(encoded.len() % 2, 0);
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = digit(pair[0]);
                let low = digit(pair[1]);
                (high << 4) | low
            })
            .collect()
    }

    fn digit(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("invalid hex digit"),
        }
    }
}
