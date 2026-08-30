//! Executable trace-conformance checker for the TLA+ cell reference model.
//!
//! This checker does not replace TLC. It gives Rust and infrastructure runs a
//! stable event vocabulary and rejects observed orderings that do not conform
//! to the named actions in `formal/ObjectKVCell.tla`.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Exact TLA+ model content checked when trace schema v1 was frozen.
pub const OBJECT_KV_CELL_TLA_SHA256: &str =
    "55d5bb137b9e3c37deace42f92b4602b022a7583b0a23a801ef707f40618a3ba";

const TRACE_SCHEMA_VERSION: u32 = 1;

/// Finite cell parameters required to replay one implementation trace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CellTraceConfigV1 {
    pub nodes: BTreeSet<String>,
    pub quorum: usize,
    pub max_media_failures: usize,
}

impl CellTraceConfigV1 {
    /// Construct a trace configuration with quorum intersection.
    ///
    /// # Errors
    ///
    /// Rejects empty node sets, invalid quorums, duplicate or empty node IDs,
    /// and failure budgets that can remove every node.
    pub fn new(
        nodes: impl IntoIterator<Item = String>,
        quorum: usize,
        max_media_failures: usize,
    ) -> Result<Self, CellTraceViolationV1> {
        let supplied = nodes.into_iter().collect::<Vec<_>>();
        let unique = supplied.iter().cloned().collect::<BTreeSet<_>>();
        if supplied.len() != unique.len() {
            return Err(CellTraceViolationV1::configuration(
                "trace configuration requires unique node IDs",
            ));
        }
        let config = Self {
            nodes: unique,
            quorum,
            max_media_failures,
        };
        config.validate()?;
        Ok(config)
    }

    /// Revalidate the TLA+ constant assumptions after deserialization.
    ///
    /// # Errors
    ///
    /// Rejects empty node sets or IDs, non-intersecting quorums, and failure
    /// budgets that can remove every node.
    pub fn validate(&self) -> Result<(), CellTraceViolationV1> {
        if self.nodes.is_empty()
            || self.nodes.iter().any(String::is_empty)
            || self.quorum == 0
            || self.quorum > self.nodes.len()
            || self.quorum.saturating_mul(2) <= self.nodes.len()
            || self.max_media_failures >= self.nodes.len()
        {
            return Err(CellTraceViolationV1::configuration(
                "trace configuration requires non-empty nodes, an intersecting quorum, and a bounded media-failure budget",
            ));
        }
        Ok(())
    }
}

/// Serving image selected by one `HydrateServingImage` action.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellServingTierV1 {
    Ram,
    Nvme,
    Rocks,
}

/// Stable event names matching actions in `formal/ObjectKVCell.tla`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CellTraceEventV1 {
    Begin {
        transaction: String,
    },
    SequenceTxn {
        transaction: String,
        version: u64,
    },
    StageInRam {
        transaction: String,
        node: String,
    },
    PersistOnStableMedia {
        transaction: String,
        node: String,
    },
    ReturnBuffered {
        transaction: String,
    },
    CommitTxn {
        transaction: String,
    },
    RejectConflict {
        transaction: String,
    },
    DeliverCommitted {
        transaction: String,
    },
    BuildObjectClosure {
        through: u64,
    },
    PrepareObjectFrontier {
        through: u64,
    },
    PopTxLogThroughPending,
    ActivateObjectFrontier,
    AdvanceGeneration,
    InstallGeneration {
        node: String,
    },
    LoseRam {
        node: String,
    },
    LoseStableMedium {
        node: String,
    },
    HydrateServingImage {
        node: String,
        tier: CellServingTierV1,
        through: u64,
    },
    ServeRead {
        node: String,
    },
}

impl CellTraceEventV1 {
    /// Return the exact reference action name.
    #[must_use]
    pub const fn action(&self) -> &'static str {
        match self {
            Self::Begin { .. } => "Begin",
            Self::SequenceTxn { .. } => "SequenceTxn",
            Self::StageInRam { .. } => "StageInRam",
            Self::PersistOnStableMedia { .. } => "PersistOnStableMedia",
            Self::ReturnBuffered { .. } => "ReturnBuffered",
            Self::CommitTxn { .. } => "CommitTxn",
            Self::RejectConflict { .. } => "RejectConflict",
            Self::DeliverCommitted { .. } => "DeliverCommitted",
            Self::BuildObjectClosure { .. } => "BuildObjectClosure",
            Self::PrepareObjectFrontier { .. } => "PrepareObjectFrontier",
            Self::PopTxLogThroughPending => "PopTxLogThroughPending",
            Self::ActivateObjectFrontier => "ActivateObjectFrontier",
            Self::AdvanceGeneration => "AdvanceGeneration",
            Self::InstallGeneration { .. } => "InstallGeneration",
            Self::LoseRam { .. } => "LoseRam",
            Self::LoseStableMedium { .. } => "LoseStableMedium",
            Self::HydrateServingImage { .. } => "HydrateServingImage",
            Self::ServeRead { .. } => "ServeRead",
        }
    }
}

/// Evidence assertion attached to an observed trace without inventing a TLA+
/// state transition. The first profile binds txLog acknowledgement to media
/// found again after process loss.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "assertion", rename_all = "snake_case")]
pub enum CellTraceAssertionV1 {
    StableQuorumAtAcknowledgement {
        transaction: String,
        acknowledged_nodes: BTreeSet<String>,
    },
}

/// One replayable observed trace plus its exact reference-model identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CellTraceRefinementV1 {
    pub schema_version: u32,
    pub model_sha256: String,
    pub scope: String,
    pub config: CellTraceConfigV1,
    pub events: Vec<CellTraceEventV1>,
    pub assertions: Vec<CellTraceAssertionV1>,
    pub passed: bool,
    pub violation: Option<CellTraceViolationV1>,
    pub final_active_generation: u64,
    pub final_commit_version: u64,
    pub final_active_object_frontier: u64,
    pub final_txlog_floor: u64,
    pub trace_sha256: String,
}

impl CellTraceRefinementV1 {
    /// Replay and seal an observed trace against the reference action contract.
    /// The result is always returned so negative controls can retain the first
    /// rejected transition as evidence.
    #[must_use]
    pub fn evaluate(
        scope: impl Into<String>,
        config: CellTraceConfigV1,
        events: Vec<CellTraceEventV1>,
        assertions: Vec<CellTraceAssertionV1>,
    ) -> Self {
        let scope = scope.into();
        let mut state = CellTraceState::new(&config);
        let mut violation = config.validate().err();
        for (index, event) in events.iter().enumerate() {
            if let Err(detail) = state.apply(event) {
                violation = Some(CellTraceViolationV1::event(index, event.action(), detail));
                break;
            }
            if let Err(detail) = state.safety() {
                violation = Some(CellTraceViolationV1::event(index, event.action(), detail));
                break;
            }
        }
        if violation.is_none() {
            for (index, assertion) in assertions.iter().enumerate() {
                if let Err(detail) = state.assert(assertion, config.quorum) {
                    violation = Some(CellTraceViolationV1::assertion(index, detail));
                    break;
                }
            }
        }
        let mut refinement = Self {
            schema_version: TRACE_SCHEMA_VERSION,
            model_sha256: OBJECT_KV_CELL_TLA_SHA256.to_owned(),
            scope,
            config,
            events,
            assertions,
            passed: violation.is_none(),
            violation,
            final_active_generation: state.active_generation,
            final_commit_version: state.commit_version,
            final_active_object_frontier: state.active_object_frontier,
            final_txlog_floor: state.txlog_floor,
            trace_sha256: String::new(),
        };
        refinement.trace_sha256 = refinement.calculated_sha256();
        refinement
    }

    fn calculated_sha256(&self) -> String {
        let mut unsigned = self.clone();
        unsigned.trace_sha256.clear();
        let bytes = serde_json::to_vec(&unsigned).expect("cell trace schema is serializable");
        format!("{:x}", Sha256::digest(bytes))
    }

    /// Validate the receipt seal, then independently replay every event and
    /// assertion and compare the complete derived result.
    ///
    /// # Errors
    ///
    /// Rejects schema, model, scope, event, replay-result, and digest drift.
    pub fn validate(&self) -> Result<(), CellTraceViolationV1> {
        self.config.validate()?;
        if self.schema_version != TRACE_SCHEMA_VERSION
            || self.model_sha256 != OBJECT_KV_CELL_TLA_SHA256
            || self.scope.is_empty()
            || self.events.is_empty()
            || self.trace_sha256 != self.calculated_sha256()
            || self.passed == self.violation.is_some()
        {
            return Err(CellTraceViolationV1::configuration(
                "cell trace receipt identity or seal is invalid",
            ));
        }
        let replay = Self::evaluate(
            self.scope.clone(),
            self.config.clone(),
            self.events.clone(),
            self.assertions.clone(),
        );
        if self.passed != replay.passed
            || self.violation != replay.violation
            || self.final_active_generation != replay.final_active_generation
            || self.final_commit_version != replay.final_commit_version
            || self.final_active_object_frontier != replay.final_active_object_frontier
            || self.final_txlog_floor != replay.final_txlog_floor
        {
            return Err(CellTraceViolationV1::configuration(
                "cell trace receipt differs from independent replay",
            ));
        }
        Ok(())
    }
}

/// First transition or assertion that could not refine the reference model.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CellTraceViolationV1 {
    pub phase: String,
    pub index: Option<usize>,
    pub action: Option<String>,
    pub detail: String,
}

impl CellTraceViolationV1 {
    fn configuration(detail: impl Into<String>) -> Self {
        Self {
            phase: "configuration".to_owned(),
            index: None,
            action: None,
            detail: detail.into(),
        }
    }

    fn event(index: usize, action: &str, detail: impl Into<String>) -> Self {
        Self {
            phase: "event".to_owned(),
            index: Some(index),
            action: Some(action.to_owned()),
            detail: detail.into(),
        }
    }

    fn assertion(index: usize, detail: impl Into<String>) -> Self {
        Self {
            phase: "assertion".to_owned(),
            index: Some(index),
            action: Some("StableQuorumAtAcknowledgement".to_owned()),
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxnState {
    Pending,
    Sequenced,
    Committed,
    Rejected,
}

#[derive(Clone, Debug)]
struct TxnRecord {
    state: TxnState,
    generation: u64,
    version: u64,
    reply_committed: bool,
}

#[derive(Clone, Debug)]
struct ServingRecord {
    tier: CellServingTierV1,
    generation: u64,
    through: u64,
    ready: bool,
}

#[derive(Clone, Debug)]
struct CellTraceState {
    nodes: BTreeSet<String>,
    quorum: usize,
    max_media_failures: usize,
    active_generation: u64,
    next_version: u64,
    commit_version: u64,
    transactions: BTreeMap<String, TxnRecord>,
    conflicts: BTreeSet<String>,
    ram_copies: BTreeMap<u64, BTreeSet<String>>,
    stable_copies: BTreeMap<u64, BTreeSet<String>>,
    node_epoch: BTreeMap<String, u64>,
    failed_media: BTreeSet<String>,
    retained_txlog: BTreeSet<u64>,
    object_built_through: u64,
    pending_object_frontier: u64,
    pending_object_generation: u64,
    active_object_frontier: u64,
    txlog_floor: u64,
    serving: BTreeMap<String, ServingRecord>,
}

impl CellTraceState {
    fn new(config: &CellTraceConfigV1) -> Self {
        Self {
            nodes: config.nodes.clone(),
            quorum: config.quorum,
            max_media_failures: config.max_media_failures,
            active_generation: 1,
            next_version: 1,
            commit_version: 0,
            transactions: BTreeMap::new(),
            conflicts: BTreeSet::new(),
            ram_copies: BTreeMap::new(),
            stable_copies: BTreeMap::new(),
            node_epoch: config.nodes.iter().map(|node| (node.clone(), 1)).collect(),
            failed_media: BTreeSet::new(),
            retained_txlog: BTreeSet::new(),
            object_built_through: 0,
            pending_object_frontier: 0,
            pending_object_generation: 0,
            active_object_frontier: 0,
            txlog_floor: 0,
            serving: BTreeMap::new(),
        }
    }

    #[allow(clippy::too_many_lines)]
    fn apply(&mut self, event: &CellTraceEventV1) -> Result<(), String> {
        match event {
            CellTraceEventV1::Begin { transaction } => {
                if transaction.is_empty() || self.transactions.contains_key(transaction) {
                    return Err("Begin requires one new non-empty transaction".to_owned());
                }
                self.transactions.insert(
                    transaction.clone(),
                    TxnRecord {
                        state: TxnState::Pending,
                        generation: self.active_generation,
                        version: 0,
                        reply_committed: false,
                    },
                );
            }
            CellTraceEventV1::SequenceTxn {
                transaction,
                version,
            } => {
                if *version == 0 || *version != self.next_version {
                    return Err("SequenceTxn requires the next unique cell version".to_owned());
                }
                let record = self.transaction_mut(transaction, TxnState::Pending)?;
                record.state = TxnState::Sequenced;
                record.version = *version;
                self.next_version = self.next_version.saturating_add(1);
            }
            CellTraceEventV1::StageInRam { transaction, node } => {
                self.require_node(node)?;
                let record = self.transaction(transaction, TxnState::Sequenced)?;
                if self.node_epoch.get(node) != Some(&record.generation) {
                    return Err(
                        "StageInRam requires the transaction generation on the node".to_owned()
                    );
                }
                self.ram_copies
                    .entry(record.version)
                    .or_default()
                    .insert(node.clone());
            }
            CellTraceEventV1::PersistOnStableMedia { transaction, node } => {
                self.require_node(node)?;
                let record = self.transaction(transaction, TxnState::Sequenced)?;
                if !self
                    .ram_copies
                    .get(&record.version)
                    .is_some_and(|nodes| nodes.contains(node))
                    || self.failed_media.contains(node)
                {
                    return Err("PersistOnStableMedia requires a RAM copy on live media".to_owned());
                }
                self.stable_copies
                    .entry(record.version)
                    .or_default()
                    .insert(node.clone());
            }
            CellTraceEventV1::ReturnBuffered { transaction } => {
                let record = self.transaction(transaction, TxnState::Sequenced)?;
                if self
                    .ram_copies
                    .get(&record.version)
                    .map_or(0, BTreeSet::len)
                    < self.quorum
                {
                    return Err("ReturnBuffered requires a RAM quorum".to_owned());
                }
            }
            CellTraceEventV1::CommitTxn { transaction } => {
                let record = self.transaction(transaction, TxnState::Sequenced)?.clone();
                if self.transactions.values().any(|other| {
                    other.state == TxnState::Sequenced && other.version < record.version
                }) || self
                    .stable_copies
                    .get(&record.version)
                    .map_or(0, BTreeSet::len)
                    < self.quorum
                    || record.generation != self.active_generation
                    || self.conflicts.contains(transaction)
                {
                    return Err("CommitTxn requires order, stable quorum, active generation, and conflict validation".to_owned());
                }
                self.transaction_mut(transaction, TxnState::Sequenced)?
                    .state = TxnState::Committed;
                self.commit_version = self.commit_version.max(record.version);
                self.retained_txlog.insert(record.version);
                for (other, state) in &self.transactions {
                    if other != transaction
                        && matches!(state.state, TxnState::Pending | TxnState::Sequenced)
                    {
                        self.conflicts.insert(other.clone());
                    }
                }
            }
            CellTraceEventV1::RejectConflict { transaction } => {
                if !self.conflicts.contains(transaction) {
                    return Err("RejectConflict requires a recorded conflict".to_owned());
                }
                self.transaction_mut(transaction, TxnState::Sequenced)?
                    .state = TxnState::Rejected;
            }
            CellTraceEventV1::DeliverCommitted { transaction } => {
                let version = self.transaction(transaction, TxnState::Committed)?.version;
                if !self.version_recoverable(version) {
                    return Err("DeliverCommitted requires recoverable committed state".to_owned());
                }
                self.transaction_mut(transaction, TxnState::Committed)?
                    .reply_committed = true;
            }
            CellTraceEventV1::BuildObjectClosure { through } => {
                if *through == 0
                    || *through > self.commit_version
                    || *through <= self.object_built_through
                    || !self.committed_through_recoverable(*through)
                {
                    return Err(
                        "BuildObjectClosure requires a newer recoverable committed prefix"
                            .to_owned(),
                    );
                }
                self.object_built_through = *through;
            }
            CellTraceEventV1::PrepareObjectFrontier { through } => {
                if self.pending_object_frontier != 0
                    || *through == 0
                    || *through > self.commit_version
                    || *through <= self.active_object_frontier
                    || *through > self.object_built_through
                {
                    return Err(
                        "PrepareObjectFrontier requires one complete newer closure".to_owned()
                    );
                }
                self.pending_object_frontier = *through;
                self.pending_object_generation = self.active_generation;
            }
            CellTraceEventV1::PopTxLogThroughPending => {
                if self.pending_object_frontier == 0
                    || self.pending_object_frontier <= self.txlog_floor
                    || self.pending_object_frontier > self.object_built_through
                    || self.pending_object_generation != self.active_generation
                {
                    return Err(
                        "PopTxLogThroughPending requires a protected current-generation frontier"
                            .to_owned(),
                    );
                }
                self.txlog_floor = self.pending_object_frontier;
                self.retained_txlog
                    .retain(|version| *version > self.pending_object_frontier);
            }
            CellTraceEventV1::ActivateObjectFrontier => {
                if self.pending_object_frontier == 0
                    || self.txlog_floor < self.pending_object_frontier
                    || self.pending_object_generation != self.active_generation
                {
                    return Err(
                        "ActivateObjectFrontier requires prior protected txLog pop".to_owned()
                    );
                }
                self.active_object_frontier = self.pending_object_frontier;
                self.pending_object_frontier = 0;
                self.pending_object_generation = 0;
            }
            CellTraceEventV1::AdvanceGeneration => {
                self.active_generation = self.active_generation.saturating_add(1);
            }
            CellTraceEventV1::InstallGeneration { node } => {
                self.require_node(node)?;
                let epoch = self.node_epoch.get_mut(node).expect("known node");
                if *epoch >= self.active_generation {
                    return Err(
                        "InstallGeneration requires a node behind the active generation".to_owned(),
                    );
                }
                *epoch = self.active_generation;
            }
            CellTraceEventV1::LoseRam { node } => {
                self.require_node(node)?;
                let owns_staged_ram = self.ram_copies.values().any(|copies| copies.contains(node));
                let serves_from_ram = self
                    .serving
                    .get(node)
                    .is_some_and(|image| image.tier == CellServingTierV1::Ram);
                if !owns_staged_ram && !serves_from_ram {
                    return Err(
                        "LoseRam requires staged RAM or a RAM serving image on the node".to_owned(),
                    );
                }
                for copies in self.ram_copies.values_mut() {
                    copies.remove(node);
                }
                if self
                    .serving
                    .get(node)
                    .is_some_and(|image| image.tier == CellServingTierV1::Ram)
                {
                    self.serving.remove(node);
                }
            }
            CellTraceEventV1::LoseStableMedium { node } => {
                self.require_node(node)?;
                if self.failed_media.contains(node)
                    || self.failed_media.len() >= self.max_media_failures
                {
                    return Err("LoseStableMedium exceeds the failure budget".to_owned());
                }
                self.failed_media.insert(node.clone());
                for copies in self.stable_copies.values_mut() {
                    copies.remove(node);
                }
                if self.serving.get(node).is_some_and(|image| {
                    matches!(
                        image.tier,
                        CellServingTierV1::Nvme | CellServingTierV1::Rocks
                    )
                }) {
                    self.serving.remove(node);
                }
            }
            CellTraceEventV1::HydrateServingImage {
                node,
                tier,
                through,
            } => {
                self.require_node(node)?;
                if *through > self.commit_version || !self.committed_through_recoverable(*through) {
                    return Err(
                        "HydrateServingImage requires a reconstructable committed prefix"
                            .to_owned(),
                    );
                }
                self.serving.insert(
                    node.clone(),
                    ServingRecord {
                        tier: *tier,
                        generation: self.active_generation,
                        through: *through,
                        ready: true,
                    },
                );
            }
            CellTraceEventV1::ServeRead { node } => {
                self.require_node(node)?;
                let image = self
                    .serving
                    .get(node)
                    .ok_or_else(|| "ServeRead requires a serving image".to_owned())?;
                if !image.ready
                    || image.generation != self.active_generation
                    || image.through != self.commit_version
                    || !self.committed_through_recoverable(image.through)
                {
                    return Err(
                        "ServeRead requires a ready current reconstructable image".to_owned()
                    );
                }
            }
        }
        Ok(())
    }

    fn assert(&self, assertion: &CellTraceAssertionV1, quorum: usize) -> Result<(), String> {
        match assertion {
            CellTraceAssertionV1::StableQuorumAtAcknowledgement {
                transaction,
                acknowledged_nodes,
            } => {
                let record = self.transactions.get(transaction).ok_or_else(|| {
                    "stable-quorum assertion names an unknown transaction".to_owned()
                })?;
                let stable = self
                    .stable_copies
                    .get(&record.version)
                    .cloned()
                    .unwrap_or_default();
                if acknowledged_nodes.len() < quorum
                    || stable.len() < quorum
                    || !stable.is_subset(acknowledged_nodes)
                {
                    return Err("acknowledgement lacks a restart-observed stable quorum".to_owned());
                }
            }
        }
        Ok(())
    }

    fn transaction(&self, id: &str, expected: TxnState) -> Result<&TxnRecord, String> {
        let record = self
            .transactions
            .get(id)
            .ok_or_else(|| "event names an unknown transaction".to_owned())?;
        if record.state != expected {
            return Err("transaction is not in the required reference state".to_owned());
        }
        Ok(record)
    }

    fn transaction_mut(&mut self, id: &str, expected: TxnState) -> Result<&mut TxnRecord, String> {
        let record = self
            .transactions
            .get_mut(id)
            .ok_or_else(|| "event names an unknown transaction".to_owned())?;
        if record.state != expected {
            return Err("transaction is not in the required reference state".to_owned());
        }
        Ok(record)
    }

    fn require_node(&self, node: &str) -> Result<(), String> {
        if !self.nodes.contains(node) {
            return Err("event names an unknown node".to_owned());
        }
        Ok(())
    }

    fn protected_object_through(&self) -> u64 {
        if self.pending_object_frontier > self.active_object_frontier {
            self.pending_object_frontier
        } else {
            self.active_object_frontier
        }
    }

    fn object_protects(&self, version: u64) -> bool {
        version > 0
            && version <= self.object_built_through
            && version <= self.protected_object_through()
    }

    fn version_recoverable(&self, version: u64) -> bool {
        self.object_protects(version)
            || (self.retained_txlog.contains(&version)
                && self
                    .stable_copies
                    .get(&version)
                    .is_some_and(|copies| !copies.is_empty()))
    }

    fn committed_through_recoverable(&self, through: u64) -> bool {
        self.transactions.values().all(|record| {
            record.state != TxnState::Committed
                || record.version > through
                || self.version_recoverable(record.version)
        })
    }

    fn safety(&self) -> Result<(), String> {
        if self.active_object_frontier > self.object_built_through
            || self.pending_object_frontier > self.object_built_through
            || self.txlog_floor > self.object_built_through
            || self.txlog_floor > self.protected_object_through()
        {
            return Err("object frontier or txLog floor exceeds protected closure".to_owned());
        }
        for record in self.transactions.values() {
            if record.state == TxnState::Committed {
                if !self.version_recoverable(record.version) {
                    return Err("committed version is not recoverable".to_owned());
                }
                if record.version > self.txlog_floor
                    && !self.retained_txlog.contains(&record.version)
                {
                    return Err("retained txLog is not an exact committed suffix".to_owned());
                }
                if record.reply_committed
                    && self
                        .stable_copies
                        .get(&record.version)
                        .map_or(0, BTreeSet::len)
                        < self.quorum
                    && !self.object_protects(record.version)
                {
                    return Err("committed reply lacks durable protection".to_owned());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CellServingTierV1, CellTraceAssertionV1, CellTraceConfigV1, CellTraceEventV1,
        CellTraceRefinementV1,
    };
    use std::collections::BTreeSet;

    fn config() -> CellTraceConfigV1 {
        CellTraceConfigV1::new(["n1", "n2", "n3"].map(str::to_owned), 2, 1).expect("valid config")
    }

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn complete_cell_trace_conforms_to_the_reference_actions() {
        let events = vec![
            CellTraceEventV1::Begin {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::SequenceTxn {
                transaction: "t1".to_owned(),
                version: 1,
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n1".to_owned(),
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n2".to_owned(),
            },
            CellTraceEventV1::PersistOnStableMedia {
                transaction: "t1".to_owned(),
                node: "n1".to_owned(),
            },
            CellTraceEventV1::PersistOnStableMedia {
                transaction: "t1".to_owned(),
                node: "n2".to_owned(),
            },
            CellTraceEventV1::CommitTxn {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::DeliverCommitted {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::BuildObjectClosure { through: 1 },
            CellTraceEventV1::PrepareObjectFrontier { through: 1 },
            CellTraceEventV1::PopTxLogThroughPending,
            CellTraceEventV1::ActivateObjectFrontier,
            CellTraceEventV1::HydrateServingImage {
                node: "n3".to_owned(),
                tier: CellServingTierV1::Rocks,
                through: 1,
            },
            CellTraceEventV1::ServeRead {
                node: "n3".to_owned(),
            },
        ];
        let assertions = vec![CellTraceAssertionV1::StableQuorumAtAcknowledgement {
            transaction: "t1".to_owned(),
            acknowledged_nodes: set(&["n1", "n2"]),
        }];
        let report = CellTraceRefinementV1::evaluate("complete-cell", config(), events, assertions);
        assert!(report.passed, "{:?}", report.violation);
        report.validate().expect("sealed trace");
        assert_eq!(report.final_commit_version, 1);
        assert_eq!(report.final_active_object_frontier, 1);
    }

    #[test]
    fn acknowledgement_without_restart_observed_quorum_is_rejected() {
        let events = vec![
            CellTraceEventV1::Begin {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::SequenceTxn {
                transaction: "t1".to_owned(),
                version: 1,
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n1".to_owned(),
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n2".to_owned(),
            },
        ];
        let assertions = vec![CellTraceAssertionV1::StableQuorumAtAcknowledgement {
            transaction: "t1".to_owned(),
            acknowledged_nodes: set(&["n1", "n2"]),
        }];
        let report =
            CellTraceRefinementV1::evaluate("early-ack-poison", config(), events, assertions);
        assert!(!report.passed);
        assert_eq!(
            report
                .violation
                .as_ref()
                .and_then(|error| error.action.as_deref()),
            Some("StableQuorumAtAcknowledgement")
        );
        report.validate().expect("sealed negative trace");
    }

    #[test]
    fn latest_read_rejects_an_image_behind_commit() {
        let events = vec![
            CellTraceEventV1::Begin {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::SequenceTxn {
                transaction: "t1".to_owned(),
                version: 1,
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n1".to_owned(),
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n2".to_owned(),
            },
            CellTraceEventV1::PersistOnStableMedia {
                transaction: "t1".to_owned(),
                node: "n1".to_owned(),
            },
            CellTraceEventV1::PersistOnStableMedia {
                transaction: "t1".to_owned(),
                node: "n2".to_owned(),
            },
            CellTraceEventV1::CommitTxn {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::HydrateServingImage {
                node: "n3".to_owned(),
                tier: CellServingTierV1::Ram,
                through: 0,
            },
            CellTraceEventV1::ServeRead {
                node: "n3".to_owned(),
            },
        ];
        let report = CellTraceRefinementV1::evaluate("behind-commit", config(), events, vec![]);
        assert!(!report.passed);
        assert_eq!(
            report.violation.as_ref().and_then(|error| error.index),
            Some(8)
        );
        assert_eq!(
            report
                .violation
                .as_ref()
                .and_then(|error| error.action.as_deref()),
            Some("ServeRead")
        );
    }

    #[test]
    fn losing_ram_discards_its_serving_image() {
        let events = vec![
            CellTraceEventV1::HydrateServingImage {
                node: "n3".to_owned(),
                tier: CellServingTierV1::Ram,
                through: 0,
            },
            CellTraceEventV1::LoseRam {
                node: "n3".to_owned(),
            },
            CellTraceEventV1::ServeRead {
                node: "n3".to_owned(),
            },
        ];
        let report = CellTraceRefinementV1::evaluate("ram-loss", config(), events, vec![]);
        assert!(!report.passed);
        assert_eq!(
            report.violation.as_ref().and_then(|error| error.index),
            Some(2)
        );
    }

    #[test]
    fn deserialized_non_intersecting_quorum_is_rejected() {
        let invalid = CellTraceConfigV1 {
            nodes: set(&["n1", "n2", "n3"]),
            quorum: 1,
            max_media_failures: 1,
        };
        let events = vec![CellTraceEventV1::Begin {
            transaction: "t1".to_owned(),
        }];
        let report = CellTraceRefinementV1::evaluate("invalid-quorum", invalid, events, vec![]);
        assert!(!report.passed);
        assert_eq!(
            report.violation.as_ref().map(|error| error.phase.as_str()),
            Some("configuration")
        );
        let error = report
            .validate()
            .expect_err("invalid configuration must not validate");
        assert!(error.detail.contains("intersecting quorum"));
    }

    #[test]
    fn activation_rejects_an_old_generation_pending_frontier() {
        let events = vec![
            CellTraceEventV1::Begin {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::SequenceTxn {
                transaction: "t1".to_owned(),
                version: 1,
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n1".to_owned(),
            },
            CellTraceEventV1::StageInRam {
                transaction: "t1".to_owned(),
                node: "n2".to_owned(),
            },
            CellTraceEventV1::PersistOnStableMedia {
                transaction: "t1".to_owned(),
                node: "n1".to_owned(),
            },
            CellTraceEventV1::PersistOnStableMedia {
                transaction: "t1".to_owned(),
                node: "n2".to_owned(),
            },
            CellTraceEventV1::CommitTxn {
                transaction: "t1".to_owned(),
            },
            CellTraceEventV1::BuildObjectClosure { through: 1 },
            CellTraceEventV1::PrepareObjectFrontier { through: 1 },
            CellTraceEventV1::PopTxLogThroughPending,
            CellTraceEventV1::AdvanceGeneration,
            CellTraceEventV1::ActivateObjectFrontier,
        ];
        let report = CellTraceRefinementV1::evaluate("stale-frontier", config(), events, vec![]);
        assert!(!report.passed);
        assert_eq!(
            report.violation.as_ref().and_then(|error| error.index),
            Some(11)
        );
    }

    #[test]
    fn validation_replays_and_rejects_resealed_derived_state() {
        let events = vec![CellTraceEventV1::Begin {
            transaction: "t1".to_owned(),
        }];
        let mut report = CellTraceRefinementV1::evaluate("tampered", config(), events, vec![]);
        report.final_commit_version = 1;
        report.trace_sha256 = report.calculated_sha256();
        let error = report.validate().expect_err("replay must reject tampering");
        assert!(error.detail.contains("differs from independent replay"));
    }
}
