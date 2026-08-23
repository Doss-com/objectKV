use crate::RequestIdentity;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const GENERATION_COMMAND_MAGIC: &[u8] = b"OKVG1";

/// Lifecycle phase owned by the external cell coordinator quorum.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationPhase {
    #[default]
    Uninitialized,
    Active,
    Fencing,
    Recovering,
}

/// Coordinator-owned transaction-system descriptor for one bounded cell.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationAuthorityState {
    pub cell_id: u64,
    pub generation: u64,
    pub phase: GenerationPhase,
    pub recovery_id: Option<u64>,
    pub transaction_system_id: Option<String>,
    pub pending_transaction_system_id: Option<String>,
    pub wal_root: Option<String>,
    pub control_root_version: u64,
    pub fenced_log_index: u64,
    pub recovered_log_index: u64,
    pub last_completed_generation: u64,
}

impl GenerationAuthorityState {
    /// Whether a linearizable coordinator read authorizes one client commit.
    #[must_use]
    pub fn authorizes(&self, generation: u64, transaction_system_id: &str) -> bool {
        self.phase == GenerationPhase::Active
            && self.generation == generation
            && self.transaction_system_id.as_deref() == Some(transaction_system_id)
    }

    /// Whether a linearizable coordinator read authorizes one quiesced handoff.
    #[must_use]
    pub fn authorizes_recovery(
        &self,
        generation: u64,
        recovery_id: u64,
        pending_transaction_system_id: &str,
    ) -> bool {
        self.phase == GenerationPhase::Recovering
            && self.generation == generation
            && self.recovery_id == Some(recovery_id)
            && self.pending_transaction_system_id.as_deref() == Some(pending_transaction_system_id)
    }

    /// Whether a linearizable coordinator read authorizes the data-log barrier.
    #[must_use]
    pub fn authorizes_fencing(
        &self,
        generation: u64,
        recovery_id: u64,
        pending_transaction_system_id: &str,
    ) -> bool {
        self.phase == GenerationPhase::Fencing
            && self.generation == generation
            && self.recovery_id == Some(recovery_id)
            && self.pending_transaction_system_id.as_deref() == Some(pending_transaction_system_id)
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn apply(
        &mut self,
        action: &GenerationAction,
        faults: GenerationAuthorityFaults,
    ) -> GenerationCommandStatus {
        match action {
            GenerationAction::Bootstrap {
                cell_id,
                generation,
                transaction_system_id,
                wal_root,
                control_root_version,
            } => {
                if self.phase != GenerationPhase::Uninitialized
                    || *cell_id == 0
                    || *generation == 0
                    || transaction_system_id.is_empty()
                    || wal_root.is_empty()
                    || *control_root_version == 0
                {
                    return GenerationCommandStatus::InvalidPhase;
                }
                *self = Self {
                    cell_id: *cell_id,
                    generation: *generation,
                    phase: GenerationPhase::Active,
                    recovery_id: None,
                    transaction_system_id: Some(transaction_system_id.clone()),
                    pending_transaction_system_id: None,
                    wal_root: Some(wal_root.clone()),
                    control_root_version: *control_root_version,
                    fenced_log_index: 0,
                    recovered_log_index: 0,
                    last_completed_generation: *generation,
                };
                GenerationCommandStatus::Accepted
            }
            GenerationAction::Prepare {
                expected_generation,
                next_generation,
                expected_control_root_version,
                recovery_id,
                next_transaction_system_id,
            } => {
                let normal = self.phase == GenerationPhase::Active
                    && *expected_generation == self.generation
                    && *next_generation == self.generation.saturating_add(1)
                    && *expected_control_root_version == self.control_root_version;
                let competing = faults.accept_competing_recovery
                    && self.phase == GenerationPhase::Fencing
                    && *next_generation == self.generation
                    && expected_generation.saturating_add(1) == self.generation
                    && *expected_control_root_version == self.control_root_version;
                if !normal && !competing {
                    return if *expected_generation < self.generation {
                        GenerationCommandStatus::StaleGeneration
                    } else if matches!(
                        self.phase,
                        GenerationPhase::Fencing | GenerationPhase::Recovering
                    ) {
                        GenerationCommandStatus::RecoveryConflict
                    } else {
                        GenerationCommandStatus::CompareFailed
                    };
                }
                if *recovery_id == 0 || next_transaction_system_id.is_empty() {
                    return GenerationCommandStatus::InvalidRequest;
                }
                self.generation = *next_generation;
                self.phase = GenerationPhase::Fencing;
                self.recovery_id = Some(*recovery_id);
                self.pending_transaction_system_id = Some(next_transaction_system_id.clone());
                self.fenced_log_index = 0;
                self.recovered_log_index = 0;
                GenerationCommandStatus::Accepted
            }
            GenerationAction::Reserve {
                generation,
                recovery_id,
                transaction_system_id,
                expected_control_root_version,
                fenced_log_index,
            } => {
                if self.phase != GenerationPhase::Fencing || *generation != self.generation {
                    return GenerationCommandStatus::InvalidPhase;
                }
                if self.recovery_id != Some(*recovery_id)
                    || self.pending_transaction_system_id.as_deref()
                        != Some(transaction_system_id.as_str())
                {
                    return GenerationCommandStatus::RecoveryConflict;
                }
                if *expected_control_root_version != self.control_root_version {
                    return GenerationCommandStatus::CompareFailed;
                }
                if *fenced_log_index == 0 {
                    return GenerationCommandStatus::MissingRecoveryProof;
                }
                self.phase = GenerationPhase::Recovering;
                self.fenced_log_index = *fenced_log_index;
                GenerationCommandStatus::Accepted
            }
            GenerationAction::Activate {
                generation,
                recovery_id,
                transaction_system_id,
                wal_root,
                expected_control_root_version,
                next_control_root_version,
                recovered_log_index,
            } => {
                if self.phase != GenerationPhase::Recovering || *generation != self.generation {
                    return GenerationCommandStatus::InvalidPhase;
                }
                if self.recovery_id != Some(*recovery_id)
                    || self.pending_transaction_system_id.as_deref()
                        != Some(transaction_system_id.as_str())
                {
                    return GenerationCommandStatus::RecoveryConflict;
                }
                if *expected_control_root_version != self.control_root_version
                    || *next_control_root_version != self.control_root_version.saturating_add(1)
                {
                    return GenerationCommandStatus::CompareFailed;
                }
                if (*recovered_log_index == 0 && !faults.activate_without_recovery_proof)
                    || transaction_system_id.is_empty()
                    || wal_root.is_empty()
                {
                    return GenerationCommandStatus::MissingRecoveryProof;
                }
                self.phase = GenerationPhase::Active;
                self.recovery_id = None;
                self.transaction_system_id = Some(transaction_system_id.clone());
                self.pending_transaction_system_id = None;
                self.wal_root = Some(wal_root.clone());
                self.control_root_version = *next_control_root_version;
                self.recovered_log_index = *recovered_log_index;
                self.last_completed_generation = *generation;
                GenerationCommandStatus::Accepted
            }
        }
    }
}

/// One command replicated by the coordinator's `OpenRaft` log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationCommand {
    pub identity: RequestIdentity,
    pub action: GenerationAction,
}

impl GenerationCommand {
    /// Encode this command into objectKV-owned application bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = GENERATION_COMMAND_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(GENERATION_COMMAND_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// State transition requested from the coordinator quorum.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GenerationAction {
    Bootstrap {
        cell_id: u64,
        generation: u64,
        transaction_system_id: String,
        wal_root: String,
        control_root_version: u64,
    },
    Prepare {
        expected_generation: u64,
        next_generation: u64,
        expected_control_root_version: u64,
        recovery_id: u64,
        next_transaction_system_id: String,
    },
    Reserve {
        generation: u64,
        recovery_id: u64,
        transaction_system_id: String,
        expected_control_root_version: u64,
        fenced_log_index: u64,
    },
    Activate {
        generation: u64,
        recovery_id: u64,
        transaction_system_id: String,
        wal_root: String,
        expected_control_root_version: u64,
        next_control_root_version: u64,
        recovered_log_index: u64,
    },
}

/// Stable semantic result of a coordinator command.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationCommandStatus {
    Accepted,
    StaleGeneration,
    CompareFailed,
    RecoveryConflict,
    MissingRecoveryProof,
    InvalidPhase,
    InvalidRequest,
}

/// Response reconstructed by replaying the coordinator log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationApplyResponse {
    pub status: GenerationCommandStatus,
    pub state: GenerationAuthorityState,
    pub applied_log_index: u64,
}

/// Bounded unsafe authority behaviors used only by negative-control nodes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationAuthorityFaults {
    pub accept_competing_recovery: bool,
    pub activate_without_recovery_proof: bool,
}

/// Role hosted by one real-process consensus node.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusProcessRole {
    #[default]
    Data,
    GenerationAuthority,
}

/// Generation identity presented by a transaction-system process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationCredential {
    pub generation: u64,
    pub transaction_system_id: String,
}

/// Coordinator endpoints and local identity used to fence data commits.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationFenceConfig {
    pub credential: GenerationCredential,
    pub recovery_id: Option<u64>,
    pub authority_nodes: BTreeMap<u64, String>,
}

/// Bounded unsafe transaction-system behaviors used by negative controls.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationFenceFaults {
    pub bypass_commit_fence: bool,
    pub bypass_apply_fence: bool,
    pub accept_apply_during_recovery: bool,
    pub accept_recovering_commits: bool,
    pub allow_preauthorized_test_write: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(action: GenerationAction) -> GenerationCommand {
        GenerationCommand {
            identity: RequestIdentity {
                client_id: 9,
                request_id: 1,
            },
            action,
        }
    }

    #[test]
    fn reserve_fences_active_generation_until_proven_activation() {
        let mut state = GenerationAuthorityState::default();
        let bootstrap = command(GenerationAction::Bootstrap {
            cell_id: 7,
            generation: 1,
            transaction_system_id: "tx-g1".to_owned(),
            wal_root: "wal-g1".to_owned(),
            control_root_version: 1,
        });
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&bootstrap.action, GenerationAuthorityFaults::default())
        );
        assert!(state.authorizes(1, "tx-g1"));

        let prepare = command(GenerationAction::Prepare {
            expected_generation: 1,
            next_generation: 2,
            expected_control_root_version: 1,
            recovery_id: 22,
            next_transaction_system_id: "tx-g2".to_owned(),
        });
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&prepare.action, GenerationAuthorityFaults::default())
        );
        assert!(!state.authorizes(1, "tx-g1"));
        assert!(!state.authorizes(2, "tx-g2"));
        assert!(state.authorizes_fencing(2, 22, "tx-g2"));

        let reserve = command(GenerationAction::Reserve {
            generation: 2,
            recovery_id: 22,
            transaction_system_id: "tx-g2".to_owned(),
            expected_control_root_version: 1,
            fenced_log_index: 17,
        });
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&reserve.action, GenerationAuthorityFaults::default())
        );
        assert!(state.authorizes_recovery(2, 22, "tx-g2"));

        let without_proof = command(GenerationAction::Activate {
            generation: 2,
            recovery_id: 22,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            recovered_log_index: 0,
        });
        assert_eq!(
            GenerationCommandStatus::MissingRecoveryProof,
            state.apply(&without_proof.action, GenerationAuthorityFaults::default())
        );
        let with_proof = GenerationAction::Activate {
            generation: 2,
            recovery_id: 22,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            recovered_log_index: 19,
        };
        assert_eq!(
            GenerationCommandStatus::Accepted,
            state.apply(&with_proof, GenerationAuthorityFaults::default())
        );
        assert!(state.authorizes(2, "tx-g2"));
    }
}
