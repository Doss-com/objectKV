use crate::rpc::{
    read_response, write_request, AddLearnerRequest, ChangeMembershipRequest, ControlWrite,
    NodeStatus, WriteAck, ADD_LEARNER, CHANGE_MEMBERSHIP, CLIENT_WRITE, DATA_GENERATION_WRITE,
    ELECT, GENERATION_READ, GENERATION_WRITE, INITIALIZE, PREAUTHORIZED_CLIENT_WRITE,
    RECOVERY_ATTEST, STATUS,
};
use crate::{
    recovery_public_key, ClientCommand, ConsensusProcessRole, GenerationAction,
    GenerationApplyResponse, GenerationAuthorityFaults, GenerationAuthorityState,
    GenerationCommand, GenerationCommandStatus, GenerationCredential, GenerationFenceConfig,
    GenerationFenceFaults, GenerationPhase, NodeId, ProcessNodeConfig, ProcessNodePolicy,
    RecoveryCertificate, RecoveryCertificateKind, RecoveryCertificateStatement,
    RecoverySignerConfig, RequestIdentity,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const CELL_ID: u64 = 7;
const AUTHORITY_NODES: [NodeId; 3] = [101, 102, 103];
const GENERATION_ONE_NODES: [NodeId; 3] = [201, 202, 203];
const GENERATION_TWO_NODES: [NodeId; 3] = [301, 302, 303];
const GENERATION_ONE: u64 = 1;
const GENERATION_TWO: u64 = 2;
const RECOVERY_ID: u64 = 2_002;
const COMPETING_RECOVERY_ID: u64 = 2_099;
const RETRY_ATTEMPTS: usize = 500;

/// Deliberately unsafe takeover behaviors used to validate the gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationProcessMode {
    Correct,
    BypassStaleCommitFence,
    AcceptWriteDuringRecovery,
    AcceptCompetingRecovery,
    ActivateWithoutRecoveryProof,
    AcceptSingleSignerFence,
    AcceptTamperedFencePosition,
    AcceptDuplicateRecoverySigner,
    AcceptStaleRecoveryCertificate,
    AcceptWrongRecoveryMembership,
}

impl GenerationProcessMode {
    /// Stable identifier used by eval configuration and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::BypassStaleCommitFence => "bypass_stale_commit_fence",
            Self::AcceptWriteDuringRecovery => "accept_write_during_recovery",
            Self::AcceptCompetingRecovery => "accept_competing_recovery",
            Self::ActivateWithoutRecoveryProof => "activate_without_recovery_proof",
            Self::AcceptSingleSignerFence => "accept_single_signer_fence",
            Self::AcceptTamperedFencePosition => "accept_tampered_fence_position",
            Self::AcceptDuplicateRecoverySigner => "accept_duplicate_recovery_signer",
            Self::AcceptStaleRecoveryCertificate => "accept_stale_recovery_certificate",
            Self::AcceptWrongRecoveryMembership => "accept_wrong_recovery_membership",
        }
    }

    const fn certificate_probe(self) -> Option<CertificateProbe> {
        match self {
            Self::AcceptSingleSignerFence => Some(CertificateProbe::SingleSignerFence),
            Self::AcceptTamperedFencePosition => Some(CertificateProbe::TamperedFencePosition),
            Self::AcceptDuplicateRecoverySigner => Some(CertificateProbe::DuplicateRecoverySigner),
            Self::AcceptStaleRecoveryCertificate => {
                Some(CertificateProbe::StaleRecoveryCertificate)
            }
            Self::AcceptWrongRecoveryMembership => Some(CertificateProbe::WrongRecoveryMembership),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CertificateProbe {
    SingleSignerFence,
    TamperedFencePosition,
    DuplicateRecoverySigner,
    StaleRecoveryCertificate,
    WrongRecoveryMembership,
}

/// Canonical semantic report for one coordinator-backed generation handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationProcessReport {
    pub seed: u64,
    pub mode: GenerationProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub data_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub learner_additions: u64,
    pub membership_changes: u64,
    pub generation_preparations: u64,
    pub generation_reservations: u64,
    pub generation_activations: u64,
    pub committed_data_writes: u64,
    pub fenced_commit_attempts: u64,
    pub fenced_commit_rejections: u64,
    pub caught_up_generation_two_nodes: u64,
    pub fence_certificate_signers: u64,
    pub recovery_certificate_signers: u64,
    pub invalid_certificate_rejections: u64,
    pub source_provider_fence_persisted: bool,
    pub source_fence_precedes_activation: bool,
    pub stale_generation_routing_rejected: bool,
    pub active_generation_routing_authorized: bool,
    pub trace_sha256: String,
}

/// Run a real-process coordinator and quiesced voter-set handoff.
///
/// # Errors
///
/// Returns an error when local process, transport, or consensus control cannot
/// execute. Semantic disagreements are recorded in the returned report.
pub fn run_generation_process_contract(
    seed: u64,
    mode: GenerationProcessMode,
    executable: &Path,
) -> Result<GenerationProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(GenerationScenario::new(seed, mode, executable)?.run())
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Default)]
struct Observations {
    coordinator_bootstrapped: bool,
    generation_one_commit_replicated: bool,
    generation_two_learners_caught_up: bool,
    data_log_fence_committed: bool,
    inflight_commit_rejected_by_data_fence: bool,
    next_generation_reserved: bool,
    old_generation_fenced: bool,
    reservation_survived_authority_failover: bool,
    competing_recovery_rejected: bool,
    membership_handoff_committed: bool,
    generation_two_leader_ready: bool,
    write_during_recovery_rejected: bool,
    activation_without_proof_rejected: bool,
    invalid_fence_certificates_rejected: bool,
    invalid_recovery_certificates_rejected: bool,
    generation_two_activated: bool,
    generation_two_continued_exactly: bool,
    removed_generation_remained_fenced: bool,
    authority_process_starts: u64,
    data_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    learner_additions: u64,
    membership_changes: u64,
    generation_preparations: u64,
    generation_reservations: u64,
    generation_activations: u64,
    committed_data_writes: u64,
    fenced_commit_attempts: u64,
    fenced_commit_rejections: u64,
    caught_up_generation_two_nodes: u64,
    fence_certificate_signers: u64,
    recovery_certificate_signers: u64,
    invalid_certificate_rejections: u64,
    final_authority: Option<GenerationAuthorityState>,
    final_payloads: BTreeMap<NodeId, Vec<Vec<u8>>>,
}

struct GenerationScenario<'a> {
    seed: u64,
    mode: GenerationProcessMode,
    executable: &'a Path,
    root: TempRoot,
    authority_addresses: BTreeMap<NodeId, String>,
    generation_one_addresses: BTreeMap<NodeId, String>,
    generation_two_addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    observations: Observations,
}

impl<'a> GenerationScenario<'a> {
    fn new(seed: u64, mode: GenerationProcessMode, executable: &'a Path) -> Result<Self, String> {
        if !executable.is_file() {
            return Err(format!(
                "generation contract executable does not exist: {}",
                executable.display()
            ));
        }
        let addresses = allocate_addresses(
            &AUTHORITY_NODES
                .into_iter()
                .chain(GENERATION_ONE_NODES)
                .chain(GENERATION_TWO_NODES)
                .collect::<Vec<_>>(),
        )?;
        Ok(Self {
            seed,
            mode,
            executable,
            root: TempRoot::new(seed, mode)?,
            authority_addresses: subset(&addresses, &AUTHORITY_NODES),
            generation_one_addresses: subset(&addresses, &GENERATION_ONE_NODES),
            generation_two_addresses: subset(&addresses, &GENERATION_TWO_NODES),
            children: ChildGroup::default(),
            observations: Observations::default(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(mut self) -> Result<GenerationProcessReport, String> {
        let generation_one_members = generation_members(&GENERATION_ONE_NODES)?;
        let generation_two_members = generation_members(&GENERATION_TWO_NODES)?;
        self.start_authority().await?;
        self.start_generation_one().await?;
        self.start_generation_two_learners().await?;

        let prepare = self
            .write_generation(
                101,
                2,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                },
            )
            .await?;
        self.observations.generation_preparations +=
            u64::from(prepare.status == GenerationCommandStatus::Accepted);

        let competing = self
            .write_generation(
                101,
                3,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: COMPETING_RECOVERY_ID,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                },
            )
            .await?;
        self.observations.competing_recovery_rejected =
            competing.status != GenerationCommandStatus::Accepted;
        self.observations.generation_preparations +=
            u64::from(competing.status == GenerationCommandStatus::Accepted);
        let effective_recovery_id = if competing.status == GenerationCommandStatus::Accepted {
            COMPETING_RECOVERY_ID
        } else {
            RECOVERY_ID
        };

        let data_prepare = self
            .write_data_generation(
                201,
                10,
                GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: effective_recovery_id,
                    next_transaction_system_id: "tx-g2".to_owned(),
                    next_transaction_system_members: generation_two_members.clone(),
                },
            )
            .await?;
        let fenced_log_position = data_prepare.applied_log_position;
        self.observations.data_log_fence_committed = data_prepare.status
            == GenerationCommandStatus::Accepted
            && data_prepare.state.phase == GenerationPhase::Fencing
            && fenced_log_position.index != 0;

        let fence_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Fence,
            &data_prepare.state,
            fenced_log_position,
            &generation_one_members,
        );
        let fence_certificate = self
            .collect_certificate(&GENERATION_ONE_NODES, fence_statement)
            .await?;
        self.observations.fence_certificate_signers =
            u64::try_from(fence_certificate.attestations.len()).unwrap_or(u64::MAX);
        if !self
            .reject_invalid_fence_certificates(&fence_certificate, effective_recovery_id)
            .await?
        {
            self.capture_final().await;
            return Ok(build_report(self.seed, self.mode, &self.observations));
        }

        self.observations.fenced_commit_attempts += 1;
        let inflight = self
            .write_preauthorized_data(201, GENERATION_ONE, "tx-g1", 19, b"INFLIGHT")
            .await;
        self.observations.committed_data_writes += u64::from(inflight.is_ok());
        self.observations.inflight_commit_rejected_by_data_fence = inflight.is_err();
        self.observations.fenced_commit_rejections += u64::from(inflight.is_err());

        let reserve = self
            .write_generation(
                101,
                4,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: effective_recovery_id,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(fence_certificate.clone()),
                },
            )
            .await?;
        self.observations.next_generation_reserved = reserve.status
            == GenerationCommandStatus::Accepted
            && reserve.state.phase == GenerationPhase::Recovering
            && reserve.state.generation == GENERATION_TWO
            && reserve.state.fenced_log_position == Some(fenced_log_position);
        self.observations.generation_reservations +=
            u64::from(reserve.status == GenerationCommandStatus::Accepted);
        let data_reserve = self
            .write_data_generation(
                201,
                11,
                GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: effective_recovery_id,
                    transaction_system_id: "tx-g2".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(fence_certificate),
                },
            )
            .await?;
        self.observations.next_generation_reserved &=
            data_reserve.status == GenerationCommandStatus::Accepted;

        self.observations.fenced_commit_attempts += 1;
        let stale = self
            .write_data(201, GENERATION_ONE, "tx-g1", 20, b"STALE")
            .await;
        self.observations.committed_data_writes += u64::from(stale.is_ok());
        self.observations.old_generation_fenced = stale.is_err();
        self.observations.fenced_commit_rejections += u64::from(stale.is_err());

        self.kill_node(101)?;
        self.observations.authority_failovers =
            u64::from(elect_until_leader(self.authority_address(102)?, 102).await);
        let recovered = retry_generation_read(self.authority_address(102)?).await?;
        self.observations.reservation_survived_authority_failover = recovered.phase
            == GenerationPhase::Recovering
            && recovered.generation == GENERATION_TWO
            && recovered.recovery_id == Some(effective_recovery_id);

        let membership = change_membership(
            self.generation_one_address(201)?,
            ChangeMembershipRequest {
                voters: GENERATION_TWO_NODES.into_iter().collect(),
                credential: credential(GENERATION_TWO, "tx-g2"),
                recovery_id: effective_recovery_id,
            },
        )
        .await;
        self.observations.membership_handoff_committed = membership
            .as_ref()
            .is_ok_and(|ack| ack.committed && ack.log_position.is_some());
        self.observations.membership_changes =
            u64::from(self.observations.membership_handoff_committed);

        self.observations.generation_two_leader_ready =
            elect_until_leader(self.generation_two_address(301)?, 301).await;

        self.observations.fenced_commit_attempts += 1;
        let early = self
            .write_data(301, GENERATION_TWO, "tx-g2", 30, b"EARLY")
            .await;
        self.observations.committed_data_writes += u64::from(early.is_ok());
        self.observations.write_during_recovery_rejected = early.is_err();
        self.observations.fenced_commit_rejections += u64::from(early.is_err());

        let membership = membership?;
        let recovered_log_position = membership
            .log_position
            .ok_or_else(|| "membership handoff did not return an exact log position".to_owned())?;
        let recovered_statement = RecoveryCertificateStatement::new(
            RecoveryCertificateKind::Recovered,
            &data_reserve.state,
            recovered_log_position,
            &generation_two_members,
        );
        let recovery_certificate = self
            .collect_certificate(&GENERATION_TWO_NODES, recovered_statement)
            .await?;
        self.observations.recovery_certificate_signers =
            u64::try_from(recovery_certificate.attestations.len()).unwrap_or(u64::MAX);
        if !self
            .reject_invalid_recovery_certificates(&recovery_certificate, effective_recovery_id)
            .await?
        {
            self.capture_final().await;
            return Ok(build_report(self.seed, self.mode, &self.observations));
        }
        let invalid_activation_action = GenerationAction::Activate {
            generation: GENERATION_TWO,
            recovery_id: effective_recovery_id,
            transaction_system_id: "tx-g2".to_owned(),
            wal_root: "wal-g2".to_owned(),
            expected_control_root_version: 1,
            next_control_root_version: 2,
            certificate: None,
        };
        let invalid_activation = self
            .write_generation(102, 5, invalid_activation_action.clone())
            .await?;
        self.observations.activation_without_proof_rejected =
            invalid_activation.status == GenerationCommandStatus::MissingRecoveryProof;

        let (activation, activation_action) =
            if invalid_activation.status == GenerationCommandStatus::Accepted {
                (invalid_activation, invalid_activation_action)
            } else {
                let action = GenerationAction::Activate {
                    generation: GENERATION_TWO,
                    recovery_id: effective_recovery_id,
                    transaction_system_id: "tx-g2".to_owned(),
                    wal_root: "wal-g2".to_owned(),
                    expected_control_root_version: 1,
                    next_control_root_version: 2,
                    certificate: Some(recovery_certificate.clone()),
                };
                (self.write_generation(102, 6, action.clone()).await?, action)
            };
        self.observations.generation_two_activated = activation.status
            == GenerationCommandStatus::Accepted
            && activation.state.authorizes(GENERATION_TWO, "tx-g2")
            && activation.state.recovered_log_position == Some(recovered_log_position);
        self.observations.generation_activations =
            u64::from(activation.status == GenerationCommandStatus::Accepted);
        let data_activation = self
            .write_data_generation(301, 12, activation_action)
            .await?;
        self.observations.generation_two_activated &=
            data_activation.status == GenerationCommandStatus::Accepted;

        let _ = retry_write_data(
            self.generation_two_address(301)?,
            credential(GENERATION_TWO, "tx-g2"),
            client_command(GENERATION_TWO, "tx-g2", self.seed, 40, b"B")?,
        )
        .await?;
        self.observations.committed_data_writes += 1;
        self.observations.generation_two_continued_exactly = wait_for_payloads(
            &self.generation_two_addresses,
            &GENERATION_TWO_NODES,
            &[b"A".to_vec(), b"B".to_vec()],
        )
        .await;

        self.observations.fenced_commit_attempts += 1;
        let removed = self
            .write_data(201, GENERATION_ONE, "tx-g1", 50, b"REMOVED")
            .await;
        self.observations.committed_data_writes += u64::from(removed.is_ok());
        self.observations.removed_generation_remained_fenced = removed.is_err();
        self.observations.fenced_commit_rejections += u64::from(removed.is_err());

        self.capture_final().await;
        Ok(build_report(self.seed, self.mode, &self.observations))
    }

    async fn start_authority(&mut self) -> Result<(), String> {
        for node_id in AUTHORITY_NODES {
            self.start_node(
                node_id,
                self.authority_addresses.clone(),
                ProcessNodePolicy {
                    role: ConsensusProcessRole::GenerationAuthority,
                    generation_authority_faults: GenerationAuthorityFaults {
                        accept_competing_recovery: self.mode
                            == GenerationProcessMode::AcceptCompetingRecovery,
                        activate_without_recovery_proof: self.mode
                            == GenerationProcessMode::ActivateWithoutRecoveryProof,
                        accept_invalid_recovery_certificate: self
                            .mode
                            .certificate_probe()
                            .is_some(),
                    },
                    ..ProcessNodePolicy::default()
                },
            )?;
            self.observations.authority_process_starts += 1;
        }
        self.wait_ready_nodes(&AUTHORITY_NODES).await?;
        retry_control(self.authority_address(101)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.authority_address(101)?, 101).await {
            return Err("coordinator leader election failed".to_owned());
        }
        let bootstrap = self
            .write_generation(
                101,
                1,
                GenerationAction::Bootstrap {
                    cell_id: CELL_ID,
                    generation: GENERATION_ONE,
                    transaction_system_id: "tx-g1".to_owned(),
                    transaction_system_members: generation_members(&GENERATION_ONE_NODES)?,
                    wal_root: "wal-g1".to_owned(),
                    control_root_version: 1,
                },
            )
            .await?;
        self.observations.coordinator_bootstrapped = bootstrap.status
            == GenerationCommandStatus::Accepted
            && bootstrap.state.authorizes(GENERATION_ONE, "tx-g1");
        Ok(())
    }

    async fn start_generation_one(&mut self) -> Result<(), String> {
        let fence = GenerationFenceConfig {
            credential: credential(GENERATION_ONE, "tx-g1"),
            recovery_id: None,
            authority_nodes: self.authority_addresses.clone(),
        };
        for node_id in GENERATION_ONE_NODES {
            self.start_node(
                node_id,
                self.generation_one_addresses.clone(),
                ProcessNodePolicy {
                    role: ConsensusProcessRole::Data,
                    generation_fence: Some(fence.clone()),
                    generation_authority_faults: GenerationAuthorityFaults {
                        activate_without_recovery_proof: self.mode
                            == GenerationProcessMode::ActivateWithoutRecoveryProof,
                        accept_invalid_recovery_certificate: self
                            .mode
                            .certificate_probe()
                            .is_some(),
                        ..GenerationAuthorityFaults::default()
                    },
                    generation_fence_faults: GenerationFenceFaults {
                        bypass_commit_fence: self.mode
                            == GenerationProcessMode::BypassStaleCommitFence,
                        bypass_apply_fence: self.mode
                            == GenerationProcessMode::BypassStaleCommitFence,
                        accept_apply_during_recovery: self.mode
                            == GenerationProcessMode::AcceptWriteDuringRecovery,
                        accept_recovering_commits: false,
                        allow_preauthorized_test_write: true,
                    },
                    recovery_signer: Some(recovery_signer(node_id)),
                    ..ProcessNodePolicy::default()
                },
            )?;
            self.observations.data_process_starts += 1;
        }
        self.wait_ready_nodes(&GENERATION_ONE_NODES).await?;
        retry_control(self.generation_one_address(201)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.generation_one_address(201)?, 201).await {
            return Err("generation-one leader election failed".to_owned());
        }
        let data_bootstrap = self
            .write_data_generation(
                201,
                1,
                GenerationAction::Bootstrap {
                    cell_id: CELL_ID,
                    generation: GENERATION_ONE,
                    transaction_system_id: "tx-g1".to_owned(),
                    transaction_system_members: generation_members(&GENERATION_ONE_NODES)?,
                    wal_root: "wal-g1".to_owned(),
                    control_root_version: 1,
                },
            )
            .await?;
        if data_bootstrap.status != GenerationCommandStatus::Accepted {
            return Err("generation-one data mirror bootstrap failed".to_owned());
        }
        let command = client_command(GENERATION_ONE, "tx-g1", self.seed, 10, b"A")?;
        let _ = retry_write_data(
            self.generation_one_address(201)?,
            credential(GENERATION_ONE, "tx-g1"),
            command,
        )
        .await?;
        self.observations.committed_data_writes += 1;
        self.observations.generation_one_commit_replicated = wait_for_payloads(
            &self.generation_one_addresses,
            &GENERATION_ONE_NODES,
            &[b"A".to_vec()],
        )
        .await;
        Ok(())
    }

    async fn start_generation_two_learners(&mut self) -> Result<(), String> {
        let fence = GenerationFenceConfig {
            credential: credential(GENERATION_TWO, "tx-g2"),
            recovery_id: Some(RECOVERY_ID),
            authority_nodes: self.authority_addresses.clone(),
        };
        for node_id in GENERATION_TWO_NODES {
            self.start_node(
                node_id,
                self.generation_two_addresses.clone(),
                ProcessNodePolicy {
                    role: ConsensusProcessRole::Data,
                    generation_fence: Some(fence.clone()),
                    generation_authority_faults: GenerationAuthorityFaults {
                        activate_without_recovery_proof: self.mode
                            == GenerationProcessMode::ActivateWithoutRecoveryProof,
                        accept_invalid_recovery_certificate: self
                            .mode
                            .certificate_probe()
                            .is_some(),
                        ..GenerationAuthorityFaults::default()
                    },
                    generation_fence_faults: GenerationFenceFaults {
                        bypass_commit_fence: false,
                        bypass_apply_fence: self.mode
                            == GenerationProcessMode::BypassStaleCommitFence,
                        accept_apply_during_recovery: self.mode
                            == GenerationProcessMode::AcceptWriteDuringRecovery,
                        accept_recovering_commits: self.mode
                            == GenerationProcessMode::AcceptWriteDuringRecovery,
                        allow_preauthorized_test_write: false,
                    },
                    recovery_signer: Some(recovery_signer(node_id)),
                    ..ProcessNodePolicy::default()
                },
            )?;
            self.observations.data_process_starts += 1;
        }
        self.wait_ready_nodes(&GENERATION_TWO_NODES).await?;
        for node_id in GENERATION_TWO_NODES {
            let ack = add_learner(
                self.generation_one_address(201)?,
                AddLearnerRequest {
                    node_id,
                    address: self.generation_two_address(node_id)?.to_owned(),
                },
            )
            .await?;
            self.observations.learner_additions += u64::from(ack.committed);
        }
        self.observations.generation_two_learners_caught_up = wait_for_payloads(
            &self.generation_two_addresses,
            &GENERATION_TWO_NODES,
            &[b"A".to_vec()],
        )
        .await;
        Ok(())
    }

    fn start_node(
        &mut self,
        node_id: NodeId,
        nodes: BTreeMap<NodeId, String>,
        policy: ProcessNodePolicy,
    ) -> Result<(), String> {
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root: self.root.node(node_id),
                nodes,
                deduplicate_requests: true,
                acknowledge_before_quorum: false,
                policy,
            },
        )
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    async fn wait_ready_nodes(&self, node_ids: &[NodeId]) -> Result<(), String> {
        for node_id in node_ids {
            wait_ready(self.address(*node_id)?).await?;
        }
        Ok(())
    }

    async fn write_generation(
        &self,
        node_id: NodeId,
        request_id: u64,
        action: GenerationAction,
    ) -> Result<GenerationApplyResponse, String> {
        retry_generation_write(
            self.address(node_id)?,
            &GenerationCommand {
                identity: RequestIdentity {
                    client_id: self.seed ^ 0x4745_4e45_5241_5445,
                    request_id,
                },
                action,
            },
        )
        .await
    }

    async fn write_data_generation(
        &self,
        node_id: NodeId,
        request_id: u64,
        action: GenerationAction,
    ) -> Result<GenerationApplyResponse, String> {
        retry_data_generation_write(
            self.address(node_id)?,
            &GenerationCommand {
                identity: RequestIdentity {
                    client_id: self.seed ^ 0x4441_5441_4745_4e45,
                    request_id,
                },
                action,
            },
        )
        .await
    }

    async fn write_data(
        &self,
        node_id: NodeId,
        generation: u64,
        transaction_system_id: &str,
        request_id: u64,
        payload: &[u8],
    ) -> Result<WriteAck, String> {
        write_data(
            self.address(node_id)?,
            credential(generation, transaction_system_id),
            client_command(
                generation,
                transaction_system_id,
                self.seed,
                request_id,
                payload,
            )?,
        )
        .await
    }

    async fn write_preauthorized_data(
        &self,
        node_id: NodeId,
        generation: u64,
        transaction_system_id: &str,
        request_id: u64,
        payload: &[u8],
    ) -> Result<WriteAck, String> {
        write_preauthorized_data(
            self.address(node_id)?,
            credential(generation, transaction_system_id),
            client_command(
                generation,
                transaction_system_id,
                self.seed,
                request_id,
                payload,
            )?,
        )
        .await
    }

    async fn collect_certificate(
        &self,
        node_ids: &[NodeId],
        statement: RecoveryCertificateStatement,
    ) -> Result<RecoveryCertificate, String> {
        let mut attestations = Vec::with_capacity(node_ids.len());
        for node_id in node_ids {
            attestations
                .push(retry_recovery_attestation(self.address(*node_id)?, &statement).await?);
        }
        Ok(RecoveryCertificate {
            statement,
            attestations,
        })
    }

    async fn reject_invalid_fence_certificates(
        &mut self,
        valid: &RecoveryCertificate,
        recovery_id: u64,
    ) -> Result<bool, String> {
        let selected = self.mode.certificate_probe();
        let probes = match selected {
            Some(CertificateProbe::SingleSignerFence) => {
                vec![CertificateProbe::SingleSignerFence]
            }
            Some(CertificateProbe::TamperedFencePosition) => {
                vec![CertificateProbe::TamperedFencePosition]
            }
            Some(_) => Vec::new(),
            None => vec![
                CertificateProbe::SingleSignerFence,
                CertificateProbe::TamperedFencePosition,
            ],
        };
        let mut all_rejected = true;
        for (offset, probe) in probes.into_iter().enumerate() {
            let certificate = invalid_certificate(valid, probe);
            let response = self
                .write_generation(
                    101,
                    100 + u64::try_from(offset).unwrap_or(u64::MAX),
                    GenerationAction::Reserve {
                        generation: GENERATION_TWO,
                        recovery_id,
                        transaction_system_id: "tx-g2".to_owned(),
                        expected_control_root_version: 1,
                        certificate: Some(certificate),
                    },
                )
                .await?;
            let rejected = response.status == GenerationCommandStatus::InvalidRecoveryProof;
            self.observations.invalid_certificate_rejections += u64::from(rejected);
            self.observations.generation_reservations +=
                u64::from(response.status == GenerationCommandStatus::Accepted);
            all_rejected &= rejected;
            if !rejected {
                break;
            }
        }
        self.observations.invalid_fence_certificates_rejected = all_rejected;
        Ok(all_rejected)
    }

    async fn reject_invalid_recovery_certificates(
        &mut self,
        valid: &RecoveryCertificate,
        recovery_id: u64,
    ) -> Result<bool, String> {
        let selected = self.mode.certificate_probe();
        let probes = match selected {
            Some(CertificateProbe::DuplicateRecoverySigner) => {
                vec![CertificateProbe::DuplicateRecoverySigner]
            }
            Some(CertificateProbe::StaleRecoveryCertificate) => {
                vec![CertificateProbe::StaleRecoveryCertificate]
            }
            Some(CertificateProbe::WrongRecoveryMembership) => {
                vec![CertificateProbe::WrongRecoveryMembership]
            }
            Some(_) => Vec::new(),
            None => vec![
                CertificateProbe::DuplicateRecoverySigner,
                CertificateProbe::StaleRecoveryCertificate,
                CertificateProbe::WrongRecoveryMembership,
            ],
        };
        let mut all_rejected = true;
        for (offset, probe) in probes.into_iter().enumerate() {
            let certificate = invalid_certificate(valid, probe);
            let response = self
                .write_generation(
                    102,
                    200 + u64::try_from(offset).unwrap_or(u64::MAX),
                    GenerationAction::Activate {
                        generation: GENERATION_TWO,
                        recovery_id,
                        transaction_system_id: "tx-g2".to_owned(),
                        wal_root: "wal-g2".to_owned(),
                        expected_control_root_version: 1,
                        next_control_root_version: 2,
                        certificate: Some(certificate),
                    },
                )
                .await?;
            let rejected = response.status == GenerationCommandStatus::InvalidRecoveryProof;
            self.observations.invalid_certificate_rejections += u64::from(rejected);
            self.observations.generation_activations +=
                u64::from(response.status == GenerationCommandStatus::Accepted);
            all_rejected &= rejected;
            if !rejected {
                break;
            }
        }
        self.observations.invalid_recovery_certificates_rejected = all_rejected;
        Ok(all_rejected)
    }

    async fn capture_final(&mut self) {
        self.observations.final_authority =
            retry_generation_read(self.authority_address(102).unwrap_or_default())
                .await
                .ok();
        for node_id in GENERATION_TWO_NODES {
            if let Ok(node) = status(self.generation_two_address(node_id).unwrap_or_default()).await
            {
                self.observations.caught_up_generation_two_nodes +=
                    u64::from(node.payloads == [b"A".to_vec(), b"B".to_vec()]);
                self.observations
                    .final_payloads
                    .insert(node_id, node.payloads);
            }
        }
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.authority_addresses
            .get(&node_id)
            .or_else(|| self.generation_one_addresses.get(&node_id))
            .or_else(|| self.generation_two_addresses.get(&node_id))
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }

    fn authority_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.authority_addresses, node_id)
    }

    fn generation_one_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.generation_one_addresses, node_id)
    }

    fn generation_two_address(&self, node_id: NodeId) -> Result<&str, String> {
        address(&self.generation_two_addresses, node_id)
    }
}

#[allow(clippy::too_many_lines)]
fn build_report(
    seed: u64,
    mode: GenerationProcessMode,
    observations: &Observations,
) -> GenerationProcessReport {
    let source_fence_precedes_activation = observations
        .final_authority
        .as_ref()
        .and_then(|authority| {
            authority
                .fenced_log_position
                .zip(authority.recovered_log_position)
        })
        .is_some_and(|(fenced, recovered)| fenced.index > 0 && recovered.index > fenced.index);
    let stale_generation_routing_rejected = observations
        .final_authority
        .as_ref()
        .is_some_and(|authority| !authority.authorizes(GENERATION_ONE, "tx-g1"));
    let active_generation_routing_authorized = observations
        .final_authority
        .as_ref()
        .is_some_and(|authority| authority.authorizes(GENERATION_TWO, "tx-g2"));
    let checks = [
        (
            "coordinator_bootstrapped",
            observations.coordinator_bootstrapped,
        ),
        (
            "generation_one_commit_replicated",
            observations.generation_one_commit_replicated,
        ),
        (
            "generation_two_learners_caught_up",
            observations.generation_two_learners_caught_up,
        ),
        (
            "quorum_fence_certificate_committed",
            observations.data_log_fence_committed
                && observations.invalid_fence_certificates_rejected
                && observations.fence_certificate_signers >= 2,
        ),
        (
            "inflight_commit_rejected_by_data_fence",
            observations.inflight_commit_rejected_by_data_fence,
        ),
        (
            "next_generation_reserved",
            observations.next_generation_reserved,
        ),
        ("old_generation_fenced", observations.old_generation_fenced),
        (
            "reservation_survived_authority_failover",
            observations.reservation_survived_authority_failover,
        ),
        (
            "competing_recovery_rejected",
            observations.competing_recovery_rejected,
        ),
        (
            "membership_handoff_committed",
            observations.membership_handoff_committed,
        ),
        (
            "generation_two_leader_ready",
            observations.generation_two_leader_ready,
        ),
        (
            "write_during_recovery_rejected",
            observations.write_during_recovery_rejected,
        ),
        (
            "quorum_recovery_certificate_required",
            observations.activation_without_proof_rejected
                && observations.invalid_recovery_certificates_rejected
                && observations.recovery_certificate_signers >= 2,
        ),
        (
            "generation_two_activated",
            observations.generation_two_activated,
        ),
        (
            "generation_two_continued_exactly",
            observations.generation_two_continued_exactly,
        ),
        (
            "removed_generation_remained_fenced",
            observations.removed_generation_remained_fenced,
        ),
    ];
    let first = checks.iter().enumerate().find(|(_, (_, passed))| !passed);
    let anomaly_count = checks.iter().filter(|(_, passed)| !passed).count() as u64;
    let first_mismatch_step = first.map(|(index, _)| (index + 1) as u64);
    let first_mismatch = first.map(|(_, (name, _))| (*name).to_owned());

    let mut trace = Sha256::new();
    trace.update(b"okv-generation-process-contract-v3");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(passed)]);
    }
    if let Some(authority) = &observations.final_authority {
        let mut canonical_authority = authority.clone();
        // Real TCP scheduling can require a different number of explicit
        // election retriggers, so Raft terms are not deterministic across
        // fresh process runs. Certificate verification still binds the exact
        // observed term, while the semantic trace normalizes terms and retains
        // the certified indexes.
        if let Some(position) = canonical_authority.fenced_log_position.as_mut() {
            position.term = 0;
        }
        if let Some(position) = canonical_authority.recovered_log_position.as_mut() {
            position.term = 0;
        }
        trace.update(serde_json::to_vec(&canonical_authority).unwrap_or_default());
    }
    for (node_id, payloads) in &observations.final_payloads {
        trace.update(node_id.to_be_bytes());
        for payload in payloads {
            trace.update((payload.len() as u64).to_be_bytes());
            trace.update(payload);
        }
    }

    GenerationProcessReport {
        seed,
        mode,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch_step,
        first_mismatch,
        authority_process_starts: observations.authority_process_starts,
        data_process_starts: observations.data_process_starts,
        process_kills: observations.process_kills,
        authority_failovers: observations.authority_failovers,
        learner_additions: observations.learner_additions,
        membership_changes: observations.membership_changes,
        generation_preparations: observations.generation_preparations,
        generation_reservations: observations.generation_reservations,
        generation_activations: observations.generation_activations,
        committed_data_writes: observations.committed_data_writes,
        fenced_commit_attempts: observations.fenced_commit_attempts,
        fenced_commit_rejections: observations.fenced_commit_rejections,
        caught_up_generation_two_nodes: observations.caught_up_generation_two_nodes,
        fence_certificate_signers: observations.fence_certificate_signers,
        recovery_certificate_signers: observations.recovery_certificate_signers,
        invalid_certificate_rejections: observations.invalid_certificate_rejections,
        source_provider_fence_persisted: observations.removed_generation_remained_fenced,
        source_fence_precedes_activation,
        stale_generation_routing_rejected,
        active_generation_routing_authorized,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

fn credential(generation: u64, transaction_system_id: &str) -> GenerationCredential {
    GenerationCredential {
        generation,
        transaction_system_id: transaction_system_id.to_owned(),
    }
}

fn invalid_certificate(
    valid: &RecoveryCertificate,
    probe: CertificateProbe,
) -> RecoveryCertificate {
    let mut certificate = valid.clone();
    match probe {
        CertificateProbe::SingleSignerFence => certificate.attestations.truncate(1),
        CertificateProbe::TamperedFencePosition => {
            certificate.statement.log_position.index =
                certificate.statement.log_position.index.saturating_add(1);
        }
        CertificateProbe::DuplicateRecoverySigner => {
            if let Some(first) = certificate.attestations.first().cloned() {
                certificate.attestations.push(first);
            }
        }
        CertificateProbe::StaleRecoveryCertificate => {
            certificate.statement.recovery_id = certificate.statement.recovery_id.saturating_sub(1);
        }
        CertificateProbe::WrongRecoveryMembership => {
            certificate.statement.membership_sha256[0] ^= 0xff;
        }
    }
    certificate
}

fn recovery_signing_seed(node_id: NodeId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"OKV-GENERATION-PROCESS-TEST-SIGNER-V1\0");
    digest.update(node_id.to_be_bytes());
    digest.finalize().to_vec()
}

fn recovery_signer(node_id: NodeId) -> RecoverySignerConfig {
    RecoverySignerConfig {
        private_key_seed: recovery_signing_seed(node_id),
    }
}

fn generation_members(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, Vec<u8>>, String> {
    node_ids
        .iter()
        .map(|node_id| {
            recovery_public_key(&recovery_signing_seed(*node_id))
                .map(|public_key| (*node_id, public_key))
        })
        .collect()
}

fn client_command(
    generation: u64,
    transaction_system_id: &str,
    seed: u64,
    request_id: u64,
    payload: &[u8],
) -> Result<Vec<u8>, String> {
    ClientCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x4441_5441_434c_4945,
            request_id,
        },
        credential: Some(credential(generation, transaction_system_id)),
        payload: payload.to_vec(),
    }
    .encode()
    .map_err(|error| error.to_string())
}

#[derive(Default)]
struct ChildGroup {
    children: BTreeMap<NodeId, Child>,
}

impl ChildGroup {
    fn start(&mut self, executable: &Path, config: &ProcessNodeConfig) -> Result<(), String> {
        if self.children.contains_key(&config.node_id) {
            return Err(format!("node {} is already running", config.node_id));
        }
        let node_id = config.node_id;
        let config_json = serde_json::to_string(config).map_err(|error| error.to_string())?;
        let child = Command::new(executable)
            .arg("consensus-node")
            .arg("--config-json")
            .arg(config_json)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start node {node_id}: {error}"))?;
        self.children.insert(node_id, child);
        Ok(())
    }

    fn kill(&mut self, node_id: NodeId) -> Result<(), String> {
        let mut child = self
            .children
            .remove(&node_id)
            .ok_or_else(|| format!("node {node_id} is not running"))?;
        child
            .kill()
            .map_err(|error| format!("failed to kill node {node_id}: {error}"))?;
        child
            .wait()
            .map_err(|error| format!("failed to reap node {node_id}: {error}"))?;
        Ok(())
    }
}

impl Drop for ChildGroup {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn add_learner(address: &str, request: AddLearnerRequest) -> Result<WriteAck, String> {
    control(address, ADD_LEARNER, &request).await
}

async fn change_membership(
    address: &str,
    request: ChangeMembershipRequest,
) -> Result<WriteAck, String> {
    control(address, CHANGE_MEMBERSHIP, &request).await
}

async fn retry_generation_write(
    address: &str,
    command: &GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, GENERATION_WRITE, command).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("generation write failed at {address}: {last}"))
}

async fn retry_data_generation_write(
    address: &str,
    command: &GenerationCommand,
) -> Result<GenerationApplyResponse, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, DATA_GENERATION_WRITE, command).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("data generation write failed at {address}: {last}"))
}

async fn retry_generation_read(address: &str) -> Result<GenerationAuthorityState, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, GENERATION_READ, &()).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("generation read failed at {address}: {last}"))
}

async fn retry_recovery_attestation(
    address: &str,
    statement: &RecoveryCertificateStatement,
) -> Result<crate::RecoveryAttestation, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, RECOVERY_ATTEST, statement).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("recovery attestation failed at {address}: {last}"))
}

async fn retry_write_data(
    address: &str,
    credential: GenerationCredential,
    app_data: Vec<u8>,
) -> Result<WriteAck, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match write_data(address, credential.clone(), app_data.clone()).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("data write failed at {address}: {last}"))
}

async fn write_data(
    address: &str,
    credential: GenerationCredential,
    app_data: Vec<u8>,
) -> Result<WriteAck, String> {
    control(
        address,
        CLIENT_WRITE,
        &ControlWrite {
            app_data,
            drop_reply_after_commit: false,
            credential: Some(credential),
        },
    )
    .await
}

async fn write_preauthorized_data(
    address: &str,
    credential: GenerationCredential,
    app_data: Vec<u8>,
) -> Result<WriteAck, String> {
    control(
        address,
        PREAUTHORIZED_CLIENT_WRITE,
        &ControlWrite {
            app_data,
            drop_reply_after_commit: false,
            credential: Some(credential),
        },
    )
    .await
}

async fn status(address: &str) -> Result<NodeStatus, String> {
    control(address, STATUS, &()).await
}

async fn retry_control<Req>(address: &str, kind: u8, request: &Req) -> Result<(), String>
where
    Req: Serialize,
{
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, kind, request).await {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("control operation failed at {address}: {last}"))
}

async fn wait_ready(address: &str) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match status(address).await {
            Ok(_) => return Ok(()),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("node did not become ready at {address}: {last}"))
}

async fn elect_until_leader(address: &str, node_id: NodeId) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let _: Result<(), String> = control(address, ELECT, &()).await;
        if status(address)
            .await
            .is_ok_and(|node| node.state == "leader" && node.leader == Some(node_id))
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn wait_for_payloads(
    addresses: &BTreeMap<NodeId, String>,
    node_ids: &[NodeId],
    expected: &[Vec<u8>],
) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        let mut exact = true;
        for node_id in node_ids {
            let Some(address) = addresses.get(node_id) else {
                return false;
            };
            let Ok(node) = status(address).await else {
                exact = false;
                continue;
            };
            if node.payloads.len() > expected.len()
                || !node
                    .payloads
                    .iter()
                    .zip(expected)
                    .all(|(actual, wanted)| actual == wanted)
            {
                return false;
            }
            exact &= node.payloads == expected;
        }
        if exact {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

async fn control<Req, Resp>(address: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    let mut stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(address))
        .await
        .map_err(|_| format!("connect timed out at {address}"))?
        .map_err(|error| error.to_string())?;
    write_request(&mut stream, kind, request)
        .await
        .map_err(|error| error.to_string())?;
    let response: Result<Resp, String> =
        tokio::time::timeout(Duration::from_secs(8), read_response(&mut stream))
            .await
            .map_err(|_| format!("response timed out at {address}"))?
            .map_err(|error| error.to_string())?;
    response
}

fn allocate_addresses(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, String>, String> {
    let mut listeners = Vec::new();
    for _ in node_ids {
        listeners
            .push(std::net::TcpListener::bind("127.0.0.1:0").map_err(|error| error.to_string())?);
    }
    let mut addresses = BTreeMap::new();
    for (node_id, listener) in node_ids.iter().zip(&listeners) {
        addresses.insert(
            *node_id,
            listener
                .local_addr()
                .map_err(|error| error.to_string())?
                .to_string(),
        );
    }
    drop(listeners);
    Ok(addresses)
}

fn subset(addresses: &BTreeMap<NodeId, String>, node_ids: &[NodeId]) -> BTreeMap<NodeId, String> {
    node_ids
        .iter()
        .filter_map(|node_id| {
            addresses
                .get(node_id)
                .map(|address| (*node_id, address.clone()))
        })
        .collect()
}

fn address(addresses: &BTreeMap<NodeId, String>, node_id: NodeId) -> Result<&str, String> {
    addresses
        .get(&node_id)
        .map(String::as_str)
        .ok_or_else(|| format!("missing address for node {node_id}"))
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: GenerationProcessMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-generation-process-{}-{seed}-{}-{sequence}",
            mode.id(),
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn node(&self, node_id: NodeId) -> PathBuf {
        self.0.join(format!("node-{node_id}"))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
