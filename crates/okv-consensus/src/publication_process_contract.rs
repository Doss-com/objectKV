use crate::rpc::{
    read_response, write_request, ControlWrite, NodeStatus, PublicationWriteRequest, WriteAck,
    ELECT, GENERATION_WRITE, INITIALIZE, PREAUTHORIZED_CLIENT_WRITE, PUBLICATION_OUTCOME,
    PUBLICATION_READ, PUBLICATION_WRITE, STATUS,
};
use crate::{
    recovery_public_key, sign_recovery_statement, ApplyResponse, ConsensusProcessRole,
    GenerationAction, GenerationApplyResponse, GenerationAuthorityFaults, GenerationCommand,
    GenerationCommandStatus, GenerationCredential, GenerationFenceFaults, NodeId,
    ProcessNodeConfig, ProcessNodePolicy, PublicationAction, PublicationAuthorityFaults,
    PublicationAuthorityState, PublicationCommand, PublicationCommandStatus,
    PublicationDeletePermit, PublicationFenceFaults, PublicationIntent, PublicationObjectIdentity,
    PublicationObjectKind, PublicationObjectReference, PublicationOutcome,
    PublicationRevisionToken, RecoveryCertificate, RecoveryCertificateKind,
    RecoveryCertificateStatement, RecoveryLogPosition, RequestIdentity,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const CELL_ID: u64 = 17;
const AUTHORITY_NODES: [NodeId; 3] = [101, 102, 103];
const GENERATION_ONE: u64 = 7;
const GENERATION_TWO: u64 = 8;
const RECOVERY_ID: u64 = 8_008;
const RETRY_ATTEMPTS: usize = 500;

/// Deliberately unsafe publication-authority behaviors used to validate the gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationProcessMode {
    Correct,
    BypassGenerationFence,
    PublishWithoutIntent,
    IgnoreRootEpoch,
    IgnoreDeleteReservation,
    DisableRequestDedup,
    AcknowledgeBeforeQuorum,
    StaleExpectedRoot,
    LocalStaleOutcomeRead,
    CrossGenerationIntentPublish,
    RetireByPlanKeyOnly,
}

impl PublicationProcessMode {
    /// Stable identifier used by eval configuration and receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::BypassGenerationFence => "bypass_generation_fence",
            Self::PublishWithoutIntent => "publish_without_intent",
            Self::IgnoreRootEpoch => "ignore_root_epoch",
            Self::IgnoreDeleteReservation => "ignore_delete_reservation",
            Self::DisableRequestDedup => "disable_request_dedup",
            Self::AcknowledgeBeforeQuorum => "acknowledge_before_quorum",
            Self::StaleExpectedRoot => "stale_expected_root",
            Self::LocalStaleOutcomeRead => "local_stale_outcome_read",
            Self::CrossGenerationIntentPublish => "cross_generation_intent_publish",
            Self::RetireByPlanKeyOnly => "retire_by_plan_key_only",
        }
    }

    const fn authority_faults(self) -> PublicationAuthorityFaults {
        PublicationAuthorityFaults {
            publish_without_intent: matches!(self, Self::PublishWithoutIntent),
            ignore_root_epoch: matches!(self, Self::IgnoreRootEpoch),
            ignore_delete_reservation: matches!(self, Self::IgnoreDeleteReservation),
            ignore_root_compare: matches!(self, Self::StaleExpectedRoot),
            allow_cross_generation_intent: matches!(self, Self::CrossGenerationIntentPublish),
            retire_by_plan_key_only: matches!(self, Self::RetireByPlanKeyOnly),
        }
    }

    const fn fence_faults(self) -> PublicationFenceFaults {
        PublicationFenceFaults {
            bypass_generation_fence: matches!(self, Self::BypassGenerationFence),
            local_stale_outcome_read: matches!(self, Self::LocalStaleOutcomeRead),
            prepare_as_previous_generation: false,
        }
    }
}

/// Canonical result of one real-process publication-authority schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationProcessReport {
    pub seed: u64,
    pub mode: PublicationProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch_step: Option<u64>,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub publication_writes: u64,
    pub generation_transitions: u64,
    pub dropped_replies: u64,
    pub recovered_outcomes: u64,
    pub duplicate_retries: u64,
    pub deletion_reservations: u64,
    pub restarted_nodes: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

/// Execute the bounded three-process publication-authority contract.
///
/// # Errors
///
/// Returns an error when a process cannot start, a bounded RPC fails, or the
/// controller cannot drive a required consensus transition.
pub fn run_publication_process_contract(
    seed: u64,
    mode: PublicationProcessMode,
    executable: &Path,
) -> Result<PublicationProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(ProcessHarness::new(seed, mode, executable)?.run())
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct Observations {
    coordinator_bootstrapped: bool,
    active_prepare_accepted: bool,
    stale_generation_rejected: bool,
    lost_reply_observed: bool,
    unknown_outcome_recovered: bool,
    prepare_survived_failover: bool,
    exact_retry_replayed: bool,
    conflicting_identity_rejected: bool,
    missing_intent_rejected: bool,
    stale_root_rejected: bool,
    matching_publish_accepted: bool,
    root_install_retired_intent: bool,
    pin_installed: bool,
    stale_pin_rejected: bool,
    exact_unpin_accepted: bool,
    stale_delete_plan_rejected: bool,
    reservation_committed: bool,
    reservation_survived_failover: bool,
    reservation_blocked_prepare: bool,
    forged_retirement_rejected: bool,
    exact_retirement_accepted: bool,
    retirement_allowed_prepare: bool,
    cross_generation_intent_rejected: bool,
    cross_generation_delete_rejected: bool,
    isolated_stale_outcome_safe: bool,
    unknown_command_rejected: bool,
    quorumless_write_not_acknowledged: bool,
    quorumless_outcome_absent: bool,
    restarted_nodes_exact: bool,
    final_state: Option<PublicationAuthorityState>,
    authority_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    publication_writes: u64,
    generation_transitions: u64,
    dropped_replies: u64,
    recovered_outcomes: u64,
    duplicate_retries: u64,
    deletion_reservations: u64,
    restarted_nodes: u64,
}

struct ProcessHarness<'a> {
    executable: &'a Path,
    seed: u64,
    mode: PublicationProcessMode,
    root: TempRoot,
    addresses: BTreeMap<NodeId, String>,
    children: ChildGroup,
    observations: Observations,
}

impl<'a> ProcessHarness<'a> {
    fn new(seed: u64, mode: PublicationProcessMode, executable: &'a Path) -> Result<Self, String> {
        Ok(Self {
            executable,
            seed,
            mode,
            root: TempRoot::new(seed, mode)?,
            addresses: allocate_addresses(&AUTHORITY_NODES)?,
            children: ChildGroup::default(),
            observations: Observations::default(),
        })
    }

    #[allow(clippy::similar_names, clippy::too_many_lines)]
    async fn run(mut self) -> Result<PublicationProcessReport, String> {
        self.start_initial_cluster().await?;

        let stale_manifest =
            object_reference("objects/stale-manifest", PublicationObjectKind::Manifest);
        let stale_intent =
            publication_intent(&stale_manifest, "objects/stale-data", "stale-root", None);
        let stale = self
            .write_publication(
                101,
                publication_command(
                    self.seed,
                    1,
                    GENERATION_ONE - 1,
                    "tx-g6",
                    PublicationAction::Prepare {
                        publication_id: "stale-publication".to_owned(),
                        intent: stale_intent,
                    },
                ),
                false,
            )
            .await?;
        self.observations.stale_generation_rejected =
            publication_status(&stale) == Some(PublicationCommandStatus::GenerationFenced);

        let main_manifest =
            object_reference("objects/main-manifest", PublicationObjectKind::Manifest);
        let main_intent =
            publication_intent(&main_manifest, "objects/main-data", "main-root", None);
        let lost_command = publication_command(
            self.seed,
            2,
            GENERATION_ONE,
            "tx-g7",
            PublicationAction::Prepare {
                publication_id: "main-publication".to_owned(),
                intent: main_intent.clone(),
            },
        );
        self.observations.publication_writes += 1;
        let lost = publication_write(self.address(101)?, &lost_command, true).await;
        self.observations.lost_reply_observed = lost.is_err();
        self.observations.dropped_replies += u64::from(lost.is_err());

        self.kill_node(101)?;
        self.observations.authority_failovers +=
            u64::from(elect_until_leader(self.address(102)?, 102).await);
        let recovered =
            retry_publication_outcome(self.address(102)?, lost_command.identity).await?;
        self.observations.unknown_outcome_recovered = recovered
            .as_ref()
            .and_then(|response| response.publication.as_ref())
            .is_some_and(|response| response.status == PublicationCommandStatus::Accepted);
        self.observations.recovered_outcomes +=
            u64::from(self.observations.unknown_outcome_recovered);
        let state = retry_publication_read(self.address(102)?).await?;
        self.observations.prepare_survived_failover = state
            .intents
            .get("main-publication")
            .is_some_and(|prepared| prepared.intent == main_intent);
        self.observations.active_prepare_accepted = self.observations.unknown_outcome_recovered;

        let retry = self
            .write_publication(102, lost_command.clone(), false)
            .await?;
        self.observations.duplicate_retries += 1;
        self.observations.exact_retry_replayed = retry.response.as_ref() == recovered.as_ref();

        let conflicting = publication_command(
            self.seed,
            2,
            GENERATION_ONE,
            "tx-g7",
            PublicationAction::Unpin {
                pin_id: "conflict".to_owned(),
                expected: object_reference(
                    "objects/conflicting-manifest",
                    PublicationObjectKind::Manifest,
                ),
            },
        );
        self.observations.conflicting_identity_rejected =
            publication_write(self.address(102)?, &conflicting, false)
                .await
                .is_err_and(|error| error.contains("conflicting application bytes"));

        let missing_manifest =
            object_reference("objects/missing-manifest", PublicationObjectKind::Manifest);
        let missing = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    3,
                    GENERATION_ONE,
                    "tx-g7",
                    publish_action(
                        "missing-publication",
                        "missing-root",
                        None,
                        &missing_manifest,
                    ),
                ),
                false,
            )
            .await?;
        self.observations.missing_intent_rejected = publication_status(&missing)
            == Some(PublicationCommandStatus::PublicationIntentMissing);

        let first_root_manifest =
            object_reference("objects/root-one-manifest", PublicationObjectKind::Manifest);
        let first_root_intent = publication_intent(
            &first_root_manifest,
            "objects/root-one-data",
            "cas-root",
            None,
        );
        let _ = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    4,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Prepare {
                        publication_id: "root-one".to_owned(),
                        intent: first_root_intent.clone(),
                    },
                ),
                false,
            )
            .await?;
        let root_one = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    5,
                    GENERATION_ONE,
                    "tx-g7",
                    publish_action("root-one", "cas-root", None, &first_root_manifest),
                ),
                false,
            )
            .await?;
        if publication_status(&root_one) != Some(PublicationCommandStatus::Accepted) {
            return Err("initial root publication did not commit".to_owned());
        }

        let stale_root_manifest = object_reference(
            "objects/root-stale-manifest",
            PublicationObjectKind::Manifest,
        );
        let stale_root_intent = publication_intent(
            &stale_root_manifest,
            "objects/root-stale-data",
            "cas-root",
            None,
        );
        let _ = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    6,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Prepare {
                        publication_id: "root-stale".to_owned(),
                        intent: stale_root_intent,
                    },
                ),
                false,
            )
            .await?;
        let stale_root = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    7,
                    GENERATION_ONE,
                    "tx-g7",
                    publish_action("root-stale", "cas-root", None, &stale_root_manifest),
                ),
                false,
            )
            .await?;
        self.observations.stale_root_rejected =
            publication_status(&stale_root) == Some(PublicationCommandStatus::RootCompareFailed);

        let main_publish = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    8,
                    GENERATION_ONE,
                    "tx-g7",
                    publish_action("main-publication", "main-root", None, &main_manifest),
                ),
                false,
            )
            .await?;
        self.observations.matching_publish_accepted =
            publication_status(&main_publish) == Some(PublicationCommandStatus::Accepted);
        let state = retry_publication_read(self.address(102)?).await?;
        self.observations.root_install_retired_intent = state.roots.get("main-root")
            == Some(&main_manifest)
            && !state.intents.contains_key("main-publication");

        let pin = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    9,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Pin {
                        pin_id: "query-pin".to_owned(),
                        expected: None,
                        manifest: main_manifest.clone(),
                    },
                ),
                false,
            )
            .await?;
        self.observations.pin_installed =
            publication_status(&pin) == Some(PublicationCommandStatus::Accepted);
        let wrong_pin = object_reference(
            "objects/wrong-pin-manifest",
            PublicationObjectKind::Manifest,
        );
        let stale_unpin = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    10,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Unpin {
                        pin_id: "query-pin".to_owned(),
                        expected: wrong_pin,
                    },
                ),
                false,
            )
            .await?;
        self.observations.stale_pin_rejected =
            publication_status(&stale_unpin) == Some(PublicationCommandStatus::PinCompareFailed);
        let exact_unpin = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    11,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Unpin {
                        pin_id: "query-pin".to_owned(),
                        expected: main_manifest.clone(),
                    },
                ),
                false,
            )
            .await?;
        self.observations.exact_unpin_accepted =
            publication_status(&exact_unpin) == Some(PublicationCommandStatus::Accepted);

        let mark_epoch = retry_publication_read(self.address(102)?)
            .await?
            .root_intent_epoch;
        let _ = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    12,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Pin {
                        pin_id: "epoch-bump".to_owned(),
                        expected: None,
                        manifest: main_manifest.clone(),
                    },
                ),
                false,
            )
            .await?;
        let stale_reserve = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    13,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::ReserveDelete {
                        plan_id: "stale-plan".to_owned(),
                        mark_epoch,
                        key: "objects/stale-delete".to_owned(),
                        identity: object_identity("objects/stale-delete"),
                    },
                ),
                false,
            )
            .await?;
        self.observations.stale_delete_plan_rejected = publication_status(&stale_reserve)
            == Some(PublicationCommandStatus::RootIntentEpochChanged);

        self.restart_canonical(101, false).await?;
        let current_index = status(self.address(102)?)
            .await?
            .last_applied_index
            .unwrap_or_default();
        if !wait_for_applied_index(self.address(101)?, current_index).await {
            return Err("restarted authority node did not catch up before failover".to_owned());
        }
        let current_epoch = retry_publication_read(self.address(102)?)
            .await?
            .root_intent_epoch;
        let reserve = self
            .write_publication(
                102,
                publication_command(
                    self.seed,
                    14,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::ReserveDelete {
                        plan_id: "delete-plan".to_owned(),
                        mark_epoch: current_epoch,
                        key: "objects/delete-target".to_owned(),
                        identity: object_identity("objects/delete-target"),
                    },
                ),
                false,
            )
            .await?;
        let permit = publication_permit(&reserve)
            .ok_or_else(|| "delete reservation did not return a permit".to_owned())?;
        self.observations.reservation_committed =
            publication_status(&reserve) == Some(PublicationCommandStatus::Accepted);
        self.observations.deletion_reservations += 1;
        if let Some(index) = reserve.log_index {
            if !wait_for_applied_index(self.address(101)?, index).await {
                return Err("delete reservation did not replicate before leader loss".to_owned());
            }
        }

        self.kill_node(102)?;
        self.observations.authority_failovers +=
            u64::from(elect_until_leader(self.address(103)?, 103).await);
        let state = retry_publication_read(self.address(103)?).await?;
        self.observations.reservation_survived_failover =
            state.deletion_reservations.get("objects/delete-target") == Some(&permit);

        let blocked_manifest =
            object_reference("objects/blocked-manifest", PublicationObjectKind::Manifest);
        let blocked_intent = publication_intent(
            &blocked_manifest,
            "objects/delete-target",
            "blocked-root",
            None,
        );
        let blocked = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    15,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Prepare {
                        publication_id: "blocked-publication".to_owned(),
                        intent: blocked_intent,
                    },
                ),
                false,
            )
            .await?;
        self.observations.reservation_blocked_prepare =
            publication_status(&blocked) == Some(PublicationCommandStatus::ObjectDeletionReserved);

        let forged = forge_permit_position(&permit)?;
        let forged_retire = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    16,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::RetireDelete { permit: forged },
                ),
                false,
            )
            .await?;
        self.observations.forged_retirement_rejected = publication_status(&forged_retire)
            == Some(PublicationCommandStatus::DeletePlanMismatch);
        let exact_retire = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    17,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::RetireDelete {
                        permit: permit.clone(),
                    },
                ),
                false,
            )
            .await?;
        self.observations.exact_retirement_accepted =
            publication_status(&exact_retire) == Some(PublicationCommandStatus::Accepted);
        let fresh = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    18,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Prepare {
                        publication_id: "fresh-after-retire".to_owned(),
                        intent: publication_intent(
                            &blocked_manifest,
                            "objects/delete-target",
                            "fresh-root",
                            None,
                        ),
                    },
                ),
                false,
            )
            .await?;
        self.observations.retirement_allowed_prepare =
            publication_status(&fresh) == Some(PublicationCommandStatus::Accepted);

        let unknown = control_with_timeout::<_, WriteAck>(
            self.address(103)?,
            PREAUTHORIZED_CLIENT_WRITE,
            &ControlWrite {
                app_data: b"OKVP9{}".to_vec(),
                drop_reply_after_commit: false,
                credential: None,
            },
            Duration::from_secs(8),
        )
        .await;
        self.observations.unknown_command_rejected =
            unknown.is_err_and(|error| error.contains("unknown objectKV command version"));

        let cross_manifest = object_reference(
            "objects/cross-generation-manifest",
            PublicationObjectKind::Manifest,
        );
        let cross_intent = publication_intent(
            &cross_manifest,
            "objects/cross-generation-data",
            "cross-generation-root",
            None,
        );
        let cross_prepare = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    19,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::Prepare {
                        publication_id: "cross-generation-publication".to_owned(),
                        intent: cross_intent,
                    },
                ),
                false,
            )
            .await?;
        if publication_status(&cross_prepare) != Some(PublicationCommandStatus::Accepted) {
            return Err("cross-generation fixture intent was not prepared".to_owned());
        }
        let cross_epoch = retry_publication_read(self.address(103)?)
            .await?
            .root_intent_epoch;
        let cross_reserve = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    20,
                    GENERATION_ONE,
                    "tx-g7",
                    PublicationAction::ReserveDelete {
                        plan_id: "cross-generation-delete".to_owned(),
                        mark_epoch: cross_epoch,
                        key: "objects/cross-generation-delete".to_owned(),
                        identity: object_identity("objects/cross-generation-delete"),
                    },
                ),
                false,
            )
            .await?;
        let cross_permit = publication_permit(&cross_reserve)
            .ok_or_else(|| "cross-generation delete fixture was not reserved".to_owned())?;
        self.transition_generation(103).await?;
        let cross_publish = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    21,
                    GENERATION_TWO,
                    "tx-g8",
                    publish_action(
                        "cross-generation-publication",
                        "cross-generation-root",
                        None,
                        &cross_manifest,
                    ),
                ),
                false,
            )
            .await?;
        self.observations.cross_generation_intent_rejected = publication_status(&cross_publish)
            == Some(PublicationCommandStatus::CrossGenerationIntent);
        let cross_retire = self
            .write_publication(
                103,
                publication_command(
                    self.seed,
                    22,
                    GENERATION_TWO,
                    "tx-g8",
                    PublicationAction::RetireDelete {
                        permit: cross_permit,
                    },
                ),
                false,
            )
            .await?;
        self.observations.cross_generation_delete_rejected = publication_status(&cross_retire)
            == Some(PublicationCommandStatus::CrossGenerationDeletePermit);

        self.run_isolated_outcome_probe(lost_command.identity)
            .await?;
        self.run_quorum_ack_probe().await?;
        self.capture_exact_restarted_state().await?;

        Ok(build_report(self.seed, self.mode, &self.observations))
    }

    async fn start_initial_cluster(&mut self) -> Result<(), String> {
        for node_id in AUTHORITY_NODES {
            self.start_canonical(node_id, false)?;
        }
        for node_id in AUTHORITY_NODES {
            wait_ready(self.address(node_id)?).await?;
        }
        retry_control(self.address(101)?, INITIALIZE, &()).await?;
        if !elect_until_leader(self.address(101)?, 101).await {
            return Err("publication authority leader election failed".to_owned());
        }
        let members = recovery_members(&[201, 202, 203])?;
        let bootstrap = retry_generation_write(
            self.address(101)?,
            &GenerationCommand {
                identity: generation_identity(self.seed, 1),
                action: GenerationAction::Bootstrap {
                    cell_id: CELL_ID,
                    generation: GENERATION_ONE,
                    transaction_system_id: "tx-g7".to_owned(),
                    transaction_system_members: members,
                    wal_root: "wal-g7".to_owned(),
                    control_root_version: 1,
                },
            },
        )
        .await?;
        self.observations.coordinator_bootstrapped = bootstrap.status
            == GenerationCommandStatus::Accepted
            && bootstrap.state.authorizes(GENERATION_ONE, "tx-g7");
        Ok(())
    }

    async fn transition_generation(&mut self, leader: NodeId) -> Result<(), String> {
        let old_members = recovery_members(&[201, 202, 203])?;
        let new_members = recovery_members(&[301, 302, 303])?;
        let prepared = retry_generation_write(
            self.address(leader)?,
            &GenerationCommand {
                identity: generation_identity(self.seed, 2),
                action: GenerationAction::Prepare {
                    expected_generation: GENERATION_ONE,
                    next_generation: GENERATION_TWO,
                    expected_control_root_version: 1,
                    recovery_id: RECOVERY_ID,
                    next_transaction_system_id: "tx-g8".to_owned(),
                    next_transaction_system_members: new_members.clone(),
                },
            },
        )
        .await?;
        if prepared.status != GenerationCommandStatus::Accepted {
            return Err("generation transition prepare failed".to_owned());
        }
        let fence_position = RecoveryLogPosition {
            term: 1,
            index: 100,
        };
        let fence = recovery_certificate(
            RecoveryCertificateKind::Fence,
            &prepared.state,
            fence_position,
            &old_members,
            &[201, 202],
        )?;
        let reserved = retry_generation_write(
            self.address(leader)?,
            &GenerationCommand {
                identity: generation_identity(self.seed, 3),
                action: GenerationAction::Reserve {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g8".to_owned(),
                    expected_control_root_version: 1,
                    certificate: Some(fence),
                },
            },
        )
        .await?;
        if reserved.status != GenerationCommandStatus::Accepted {
            return Err("generation transition reserve failed".to_owned());
        }
        let recovered_position = RecoveryLogPosition {
            term: 1,
            index: 101,
        };
        let recovered = recovery_certificate(
            RecoveryCertificateKind::Recovered,
            &reserved.state,
            recovered_position,
            &new_members,
            &[301, 302],
        )?;
        let activated = retry_generation_write(
            self.address(leader)?,
            &GenerationCommand {
                identity: generation_identity(self.seed, 4),
                action: GenerationAction::Activate {
                    generation: GENERATION_TWO,
                    recovery_id: RECOVERY_ID,
                    transaction_system_id: "tx-g8".to_owned(),
                    wal_root: "wal-g8".to_owned(),
                    expected_control_root_version: 1,
                    next_control_root_version: 2,
                    certificate: Some(recovered),
                },
            },
        )
        .await?;
        if activated.status != GenerationCommandStatus::Accepted
            || !activated.state.authorizes(GENERATION_TWO, "tx-g8")
        {
            return Err("generation transition activation failed".to_owned());
        }
        self.observations.generation_transitions += 1;
        Ok(())
    }

    async fn run_isolated_outcome_probe(
        &mut self,
        identity: RequestIdentity,
    ) -> Result<(), String> {
        let probe_address = allocate_addresses(&[102])?
            .remove(&102)
            .ok_or_else(|| "missing isolated probe address".to_owned())?;
        self.start_node(
            102,
            BTreeMap::from([(102, probe_address.clone())]),
            self.root.probe(102),
            false,
        )?;
        wait_ready(&probe_address).await?;
        let result = control_with_timeout::<_, Option<ApplyResponse>>(
            &probe_address,
            PUBLICATION_OUTCOME,
            &identity,
            Duration::from_secs(2),
        )
        .await;
        self.observations.isolated_stale_outcome_safe = !matches!(result, Ok(None));
        self.kill_node(102)?;
        self.restart_canonical(102, false).await?;
        let expected = status(self.address(103)?)
            .await?
            .last_applied_index
            .unwrap_or_default();
        if !wait_for_applied_index(self.address(102)?, expected).await {
            return Err("canonical node did not catch up after isolated outcome probe".to_owned());
        }
        Ok(())
    }

    async fn run_quorum_ack_probe(&mut self) -> Result<(), String> {
        if self.mode == PublicationProcessMode::AcknowledgeBeforeQuorum {
            self.kill_node(101)?;
            self.restart_canonical(101, true).await?;
            let expected = status(self.address(103)?)
                .await?
                .last_applied_index
                .unwrap_or_default();
            if !wait_for_applied_index(self.address(101)?, expected).await {
                return Err("acknowledgement probe leader did not catch up".to_owned());
            }
        }
        if !elect_until_leader(self.address(101)?, 101).await {
            return Err("could not elect acknowledgement probe leader".to_owned());
        }
        self.kill_node(102)?;
        self.kill_node(103)?;
        let command = publication_command(
            self.seed,
            23,
            GENERATION_TWO,
            "tx-g8",
            PublicationAction::Prepare {
                publication_id: "quorumless-publication".to_owned(),
                intent: publication_intent(
                    &object_reference(
                        "objects/quorumless-manifest",
                        PublicationObjectKind::Manifest,
                    ),
                    "objects/quorumless-data",
                    "quorumless-root",
                    None,
                ),
            },
        );
        let result = control_with_timeout::<_, WriteAck>(
            self.address(101)?,
            PUBLICATION_WRITE,
            &PublicationWriteRequest {
                command: command.clone(),
                drop_reply_after_commit: false,
            },
            Duration::from_millis(750),
        )
        .await;
        self.observations.quorumless_write_not_acknowledged = result.is_err();
        self.kill_node(101)?;
        self.restart_canonical(102, false).await?;
        self.restart_canonical(103, false).await?;
        if !elect_until_leader(self.address(102)?, 102).await {
            return Err("survivors did not elect after acknowledgement probe".to_owned());
        }
        let outcome = retry_publication_outcome(self.address(102)?, command.identity).await?;
        self.observations.quorumless_outcome_absent = outcome.is_none();
        self.restart_canonical(101, false).await?;
        let expected = status(self.address(102)?)
            .await?
            .last_applied_index
            .unwrap_or_default();
        if !wait_for_applied_index(self.address(101)?, expected).await {
            return Err("former probe leader did not reconcile its uncommitted tail".to_owned());
        }
        Ok(())
    }

    async fn capture_exact_restarted_state(&mut self) -> Result<(), String> {
        let mut states = Vec::new();
        for node_id in AUTHORITY_NODES {
            if !elect_until_leader(self.address(node_id)?, node_id).await {
                return Err(format!(
                    "could not elect node {node_id} for exact-state probe"
                ));
            }
            states.push(retry_publication_read(self.address(node_id)?).await?);
        }
        self.observations.restarted_nodes_exact = states
            .first()
            .is_some_and(|first| states.iter().all(|state| state == first));
        self.observations.final_state = states.into_iter().next();
        Ok(())
    }

    async fn write_publication(
        &mut self,
        node_id: NodeId,
        command: PublicationCommand,
        drop_reply_after_commit: bool,
    ) -> Result<WriteAck, String> {
        self.observations.publication_writes += 1;
        retry_publication_write(self.address(node_id)?, &command, drop_reply_after_commit).await
    }

    fn start_canonical(
        &mut self,
        node_id: NodeId,
        acknowledge_before_quorum: bool,
    ) -> Result<(), String> {
        self.start_node(
            node_id,
            self.addresses.clone(),
            self.root.node(node_id),
            acknowledge_before_quorum,
        )
    }

    async fn restart_canonical(
        &mut self,
        node_id: NodeId,
        acknowledge_before_quorum: bool,
    ) -> Result<(), String> {
        self.start_canonical(node_id, acknowledge_before_quorum)?;
        wait_ready(self.address(node_id)?).await?;
        self.observations.restarted_nodes += 1;
        Ok(())
    }

    fn start_node(
        &mut self,
        node_id: NodeId,
        nodes: BTreeMap<NodeId, String>,
        root: PathBuf,
        acknowledge_before_quorum: bool,
    ) -> Result<(), String> {
        self.children.start(
            self.executable,
            &ProcessNodeConfig {
                node_id,
                root,
                nodes,
                deduplicate_requests: self.mode != PublicationProcessMode::DisableRequestDedup,
                acknowledge_before_quorum,
                policy: ProcessNodePolicy {
                    role: ConsensusProcessRole::GenerationAuthority,
                    generation_authority_faults: GenerationAuthorityFaults::default(),
                    generation_fence_faults: GenerationFenceFaults {
                        allow_preauthorized_test_write: true,
                        ..GenerationFenceFaults::default()
                    },
                    publication_authority_faults: self.mode.authority_faults(),
                    publication_fence_faults: self.mode.fence_faults(),
                    ..ProcessNodePolicy::default()
                },
            },
        )?;
        self.observations.authority_process_starts += 1;
        Ok(())
    }

    fn kill_node(&mut self, node_id: NodeId) -> Result<(), String> {
        self.children.kill(node_id)?;
        self.observations.process_kills += 1;
        Ok(())
    }

    fn address(&self, node_id: NodeId) -> Result<&str, String> {
        self.addresses
            .get(&node_id)
            .map(String::as_str)
            .ok_or_else(|| format!("missing address for node {node_id}"))
    }
}

#[allow(clippy::too_many_lines)]
fn build_report(
    seed: u64,
    mode: PublicationProcessMode,
    observations: &Observations,
) -> PublicationProcessReport {
    let checks = BTreeMap::from([
        (
            "active_generation_authorizes_publication".to_owned(),
            observations.coordinator_bootstrapped && observations.active_prepare_accepted,
        ),
        (
            "stale_generation_is_fenced".to_owned(),
            observations.stale_generation_rejected,
        ),
        (
            "unknown_outcome_resolves_by_request_identity".to_owned(),
            observations.lost_reply_observed && observations.unknown_outcome_recovered,
        ),
        (
            "outcome_resolution_is_linearizable".to_owned(),
            observations.unknown_outcome_recovered && observations.isolated_stale_outcome_safe,
        ),
        (
            "prepare_survives_authority_leader_loss".to_owned(),
            observations.prepare_survived_failover,
        ),
        (
            "same_request_replays_exact_outcome".to_owned(),
            observations.exact_retry_replayed,
        ),
        (
            "conflicting_request_identity_is_rejected".to_owned(),
            observations.conflicting_identity_rejected,
        ),
        (
            "publish_requires_matching_intent".to_owned(),
            observations.missing_intent_rejected,
        ),
        (
            "root_publication_requires_expected_prior_root".to_owned(),
            observations.stale_root_rejected,
        ),
        (
            "matching_publish_commits".to_owned(),
            observations.matching_publish_accepted,
        ),
        (
            "root_install_retires_intent_atomically".to_owned(),
            observations.root_install_retired_intent,
        ),
        (
            "pin_mutation_requires_expected_value".to_owned(),
            observations.pin_installed
                && observations.stale_pin_rejected
                && observations.exact_unpin_accepted,
        ),
        (
            "root_epoch_fences_stale_delete_plan".to_owned(),
            observations.stale_delete_plan_rejected,
        ),
        (
            "delete_reservation_is_committed".to_owned(),
            observations.reservation_committed,
        ),
        (
            "delete_reservation_survives_authority_leader_loss".to_owned(),
            observations.reservation_survived_failover,
        ),
        (
            "delete_reservation_blocks_intersecting_prepare".to_owned(),
            observations.reservation_blocked_prepare,
        ),
        (
            "retirement_requires_exact_grant".to_owned(),
            observations.forged_retirement_rejected && observations.exact_retirement_accepted,
        ),
        (
            "retirement_allows_fresh_prepare".to_owned(),
            observations.retirement_allowed_prepare,
        ),
        (
            "intent_owner_generation_is_fenced".to_owned(),
            observations.cross_generation_intent_rejected,
        ),
        (
            "delete_permit_owner_generation_is_fenced".to_owned(),
            observations.cross_generation_delete_rejected,
        ),
        (
            "local_stale_outcome_is_not_authoritative".to_owned(),
            observations.isolated_stale_outcome_safe,
        ),
        (
            "unknown_publication_command_fails_closed".to_owned(),
            observations.unknown_command_rejected,
        ),
        (
            "acknowledgement_requires_quorum".to_owned(),
            observations.quorumless_write_not_acknowledged
                && observations.quorumless_outcome_absent,
        ),
        (
            "restarted_nodes_recover_exact_state".to_owned(),
            observations.restarted_nodes_exact,
        ),
    ]);
    let first = checks.iter().enumerate().find(|(_, (_, passed))| !**passed);
    let anomaly_count = checks.values().filter(|passed| !**passed).count() as u64;
    let first_mismatch_step = first.map(|(index, _)| (index + 1) as u64);
    let first_mismatch = first.map(|(_, (name, _))| name.clone());

    let mut trace = Sha256::new();
    trace.update(b"okv-publication-process-contract-v1");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    if let Some(state) = &observations.final_state {
        let mut canonical = serde_json::to_value(state).unwrap_or_default();
        normalize_terms(&mut canonical);
        trace.update(serde_json::to_vec(&canonical).unwrap_or_default());
    }

    PublicationProcessReport {
        seed,
        mode,
        executed_checks: checks.len() as u64,
        anomaly_count,
        first_mismatch_step,
        first_mismatch,
        authority_process_starts: observations.authority_process_starts,
        process_kills: observations.process_kills,
        authority_failovers: observations.authority_failovers,
        publication_writes: observations.publication_writes,
        generation_transitions: observations.generation_transitions,
        dropped_replies: observations.dropped_replies,
        recovered_outcomes: observations.recovered_outcomes,
        duplicate_retries: observations.duplicate_retries,
        deletion_reservations: observations.deletion_reservations,
        restarted_nodes: observations.restarted_nodes,
        checks,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

fn normalize_terms(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, nested) in fields {
                if key == "term" {
                    *nested = serde_json::Value::from(0_u64);
                } else {
                    normalize_terms(nested);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                normalize_terms(nested);
            }
        }
        _ => {}
    }
}

fn publication_command(
    seed: u64,
    request_id: u64,
    generation: u64,
    transaction_system_id: &str,
    action: PublicationAction,
) -> PublicationCommand {
    PublicationCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x5055_424c_4943_4154,
            request_id,
        },
        credential: GenerationCredential {
            generation,
            transaction_system_id: transaction_system_id.to_owned(),
        },
        action,
    }
}

fn generation_identity(seed: u64, request_id: u64) -> RequestIdentity {
    RequestIdentity {
        client_id: seed ^ 0x4745_4e45_5241_5445,
        request_id,
    }
}

fn publish_action(
    publication_id: &str,
    destination_root: &str,
    expected_prior_root: Option<PublicationObjectReference>,
    manifest: &PublicationObjectReference,
) -> PublicationAction {
    PublicationAction::Publish {
        publication_id: publication_id.to_owned(),
        destination_root: destination_root.to_owned(),
        expected_prior_root,
        manifest: manifest.clone(),
    }
}

fn publication_intent(
    manifest: &PublicationObjectReference,
    child: &str,
    destination_root: &str,
    expected_prior_root: Option<PublicationObjectReference>,
) -> PublicationIntent {
    PublicationIntent {
        object_keys: BTreeSet::from([manifest.key.clone(), child.to_owned()]),
        manifest: manifest.clone(),
        destination_root: destination_root.to_owned(),
        expected_prior_root,
    }
}

fn object_reference(key: &str, kind: PublicationObjectKind) -> PublicationObjectReference {
    PublicationObjectReference {
        kind,
        key: key.to_owned(),
        length: key.len() as u64,
        sha256: sha256(key.as_bytes()),
    }
}

fn object_identity(key: &str) -> PublicationObjectIdentity {
    PublicationObjectIdentity {
        revision: PublicationRevisionToken {
            e_tag: Some(format!("etag-{}", &sha256(key.as_bytes())[..16])),
            version: None,
        },
        length: key.len() as u64,
        sha256: sha256(key.as_bytes()),
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn publication_status(ack: &WriteAck) -> Option<PublicationCommandStatus> {
    ack.response
        .as_ref()
        .and_then(|response| response.publication.as_ref())
        .map(|response| response.status)
}

fn publication_permit(ack: &WriteAck) -> Option<PublicationDeletePermit> {
    ack.response
        .as_ref()
        .and_then(|response| response.publication.as_ref())
        .and_then(|response| response.outcome.as_ref())
        .and_then(|outcome| match outcome {
            PublicationOutcome::DeleteReserved { permit } => Some(permit.clone()),
            PublicationOutcome::Applied => None,
        })
}

fn forge_permit_position(
    permit: &PublicationDeletePermit,
) -> Result<PublicationDeletePermit, String> {
    let mut value = serde_json::to_value(permit).map_err(|error| error.to_string())?;
    let position = value
        .get_mut("authority_position")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| "delete permit did not encode authority position".to_owned())?;
    let index = position
        .get("index")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "delete permit did not encode authority index".to_owned())?;
    position.insert(
        "index".to_owned(),
        serde_json::Value::from(index.saturating_sub(1)),
    );
    serde_json::from_value(value).map_err(|error| error.to_string())
}

fn recovery_seed(node_id: NodeId) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"OKV-PUBLICATION-PROCESS-RECOVERY-SIGNER-V1\0");
    digest.update(node_id.to_be_bytes());
    digest.finalize().to_vec()
}

fn recovery_members(node_ids: &[NodeId]) -> Result<BTreeMap<NodeId, Vec<u8>>, String> {
    node_ids
        .iter()
        .map(|node_id| {
            recovery_public_key(&recovery_seed(*node_id)).map(|public_key| (*node_id, public_key))
        })
        .collect()
}

fn recovery_certificate(
    kind: RecoveryCertificateKind,
    state: &crate::GenerationAuthorityState,
    position: RecoveryLogPosition,
    members: &BTreeMap<NodeId, Vec<u8>>,
    signers: &[NodeId],
) -> Result<RecoveryCertificate, String> {
    let statement = RecoveryCertificateStatement::new(kind, state, position, members);
    let attestations = signers
        .iter()
        .map(|signer| sign_recovery_statement(*signer, &recovery_seed(*signer), &statement))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecoveryCertificate {
        statement,
        attestations,
    })
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

async fn retry_publication_write(
    address: &str,
    command: &PublicationCommand,
    drop_reply_after_commit: bool,
) -> Result<WriteAck, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match publication_write(address, command, drop_reply_after_commit).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        if drop_reply_after_commit {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("publication write failed at {address}: {last}"))
}

async fn publication_write(
    address: &str,
    command: &PublicationCommand,
    drop_reply_after_commit: bool,
) -> Result<WriteAck, String> {
    control(
        address,
        PUBLICATION_WRITE,
        &PublicationWriteRequest {
            command: command.clone(),
            drop_reply_after_commit,
        },
    )
    .await
}

async fn retry_publication_read(address: &str) -> Result<PublicationAuthorityState, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, PUBLICATION_READ, &()).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("publication read failed at {address}: {last}"))
}

async fn retry_publication_outcome(
    address: &str,
    identity: RequestIdentity,
) -> Result<Option<ApplyResponse>, String> {
    let mut last = String::new();
    for _ in 0..RETRY_ATTEMPTS {
        match control(address, PUBLICATION_OUTCOME, &identity).await {
            Ok(response) => return Ok(response),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "publication outcome read failed at {address}: {last}"
    ))
}

async fn status(address: &str) -> Result<NodeStatus, String> {
    control(address, STATUS, &()).await
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

async fn wait_for_applied_index(address: &str, expected: u64) -> bool {
    for _ in 0..RETRY_ATTEMPTS {
        if status(address).await.is_ok_and(|node| {
            node.last_applied_index
                .is_some_and(|index| index >= expected)
        }) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
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

async fn control<Req, Resp>(address: &str, kind: u8, request: &Req) -> Result<Resp, String>
where
    Req: Serialize,
    Resp: DeserializeOwned,
{
    control_with_timeout(address, kind, request, Duration::from_secs(8)).await
}

async fn control_with_timeout<Req, Resp>(
    address: &str,
    kind: u8,
    request: &Req,
    response_timeout: Duration,
) -> Result<Resp, String>
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
        tokio::time::timeout(response_timeout, read_response(&mut stream))
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

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: PublicationProcessMode) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-publication-process-{}-{seed}-{}-{sequence}",
            mode.id(),
            std::process::id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn node(&self, node_id: NodeId) -> PathBuf {
        self.0.join(format!("node-{node_id}"))
    }

    fn probe(&self, node_id: NodeId) -> PathBuf {
        self.0.join(format!("probe-node-{node_id}"))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
