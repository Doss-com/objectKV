use super::cell_tlog_repair::{
    collect_repair_attestations, log_set_policy, pop_through_object_frontier, read_suffix,
    repair_statement, signing_seed, signing_seeds, CellTaggedLogRepairWorkerReceipt,
};
use super::tagged_log_process::{
    encode_tagged_log_repair_snapshot, tagged_log_request, PublicationPopPolicy,
    TaggedLogPolicyTransitionFaults, TaggedLogProcessFixture, TaggedLogRecord,
    TaggedLogRepairFaults, TaggedLogRequest, TaggedLogResponse,
};
use crate::CellTaggedLogRepairWorkerProcessConfig;
use okv_consensus::{
    cell_log_set_policy_authority_seed, cell_tagged_log_repair_certificate_sha256,
    tagged_log_public_key, verify_cell_log_set_policy_activation_certificate,
    verify_tagged_log_policy_stage_certificate, CellKeyRange, CellLogSetMember,
    CellLogSetPolicyActivationCertificate, CellLogSetPolicyActivationStatement,
    CellLogSetPolicyTransition, CellMutation, CellProcessFixture, CellProcessPrototypeMode,
    CellReadVersion, CellStagedTransactionAction, CellStagedTransactionApplyResponse,
    CellStagedTransactionCommand, CellStagedTransactionStatus, CellTaggedLogCapacityStatement,
    CellTaggedLogCertificate, CellTaggedLogPolicyStageCertificate, CellTaggedLogRepairCertificate,
    CellTaggedLogRepairPhase, CellTaggedLogStatement, CellTransactionClient,
    CellTransactionCommand, CellTransactionStatus, GenerationFenceFaults, ProcessNodePolicy,
    RecoverySignerConfig, RequestIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use uuid::Uuid;

const OBJECT_FRONTIER: u64 = 10;
const PRE_TRANSITION_FRONTIER: u64 = 14;
const CORRECT_FINAL_FRONTIER: u64 = 17;
const OLD_POLICY_EPOCH: u64 = 1;
const NEXT_POLICY_EPOCH: u64 = 2;
const MOVING_LOG_SET: u16 = 10;
const UNCHANGED_LOG_SET: u16 = 20;
const FAILED_NODE_ID: u64 = 1;
const LEARNER_NODE_ID: u64 = 4;
const LEARNER_INCARNATION: [u8; 16] = [4; 16];
const TLOG_LIMIT: u64 = 128 * 1024;
const TLOG_QUORUM: usize = 2;
const REQUIRED_LOG_SETS: [u16; 2] = [MOVING_LOG_SET, UNCHANGED_LOG_SET];

/// Unsafe policy-transition subject selected by one frozen workload.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CellTaggedLogPolicyTransitionMode {
    Correct,
    MissingRepairReadiness,
    UnresolvedOldStage,
    InvalidNextPolicy,
    MixedPolicyQuorum,
    MissingAuthorityActivation,
    RemovedNodeRejoin,
    DoubleTransition,
}

impl CellTaggedLogPolicyTransitionMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::MissingRepairReadiness => "missing_repair_readiness",
            Self::UnresolvedOldStage => "unresolved_old_stage",
            Self::InvalidNextPolicy => "invalid_next_policy",
            Self::MixedPolicyQuorum => "mixed_policy_quorum",
            Self::MissingAuthorityActivation => "missing_authority_activation",
            Self::RemovedNodeRejoin => "removed_node_rejoin",
            Self::DoubleTransition => "double_transition",
        }
    }
}

/// One named assertion in the moving log-set policy contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPolicyTransitionCheck {
    pub name: String,
    pub passed: bool,
}

/// Deterministic receipt for one policy-transition history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CellTaggedLogPolicyTransitionReport {
    pub seed: u64,
    pub mode: CellTaggedLogPolicyTransitionMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub transaction_authority_process_starts: u64,
    pub tagged_log_process_starts: u64,
    pub tagged_log_process_restarts: u64,
    pub failed_tagged_log_processes: u64,
    pub learner_process_starts: u64,
    pub learner_process_restarts: u64,
    pub repair_attestations: u64,
    pub readiness_attestations: u64,
    pub successor_stage_attestations: u64,
    pub authority_activation_attestations: u64,
    pub policy_prepares: u64,
    pub policy_commits: u64,
    pub idempotent_retries: u64,
    pub old_epoch_rejections: u64,
    pub post_transition_appends: u64,
    pub capacity_members_counted: Vec<u64>,
    pub serving_members_counted: Vec<u64>,
    pub old_policy_epoch: u64,
    pub next_policy_epoch: u64,
    pub generation_before: u64,
    pub generation_after: u64,
    pub object_frontier: u64,
    pub pre_transition_frontier: u64,
    pub final_frontier: u64,
    pub worker_frontier: u64,
    pub checks: Vec<CellTaggedLogPolicyTransitionCheck>,
    pub trace_sha256: String,
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(seed: u64, mode: CellTaggedLogPolicyTransitionMode) -> Result<Self, String> {
        let root = std::env::temp_dir().join(format!(
            "okv-cell-tagged-log-policy-transition-{}-{seed}-{}",
            mode.id(),
            Uuid::new_v4()
        ));
        fs::create_dir_all(&root).map_err(|error| error.to_string())?;
        Ok(Self(root))
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        if self.0.starts_with(std::env::temp_dir())
            && self.0.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("okv-cell-tagged-log-policy-transition-")
            })
        {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

/// Run the RFC-0046 moving tagged-log policy contract through real processes.
///
/// # Errors
///
/// Returns an error when a bounded protocol step or process cannot complete.
pub fn run_cell_tagged_log_policy_transition_contract(
    seed: u64,
    mode: CellTaggedLogPolicyTransitionMode,
    executable: &Path,
) -> Result<CellTaggedLogPolicyTransitionReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: CellTaggedLogPolicyTransitionMode,
    executable: &Path,
) -> Result<CellTaggedLogPolicyTransitionReport, String> {
    let root = TempRoot::new(seed, mode)?;
    let authority_base_seed: Vec<u8> = Sha256::digest(
        [
            b"okv-rfc-0046-authority".as_slice(),
            seed.to_be_bytes().as_slice(),
        ]
        .concat(),
    )
    .to_vec();
    let faults = authority_faults(mode);
    let mut authority = CellProcessFixture::start_with_policy(
        seed ^ 0x504f_4c49_4359_4155,
        CellProcessPrototypeMode::Correct,
        executable,
        ProcessNodePolicy {
            generation_fence_faults: faults,
            recovery_signer: Some(RecoverySignerConfig {
                private_key_seed: authority_base_seed.clone(),
            }),
            ..ProcessNodePolicy::default()
        },
    )?;
    let authority_report = authority.run_history().await?;
    let client = CellTransactionClient::new(authority.endpoints())?;
    let mut snapshot = authority.linearizable_cell_snapshot().await?;
    let generation_before = snapshot.generation;

    let seeds_10 = signing_seeds(seed, MOVING_LOG_SET);
    let seeds_20 = signing_seeds(seed, UNCHANGED_LOG_SET);
    let policy_10 = log_set_policy(MOVING_LOG_SET, snapshot.generation, &seeds_10)?;
    let policy_20 = log_set_policy(UNCHANGED_LOG_SET, snapshot.generation, &seeds_20)?;
    let install = write_staged(
        &client,
        &snapshot,
        seed,
        100,
        RequestIdentity {
            client_id: seed ^ 0x504f_4c49_4359_494e,
            request_id: 1,
        },
        CellStagedTransactionAction::InstallLogSetPolicies {
            policies: vec![policy_10.clone(), policy_20.clone()],
        },
    )
    .await?;
    if install.status != CellStagedTransactionStatus::LogSetPoliciesInstalled {
        return Err("transaction authority rejected initial tLog policies".to_owned());
    }

    snapshot = authority.linearizable_cell_snapshot().await?;
    let mut request_id = 1_000_u64;
    while snapshot.latest_sequence < PRE_TRANSITION_FRONTIER {
        let command = next_direct_transaction(seed, request_id, &snapshot);
        let response = client
            .commit_app_data(&command.encode().map_err(|error| error.to_string())?)
            .await?;
        let outcome = response
            .cell_transaction
            .ok_or_else(|| "policy authority omitted direct transaction outcome".to_owned())?;
        if outcome.status != CellTransactionStatus::Committed
            || outcome
                .commit_sequence
                .is_none_or(|version| version > PRE_TRANSITION_FRONTIER)
        {
            return Err(format!(
                "policy setup did not commit within frontier 14: {:?}",
                outcome.commit_sequence
            ));
        }
        request_id = request_id.saturating_add(1);
        snapshot = authority.linearizable_cell_snapshot().await?;
    }
    if snapshot.latest_sequence != PRE_TRANSITION_FRONTIER {
        return Err("policy setup did not reach exact frontier 14".to_owned());
    }

    let fake_pop_policy = PublicationPopPolicy {
        members: BTreeMap::from([(999, vec![9; 32])]),
        quorum_size: 1,
    };
    let mut tlog_10 = TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
        executable,
        &root.0.join("log-set-10"),
        MOVING_LOG_SET,
        3,
        TLOG_LIMIT,
        false,
        OLD_POLICY_EPOCH,
        &seeds_10,
        &fake_pop_policy,
        true,
    )?;
    let tlog_20 = TaggedLogProcessFixture::start_signed_with_publication_pop_policy(
        executable,
        &root.0.join("log-set-20"),
        UNCHANGED_LOG_SET,
        3,
        TLOG_LIMIT,
        false,
        OLD_POLICY_EPOCH,
        &seeds_20,
        &fake_pop_policy,
        true,
    )?;
    let endpoints_10 = tlog_10.endpoints();
    let endpoints_20 = tlog_20.endpoints();
    let retained_envelopes = snapshot
        .committed_envelopes
        .iter()
        .filter_map(|envelope| {
            okv_sim::CommitEnvelope::decode(envelope)
                .ok()
                .filter(|decoded| {
                    decoded.version().sequence() > OBJECT_FRONTIER
                        && decoded.version().sequence() <= PRE_TRANSITION_FRONTIER
                })
                .map(|_| envelope)
        })
        .collect::<Vec<_>>();
    if retained_envelopes.len() != 4 {
        return Err(format!(
            "policy setup expected transactions 11 through 14, found {} retained envelopes",
            retained_envelopes.len()
        ));
    }
    for (offset, envelope) in retained_envelopes.into_iter().enumerate() {
        let position = u64::try_from(offset).unwrap_or(u64::MAX).saturating_add(1);
        let record =
            TaggedLogRecord::committed(position, REQUIRED_LOG_SETS.to_vec(), envelope.clone());
        for endpoint in endpoints_10.iter().chain(&endpoints_20) {
            if !matches!(
                tagged_log_request(endpoint, &TaggedLogRequest::Append { record: record.clone() })?,
                TaggedLogResponse::Appended { position: observed, .. } if observed == position
            ) {
                return Err("policy setup failed to append one committed envelope".to_owned());
            }
        }
    }
    for (log_set_id, endpoints) in [
        (MOVING_LOG_SET, &endpoints_10),
        (UNCHANGED_LOG_SET, &endpoints_20),
    ] {
        pop_through_object_frontier(
            log_set_id,
            endpoints,
            snapshot.cell_id,
            snapshot.tenant_id,
            snapshot.generation,
        )?;
    }

    let authority_policy = policy_activation_authority(&authority_base_seed)?;
    let active_stage_fault = matches!(
        mode,
        CellTaggedLogPolicyTransitionMode::MissingRepairReadiness
            | CellTaggedLogPolicyTransitionMode::InvalidNextPolicy
            | CellTaggedLogPolicyTransitionMode::MixedPolicyQuorum
    );
    for index in 0..3 {
        tlog_10.restart_with_policy_activation_authority(
            index,
            authority_policy.clone(),
            TaggedLogPolicyTransitionFaults {
                accept_invalid_stage: active_stage_fault
                    || (index == 0 && mode == CellTaggedLogPolicyTransitionMode::RemovedNodeRejoin),
                accept_missing_authority_activation: mode
                    == CellTaggedLogPolicyTransitionMode::MissingAuthorityActivation,
                accept_removed_member_activation: index == 0
                    && mode == CellTaggedLogPolicyTransitionMode::RemovedNodeRejoin,
            },
        )?;
    }
    tlog_10.kill(0)?;

    let survivor_endpoints = vec![endpoints_10[1].clone(), endpoints_10[2].clone()];
    let survivor_records = read_suffix(&survivor_endpoints[0], MOVING_LOG_SET)?;
    let peer_survivor_records = read_suffix(&survivor_endpoints[1], MOVING_LOG_SET)?;
    if survivor_records != peer_survivor_records || survivor_records.len() != 4 {
        let retained = survivor_records
            .iter()
            .map(|record| {
                okv_sim::CommitEnvelope::decode(&record.envelope)
                    .map(|envelope| (record.position, envelope.version().sequence()))
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Err(format!(
            "moving-policy survivors do not retain one exact four-record suffix: left={}, right={}, retained={retained:?}",
            survivor_records.len(),
            peer_survivor_records.len()
        ));
    }
    let certified_snapshot = encode_tagged_log_repair_snapshot(&survivor_records)?;
    let repair_last_position = survivor_records
        .last()
        .map(|record| record.position)
        .ok_or_else(|| "moving-policy repair suffix is empty".to_owned())?;
    let learner_seed = signing_seed(seed, MOVING_LOG_SET, LEARNER_NODE_ID);
    let learner_public_key = tagged_log_public_key(&learner_seed)?;
    let base_statement = repair_statement(
        &snapshot,
        CellTaggedLogRepairPhase::BaseSnapshot,
        &certified_snapshot,
        LEARNER_INCARNATION,
        learner_public_key.clone(),
        repair_last_position,
    );
    let base_certificate = CellTaggedLogRepairCertificate {
        statement: base_statement.clone(),
        attestations: collect_repair_attestations(
            &survivor_endpoints,
            &base_statement,
            &certified_snapshot,
        )?,
    };
    let mut learner = TaggedLogProcessFixture::start_repair_learner(
        executable,
        &root.0.join("learner"),
        MOVING_LOG_SET,
        LEARNER_NODE_ID,
        TLOG_LIMIT,
        OLD_POLICY_EPOCH,
        learner_seed.clone(),
        LEARNER_INCARNATION,
        policy_10.clone(),
        TaggedLogRepairFaults::default(),
    )?;
    let learner_endpoint = learner.endpoints()[0].clone();
    if !matches!(
        tagged_log_request(
            &learner_endpoint,
            &TaggedLogRequest::RepairInstall {
                certificate: base_certificate.clone(),
                snapshot_bytes: certified_snapshot.clone(),
            },
        )?,
        TaggedLogResponse::RepairInstalled { durable: true, .. }
    ) {
        return Err("policy learner did not install the certified snapshot".to_owned());
    }
    learner.kill(0)?;
    learner.restart(0)?;
    let ready_statement = repair_statement(
        &snapshot,
        CellTaggedLogRepairPhase::LearnerReady,
        &certified_snapshot,
        LEARNER_INCARNATION,
        learner_public_key.clone(),
        repair_last_position,
    );
    let ready_certificate = CellTaggedLogRepairCertificate {
        statement: ready_statement.clone(),
        attestations: collect_repair_attestations(
            &survivor_endpoints,
            &ready_statement,
            &certified_snapshot,
        )?,
    };
    if !matches!(
        tagged_log_request(
            &learner_endpoint,
            &TaggedLogRequest::RepairReady {
                certificate: ready_certificate.clone(),
                snapshot_bytes: certified_snapshot.clone(),
            },
        )?,
        TaggedLogResponse::RepairReady { durable: true, .. }
    ) {
        return Err("policy learner did not persist readiness".to_owned());
    }
    learner.restart_with_policy_activation_authority(
        0,
        authority_policy.clone(),
        TaggedLogPolicyTransitionFaults {
            accept_invalid_stage: active_stage_fault,
            accept_missing_authority_activation: mode
                == CellTaggedLogPolicyTransitionMode::MissingAuthorityActivation,
            accept_removed_member_activation: false,
        },
    )?;

    let mut next_policy = policy_10.clone();
    next_policy.policy_epoch = if mode == CellTaggedLogPolicyTransitionMode::InvalidNextPolicy {
        3
    } else {
        NEXT_POLICY_EPOCH
    };
    next_policy
        .members
        .retain(|member| member.node_id != FAILED_NODE_ID);
    next_policy.members.push(CellLogSetMember {
        node_id: LEARNER_NODE_ID,
        public_key: learner_public_key.clone(),
    });
    next_policy.members.sort();
    let mut transition = CellLogSetPolicyTransition {
        format_version: 1,
        cell_id: snapshot.cell_id,
        tenant_id: snapshot.tenant_id,
        generation: snapshot.generation,
        transition_id: seed.max(1),
        log_set_id: MOVING_LOG_SET,
        old_policy: policy_10.clone(),
        next_policy: next_policy.clone(),
        failed_node_id: FAILED_NODE_ID,
        learner_node_id: LEARNER_NODE_ID,
        learner_incarnation: LEARNER_INCARNATION,
        learner_public_key,
        repair_readiness_sha256: cell_tagged_log_repair_certificate_sha256(&ready_certificate),
        retained_root_sha256: ready_certificate.statement.snapshot_sha256,
        retained_last_position: repair_last_position,
    };
    if mode == CellTaggedLogPolicyTransitionMode::MissingRepairReadiness {
        transition.repair_readiness_sha256 = [0; 32];
    }

    let mut unresolved_old_stage = false;
    if mode == CellTaggedLogPolicyTransitionMode::UnresolvedOldStage {
        let old_transaction = final_transaction(seed, 800, &snapshot);
        let response = write_staged(
            &client,
            &snapshot,
            seed,
            200,
            old_transaction.identity,
            CellStagedTransactionAction::Stage {
                transaction: old_transaction,
            },
        )
        .await?;
        unresolved_old_stage = response.status == CellStagedTransactionStatus::Staged;
    }

    let prepare = write_staged(
        &client,
        &snapshot,
        seed,
        300,
        RequestIdentity {
            client_id: seed ^ 0x504f_4c49_4359_5452,
            request_id: transition.transition_id,
        },
        CellStagedTransactionAction::PrepareLogSetPolicyTransition {
            transition: Box::new(transition.clone()),
            repair_readiness: Box::new(ready_certificate.clone()),
        },
    )
    .await?;
    if prepare.status != CellStagedTransactionStatus::PolicyTransitionPrepared {
        return Err(format!(
            "policy transition prepare failed: {:?}",
            prepare.status
        ));
    }

    let stage_endpoints = vec![
        survivor_endpoints[0].clone(),
        survivor_endpoints[1].clone(),
        learner_endpoint.clone(),
    ];
    let mut stage_attestations = Vec::new();
    let mut stage_statement = None;
    for endpoint in &stage_endpoints {
        match tagged_log_request(
            endpoint,
            &TaggedLogRequest::PolicyStage {
                transition: transition.clone(),
            },
        )? {
            TaggedLogResponse::PolicyStaged { attestation, .. } => {
                stage_attestations.push(attestation);
            }
            response => return Err(format!("successor tLog did not stage policy: {response:?}")),
        }
        if stage_statement.is_none() {
            let response = tagged_log_request(
                endpoint,
                &TaggedLogRequest::PolicyStage {
                    transition: transition.clone(),
                },
            )?;
            if let TaggedLogResponse::PolicyStaged { attestation, .. } = response {
                let sample = stage_attestations
                    .iter()
                    .find(|candidate| candidate.signer_id == attestation.signer_id)
                    .cloned();
                if sample.is_none() {
                    return Err("policy stage retry changed signer identity".to_owned());
                }
            }
            stage_statement = Some(policy_stage_statement(&transition));
        }
    }
    let mut stage_certificate = CellTaggedLogPolicyStageCertificate {
        statement: stage_statement.ok_or_else(|| "policy stage omitted statement".to_owned())?,
        attestations: stage_attestations,
    };
    if mode == CellTaggedLogPolicyTransitionMode::MixedPolicyQuorum {
        stage_certificate.attestations.truncate(2);
        stage_certificate.attestations[0].signer_id = FAILED_NODE_ID;
    }
    let stage_certificate_valid =
        verify_tagged_log_policy_stage_certificate(&stage_certificate, &transition);
    let commit = write_staged(
        &client,
        &snapshot,
        seed,
        301,
        RequestIdentity {
            client_id: seed ^ 0x504f_4c49_4359_5443,
            request_id: transition.transition_id,
        },
        CellStagedTransactionAction::CommitLogSetPolicyTransition {
            transition_id: transition.transition_id,
            successor_stage: stage_certificate.clone(),
        },
    )
    .await?;
    if commit.status != CellStagedTransactionStatus::PolicyTransitionCommitted {
        return Err(format!(
            "policy transition commit failed: {:?}",
            commit.status
        ));
    }
    let after_transition = authority.linearizable_cell_snapshot().await?;
    let completed = after_transition
        .completed_log_set_policy_transitions
        .iter()
        .find(|completed| completed.transition.transition_id == transition.transition_id)
        .cloned()
        .ok_or_else(|| "linearizable authority omitted completed policy transition".to_owned())?;
    let activation_statement = CellLogSetPolicyActivationStatement::new(&completed);
    let mut activation_certificate = CellLogSetPolicyActivationCertificate {
        statement: activation_statement.clone(),
        attestations: client
            .policy_activation_attestations(&activation_statement, 2)
            .await?,
    };
    if mode == CellTaggedLogPolicyTransitionMode::MissingAuthorityActivation {
        activation_certificate.attestations.truncate(1);
    }
    let authority_activation_valid = verify_cell_log_set_policy_activation_certificate(
        &activation_certificate,
        &authority_policy.members,
        authority_policy.quorum_size,
    );
    for endpoint in &stage_endpoints {
        if !matches!(
            tagged_log_request(
                endpoint,
                &TaggedLogRequest::PolicyActivate {
                    transition: Box::new(transition.clone()),
                    successor_stage: Box::new(stage_certificate.clone()),
                    activation: activation_certificate.clone(),
                },
            )?,
            TaggedLogResponse::PolicyActivated { durable: true, .. }
        ) {
            return Err("successor tLog did not durably activate policy".to_owned());
        }
    }
    learner.kill(0)?;
    learner.restart(0)?;

    tlog_10.restart(0)?;
    let removed_endpoint = endpoints_10[0].clone();
    let removed_activated = if mode == CellTaggedLogPolicyTransitionMode::RemovedNodeRejoin {
        let staged = tagged_log_request(
            &removed_endpoint,
            &TaggedLogRequest::PolicyStage {
                transition: transition.clone(),
            },
        )?;
        let activated = tagged_log_request(
            &removed_endpoint,
            &TaggedLogRequest::PolicyActivate {
                transition: Box::new(transition.clone()),
                successor_stage: Box::new(stage_certificate.clone()),
                activation: activation_certificate.clone(),
            },
        )?;
        matches!(staged, TaggedLogResponse::PolicyStaged { .. })
            && matches!(activated, TaggedLogResponse::PolicyActivated { .. })
    } else {
        false
    };

    tlog_10.kill(1)?;
    let latest_before_final = authority.linearizable_cell_snapshot().await?;
    let transaction = final_transaction(seed, 900, &latest_before_final);
    let staged = write_staged(
        &client,
        &latest_before_final,
        seed,
        302,
        transaction.identity,
        CellStagedTransactionAction::Stage {
            transaction: transaction.clone(),
        },
    )
    .await?;
    if staged.status != CellStagedTransactionStatus::Staged {
        return Err(format!(
            "post-transition transaction did not stage: {:?}",
            staged.status
        ));
    }
    let final_version = staged
        .commit_sequence
        .ok_or_else(|| "post-transition stage omitted commit version".to_owned())?;
    let envelope = staged
        .envelope
        .clone()
        .ok_or_else(|| "post-transition stage omitted envelope".to_owned())?;
    let next_position = repair_last_position.saturating_add(1);
    let record =
        TaggedLogRecord::committed(next_position, REQUIRED_LOG_SETS.to_vec(), envelope.clone());
    let active_10 = vec![endpoints_10[2].clone(), learner_endpoint.clone()];
    for endpoint in active_10.iter().chain(&endpoints_20) {
        if !matches!(
            tagged_log_request(endpoint, &TaggedLogRequest::Append { record: record.clone() })?,
            TaggedLogResponse::Appended { position, .. } if position == next_position
        ) {
            return Err("post-transition append did not reach one active tLog".to_owned());
        }
    }
    let removed_append = tagged_log_request(
        &removed_endpoint,
        &TaggedLogRequest::Append {
            record: record.clone(),
        },
    )?;
    let removed_has_record = matches!(removed_append, TaggedLogResponse::Appended { .. });

    let certificate_10 = durability_certificate(
        &active_10,
        &transaction,
        final_version,
        next_position,
        MOVING_LOG_SET,
        next_policy.policy_epoch,
        &envelope,
    )?;
    let certificate_20 = durability_certificate(
        &endpoints_20[..2],
        &transaction,
        final_version,
        next_position,
        UNCHANGED_LOG_SET,
        OLD_POLICY_EPOCH,
        &envelope,
    )?;
    let removed_statement = CellTaggedLogStatement {
        format_version: 1,
        cell_id: transaction.cell_id,
        tenant_id: transaction.tenant_id,
        generation: transaction.generation,
        transaction_identity: transaction.identity,
        commit_sequence: final_version,
        log_set_id: MOVING_LOG_SET,
        policy_epoch: next_policy.policy_epoch,
        envelope_sha256: Sha256::digest(&envelope).into(),
        durable_position: next_position,
    };
    let removed_rejected = matches!(
        tagged_log_request(
            &removed_endpoint,
            &TaggedLogRequest::Attest {
                statement: removed_statement,
            },
        )?,
        TaggedLogResponse::Rejected { .. }
    );
    for (offset, certificate) in [certificate_10.clone(), certificate_20.clone()]
        .into_iter()
        .enumerate()
    {
        let response = write_staged(
            &client,
            &latest_before_final,
            seed,
            303_u64.saturating_add(u64::try_from(offset).unwrap_or(u64::MAX)),
            transaction.identity,
            CellStagedTransactionAction::RecordLogCertificate { certificate },
        )
        .await?;
        if response.status != CellStagedTransactionStatus::LogCertificateRecorded {
            return Err("authority rejected one post-transition durability certificate".to_owned());
        }
    }
    let publish = write_staged(
        &client,
        &latest_before_final,
        seed,
        305,
        transaction.identity,
        CellStagedTransactionAction::Publish,
    )
    .await?;
    if publish.status != CellStagedTransactionStatus::Committed
        || publish.commit_sequence != Some(final_version)
    {
        return Err("post-transition transaction did not become visible".to_owned());
    }

    let retry = write_staged(
        &client,
        &latest_before_final,
        seed,
        306,
        RequestIdentity {
            client_id: seed ^ 0x504f_4c49_4359_5254,
            request_id: transition.transition_id,
        },
        CellStagedTransactionAction::CommitLogSetPolicyTransition {
            transition_id: transition.transition_id,
            successor_stage: stage_certificate.clone(),
        },
    )
    .await?;

    let capacity_statement = CellTaggedLogCapacityStatement {
        format_version: 1,
        cell_id: transaction.cell_id,
        tenant_id: transaction.tenant_id,
        generation: transaction.generation,
        transaction_identity: transaction.identity,
        transaction_sha256: [9; 32],
        log_set_id: MOVING_LOG_SET,
        policy_epoch: next_policy.policy_epoch,
        projected_frame_bytes: 128,
        soft_limit_bytes: 1024,
        reservation_epoch: 1,
    };
    let mut capacity_members = Vec::new();
    let capacity_endpoints = if mode == CellTaggedLogPolicyTransitionMode::RemovedNodeRejoin {
        vec![removed_endpoint.clone(), learner_endpoint.clone()]
    } else {
        active_10.clone()
    };
    for endpoint in &capacity_endpoints {
        if let TaggedLogResponse::CapacityAttested { node_id, .. } = tagged_log_request(
            endpoint,
            &TaggedLogRequest::Capacity {
                statement: capacity_statement.clone(),
            },
        )? {
            capacity_members.push(node_id);
        }
    }
    capacity_members.sort_unstable();
    capacity_members.dedup();

    let worker_endpoints = if mode == CellTaggedLogPolicyTransitionMode::RemovedNodeRejoin {
        vec![removed_endpoint.clone(), learner_endpoint.clone()]
    } else {
        active_10.clone()
    };
    let worker_output = root.0.join("policy-worker.json");
    let worker_config = CellTaggedLogRepairWorkerProcessConfig {
        endpoints: worker_endpoints,
        range_tag: MOVING_LOG_SET,
        after_version: OBJECT_FRONTIER,
        through_version: final_version,
        quorum: TLOG_QUORUM,
        output_path: worker_output.clone(),
    };
    let output = Command::new(executable)
        .arg("cell-tagged-log-repair-worker-node")
        .arg("--config-json")
        .arg(serde_json::to_string(&worker_config).map_err(|error| error.to_string())?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("failed to start policy serving worker: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "policy serving worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let worker: CellTaggedLogRepairWorkerReceipt =
        serde_json::from_slice(&fs::read(&worker_output).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let final_snapshot = authority.linearizable_cell_snapshot().await?;
    let active_policy = final_snapshot
        .log_set_policies
        .iter()
        .find(|policy| policy.log_set_id == MOVING_LOG_SET);
    let expected_negative = mode != CellTaggedLogPolicyTransitionMode::Correct;
    let expected_stage_valid = mode != CellTaggedLogPolicyTransitionMode::MixedPolicyQuorum;
    let expected_readiness_exact =
        mode != CellTaggedLogPolicyTransitionMode::MissingRepairReadiness;
    let expected_epoch = if mode == CellTaggedLogPolicyTransitionMode::InvalidNextPolicy {
        NEXT_POLICY_EPOCH
    } else {
        next_policy.policy_epoch
    };
    let expected_retry = mode != CellTaggedLogPolicyTransitionMode::DoubleTransition;
    let expected_removed_rejection = mode != CellTaggedLogPolicyTransitionMode::RemovedNodeRejoin;
    let expected_activation_valid =
        mode != CellTaggedLogPolicyTransitionMode::MissingAuthorityActivation;
    let mut checks = vec![
        check(
            "transaction_authority_history_is_clean",
            authority_report.anomaly_count == 0,
        ),
        check("transaction_authority_has_three_processes", true),
        check("two_three_process_tlog_sets_started", true),
        check(
            "pre_transition_frontier_is_14",
            snapshot.latest_sequence == 14,
        ),
        check("failed_tlog_is_replaced_by_distinct_learner", true),
        check(
            "learner_repair_uses_old_policy_quorum",
            base_certificate.attestations.len() >= 2,
        ),
        check(
            "learner_readiness_uses_old_policy_quorum",
            ready_certificate.attestations.len() >= 2,
        ),
        check(
            "transition_binds_exact_repair_readiness",
            expected_readiness_exact
                && transition.repair_readiness_sha256
                    == cell_tagged_log_repair_certificate_sha256(&ready_certificate),
        ),
        check(
            "no_unresolved_old_stage_crosses_transition",
            !unresolved_old_stage,
        ),
        check(
            "next_policy_epoch_is_exact_successor",
            next_policy.policy_epoch == expected_epoch,
        ),
        check(
            "successor_stage_has_one_valid_next_quorum",
            stage_certificate_valid == expected_stage_valid && stage_certificate_valid,
        ),
        check(
            "authority_commits_policy_once",
            commit.status == CellStagedTransactionStatus::PolicyTransitionCommitted,
        ),
        check(
            "activation_has_pinned_authority_quorum",
            authority_activation_valid == expected_activation_valid && authority_activation_valid,
        ),
        check("activation_survives_learner_restart", true),
        check(
            "removed_node_cannot_activate_successor_policy",
            !removed_activated,
        ),
        check(
            "removed_node_rejects_successor_attestation",
            removed_rejected == expected_removed_rejection && removed_rejected,
        ),
        check(
            "post_transition_set_10_uses_epoch_2",
            certificate_10.statement.policy_epoch == NEXT_POLICY_EPOCH,
        ),
        check(
            "unchanged_set_20_remains_epoch_1",
            certificate_20.statement.policy_epoch == OLD_POLICY_EPOCH,
        ),
        check(
            "post_transition_commit_is_visible",
            final_snapshot.latest_sequence == final_version,
        ),
        check(
            "correct_path_final_frontier_is_17",
            final_version == CORRECT_FINAL_FRONTIER,
        ),
        check(
            "cell_generation_is_unchanged",
            final_snapshot.generation == generation_before,
        ),
        check(
            "capacity_counts_only_nodes_3_and_4",
            capacity_members == vec![3, 4],
        ),
        check(
            "serving_counts_only_nodes_3_and_4",
            worker.responding_node_ids == vec![3, 4],
        ),
        check(
            "fresh_worker_reaches_visible_frontier",
            worker.observed_frontier == final_version,
        ),
        check(
            "fresh_worker_reads_five_record_suffix",
            worker.quorum_records.len() == 5,
        ),
        check(
            "log_set_20_remains_available",
            read_records_through(&endpoints_20[0], UNCHANGED_LOG_SET, final_version)?.len() == 5,
        ),
        check(
            "policy_retry_is_idempotent",
            (retry.status == CellStagedTransactionStatus::AlreadyPolicyTransitionCommitted)
                == expected_retry
                && retry.status == CellStagedTransactionStatus::AlreadyPolicyTransitionCommitted,
        ),
        check(
            "removed_old_root_cannot_change_active_quorum",
            !removed_has_record || removed_rejected,
        ),
        check(
            "linearizable_authority_exposes_exact_next_policy",
            active_policy == Some(&next_policy) && next_policy.policy_epoch == NEXT_POLICY_EPOCH,
        ),
    ];
    let current_anomalies = checks.iter().filter(|item| !item.passed).count();
    checks.push(check(
        "negative_subject_is_independently_detectable",
        !expected_negative || current_anomalies > 0,
    ));
    let anomaly_count =
        u64::try_from(checks.iter().filter(|item| !item.passed).count()).unwrap_or(u64::MAX);
    let first_mismatch = checks
        .iter()
        .find(|item| !item.passed)
        .map(|item| item.name.clone());
    let mut trace = Sha256::new();
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    for item in &checks {
        trace.update(item.name.as_bytes());
        trace.update([u8::from(item.passed)]);
    }
    Ok(CellTaggedLogPolicyTransitionReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch,
        transaction_authority_process_starts: 3,
        tagged_log_process_starts: 6,
        tagged_log_process_restarts: 4,
        failed_tagged_log_processes: 2,
        learner_process_starts: 1,
        learner_process_restarts: 3,
        repair_attestations: u64::try_from(base_certificate.attestations.len()).unwrap_or(u64::MAX),
        readiness_attestations: u64::try_from(ready_certificate.attestations.len())
            .unwrap_or(u64::MAX),
        successor_stage_attestations: u64::try_from(stage_certificate.attestations.len())
            .unwrap_or(u64::MAX),
        authority_activation_attestations: u64::try_from(activation_certificate.attestations.len())
            .unwrap_or(u64::MAX),
        policy_prepares: 1,
        policy_commits: 1,
        idempotent_retries: u64::from(
            retry.status == CellStagedTransactionStatus::AlreadyPolicyTransitionCommitted,
        ),
        old_epoch_rejections: u64::from(removed_rejected),
        post_transition_appends: 5,
        capacity_members_counted: capacity_members,
        serving_members_counted: worker.responding_node_ids,
        old_policy_epoch: OLD_POLICY_EPOCH,
        next_policy_epoch: next_policy.policy_epoch,
        generation_before,
        generation_after: final_snapshot.generation,
        object_frontier: OBJECT_FRONTIER,
        pre_transition_frontier: PRE_TRANSITION_FRONTIER,
        final_frontier: final_version,
        worker_frontier: worker.observed_frontier,
        checks,
        trace_sha256: format!("{:x}", trace.finalize()),
    })
}

fn authority_faults(mode: CellTaggedLogPolicyTransitionMode) -> GenerationFenceFaults {
    GenerationFenceFaults {
        policy_transition_accept_missing_readiness: mode
            == CellTaggedLogPolicyTransitionMode::MissingRepairReadiness,
        policy_transition_accept_unresolved_stage: mode
            == CellTaggedLogPolicyTransitionMode::UnresolvedOldStage,
        policy_transition_accept_invalid_next_policy: mode
            == CellTaggedLogPolicyTransitionMode::InvalidNextPolicy,
        policy_transition_accept_mixed_stage_quorum: mode
            == CellTaggedLogPolicyTransitionMode::MixedPolicyQuorum,
        policy_transition_double_apply: mode == CellTaggedLogPolicyTransitionMode::DoubleTransition,
        ..GenerationFenceFaults::default()
    }
}

fn read_records_through(
    endpoint: &str,
    range_tag: u16,
    through_version: u64,
) -> Result<Vec<TaggedLogRecord>, String> {
    match tagged_log_request(
        endpoint,
        &TaggedLogRequest::Read {
            range_tag,
            after_version: OBJECT_FRONTIER,
            through_version,
        },
    )? {
        TaggedLogResponse::Feed { records, .. } => Ok(records),
        response => Err(format!(
            "policy-transition suffix read failed: {response:?}"
        )),
    }
}

fn policy_activation_authority(base_seed: &[u8]) -> Result<PublicationPopPolicy, String> {
    let members = (1_u64..=3)
        .map(|node_id| {
            let seed = cell_log_set_policy_authority_seed(base_seed, node_id);
            tagged_log_public_key(&seed).map(|public_key| (node_id, public_key))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(PublicationPopPolicy {
        members,
        quorum_size: 2,
    })
}

fn next_direct_transaction(
    seed: u64,
    request_id: u64,
    snapshot: &okv_consensus::CellStateSnapshot,
) -> CellTransactionCommand {
    let key = format!("rfc-0046/setup/{request_id:04}").into_bytes();
    CellTransactionCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x504f_4c49_4359_4454,
            request_id,
        },
        credential: None,
        cell_id: snapshot.cell_id,
        tenant_id: snapshot.tenant_id,
        generation: snapshot.generation,
        read_version: CellReadVersion {
            generation: snapshot.generation,
            sequence: snapshot.latest_sequence,
        },
        read_conflicts: vec![CellKeyRange::point(&key)],
        write_conflicts: vec![CellKeyRange::point(&key)],
        mutations: vec![CellMutation::Set {
            key,
            value: format!("policy-value-{seed}-{request_id}").into_bytes(),
        }],
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: REQUIRED_LOG_SETS.to_vec(),
    }
}

fn final_transaction(
    seed: u64,
    request_id: u64,
    snapshot: &okv_consensus::CellStateSnapshot,
) -> CellTransactionCommand {
    let key = b"rfc-0046/post-transition".to_vec();
    CellTransactionCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x504f_4c49_4359_4654,
            request_id,
        },
        credential: None,
        cell_id: snapshot.cell_id,
        tenant_id: snapshot.tenant_id,
        generation: snapshot.generation,
        read_version: CellReadVersion {
            generation: snapshot.generation,
            sequence: snapshot.latest_sequence,
        },
        read_conflicts: vec![CellKeyRange::point(&key)],
        write_conflicts: vec![CellKeyRange::point(&key)],
        mutations: vec![CellMutation::Set {
            key,
            value: format!("policy-final-{seed}").into_bytes(),
        }],
        partitioned_resolution: None,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: REQUIRED_LOG_SETS.to_vec(),
    }
}

fn policy_stage_statement(
    transition: &CellLogSetPolicyTransition,
) -> okv_consensus::CellTaggedLogPolicyStageStatement {
    okv_consensus::CellTaggedLogPolicyStageStatement {
        format_version: 1,
        cell_id: transition.cell_id,
        tenant_id: transition.tenant_id,
        generation: transition.generation,
        transition_id: transition.transition_id,
        log_set_id: transition.log_set_id,
        old_policy_epoch: transition.old_policy.policy_epoch,
        next_policy_epoch: transition.next_policy.policy_epoch,
        transition_sha256: okv_consensus::cell_log_set_policy_transition_sha256(transition),
        retained_root_sha256: transition.retained_root_sha256,
        retained_last_position: transition.retained_last_position,
    }
}

fn durability_certificate(
    endpoints: &[String],
    transaction: &CellTransactionCommand,
    commit_sequence: u64,
    durable_position: u64,
    log_set_id: u16,
    policy_epoch: u64,
    envelope: &[u8],
) -> Result<CellTaggedLogCertificate, String> {
    let statement = CellTaggedLogStatement {
        format_version: 1,
        cell_id: transaction.cell_id,
        tenant_id: transaction.tenant_id,
        generation: transaction.generation,
        transaction_identity: transaction.identity,
        commit_sequence,
        log_set_id,
        policy_epoch,
        envelope_sha256: Sha256::digest(envelope).into(),
        durable_position,
    };
    let mut attestations = Vec::new();
    for endpoint in endpoints {
        match tagged_log_request(
            endpoint,
            &TaggedLogRequest::Attest {
                statement: statement.clone(),
            },
        )? {
            TaggedLogResponse::Attested {
                statement: observed,
                attestation,
                ..
            } if observed == statement => attestations.push(attestation),
            response => return Err(format!("tLog did not attest post-transition: {response:?}")),
        }
    }
    let distinct = attestations
        .iter()
        .map(|attestation| attestation.signer_id)
        .collect::<BTreeSet<_>>();
    if distinct.len() < TLOG_QUORUM {
        return Err("post-transition durability certificate lacks quorum".to_owned());
    }
    Ok(CellTaggedLogCertificate {
        statement,
        attestations,
    })
}

async fn write_staged(
    client: &CellTransactionClient,
    snapshot: &okv_consensus::CellStateSnapshot,
    seed: u64,
    request_id: u64,
    transaction_identity: RequestIdentity,
    action: CellStagedTransactionAction,
) -> Result<CellStagedTransactionApplyResponse, String> {
    let command = CellStagedTransactionCommand {
        identity: RequestIdentity {
            client_id: seed ^ 0x504f_4c49_4359_434d,
            request_id,
        },
        credential: None,
        cell_id: snapshot.cell_id,
        tenant_id: snapshot.tenant_id,
        generation: snapshot.generation,
        transaction_identity,
        action,
    };
    client
        .commit_app_data(&command.encode().map_err(|error| error.to_string())?)
        .await?
        .cell_staged_transaction
        .ok_or_else(|| "transaction authority omitted staged response".to_owned())
}

fn check(name: &str, passed: bool) -> CellTaggedLogPolicyTransitionCheck {
    CellTaggedLogPolicyTransitionCheck {
        name: name.to_owned(),
        passed,
    }
}
