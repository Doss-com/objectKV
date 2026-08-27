//! Physical G4.7 composition of immutable row objects, publication authority,
//! data safe-pop, quorum attestation, and post-pop recovery.

use okv_consensus::{
    object_frontier_certificate_statement, GenerationClient, GenerationCredential,
    ObjectFrontierAdvance, ObjectFrontierCertificate, ObjectFrontierRecord, PublicationAction,
    PublicationAuthorityPosition, PublicationAuthorityProcessFixture, PublicationCommand,
    PublicationCommandStatus, PublicationIntent, PublicationObjectKind, PublicationObjectReference,
    RequestIdentity, RetainedTransactionReadRequest, TransactionAuthorityProcessFixture,
    TransactionLogStorageStatsRequest,
};
use okv_object::{
    advance_validated_row_object_frontier, content_sha256, encode_row_object_set,
    filesystem_backend, read_point_from_full_object, validate_row_object_frontier, ObjectClient,
    PointReadOutcome, RowObjectManifestV1, RowObjectReference, RowRecord, RowSegmentIndex,
    WriteCondition,
};
use okv_transaction::{KeyRange, Mutation, TransactionCommand, TransactionStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;

const GENERATION: u64 = 7;
const TRANSACTION_SYSTEM_ID: &str = "tx-g7";
const KEY_COUNT: u64 = 16;
const VALUE_BYTES: usize = 128;
const TARGET_OBJECT_BYTES: usize = 16 * 1024;
const TARGET_BLOCK_BYTES: usize = 4 * 1024;

/// Frozen subject or negative-control behavior for G4.7.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectFrontierMode {
    Candidate,
    MissingPendingControl,
    ForgedCoverageControl,
    SubquorumControl,
}

impl ObjectFrontierMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::MissingPendingControl => "missing_pending_control",
            Self::ForgedCoverageControl => "forged_coverage_control",
            Self::SubquorumControl => "subquorum_control",
        }
    }
}

/// One physical G4.7 receipt before suite-level aggregation.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize)]
pub struct ObjectFrontierReport {
    pub seed: u64,
    pub mode: ObjectFrontierMode,
    pub authority_processes: u64,
    pub data_processes: u64,
    pub committed_transactions: u64,
    pub high_watermark: u64,
    pub manifest_covered_through: u64,
    pub requested_frontier: u64,
    pub pending_frontier_protected: bool,
    pub pending_frontier_retained: bool,
    pub closure_validated: bool,
    pub closure_objects: u64,
    pub closure_bytes: u64,
    pub retained_records_before: u64,
    pub retained_records_after: u64,
    pub physical_pop_applied: bool,
    pub popped_records: u64,
    pub persisted_retention_floor: u64,
    pub stale_cursor_rejected: bool,
    pub exact_pop_retry: bool,
    pub certificate_signers: u64,
    pub activation_accepted: bool,
    pub unsafe_transition_rejected: bool,
    pub active_frontier_exact: bool,
    pub data_leader_failover: bool,
    pub authority_leader_failover: bool,
    pub restarted_data_voter: bool,
    pub recovered_state_exact: bool,
    pub correctness_anomalies: u64,
    pub prepare_seconds: f64,
    pub validation_seconds: f64,
    pub pop_seconds: f64,
    pub certificate_seconds: f64,
    pub activation_seconds: f64,
    pub recovery_seconds: f64,
    pub semantic_sha256: String,
}

#[derive(Clone, Debug)]
struct PublishedClosure {
    root: String,
    reference: PublicationObjectReference,
    manifest: RowObjectManifestV1,
}

/// Run the complete G4.7 protocol from a synchronous eval boundary.
///
/// # Errors
///
/// Returns an error when process startup or required candidate mechanics fail.
pub fn run_object_frontier_contract(
    seed: u64,
    mode: ObjectFrontierMode,
    executable: &Path,
) -> Result<ObjectFrontierReport, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_object_frontier_contract_async(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_object_frontier_contract_async(
    seed: u64,
    mode: ObjectFrontierMode,
    executable: &Path,
) -> Result<ObjectFrontierReport, String> {
    let object_root = TempDir::new().map_err(|error| error.to_string())?;
    let mut authority = PublicationAuthorityProcessFixture::start(executable, seed).await?;
    let publication = authority.client()?;
    let generation = GenerationClient::new(authority.endpoints())?;
    let mut data = TransactionAuthorityProcessFixture::start_fenced(
        executable,
        seed.saturating_add(10_000),
        authority.authority_nodes(),
    )
    .await?;
    let authority_processes = u64::try_from(authority.process_count()).unwrap_or(u64::MAX);
    let data_processes = u64::try_from(data.process_count()).unwrap_or(u64::MAX);
    let txlog = data.client()?;
    let credential = GenerationCredential {
        generation: GENERATION,
        transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
    };

    let mut records = Vec::new();
    let mut expected = BTreeMap::new();
    for ordinal in 0..KEY_COUNT {
        let key = format!("g47/{ordinal:04}").into_bytes();
        let value = deterministic_value(seed, ordinal);
        let command = TransactionCommand {
            read_version: records
                .last()
                .map_or(0, |record: &RowRecord| record.version),
            read_conflicts: Vec::new(),
            write_conflicts: vec![KeyRange::point(&key)],
            mutations: vec![Mutation::Set {
                key: key.clone(),
                value: value.clone(),
            }],
        };
        let response = txlog
            .commit_fenced(
                RequestIdentity {
                    client_id: seed.max(1).saturating_add(20_000),
                    request_id: ordinal.saturating_add(1),
                },
                &credential,
                &command,
            )
            .await?;
        let TransactionStatus::Committed { commit_version } = response.status else {
            return Err("G4.7 setup transaction did not commit".to_owned());
        };
        records.push(RowRecord::value(&key, commit_version, &value));
        expected.insert(key, value);
    }
    let high_watermark = records
        .last()
        .map(|record| record.version)
        .ok_or_else(|| "G4.7 generated no transaction records".to_owned())?;
    let manifest_covered_through = if mode == ObjectFrontierMode::ForgedCoverageControl {
        records
            .get(records.len().saturating_sub(2))
            .map_or(high_watermark, |record| record.version)
    } else {
        high_watermark
    };
    let manifest_records = records
        .iter()
        .filter(|record| record.version <= manifest_covered_through)
        .cloned()
        .collect::<Vec<_>>();

    let prepare_started = Instant::now();
    let closure = publish_closure(
        seed,
        manifest_covered_through,
        &manifest_records,
        object_root.path(),
        &publication,
    )
    .await?;
    let requested_frontier = if mode == ObjectFrontierMode::ForgedCoverageControl {
        high_watermark
    } else {
        manifest_covered_through
    };
    let frontier = if mode == ObjectFrontierMode::MissingPendingControl {
        ObjectFrontierRecord {
            owner_generation: GENERATION,
            source_root: closure.root.clone(),
            manifest: closure.reference.clone(),
            covered_through: requested_frontier,
            prepared_at: PublicationAuthorityPosition { term: 1, index: 1 },
        }
    } else {
        let response = publication
            .commit(&publication_command(
                seed,
                30_002,
                PublicationAction::PrepareObjectFrontier {
                    source_root: closure.root.clone(),
                    manifest: closure.reference.clone(),
                    covered_through: requested_frontier,
                    expected_active: None,
                },
            ))
            .await?;
        if response.status != PublicationCommandStatus::Accepted {
            return Err(format!(
                "object-frontier prepare was rejected: {:?}",
                response.status
            ));
        }
        response
            .state
            .pending_object_frontier
            .ok_or_else(|| "accepted prepare did not retain a pending frontier".to_owned())?
    };
    let prepare_seconds = prepare_started.elapsed().as_secs_f64();
    let publication_before = publication.read().await?;
    let pending_frontier_protected = publication_before
        .object_frontier_manifests()
        .contains(&&frontier.manifest);

    let backend = filesystem_backend(object_root.path()).map_err(|error| error.to_string())?;
    let object_client = ObjectClient::new(backend.clone());
    let validation_started = Instant::now();
    let validated = validate_row_object_frontier(&object_client, &frontier).await;
    let validation_seconds = validation_started.elapsed().as_secs_f64();
    let closure_validated = validated.is_ok();
    let (closure_objects, closure_bytes) = validated.as_ref().map_or((0, 0), |proof| {
        (proof.closure_objects(), proof.closure_bytes())
    });

    let before = txlog
        .storage_stats(TransactionLogStorageStatsRequest::default())
        .await?;
    if mode == ObjectFrontierMode::ForgedCoverageControl {
        let unsafe_transition_rejected = validated.is_err();
        return Ok(control_report(
            seed,
            mode,
            &authority,
            &data,
            high_watermark,
            manifest_covered_through,
            requested_frontier,
            pending_frontier_protected,
            closure_validated,
            closure_objects,
            closure_bytes,
            before.retained_records,
            unsafe_transition_rejected,
            prepare_seconds,
            validation_seconds,
        ));
    }
    let validated = validated?;

    if mode == ObjectFrontierMode::MissingPendingControl {
        let rejected = txlog
            .advance_object_frontier_once(
                RequestIdentity {
                    client_id: seed.max(1).saturating_add(30_000),
                    request_id: 1,
                },
                &credential,
                &ObjectFrontierAdvance {
                    frontier: frontier.clone(),
                },
            )
            .await
            .is_err();
        return Ok(control_report(
            seed,
            mode,
            &authority,
            &data,
            high_watermark,
            manifest_covered_through,
            requested_frontier,
            pending_frontier_protected,
            closure_validated,
            closure_objects,
            closure_bytes,
            before.retained_records,
            rejected,
            prepare_seconds,
            validation_seconds,
        ));
    }

    let pop_identity = RequestIdentity {
        client_id: seed.max(1).saturating_add(30_000),
        request_id: 1,
    };
    let pop_started = Instant::now();
    let popped =
        advance_validated_row_object_frontier(&txlog, pop_identity, &credential, &validated)
            .await?;
    let pop_seconds = pop_started.elapsed().as_secs_f64();
    let retry =
        advance_validated_row_object_frontier(&txlog, pop_identity, &credential, &validated)
            .await?;
    let exact_pop_retry = retry == popped;
    let after = txlog
        .storage_stats(TransactionLogStorageStatsRequest::default())
        .await?;
    let stale_cursor_rejected = txlog
        .read(RetainedTransactionReadRequest {
            after_version_exclusive: 0,
            after_batch_order_exclusive: None,
            through_version_inclusive: Some(requested_frontier),
            max_records: 64,
        })
        .await
        .is_err();

    let certificate_started = Instant::now();
    let generation_state = generation.read().await?;
    let statement = object_frontier_certificate_statement(
        &generation_state,
        frontier.clone(),
        popped.applied_log_position,
    );
    let attestations = txlog.attest_object_frontier(&statement).await?;
    let certificate_signers = u64::try_from(attestations.len()).unwrap_or(u64::MAX);
    let mut certificate = ObjectFrontierCertificate {
        statement,
        attestations,
    };
    if mode == ObjectFrontierMode::SubquorumControl {
        certificate.attestations.truncate(1);
    }
    let certificate_seconds = certificate_started.elapsed().as_secs_f64();

    let activation_started = Instant::now();
    let activation = publication
        .commit(&publication_command(
            seed,
            30_003,
            PublicationAction::ActivateObjectFrontier {
                expected_pending: frontier.clone(),
                certificate,
            },
        ))
        .await?;
    let activation_seconds = activation_started.elapsed().as_secs_f64();
    let activation_accepted = activation.status == PublicationCommandStatus::Accepted;
    let unsafe_transition_rejected = mode == ObjectFrontierMode::SubquorumControl
        && activation.status == PublicationCommandStatus::ObjectFrontierCertificateInvalid;
    let active_frontier_exact = activation.state.active_object_frontier.as_ref() == Some(&frontier);
    let pending_frontier_retained =
        activation.state.pending_object_frontier.as_ref() == Some(&frontier);

    let recovery_started = Instant::now();
    let recovered_state_exact = recover_exact_state(
        &object_client,
        &closure.manifest,
        requested_frontier,
        &expected,
    )
    .await?;
    let recovery_seconds = recovery_started.elapsed().as_secs_f64();

    let mut data_leader_failover = false;
    let mut authority_leader_failover = false;
    let mut restarted_data_voter = false;
    if mode == ObjectFrontierMode::Candidate {
        data.kill_initial_leader_and_elect_successor().await?;
        data_leader_failover = txlog
            .read(RetainedTransactionReadRequest {
                after_version_exclusive: requested_frontier,
                after_batch_order_exclusive: None,
                through_version_inclusive: Some(requested_frontier),
                max_records: 64,
            })
            .await
            .is_ok();
        data.restart_initial_voter().await?;
        restarted_data_voter = data.process_count() == 3;
        authority.kill_initial_leader_and_elect_successor().await?;
        authority_leader_failover = publication
            .read()
            .await
            .is_ok_and(|state| state.active_object_frontier.as_ref() == Some(&frontier));
    }

    let correctness_anomalies = candidate_anomalies(
        mode,
        pending_frontier_protected,
        closure_validated,
        &popped,
        &before,
        &after,
        stale_cursor_rejected,
        exact_pop_retry,
        certificate_signers,
        activation_accepted,
        unsafe_transition_rejected,
        active_frontier_exact,
        pending_frontier_retained,
        data_leader_failover,
        authority_leader_failover,
        restarted_data_voter,
        recovered_state_exact,
    );
    let semantic_sha256 = semantic_digest(
        mode,
        high_watermark,
        manifest_covered_through,
        requested_frontier,
        after.retention_floor,
        after.retained_records,
        activation.status,
        recovered_state_exact,
    );
    Ok(ObjectFrontierReport {
        seed,
        mode,
        authority_processes,
        data_processes,
        committed_transactions: KEY_COUNT,
        high_watermark,
        manifest_covered_through,
        requested_frontier,
        pending_frontier_protected,
        pending_frontier_retained,
        closure_validated,
        closure_objects,
        closure_bytes,
        retained_records_before: before.retained_records,
        retained_records_after: after.retained_records,
        physical_pop_applied: true,
        popped_records: popped.popped_records,
        persisted_retention_floor: after.retention_floor,
        stale_cursor_rejected,
        exact_pop_retry,
        certificate_signers,
        activation_accepted,
        unsafe_transition_rejected,
        active_frontier_exact,
        data_leader_failover,
        authority_leader_failover,
        restarted_data_voter,
        recovered_state_exact,
        correctness_anomalies,
        prepare_seconds,
        validation_seconds,
        pop_seconds,
        certificate_seconds,
        activation_seconds,
        recovery_seconds,
        semantic_sha256,
    })
}

async fn publish_closure(
    seed: u64,
    covered_through: u64,
    records: &[RowRecord],
    object_root: &Path,
    publication: &okv_consensus::PublicationClient,
) -> Result<PublishedClosure, String> {
    let encoded =
        encode_row_object_set(GENERATION, records, TARGET_OBJECT_BYTES, TARGET_BLOCK_BYTES)?;
    let prefix = format!("rows-g47/{seed}");
    let references = encoded
        .iter()
        .map(|segment| RowObjectReference::from_encoded(&prefix, segment))
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = RowObjectManifestV1::new(GENERATION, covered_through, references.clone())?;
    let manifest_bytes = manifest.encode()?;
    let reference = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: format!(
            "{prefix}/manifest/sha256/{}",
            content_sha256(&manifest_bytes)
        ),
        length: u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX),
        sha256: content_sha256(&manifest_bytes),
    };
    let root = format!("object-frontier/root/{seed}");
    let publication_id = format!("object-frontier-publication/{seed}");
    let mut object_keys = BTreeSet::from([reference.key.clone()]);
    for child in &references {
        object_keys.insert(child.data_key.clone());
        object_keys.insert(child.index_key.clone());
    }
    let prepared = publication
        .commit(&publication_command(
            seed,
            30_000,
            PublicationAction::Prepare {
                publication_id: publication_id.clone(),
                intent: PublicationIntent {
                    object_keys,
                    manifest: reference.clone(),
                    destination_root: root.clone(),
                    expected_prior_root: None,
                },
            },
        ))
        .await?;
    if prepared.status != PublicationCommandStatus::Accepted {
        return Err("G4.7 publication prepare was rejected".to_owned());
    }

    let backend = filesystem_backend(object_root).map_err(|error| error.to_string())?;
    for (segment, child) in encoded.iter().zip(&references) {
        backend
            .put(
                &child.data_key,
                segment.data.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
        backend
            .put(
                &child.index_key,
                segment.index.clone(),
                WriteCondition::Create,
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    backend
        .put(
            &reference.key,
            manifest_bytes.into(),
            WriteCondition::Create,
        )
        .await
        .map_err(|error| error.to_string())?;
    let published = publication
        .commit(&publication_command(
            seed,
            30_001,
            PublicationAction::Publish {
                publication_id,
                destination_root: root.clone(),
                expected_prior_root: None,
                manifest: reference.clone(),
            },
        ))
        .await?;
    if published.status != PublicationCommandStatus::Accepted
        || published.state.roots.get(&root) != Some(&reference)
    {
        return Err("G4.7 publication root was not installed exactly".to_owned());
    }
    Ok(PublishedClosure {
        root,
        reference,
        manifest,
    })
}

async fn recover_exact_state(
    client: &ObjectClient,
    manifest: &RowObjectManifestV1,
    version: u64,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<bool, String> {
    let mut cached = BTreeMap::new();
    for reference in &manifest.segments {
        let (index_bytes, _) = client
            .read_full_verified(
                &reference.index_key,
                None,
                reference.index_bytes,
                &reference.index_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        let index = RowSegmentIndex::decode(&index_bytes)?;
        let (data_bytes, _) = client
            .read_full_verified(
                &reference.data_key,
                None,
                reference.data_bytes,
                &reference.data_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        cached.insert(reference.data_key.clone(), (index, data_bytes));
    }
    for (key, expected_value) in expected {
        let Some(reference) = manifest.locate(key) else {
            return Ok(false);
        };
        let Some((index, data)) = cached.get(&reference.data_key) else {
            return Ok(false);
        };
        let point = read_point_from_full_object(data, index, key, version)?;
        match point.outcome {
            PointReadOutcome::Value(value) if value.as_ref() == expected_value.as_slice() => {}
            PointReadOutcome::Value(_) | PointReadOutcome::Tombstone | PointReadOutcome::Absent => {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn publication_command(
    seed: u64,
    request_id: u64,
    action: PublicationAction,
) -> PublicationCommand {
    PublicationCommand {
        identity: RequestIdentity {
            client_id: seed.max(1).saturating_add(40_000),
            request_id,
        },
        credential: GenerationCredential {
            generation: GENERATION,
            transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
        },
        action,
    }
}

fn deterministic_value(seed: u64, ordinal: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(VALUE_BYTES);
    let mut block = 0_u64;
    while value.len() < VALUE_BYTES {
        let mut digest = Sha256::new();
        digest.update(b"OKV-G47-VALUE-V1\0");
        digest.update(seed.to_be_bytes());
        digest.update(ordinal.to_be_bytes());
        digest.update(block.to_be_bytes());
        value.extend_from_slice(&digest.finalize());
        block = block.saturating_add(1);
    }
    value.truncate(VALUE_BYTES);
    value
}

#[allow(clippy::too_many_arguments)]
fn control_report(
    seed: u64,
    mode: ObjectFrontierMode,
    authority: &PublicationAuthorityProcessFixture,
    data: &TransactionAuthorityProcessFixture,
    high_watermark: u64,
    manifest_covered_through: u64,
    requested_frontier: u64,
    pending_frontier_protected: bool,
    closure_validated: bool,
    closure_objects: u64,
    closure_bytes: u64,
    retained_records: u64,
    unsafe_transition_rejected: bool,
    prepare_seconds: f64,
    validation_seconds: f64,
) -> ObjectFrontierReport {
    ObjectFrontierReport {
        seed,
        mode,
        authority_processes: u64::try_from(authority.process_count()).unwrap_or(u64::MAX),
        data_processes: u64::try_from(data.process_count()).unwrap_or(u64::MAX),
        committed_transactions: KEY_COUNT,
        high_watermark,
        manifest_covered_through,
        requested_frontier,
        pending_frontier_protected,
        pending_frontier_retained: pending_frontier_protected,
        closure_validated,
        closure_objects,
        closure_bytes,
        retained_records_before: retained_records,
        retained_records_after: retained_records,
        physical_pop_applied: false,
        popped_records: 0,
        persisted_retention_floor: 0,
        stale_cursor_rejected: false,
        exact_pop_retry: false,
        certificate_signers: 0,
        activation_accepted: false,
        unsafe_transition_rejected,
        active_frontier_exact: false,
        data_leader_failover: false,
        authority_leader_failover: false,
        restarted_data_voter: false,
        recovered_state_exact: false,
        correctness_anomalies: u64::from(!unsafe_transition_rejected),
        prepare_seconds,
        validation_seconds,
        pop_seconds: 0.0,
        certificate_seconds: 0.0,
        activation_seconds: 0.0,
        recovery_seconds: 0.0,
        semantic_sha256: semantic_digest(
            mode,
            high_watermark,
            manifest_covered_through,
            requested_frontier,
            0,
            retained_records,
            PublicationCommandStatus::ObjectFrontierCertificateInvalid,
            false,
        ),
    }
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn candidate_anomalies(
    mode: ObjectFrontierMode,
    pending_protected: bool,
    closure_validated: bool,
    popped: &okv_consensus::ObjectFrontierApplyResponse,
    before: &okv_consensus::TransactionLogStorageStats,
    after: &okv_consensus::TransactionLogStorageStats,
    stale_cursor_rejected: bool,
    exact_pop_retry: bool,
    certificate_signers: u64,
    activation_accepted: bool,
    unsafe_transition_rejected: bool,
    active_frontier_exact: bool,
    pending_frontier_retained: bool,
    data_leader_failover: bool,
    authority_leader_failover: bool,
    restarted_data_voter: bool,
    recovered_state_exact: bool,
) -> u64 {
    if mode == ObjectFrontierMode::SubquorumControl {
        return u64::from(
            !pending_protected
                || !closure_validated
                || popped.popped_records != before.retained_records
                || after.retained_records != 0
                || !stale_cursor_rejected
                || !exact_pop_retry
                || certificate_signers < 2
                || activation_accepted
                || !unsafe_transition_rejected
                || active_frontier_exact
                || !pending_frontier_retained
                || !recovered_state_exact,
        );
    }
    u64::from(
        !pending_protected
            || !closure_validated
            || popped.popped_records != before.retained_records
            || after.retained_records != 0
            || after.retention_floor != popped.frontier.covered_through
            || !stale_cursor_rejected
            || !exact_pop_retry
            || certificate_signers < 2
            || !activation_accepted
            || !active_frontier_exact
            || pending_frontier_retained
            || !data_leader_failover
            || !authority_leader_failover
            || !restarted_data_voter
            || !recovered_state_exact,
    )
}

#[allow(clippy::too_many_arguments)]
fn semantic_digest(
    mode: ObjectFrontierMode,
    high_watermark: u64,
    manifest_covered_through: u64,
    requested_frontier: u64,
    retention_floor: u64,
    retained_records: u64,
    activation_status: PublicationCommandStatus,
    recovered_state_exact: bool,
) -> String {
    let encoded = serde_json::to_vec(&(
        mode,
        high_watermark,
        manifest_covered_through,
        requested_frontier,
        retention_floor,
        retained_records,
        activation_status,
        recovered_state_exact,
    ))
    .unwrap_or_default();
    format!("{:x}", Sha256::digest(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_values_are_stable_and_distinct() {
        assert_eq!(deterministic_value(7, 3), deterministic_value(7, 3));
        assert_ne!(deterministic_value(7, 3), deterministic_value(7, 4));
        assert_eq!(deterministic_value(7, 3).len(), VALUE_BYTES);
    }
}
