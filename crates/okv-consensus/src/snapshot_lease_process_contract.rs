use crate::{
    CollectionJobToken, CollectionReceipt, GenerationCredential, PublicationAction,
    PublicationApplyResponse, PublicationAuthorityProcessFixture, PublicationAuthorityState,
    PublicationCommand, PublicationCommandStatus, PublicationDeletePermit, PublicationIntent,
    PublicationObjectIdentity, PublicationObjectKind, PublicationObjectReference,
    PublicationOutcome, PublicationRevisionToken, RequestIdentity, SnapshotClosure,
    SnapshotLeaseToken,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const GENERATION: u64 = 7;
const TRANSACTION_SYSTEM_ID: &str = "tx-g7";

/// Correct subject for the first replicated lease-authority process gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotLeaseProcessMode {
    Correct,
    DisableRequestDedup,
    AcceptBackdatedLease,
    OmitLeaseRootEpoch,
    IgnoreCollectionRangeEpoch,
    AdvanceCollectionWithoutPublication,
    IgnoreCollectionInputRoot,
}

impl SnapshotLeaseProcessMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::DisableRequestDedup => "disable_request_dedup",
            Self::AcceptBackdatedLease => "accept_backdated_lease",
            Self::OmitLeaseRootEpoch => "omit_lease_root_epoch",
            Self::IgnoreCollectionRangeEpoch => "ignore_collection_range_epoch",
            Self::AdvanceCollectionWithoutPublication => "advance_collection_without_publication",
            Self::IgnoreCollectionInputRoot => "ignore_collection_input_root",
        }
    }

    fn authority_faults(self) -> crate::PublicationAuthorityFaults {
        crate::PublicationAuthorityFaults {
            accept_backdated_lease: matches!(self, Self::AcceptBackdatedLease),
            omit_lease_root_epoch: matches!(self, Self::OmitLeaseRootEpoch),
            ignore_collection_range_epoch: matches!(self, Self::IgnoreCollectionRangeEpoch),
            advance_collection_without_publication: matches!(
                self,
                Self::AdvanceCollectionWithoutPublication
            ),
            ignore_collection_input_root: matches!(self, Self::IgnoreCollectionInputRoot),
            ..crate::PublicationAuthorityFaults::default()
        }
    }
}

#[derive(Clone, Copy)]
struct ProcessCounts {
    starts: u64,
    kills: u64,
    failovers: u64,
    dropped_replies: u64,
    recovered_outcomes: u64,
    exact_retries: u64,
}

impl ProcessCounts {
    const fn initial() -> Self {
        Self {
            starts: 3,
            kills: 0,
            failovers: 0,
            dropped_replies: 0,
            recovered_outcomes: 0,
            exact_retries: 0,
        }
    }
}

/// Canonical receipt for one three-process lease and collection history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotLeaseProcessReport {
    pub seed: u64,
    pub mode: SnapshotLeaseProcessMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub dropped_replies: u64,
    pub recovered_outcomes: u64,
    pub exact_retries: u64,
    pub final_active_leases: u64,
    pub final_minimum_readable_version: u64,
    pub final_clock_tick: u64,
    pub final_prepared_jobs: u64,
    pub final_collected_through: u64,
    pub final_root_epoch: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

/// Execute the frozen RFC-0060 history through three real authority processes.
///
/// # Errors
///
/// Returns an error when a process cannot start, a consensus transition cannot
/// complete, or a required durable outcome is absent.
pub fn run_snapshot_lease_process_contract(
    seed: u64,
    mode: SnapshotLeaseProcessMode,
    executable: &Path,
) -> Result<SnapshotLeaseProcessReport, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_history(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_history(
    seed: u64,
    mode: SnapshotLeaseProcessMode,
    executable: &Path,
) -> Result<SnapshotLeaseProcessReport, String> {
    let mut fixture = PublicationAuthorityProcessFixture::start_with_faults(
        executable,
        seed,
        mode != SnapshotLeaseProcessMode::DisableRequestDedup,
        mode.authority_faults(),
    )
    .await?;
    let initial = fixture.client_starting_with(101)?;
    let input_manifest = object_reference("objects/cell/m0.manifest");
    let input_data = "objects/cell/data.sst";
    let output_manifest = object_reference("collections/j1/m1.manifest");
    let output_data = "collections/j1/data.sst";

    require_accepted(
        &initial
            .commit(&command(
                seed,
                100,
                PublicationAction::Prepare {
                    publication_id: "cell-m0".to_owned(),
                    intent: PublicationIntent {
                        object_keys: BTreeSet::from([
                            input_manifest.key.clone(),
                            input_data.to_owned(),
                        ]),
                        manifest: input_manifest.clone(),
                        destination_root: "cell-root".to_owned(),
                        expected_prior_root: None,
                    },
                },
            ))
            .await?,
        "prepare initial cell manifest",
    )?;
    require_accepted(
        &initial
            .commit(&command(
                seed,
                101,
                PublicationAction::Publish {
                    publication_id: "cell-m0".to_owned(),
                    destination_root: "cell-root".to_owned(),
                    expected_prior_root: None,
                    manifest: input_manifest.clone(),
                },
            ))
            .await?,
        "publish initial cell manifest",
    )?;
    require_accepted(
        &initial
            .commit(&command(
                seed,
                102,
                PublicationAction::ConfigureCollectionRoot {
                    expected: None,
                    destination_root: "cell-root".to_owned(),
                },
            ))
            .await?,
        "configure top-level collection root",
    )?;
    require_accepted(
        &initial
            .commit(&command(
                seed,
                103,
                PublicationAction::ObserveCommittedFrontier {
                    committed_frontier: 256,
                },
            ))
            .await?,
        "observe commit frontier 256",
    )?;
    require_accepted(
        &initial
            .commit(&command(
                seed,
                104,
                PublicationAction::SetRetentionWindow {
                    expected_policy_epoch: 0,
                    retention_window: 64,
                },
            ))
            .await?,
        "set retention window",
    )?;

    let closure = SnapshotClosure {
        manifest: input_manifest.clone(),
        object_keys: BTreeSet::from([input_manifest.key.clone(), input_data.to_owned()]),
    };
    let backdated = initial
        .commit(&command(
            seed,
            105,
            PublicationAction::AcquireLease {
                lease_id: "lease-too-old".to_owned(),
                tenant_id: "tenant-1".to_owned(),
                snapshot_version: 191,
                owner: "query-old".to_owned(),
                purpose: "olap".to_owned(),
                deadline_tick: 10,
                closure: closure.clone(),
            },
        ))
        .await?;
    let backdated_rejected = backdated.status == PublicationCommandStatus::SnapshotBelowFloor;
    if mode == SnapshotLeaseProcessMode::AcceptBackdatedLease {
        let state = initial.read().await?;
        return build_fault_report(
            seed,
            mode,
            BTreeMap::from([
                ("backdated_lease_rejected".to_owned(), backdated_rejected),
                (
                    "minimum_readable_version_is_respected".to_owned(),
                    !state.leases.contains_key("lease-too-old"),
                ),
            ]),
            &state,
            ProcessCounts::initial(),
        );
    }

    let root_epoch_before_acquire = initial.read().await?.root_intent_epoch;
    let acquire_a = command(
        seed,
        106,
        PublicationAction::AcquireLease {
            lease_id: "lease-a".to_owned(),
            tenant_id: "tenant-1".to_owned(),
            snapshot_version: 200,
            owner: "query-a".to_owned(),
            purpose: "olap".to_owned(),
            deadline_tick: 10,
            closure: closure.clone(),
        },
    );
    let acquire_reply_dropped = initial
        .commit_with_dropped_reply_for_eval(&acquire_a)
        .await
        .is_err();
    fixture.kill_leader_and_elect_successor(101, 102).await?;
    let leader_102 = fixture.client_starting_with(102)?;
    let recovered_acquire = match leader_102.outcome(acquire_a.identity).await? {
        Some(outcome) => outcome,
        None if mode == SnapshotLeaseProcessMode::DisableRequestDedup => {
            let state = leader_102.read().await?;
            return build_missing_outcome_report(seed, mode, acquire_reply_dropped, &state);
        }
        None => {
            return Err(format!(
                "durable publication outcome is absent for {:?}",
                acquire_a.identity
            ));
        }
    };
    let lease_a = lease_token(&recovered_acquire, "recover lease A")?;
    let acquire_retry = leader_102.commit(&acquire_a).await?;
    let acquire_exact = recovered_acquire == acquire_retry;
    if mode == SnapshotLeaseProcessMode::OmitLeaseRootEpoch {
        let state = leader_102.read().await?;
        return build_fault_report(
            seed,
            mode,
            BTreeMap::from([
                (
                    "acquire_lost_reply_observed".to_owned(),
                    acquire_reply_dropped,
                ),
                (
                    "acquire_outcome_recovered_exactly".to_owned(),
                    acquire_exact,
                ),
                (
                    "lease_acquire_changes_root_epoch".to_owned(),
                    state.root_intent_epoch > root_epoch_before_acquire,
                ),
                (
                    "committed_lease_closure_is_durable".to_owned(),
                    state
                        .leases
                        .get("lease-a")
                        .is_some_and(|lease| lease.closure == closure),
                ),
            ]),
            &state,
            ProcessCounts {
                starts: 3,
                kills: 1,
                failovers: 1,
                dropped_replies: u64::from(acquire_reply_dropped),
                recovered_outcomes: 1,
                exact_retries: 1,
            },
        );
    }
    fixture.restart_node(executable, 101, 102).await?;

    let acquire_b = leader_102
        .commit(&command(
            seed,
            107,
            PublicationAction::AcquireLease {
                lease_id: "lease-b".to_owned(),
                tenant_id: "tenant-1".to_owned(),
                snapshot_version: 224,
                owner: "query-b".to_owned(),
                purpose: "olap".to_owned(),
                deadline_tick: 30,
                closure: closure.clone(),
            },
        ))
        .await?;
    let lease_b = lease_token(&acquire_b, "acquire lease B")?;

    let renew_a = command(
        seed,
        108,
        PublicationAction::RenewLease {
            lease_id: "lease-a".to_owned(),
            expected_lease_epoch: lease_a.lease_epoch,
            new_deadline_tick: 15,
        },
    );
    let renew_reply_dropped = leader_102
        .commit_with_dropped_reply_for_eval(&renew_a)
        .await
        .is_err();
    fixture.kill_leader_and_elect_successor(102, 103).await?;
    let leader_103 = fixture.client_starting_with(103)?;
    let recovered_renewal = required_outcome(&leader_103, renew_a.identity).await?;
    let renewed_a = lease_token(&recovered_renewal, "recover renewal A")?;
    let renewal_retry = leader_103.commit(&renew_a).await?;
    let renewal_exact = recovered_renewal == renewal_retry && renewed_a.deadline_tick == 15;

    require_accepted(
        &leader_103
            .commit(&command(
                seed,
                109,
                PublicationAction::ObserveCommittedFrontier {
                    committed_frontier: 288,
                },
            ))
            .await?,
        "observe commit frontier 288",
    )?;
    require_accepted(
        &leader_103
            .commit(&command(
                seed,
                110,
                PublicationAction::ObserveRangeMapEpoch { range_map_epoch: 9 },
            ))
            .await?,
        "observe range-map epoch",
    )?;
    let prepared = leader_103
        .commit(&command(
            seed,
            111,
            PublicationAction::PrepareCollection {
                job_id: "j1".to_owned(),
                frozen_floor: 200,
                input_manifest: input_manifest.clone(),
                destination_root: "cell-root".to_owned(),
                range_map_epoch: 9,
                expected_collected_through: 0,
                output_namespace: "collections/j1/".to_owned(),
            },
        ))
        .await?;
    let collection = collection_token(&prepared, "prepare collection J")?;

    if mode == SnapshotLeaseProcessMode::AdvanceCollectionWithoutPublication {
        let state = leader_103.read().await?;
        return build_fault_report(
            seed,
            mode,
            BTreeMap::from([
                (
                    "collected_frontier_requires_publication".to_owned(),
                    state.physically_collected_through == 0,
                ),
                (
                    "unpublished_collection_keeps_input_root".to_owned(),
                    state.roots.get("cell-root") == Some(&input_manifest)
                        && state.collection_jobs.contains_key("j1"),
                ),
            ]),
            &state,
            ProcessCounts {
                starts: 4,
                kills: 2,
                failovers: 2,
                dropped_replies: u64::from(acquire_reply_dropped) + u64::from(renew_reply_dropped),
                recovered_outcomes: 2,
                exact_retries: 2,
            },
        );
    }

    if mode == SnapshotLeaseProcessMode::IgnoreCollectionRangeEpoch {
        require_accepted(
            &leader_103
                .commit(&command(
                    seed,
                    900,
                    PublicationAction::ObserveRangeMapEpoch {
                        range_map_epoch: 10,
                    },
                ))
                .await?,
            "advance range-map epoch after collection prepare",
        )?;
        let stale_receipt = CollectionReceipt {
            token: collection,
            output_manifest: output_manifest.clone(),
            object_keys: BTreeSet::from([output_manifest.key.clone(), output_data.to_owned()]),
        };
        let response = leader_103
            .commit(&command(
                seed,
                901,
                PublicationAction::PublishCollection {
                    receipt: stale_receipt,
                },
            ))
            .await?;
        let state = leader_103.read().await?;
        return build_fault_report(
            seed,
            mode,
            BTreeMap::from([
                (
                    "stale_range_epoch_rejected".to_owned(),
                    response.status == PublicationCommandStatus::RangeMapEpochMismatch,
                ),
                (
                    "stale_receipt_leaves_root_and_frontier_unchanged".to_owned(),
                    state.roots.get("cell-root") == Some(&input_manifest)
                        && state.physically_collected_through == 0,
                ),
            ]),
            &state,
            ProcessCounts {
                starts: 4,
                kills: 2,
                failovers: 2,
                dropped_replies: u64::from(acquire_reply_dropped) + u64::from(renew_reply_dropped),
                recovered_outcomes: 2,
                exact_retries: 2,
            },
        );
    }

    if mode == SnapshotLeaseProcessMode::IgnoreCollectionInputRoot {
        let intervening_manifest = object_reference("objects/cell/mx.manifest");
        require_accepted(
            &leader_103
                .commit(&command(
                    seed,
                    900,
                    PublicationAction::Prepare {
                        publication_id: "cell-mx".to_owned(),
                        intent: PublicationIntent {
                            object_keys: BTreeSet::from([intervening_manifest.key.clone()]),
                            manifest: intervening_manifest.clone(),
                            destination_root: "cell-root".to_owned(),
                            expected_prior_root: Some(input_manifest.clone()),
                        },
                    },
                ))
                .await?,
            "prepare intervening root",
        )?;
        require_accepted(
            &leader_103
                .commit(&command(
                    seed,
                    901,
                    PublicationAction::Publish {
                        publication_id: "cell-mx".to_owned(),
                        destination_root: "cell-root".to_owned(),
                        expected_prior_root: Some(input_manifest.clone()),
                        manifest: intervening_manifest,
                    },
                ))
                .await?,
            "publish intervening root",
        )?;
        let stale_receipt = CollectionReceipt {
            token: collection,
            output_manifest: output_manifest.clone(),
            object_keys: BTreeSet::from([output_manifest.key.clone(), output_data.to_owned()]),
        };
        let response = leader_103
            .commit(&command(
                seed,
                902,
                PublicationAction::PublishCollection {
                    receipt: stale_receipt,
                },
            ))
            .await?;
        let state = leader_103.read().await?;
        return build_fault_report(
            seed,
            mode,
            BTreeMap::from([
                (
                    "changed_input_root_rejected".to_owned(),
                    response.status == PublicationCommandStatus::RootCompareFailed,
                ),
                (
                    "changed_input_leaves_frontier_unchanged".to_owned(),
                    state.physically_collected_through == 0,
                ),
            ]),
            &state,
            ProcessCounts {
                starts: 4,
                kills: 2,
                failovers: 2,
                dropped_replies: u64::from(acquire_reply_dropped) + u64::from(renew_reply_dropped),
                recovered_outcomes: 2,
                exact_retries: 2,
            },
        );
    }

    require_accepted(
        &leader_103
            .commit(&command(
                seed,
                112,
                PublicationAction::AdvanceLeaseClock {
                    expected_tick: 0,
                    next_tick: 15,
                },
            ))
            .await?,
        "expire lease A",
    )?;
    let before_publish = leader_103.read().await?;
    let frozen_job_survived_floor_advance = before_publish.minimum_readable_version == 224
        && before_publish.physically_collected_through == 0
        && before_publish.roots.get("cell-root") == Some(&input_manifest)
        && before_publish
            .collection_jobs
            .get("j1")
            .is_some_and(|job| job.frozen_floor == 200)
        && !before_publish.leases.contains_key("lease-a")
        && before_publish.leases.contains_key("lease-b");

    let receipt = CollectionReceipt {
        token: collection,
        output_manifest: output_manifest.clone(),
        object_keys: BTreeSet::from([output_manifest.key.clone(), output_data.to_owned()]),
    };
    let publish = command(
        seed,
        113,
        PublicationAction::PublishCollection {
            receipt: receipt.clone(),
        },
    );
    let publish_reply_dropped = leader_103
        .commit_with_dropped_reply_for_eval(&publish)
        .await
        .is_err();
    fixture.restart_node(executable, 102, 103).await?;
    fixture.kill_leader_and_elect_successor(103, 101).await?;
    let leader_101 = fixture.client_starting_with(101)?;
    let recovered_publish = required_outcome(&leader_101, publish.identity).await?;
    let publish_retry = leader_101.commit(&publish).await?;
    let publish_exact = recovered_publish == publish_retry
        && recovered_publish.outcome
            == Some(PublicationOutcome::CollectionPublished {
                receipt: receipt.clone(),
            });
    let after_publish = leader_101.read().await?;
    let replacement_published_once = after_publish.physically_collected_through == 200
        && after_publish.roots.get("cell-root") == Some(&output_manifest)
        && after_publish.collection_jobs.is_empty()
        && after_publish.leases.contains_key("lease-b");

    let lease_root_blocked_delete = leader_101
        .commit(&command(
            seed,
            114,
            PublicationAction::ReserveDelete {
                plan_id: "blocked-by-lease".to_owned(),
                mark_epoch: after_publish.root_intent_epoch,
                key: input_data.to_owned(),
                identity: object_identity(input_data),
            },
        ))
        .await?
        .status
        == PublicationCommandStatus::ObjectNamedByIntent;
    let stale_mark = leader_101.read().await?.root_intent_epoch;
    require_accepted(
        &leader_101
            .commit(&command(
                seed,
                115,
                PublicationAction::ReleaseLease {
                    lease_id: "lease-b".to_owned(),
                    expected_lease_epoch: lease_b.lease_epoch,
                },
            ))
            .await?,
        "release lease B",
    )?;
    let stale_delete_rejected = leader_101
        .commit(&command(
            seed,
            116,
            PublicationAction::ReserveDelete {
                plan_id: "stale-delete".to_owned(),
                mark_epoch: stale_mark,
                key: input_data.to_owned(),
                identity: object_identity(input_data),
            },
        ))
        .await?
        .status
        == PublicationCommandStatus::RootIntentEpochChanged;
    let fresh_epoch = leader_101.read().await?.root_intent_epoch;
    let reserved = leader_101
        .commit(&command(
            seed,
            117,
            PublicationAction::ReserveDelete {
                plan_id: "fresh-delete".to_owned(),
                mark_epoch: fresh_epoch,
                key: input_data.to_owned(),
                identity: object_identity(input_data),
            },
        ))
        .await?;
    let permit = delete_permit(&reserved, "reserve old object deletion")?;

    fixture.restart_node(executable, 103, 101).await?;
    fixture.kill_leader_and_elect_successor(101, 102).await?;
    let leader_102 = fixture.client_starting_with(102)?;
    let reservation_survived_restart = leader_102
        .read()
        .await?
        .deletion_reservations
        .get(input_data)
        == Some(&permit);
    require_accepted(
        &leader_102
            .commit(&command(
                seed,
                118,
                PublicationAction::RetireDelete { permit },
            ))
            .await?,
        "retire exact deletion permit",
    )?;
    fixture.restart_node(executable, 101, 102).await?;
    let final_state = leader_102.read().await?;
    let final_state_exact = exact_final_state(&final_state, &output_manifest);

    let checks = BTreeMap::from([
        ("backdated_lease_rejected".to_owned(), backdated_rejected),
        (
            "acquire_lost_reply_observed".to_owned(),
            acquire_reply_dropped,
        ),
        (
            "acquire_outcome_recovered_exactly".to_owned(),
            acquire_exact,
        ),
        ("renew_lost_reply_observed".to_owned(), renew_reply_dropped),
        ("renew_outcome_recovered_exactly".to_owned(), renewal_exact),
        (
            "prepared_job_keeps_frozen_floor".to_owned(),
            frozen_job_survived_floor_advance,
        ),
        (
            "unpublished_worker_output_keeps_old_root".to_owned(),
            before_publish.roots.get("cell-root") == Some(&input_manifest),
        ),
        (
            "publish_lost_reply_observed".to_owned(),
            publish_reply_dropped,
        ),
        (
            "publish_outcome_recovered_exactly".to_owned(),
            publish_exact,
        ),
        (
            "replacement_root_and_frontier_advance_once".to_owned(),
            replacement_published_once,
        ),
        (
            "lease_closure_blocks_deletion".to_owned(),
            lease_root_blocked_delete,
        ),
        (
            "lease_release_invalidates_stale_mark".to_owned(),
            stale_delete_rejected,
        ),
        (
            "delete_reservation_survives_restart".to_owned(),
            reservation_survived_restart,
        ),
        ("final_state_is_exact".to_owned(), final_state_exact),
    ]);
    build_fault_report(
        seed,
        mode,
        checks,
        &final_state,
        ProcessCounts {
            starts: 7,
            kills: 4,
            failovers: 4,
            dropped_replies: 3,
            recovered_outcomes: 3,
            exact_retries: 3,
        },
    )
}

fn build_fault_report(
    seed: u64,
    mode: SnapshotLeaseProcessMode,
    checks: BTreeMap<String, bool>,
    final_state: &PublicationAuthorityState,
    counts: ProcessCounts,
) -> Result<SnapshotLeaseProcessReport, String> {
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(check, _)| check.clone())
        .collect::<Vec<_>>();
    let semantic_state = semantic_state(final_state);
    let trace = serde_json::to_vec(&(seed, mode, &checks, semantic_state))
        .map_err(|error| error.to_string())?;
    Ok(SnapshotLeaseProcessReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        authority_process_starts: counts.starts,
        process_kills: counts.kills,
        authority_failovers: counts.failovers,
        dropped_replies: counts.dropped_replies,
        recovered_outcomes: counts.recovered_outcomes,
        exact_retries: counts.exact_retries,
        final_active_leases: u64::try_from(final_state.leases.len()).unwrap_or(u64::MAX),
        final_minimum_readable_version: final_state.minimum_readable_version,
        final_clock_tick: final_state.lease_clock_tick,
        final_prepared_jobs: u64::try_from(final_state.collection_jobs.len()).unwrap_or(u64::MAX),
        final_collected_through: final_state.physically_collected_through,
        final_root_epoch: final_state.root_intent_epoch,
        checks,
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

fn build_missing_outcome_report(
    seed: u64,
    mode: SnapshotLeaseProcessMode,
    reply_dropped: bool,
    state: &PublicationAuthorityState,
) -> Result<SnapshotLeaseProcessReport, String> {
    let checks = BTreeMap::from([
        (
            "committed_lease_survives_failover".to_owned(),
            state.leases.contains_key("lease-a"),
        ),
        ("lost_reply_observed".to_owned(), reply_dropped),
        ("lost_reply_has_durable_outcome".to_owned(), false),
    ]);
    build_fault_report(
        seed,
        mode,
        checks,
        state,
        ProcessCounts {
            starts: 3,
            kills: 1,
            failovers: 1,
            dropped_replies: u64::from(reply_dropped),
            recovered_outcomes: 0,
            exact_retries: 0,
        },
    )
}

fn command(seed: u64, request_id: u64, action: PublicationAction) -> PublicationCommand {
    PublicationCommand {
        identity: RequestIdentity {
            client_id: seed.max(1),
            request_id,
        },
        credential: GenerationCredential {
            generation: GENERATION,
            transaction_system_id: TRANSACTION_SYSTEM_ID.to_owned(),
        },
        action,
    }
}

fn object_reference(key: &str) -> PublicationObjectReference {
    PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: key.to_owned(),
        length: u64::try_from(key.len()).unwrap_or(u64::MAX),
        sha256: format!("{:x}", Sha256::digest(key.as_bytes())),
    }
}

fn object_identity(key: &str) -> PublicationObjectIdentity {
    PublicationObjectIdentity {
        revision: PublicationRevisionToken {
            e_tag: Some(format!(
                "etag-{}",
                &format!("{:x}", Sha256::digest(key))[..16]
            )),
            version: None,
        },
        length: u64::try_from(key.len()).unwrap_or(u64::MAX),
        sha256: format!("{:x}", Sha256::digest(key.as_bytes())),
    }
}

fn require_accepted(response: &PublicationApplyResponse, step: &str) -> Result<(), String> {
    if response.status == PublicationCommandStatus::Accepted {
        Ok(())
    } else {
        Err(format!("{step} returned {:?}", response.status))
    }
}

async fn required_outcome(
    client: &crate::PublicationClient,
    identity: RequestIdentity,
) -> Result<PublicationApplyResponse, String> {
    client
        .outcome(identity)
        .await?
        .ok_or_else(|| format!("durable publication outcome is absent for {identity:?}"))
}

fn lease_token(
    response: &PublicationApplyResponse,
    step: &str,
) -> Result<SnapshotLeaseToken, String> {
    require_accepted(response, step)?;
    match &response.outcome {
        Some(
            PublicationOutcome::LeaseAcquired { token }
            | PublicationOutcome::LeaseRenewed { token },
        ) => Ok(token.clone()),
        other => Err(format!("{step} returned unexpected outcome {other:?}")),
    }
}

fn collection_token(
    response: &PublicationApplyResponse,
    step: &str,
) -> Result<CollectionJobToken, String> {
    require_accepted(response, step)?;
    match &response.outcome {
        Some(PublicationOutcome::CollectionPrepared { token }) => Ok(token.clone()),
        other => Err(format!("{step} returned unexpected outcome {other:?}")),
    }
}

fn delete_permit(
    response: &PublicationApplyResponse,
    step: &str,
) -> Result<PublicationDeletePermit, String> {
    require_accepted(response, step)?;
    match &response.outcome {
        Some(PublicationOutcome::DeleteReserved { permit }) => Ok(permit.clone()),
        other => Err(format!("{step} returned unexpected outcome {other:?}")),
    }
}

fn exact_final_state(
    state: &PublicationAuthorityState,
    output_manifest: &PublicationObjectReference,
) -> bool {
    state.observed_commit_frontier == 288
        && state.retention_window == Some(64)
        && state.policy_floor == 224
        && state.minimum_readable_version == 224
        && state.physically_collected_through == 200
        && state.lease_clock_tick == 15
        && state.leases.is_empty()
        && state.collection_jobs.is_empty()
        && state.collection_root.as_deref() == Some("cell-root")
        && state.roots.get("cell-root") == Some(output_manifest)
        && state.deletion_reservations.is_empty()
}

fn semantic_state(state: &PublicationAuthorityState) -> PublicationAuthorityState {
    let mut semantic = state.clone();
    semantic.revision = crate::PublicationAuthorityPosition::default();
    for lease in semantic.leases.values_mut() {
        lease.authority_position = crate::PublicationAuthorityPosition::default();
    }
    for job in semantic.collection_jobs.values_mut() {
        job.authority_position = crate::PublicationAuthorityPosition::default();
    }
    semantic
}
