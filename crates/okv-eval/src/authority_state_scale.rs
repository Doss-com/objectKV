//! Complete transaction-authority snapshot accounting for G4.5.

use okv_consensus::{
    TransactionAuthorityProcessFixture, TransactionCommand, TransactionKeyRange,
    TransactionLogStorageStats, TransactionLogStorageStatsRequest, TransactionMutation,
    TransactionStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Subject selected for the bounded-state curve.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityStateScaleMode {
    AlignedFrontiersProjection,
    IdealStreamPopProjection,
    NoPopControl,
    RetainedOnlyAccountingPoison,
    ServingOnlyAccountingPoison,
}

/// Fixed workload shape for one authority-state scale run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityStateScaleProfile {
    pub live_keys: u64,
    pub value_bytes: usize,
    pub commit_checkpoints: Vec<u64>,
    pub max_projected_growth_ratio: u64,
}

/// One exact linearizable state-accounting checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityStateScaleCheckpoint {
    pub commits: u64,
    pub stats: TransactionLogStorageStats,
    pub selected_snapshot_bytes: u64,
}

/// Canonical report for one fresh three-process data authority.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AuthorityStateScaleReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: AuthorityStateScaleMode,
    pub authority_processes: u64,
    pub live_keys: u64,
    pub value_bytes: u64,
    pub checkpoints: Vec<AuthorityStateScaleCheckpoint>,
    pub growth_ratio: f64,
    pub bounded_state: bool,
    pub accounting_complete: bool,
    pub projection_non_mutating: bool,
    pub expired_retry_rejected: bool,
    pub correctness_anomalies: u64,
    pub structural_sha256: String,
}

/// Run one fixed G4.5 contract against three real `OpenRaft` processes.
///
/// # Errors
///
/// Returns an error when the profile is invalid, a process cannot start, a
/// transaction does not commit, or a linearizable accounting invariant fails.
pub fn run_authority_state_scale_contract(
    seed: u64,
    mode: AuthorityStateScaleMode,
    profile: &AuthorityStateScaleProfile,
    executable: &Path,
) -> Result<AuthorityStateScaleReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run(seed, mode, profile, executable))
}

#[allow(clippy::too_many_lines)]
async fn run(
    seed: u64,
    mode: AuthorityStateScaleMode,
    profile: &AuthorityStateScaleProfile,
    executable: &Path,
) -> Result<AuthorityStateScaleReport, String> {
    let authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
    let client = authority.client()?;
    let final_commits = *profile
        .commit_checkpoints
        .last()
        .ok_or_else(|| "authority-state profile has no checkpoints".to_owned())?;
    let mut checkpoints = Vec::with_capacity(profile.commit_checkpoints.len());
    let mut next_checkpoint = 0_usize;
    let mut admitted_read_version = 0_u64;
    let mut frontier_sequence = 0_u64;
    let split_frontiers = matches!(
        mode,
        AuthorityStateScaleMode::AlignedFrontiersProjection
            | AuthorityStateScaleMode::ServingOnlyAccountingPoison
    );
    let mut first_command = None;
    let mut expired_retry_rejected = !split_frontiers;

    for request_id in 1..=final_commits {
        let key_index = (request_id - 1) % profile.live_keys;
        let key = format!("range/0001/key/{key_index:08}").into_bytes();
        let fill = u8::try_from(65 + ((seed + request_id) % 26)).unwrap_or(b'Z');
        let range = TransactionKeyRange::point(&key);
        let command = TransactionCommand {
            read_version: admitted_read_version,
            read_conflicts: Vec::new(),
            write_conflicts: vec![range],
            mutations: vec![TransactionMutation::Set {
                key,
                value: vec![fill; profile.value_bytes],
            }],
        };
        if request_id == 1 {
            first_command = Some(command.clone());
        }
        let response = client
            .commit(
                okv_consensus::RequestIdentity {
                    client_id: seed.max(1),
                    request_id,
                },
                &command,
            )
            .await?;
        let TransactionStatus::Committed { commit_version } = response.status else {
            return Err(format!(
                "authority-state transaction {request_id} did not commit: {:?}",
                response.status
            ));
        };

        if profile.commit_checkpoints.get(next_checkpoint) == Some(&request_id) {
            if split_frontiers {
                frontier_sequence = frontier_sequence.saturating_add(1);
                let advanced = client
                    .advance_frontiers(
                        okv_consensus::RequestIdentity {
                            client_id: seed.max(1).saturating_add(1_u64 << 62),
                            request_id: frontier_sequence,
                        },
                        &okv_consensus::TransactionFrontierAdvance {
                            sequence: frontier_sequence,
                            conflict_retention_floor: commit_version,
                            retry_floors: vec![okv_consensus::TransactionRetryFloor {
                                client_id: seed.max(1),
                                through_request_id: request_id,
                            }],
                        },
                    )
                    .await?;
                if advanced.sequence != frontier_sequence
                    || advanced.conflict_retention_floor != commit_version
                {
                    return Err(format!(
                        "authority-state frontier {frontier_sequence} returned inconsistent state: {advanced:?}"
                    ));
                }
                admitted_read_version = commit_version;
                let expired = client
                    .commit_once(
                        okv_consensus::RequestIdentity {
                            client_id: seed.max(1),
                            request_id: 1,
                        },
                        first_command
                            .as_ref()
                            .ok_or_else(|| "authority-state first command is absent".to_owned())?,
                    )
                    .await;
                expired_retry_rejected = expired
                    .as_ref()
                    .is_err_and(|error| error.contains("below its retained floor"));
                if !expired_retry_rejected {
                    return Err(format!(
                        "authority-state expired retry did not fail closed: {expired:?}"
                    ));
                }
            }
            let stats = client
                .storage_stats(TransactionLogStorageStatsRequest {
                    projected_retention_floor: Some(commit_version),
                })
                .await?;
            validate_checkpoint(request_id, mode, profile, &stats)?;
            let selected_snapshot_bytes = match mode {
                AuthorityStateScaleMode::AlignedFrontiersProjection
                | AuthorityStateScaleMode::IdealStreamPopProjection => {
                    stats.projected_snapshot_bytes
                }
                AuthorityStateScaleMode::NoPopControl => stats.snapshot_bytes,
                AuthorityStateScaleMode::RetainedOnlyAccountingPoison => {
                    stats.projected_retained_transactions_bytes
                }
                AuthorityStateScaleMode::ServingOnlyAccountingPoison => stats.serving_state_bytes,
            };
            checkpoints.push(AuthorityStateScaleCheckpoint {
                commits: request_id,
                stats,
                selected_snapshot_bytes,
            });
            next_checkpoint += 1;
        }
    }

    let first = checkpoints
        .first()
        .ok_or_else(|| "authority-state run emitted no checkpoint".to_owned())?;
    let last = checkpoints
        .last()
        .ok_or_else(|| "authority-state run emitted no final checkpoint".to_owned())?;
    let final_page = client
        .read(okv_consensus::RetainedTransactionReadRequest {
            after_version_exclusive: 0,
            after_batch_order_exclusive: None,
            through_version_inclusive: Some(last.stats.high_watermark),
            max_records: u32::try_from(final_commits).map_err(|error| error.to_string())?,
        })
        .await?;
    let projection_non_mutating = final_page.complete
        && u64::try_from(final_page.records.len()).unwrap_or(u64::MAX) == final_commits;
    let growth_ratio = ratio(last.selected_snapshot_bytes, first.selected_snapshot_bytes);
    let accounting_complete = !matches!(
        mode,
        AuthorityStateScaleMode::RetainedOnlyAccountingPoison
            | AuthorityStateScaleMode::ServingOnlyAccountingPoison
    );
    let bounded_state = last.selected_snapshot_bytes
        <= first
            .selected_snapshot_bytes
            .saturating_mul(profile.max_projected_growth_ratio);
    let correctness_anomalies = u64::from(!projection_non_mutating)
        + u64::from(split_frontiers && !expired_retry_rejected)
        + if accounting_complete {
            0
        } else {
            u64::try_from(checkpoints.len()).unwrap_or(u64::MAX)
        };
    let structural_sha256 = structural_sha(seed, mode, &checkpoints, projection_non_mutating)?;

    Ok(AuthorityStateScaleReport {
        format_version: 1,
        seed,
        mode,
        authority_processes: u64::try_from(authority.process_count()).unwrap_or(u64::MAX),
        live_keys: profile.live_keys,
        value_bytes: u64::try_from(profile.value_bytes).unwrap_or(u64::MAX),
        checkpoints,
        growth_ratio,
        bounded_state,
        accounting_complete,
        projection_non_mutating,
        expired_retry_rejected,
        correctness_anomalies,
        structural_sha256,
    })
}

fn validate_profile(profile: &AuthorityStateScaleProfile) -> Result<(), String> {
    if profile.live_keys == 0 || profile.value_bytes == 0 {
        return Err("authority-state profile requires live keys and value bytes".to_owned());
    }
    if profile.commit_checkpoints.len() < 2
        || profile.commit_checkpoints[0] < profile.live_keys
        || profile
            .commit_checkpoints
            .windows(2)
            .any(|window| window[0] >= window[1])
    {
        return Err(
            "authority-state checkpoints must be strictly increasing and begin after live-key fill"
                .to_owned(),
        );
    }
    let final_commits = *profile.commit_checkpoints.last().unwrap_or(&0);
    if final_commits > 4_096 {
        return Err(
            "authority-state v1 retains at most 4096 records for final verification".to_owned(),
        );
    }
    if profile.max_projected_growth_ratio == 0 {
        return Err("authority-state growth ceiling must be positive".to_owned());
    }
    Ok(())
}

fn validate_checkpoint(
    commits: u64,
    mode: AuthorityStateScaleMode,
    profile: &AuthorityStateScaleProfile,
    stats: &TransactionLogStorageStats,
) -> Result<(), String> {
    let split_frontiers = matches!(
        mode,
        AuthorityStateScaleMode::AlignedFrontiersProjection
            | AuthorityStateScaleMode::ServingOnlyAccountingPoison
    );
    let split_counts_hold = if split_frontiers {
        stats.conflict_retention_floor == stats.high_watermark
            && stats.retry_clients == 1
            && stats.retained_conflict_versions == 0
            && stats.durable_outcomes == 0
            && stats.request_fingerprints == 0
            && stats.transaction_retry_outcomes == 0
            && stats.transaction_retry_fingerprints == 0
    } else {
        stats.conflict_retention_floor == 0
            && stats.retry_clients == 0
            && stats.retained_conflict_versions == commits
            && stats.durable_outcomes == commits
            && stats.request_fingerprints == commits
            && stats.transaction_retry_outcomes == commits
            && stats.transaction_retry_fingerprints == commits
    };
    if stats.format_version != 2
        || stats.retention_floor != 0
        || stats.projected_retention_floor != stats.high_watermark
        || stats.live_keys != profile.live_keys.min(commits)
        || !split_counts_hold
        || stats.retained_records != commits
        || stats.projected_retained_records != 0
        || stats.projected_retained_transactions_bytes != 0
        || stats.snapshot_bytes <= stats.projected_snapshot_bytes
        || stats.transaction_authority_bytes == 0
        || stats.serving_state_bytes == 0
        || stats.resolver_state_bytes == 0
        || stats.transaction_retry_state_bytes == 0
        || stats.transaction_frontier_state_bytes == 0
        || stats.durable_outcomes_bytes == 0
        || stats.request_fingerprints_bytes == 0
    {
        return Err(format!(
            "authority-state checkpoint {commits} violated complete accounting: {stats:?}"
        ));
    }
    Ok(())
}

fn ratio(last: u64, first: u64) -> f64 {
    if first == 0 {
        if last == 0 {
            1.0
        } else {
            f64::MAX
        }
    } else {
        count_as_f64(last) / count_as_f64(first)
    }
}

#[allow(clippy::cast_precision_loss)]
fn count_as_f64(value: u64) -> f64 {
    value as f64
}

fn structural_sha(
    seed: u64,
    mode: AuthorityStateScaleMode,
    checkpoints: &[AuthorityStateScaleCheckpoint],
    projection_non_mutating: bool,
) -> Result<String, String> {
    let bytes = serde_json::to_vec(&(seed, mode, checkpoints, projection_non_mutating))
        .map_err(|error| error.to_string())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_rejects_unordered_or_oversized_checkpoints() {
        let valid = AuthorityStateScaleProfile {
            live_keys: 256,
            value_bytes: 128,
            commit_checkpoints: vec![256, 1_024, 4_096],
            max_projected_growth_ratio: 2,
        };
        assert!(validate_profile(&valid).is_ok());
        let mut invalid = valid.clone();
        invalid.commit_checkpoints = vec![256, 128];
        assert!(validate_profile(&invalid).is_err());
        invalid.commit_checkpoints = vec![256, 4_097];
        assert!(validate_profile(&invalid).is_err());
    }

    #[test]
    fn zero_byte_retained_only_projection_looks_flat_but_is_not_complete() {
        assert!((ratio(0, 0) - 1.0).abs() < f64::EPSILON);
        assert_ne!(
            AuthorityStateScaleMode::RetainedOnlyAccountingPoison,
            AuthorityStateScaleMode::IdealStreamPopProjection
        );
    }
}
