//! RFC-0044 fresh-authority anchor distribution and exact-retry falsifier.

use okv_consensus::{
    RequestIdentity, RetainedTransactionReadRequest, TransactionAuthorityProcessFixture,
    TransactionLogClient, TransactionLogStorageStatsRequest,
};
use okv_transaction::{TransactionCommand, TransactionStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::Path;

/// RFC-0044 phase-0 subject or deliberate freshness-bypass poison.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureAnchorMode {
    Candidate,
    SecondIdentityBypassPoison,
}

impl FixtureAnchorMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::SecondIdentityBypassPoison => "second_identity_bypass_poison",
        }
    }
}

/// One fresh authority's exact empty-anchor result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FixtureAnchorObservation {
    pub anchor_version: u64,
    pub anchor_records: u64,
    pub anchor_mutations: u64,
    pub live_keys: u64,
    pub lost_response_observed: bool,
    pub exact_retry_returned_original: bool,
    pub second_identity_guard_rejected: bool,
}

/// Phase-0 evidence emitted before object descriptor implementation begins.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FixtureAnchorReport {
    pub format_version: u32,
    pub seed: u64,
    pub mode: FixtureAnchorMode,
    pub release_build: bool,
    pub requested_fresh_authorities: u64,
    pub authority_processes_started: u64,
    pub observations: Vec<FixtureAnchorObservation>,
    pub anchor_version_stable: bool,
    pub second_identity_bypass_detected: bool,
    pub correctness_anomalies: u64,
    pub semantic_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FixtureAnchor {
    pub version: u64,
    pub records: u64,
    pub mutations: u64,
    pub live_keys: u64,
    pub lost_response_observed: bool,
    pub exact_retry_returned_original: bool,
}

/// Establish one evaluator-only object-base version on a fresh authority.
///
/// The precondition check is intentionally outside the transaction authority.
/// It prevents the evaluator from submitting a second anchor identity. It is
/// not a production restore primitive.
pub(crate) async fn establish_fixture_anchor(
    client: &TransactionLogClient,
    identity: RequestIdentity,
) -> Result<FixtureAnchor, String> {
    let before = client
        .storage_stats(TransactionLogStorageStatsRequest::default())
        .await?;
    if before.high_watermark != 0 || before.live_keys != 0 || before.retained_records != 0 {
        return Err("fixture anchor requires a fresh transaction authority".to_owned());
    }

    let command = empty_anchor_command();
    client
        .commit_with_lost_response_once(identity, &command)
        .await?;
    let committed_without_reply = client
        .storage_stats(TransactionLogStorageStatsRequest::default())
        .await?;
    if committed_without_reply.high_watermark == 0
        || committed_without_reply.live_keys != 0
        || committed_without_reply.retained_records != 1
    {
        return Err(
            "lost-response probe did not observe the committed empty anchor before retry"
                .to_owned(),
        );
    }
    let recovered = client.commit(identity, &command).await?;
    let replay = client.commit(identity, &command).await?;
    let TransactionStatus::Committed { commit_version } = recovered.status else {
        return Err("fixture anchor retry did not recover a committed outcome".to_owned());
    };
    if replay != recovered {
        return Err("fixture anchor exact retry changed its outcome".to_owned());
    }
    if commit_version != committed_without_reply.high_watermark {
        return Err("fixture anchor retry did not recover the pre-retry commit version".to_owned());
    }

    let retained = client
        .read(RetainedTransactionReadRequest {
            after_version_exclusive: 0,
            after_batch_order_exclusive: None,
            through_version_inclusive: Some(commit_version),
            max_records: 2,
        })
        .await?;
    let after = client
        .storage_stats(TransactionLogStorageStatsRequest::default())
        .await?;
    let mutation_count = retained
        .records
        .iter()
        .map(|record| record.command.mutations.len())
        .sum::<usize>();
    if retained.records.len() != 1
        || !retained.complete
        || retained.records[0].commit_version != commit_version
        || retained.records[0].batch_order != 0
        || retained.records[0].command != command
        || mutation_count != 0
        || after.high_watermark != commit_version
        || after.live_keys != 0
        || after.retained_records != 1
    {
        return Err("fixture anchor did not produce one empty retained record".to_owned());
    }

    Ok(FixtureAnchor {
        version: commit_version,
        records: 1,
        mutations: 0,
        live_keys: 0,
        lost_response_observed: true,
        exact_retry_returned_original: true,
    })
}

/// Run the RFC-0044 phase-0 anchor falsifier through real OpenRaft processes.
///
/// # Errors
///
/// Returns an error for an invalid profile, process failure, unstable
/// authority state, or an unexpected retry outcome.
pub fn run_fixture_anchor_contract(
    seed: u64,
    mode: FixtureAnchorMode,
    fresh_authorities: usize,
    executable: &Path,
) -> Result<FixtureAnchorReport, String> {
    if fresh_authorities < 2 || fresh_authorities > 32 {
        return Err("fixture anchor requires 2..=32 fresh authorities".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, fresh_authorities, executable))
}

async fn run_contract(
    seed: u64,
    mode: FixtureAnchorMode,
    fresh_authorities: usize,
    executable: &Path,
) -> Result<FixtureAnchorReport, String> {
    let runs = if mode == FixtureAnchorMode::Candidate {
        fresh_authorities
    } else {
        1
    };
    let identity = RequestIdentity {
        client_id: seed.max(1),
        request_id: 1,
    };
    let mut observations = Vec::with_capacity(runs);
    let mut authority_processes_started = 0_u64;
    let mut second_identity_bypass_detected = false;

    for _ in 0..runs {
        let authority = TransactionAuthorityProcessFixture::start(executable, seed).await?;
        authority_processes_started = authority_processes_started
            .saturating_add(u64::try_from(authority.process_count()).unwrap_or(u64::MAX));
        let client = authority.client()?;
        let anchor = establish_fixture_anchor(&client, identity).await?;
        let second_identity = RequestIdentity {
            client_id: identity.client_id,
            request_id: identity.request_id.saturating_add(1),
        };
        let second_identity_guard_rejected = establish_fixture_anchor(&client, second_identity)
            .await
            .is_err_and(|error| error.contains("requires a fresh transaction authority"));

        if mode == FixtureAnchorMode::SecondIdentityBypassPoison {
            let second = client
                .commit(second_identity, &empty_anchor_command())
                .await?;
            let after = client
                .storage_stats(TransactionLogStorageStatsRequest::default())
                .await?;
            second_identity_bypass_detected = matches!(
                second.status,
                TransactionStatus::Committed { commit_version }
                    if commit_version > anchor.version
            ) && after.retained_records == 2
                && after.live_keys == 0;
        }

        observations.push(FixtureAnchorObservation {
            anchor_version: anchor.version,
            anchor_records: anchor.records,
            anchor_mutations: anchor.mutations,
            live_keys: anchor.live_keys,
            lost_response_observed: anchor.lost_response_observed,
            exact_retry_returned_original: anchor.exact_retry_returned_original,
            second_identity_guard_rejected,
        });
    }

    let versions = observations
        .iter()
        .map(|observation| observation.anchor_version)
        .collect::<BTreeSet<_>>();
    let anchor_version_stable = versions.len() == 1;
    let candidate_exact = observations.len() == fresh_authorities
        && anchor_version_stable
        && observations.iter().all(|observation| {
            observation.anchor_version > 0
                && observation.anchor_records == 1
                && observation.anchor_mutations == 0
                && observation.live_keys == 0
                && observation.lost_response_observed
                && observation.exact_retry_returned_original
                && observation.second_identity_guard_rejected
        });
    let poison_exact = observations.len() == 1
        && observations[0].second_identity_guard_rejected
        && second_identity_bypass_detected;
    let correctness_anomalies = match mode {
        FixtureAnchorMode::Candidate => u64::from(!candidate_exact),
        FixtureAnchorMode::SecondIdentityBypassPoison => u64::from(!poison_exact),
    };
    let semantic_sha256 = semantic_sha(&(
        seed,
        mode.id(),
        fresh_authorities,
        &observations,
        anchor_version_stable,
        second_identity_bypass_detected,
        correctness_anomalies,
    ))?;

    Ok(FixtureAnchorReport {
        format_version: 1,
        seed,
        mode,
        release_build: !cfg!(debug_assertions),
        requested_fresh_authorities: u64::try_from(fresh_authorities).unwrap_or(u64::MAX),
        authority_processes_started,
        observations,
        anchor_version_stable,
        second_identity_bypass_detected,
        correctness_anomalies,
        semantic_sha256,
    })
}

fn empty_anchor_command() -> TransactionCommand {
    TransactionCommand {
        read_version: 0,
        read_conflicts: Vec::new(),
        write_conflicts: Vec::new(),
        mutations: Vec::new(),
    }
}

fn semantic_sha(value: &impl Serialize) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|error| error.to_string())?)
    ))
}

#[cfg(test)]
mod tests {
    use super::{empty_anchor_command, FixtureAnchorMode};

    #[test]
    fn anchor_command_is_canonical_and_empty() {
        let command = empty_anchor_command();
        assert_eq!(command.read_version, 0);
        assert!(command.read_conflicts.is_empty());
        assert!(command.write_conflicts.is_empty());
        assert!(command.mutations.is_empty());
        assert_eq!(FixtureAnchorMode::Candidate.id(), "candidate");
    }
}
