//! Compound provider-incarnation contract for GP2.5.4.

use okv_consensus::{
    run_generation_process_contract, run_publication_process_contract, GenerationProcessMode,
    PublicationProcessMode,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

/// Deliberately unsafe incarnation behavior used by the frozen poison.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderIncarnationMode {
    Correct,
    AcceptStaleSourceIncarnation,
}

impl ProviderIncarnationMode {
    /// Stable identifier used by eval receipts.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::AcceptStaleSourceIncarnation => "accept_stale_source_incarnation",
        }
    }
}

/// Canonical semantic report for one external-authority provider handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderIncarnationReport {
    pub seed: u64,
    pub mode: ProviderIncarnationMode,
    pub executed_checks: u64,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub authority_process_starts: u64,
    pub data_process_starts: u64,
    pub process_kills: u64,
    pub authority_failovers: u64,
    pub fenced_commit_attempts: u64,
    pub fenced_commit_rejections: u64,
    pub publication_writes: u64,
    pub checks: BTreeMap<String, bool>,
    pub trace_sha256: String,
}

#[derive(Clone, Copy, Debug)]
struct Observations {
    external_authority_separate: bool,
    source_fence_precedes_activation: bool,
    stale_commit_rejected: bool,
    stale_route_rejected: bool,
    stale_publication_rejected: bool,
    destination_operations_authorized: bool,
}

/// Execute the real-process generation and publication contracts as one
/// provider-incarnation proof.
///
/// # Errors
///
/// Returns an error when either process topology cannot execute. Semantic
/// disagreements remain in the returned report.
pub fn run_provider_incarnation_contract(
    seed: u64,
    mode: ProviderIncarnationMode,
    executable: &Path,
) -> Result<ProviderIncarnationReport, String> {
    let generation_mode = match mode {
        ProviderIncarnationMode::Correct => GenerationProcessMode::Correct,
        ProviderIncarnationMode::AcceptStaleSourceIncarnation => {
            GenerationProcessMode::BypassStaleCommitFence
        }
    };
    let publication_mode = match mode {
        ProviderIncarnationMode::Correct => PublicationProcessMode::Correct,
        ProviderIncarnationMode::AcceptStaleSourceIncarnation => {
            PublicationProcessMode::BypassGenerationFence
        }
    };
    let generation = run_generation_process_contract(seed, generation_mode, executable)?;
    let publication = run_publication_process_contract(seed, publication_mode, executable)?;
    let stale_publication_rejected = publication
        .checks
        .get("stale_generation_is_fenced")
        .copied()
        .unwrap_or(false);
    let destination_publication_authorized = publication
        .checks
        .get("active_generation_authorizes_publication")
        .copied()
        .unwrap_or(false)
        && publication
            .checks
            .get("matching_publish_commits")
            .copied()
            .unwrap_or(false);
    let observations = Observations {
        external_authority_separate: generation.authority_process_starts == 3
            && generation.data_process_starts == 6,
        source_fence_precedes_activation: generation.source_provider_fence_persisted
            && generation.source_fence_precedes_activation,
        stale_commit_rejected: generation.fenced_commit_attempts > 0
            && generation.fenced_commit_rejections == generation.fenced_commit_attempts,
        stale_route_rejected: generation.stale_generation_routing_rejected
            && mode == ProviderIncarnationMode::Correct,
        stale_publication_rejected,
        destination_operations_authorized: generation.active_generation_routing_authorized
            && generation.committed_data_writes >= 2
            && destination_publication_authorized,
    };
    Ok(build_report(
        seed,
        mode,
        observations,
        generation.authority_process_starts + publication.authority_process_starts,
        generation.data_process_starts,
        generation.process_kills + publication.process_kills,
        generation.authority_failovers + publication.authority_failovers,
        generation.fenced_commit_attempts,
        generation.fenced_commit_rejections,
        publication.publication_writes,
        &generation.trace_sha256,
        &publication.trace_sha256,
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_report(
    seed: u64,
    mode: ProviderIncarnationMode,
    observations: Observations,
    authority_process_starts: u64,
    data_process_starts: u64,
    process_kills: u64,
    authority_failovers: u64,
    fenced_commit_attempts: u64,
    fenced_commit_rejections: u64,
    publication_writes: u64,
    generation_trace: &str,
    publication_trace: &str,
) -> ProviderIncarnationReport {
    let checks = BTreeMap::from([
        (
            "external_authority_is_separate_from_provider_identities".to_owned(),
            observations.external_authority_separate,
        ),
        (
            "source_provider_fence_precedes_destination_activation".to_owned(),
            observations.source_fence_precedes_activation,
        ),
        (
            "newer_incarnation_fences_old_commit_authority".to_owned(),
            observations.stale_commit_rejected,
        ),
        (
            "newer_incarnation_fences_old_routing".to_owned(),
            observations.stale_route_rejected,
        ),
        (
            "newer_incarnation_fences_old_object_publication".to_owned(),
            observations.stale_publication_rejected,
        ),
        (
            "destination_incarnation_routes_commits_and_publishes".to_owned(),
            observations.destination_operations_authorized,
        ),
    ]);
    let anomaly_count =
        u64::try_from(checks.values().filter(|passed| !**passed).count()).unwrap_or(u64::MAX);
    let first_mismatch = checks
        .iter()
        .find_map(|(name, passed)| (!passed).then(|| name.clone()));
    let mut trace = Sha256::new();
    trace.update(b"okv-provider-incarnation-contract-v1\0");
    trace.update(seed.to_be_bytes());
    trace.update(mode.id().as_bytes());
    trace.update(generation_trace.as_bytes());
    trace.update(publication_trace.as_bytes());
    for (name, passed) in &checks {
        trace.update(name.as_bytes());
        trace.update([u8::from(*passed)]);
    }
    ProviderIncarnationReport {
        seed,
        mode,
        executed_checks: u64::try_from(checks.len()).unwrap_or(u64::MAX),
        anomaly_count,
        first_mismatch,
        authority_process_starts,
        data_process_starts,
        process_kills,
        authority_failovers,
        fenced_commit_attempts,
        fenced_commit_rejections,
        publication_writes,
        checks,
        trace_sha256: format!("{:x}", trace.finalize()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observations() -> Observations {
        Observations {
            external_authority_separate: true,
            source_fence_precedes_activation: true,
            stale_commit_rejected: true,
            stale_route_rejected: true,
            stale_publication_rejected: true,
            destination_operations_authorized: true,
        }
    }

    #[test]
    fn correct_compound_fence_has_no_anomalies() {
        let report = build_report(
            1,
            ProviderIncarnationMode::Correct,
            observations(),
            6,
            6,
            2,
            2,
            4,
            4,
            10,
            "generation",
            "publication",
        );
        assert_eq!(report.anomaly_count, 0);
        assert!(report.checks.values().all(|passed| *passed));
    }

    #[test]
    fn stale_source_poison_crosses_all_three_fenced_surfaces() {
        let mut unsafe_observations = observations();
        unsafe_observations.stale_commit_rejected = false;
        unsafe_observations.stale_route_rejected = false;
        unsafe_observations.stale_publication_rejected = false;
        let report = build_report(
            1,
            ProviderIncarnationMode::AcceptStaleSourceIncarnation,
            unsafe_observations,
            6,
            6,
            2,
            2,
            4,
            1,
            10,
            "generation",
            "publication",
        );
        assert_eq!(report.anomaly_count, 3);
        assert!(!report.checks["newer_incarnation_fences_old_commit_authority"]);
        assert!(!report.checks["newer_incarnation_fences_old_routing"]);
        assert!(!report.checks["newer_incarnation_fences_old_object_publication"]);
    }
}
