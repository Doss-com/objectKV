//! Source-pinned semantic preflight for incumbent transaction planes.

use okv_plane::{preflight, Capability, PreflightResult, ProviderProfile};

/// One named preflight assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Gate {
    pub id: &'static str,
    pub passed: bool,
    pub detail: String,
}

/// Complete preflight result plus its named hard gates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Report {
    pub result: PreflightResult,
    pub gates: Vec<Gate>,
}

/// Execute the source-derived provider mapping selected by one workload.
///
/// # Errors
///
/// Returns an error when the subject or backend is not one of the frozen
/// RFC-0041 identities.
pub fn run(subject: &str, backend: &str) -> Result<Report, String> {
    let profile = match subject {
        "foundationdb-7.4.6" => {
            require_backend(
                backend,
                "foundationdb-7.4.6+explicit-versionstamped-retained-keys",
            )?;
            ProviderProfile::foundationdb_7_4_6()
        }
        "tikv-8.5.7" => {
            require_backend(backend, "tikv-8.5.7+tikv-client-88688d6")?;
            ProviderProfile::tikv_8_5_7()
        }
        "tikv-false-serializable-label" => {
            require_backend(backend, "tikv-8.5.7+unsafe-serializable-label")?;
            let mut profile = ProviderProfile::tikv_8_5_7();
            profile
                .advertised_capabilities
                .insert(Capability::StrictSerializableConflicts);
            profile
                .advertised_capabilities
                .insert(Capability::AtomicRetainedChangeAndOutcome);
            "tikv-8.5.7-unsafe-serializable-label".clone_into(&mut profile.id);
            profile
        }
        other => return Err(format!("unsupported provider preflight subject {other}")),
    };
    let result = preflight(&profile);
    let gates = vec![
        Gate {
            id: "strict_serializable_write_skew",
            passed: result.write_skew_commits <= 1,
            detail: format!(
                "{} of 2 disjoint-write transactions committed",
                result.write_skew_commits
            ),
        },
        Gate {
            id: "all_required_capabilities",
            passed: result.unsupported_capabilities.is_empty(),
            detail: format!(
                "{} of {} required capabilities unsupported",
                result.unsupported_capabilities.len(),
                result.required_capabilities
            ),
        },
        Gate {
            id: "atomic_retained_change_and_outcome",
            passed: !result
                .unsupported_capabilities
                .contains(&Capability::AtomicRetainedChangeAndOutcome),
            detail: "user mutation, retained command, and outcome share one transaction".to_owned(),
        },
        Gate {
            id: "eligible_for_live_spike",
            passed: result.eligible_for_live_spike,
            detail: format!(
                "preflight correctness anomalies={}",
                result.correctness_anomalies
            ),
        },
    ];
    Ok(Report { result, gates })
}

fn require_backend(observed: &str, expected: &str) -> Result<(), String> {
    if observed == expected {
        Ok(())
    } else {
        Err(format!(
            "provider preflight requires backend {expected}, got {observed}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foundationdb_subject_passes_static_preflight() {
        let report = run(
            "foundationdb-7.4.6",
            "foundationdb-7.4.6+explicit-versionstamped-retained-keys",
        )
        .expect("preflight");
        assert!(report.gates.iter().all(|gate| gate.passed));
    }

    #[test]
    fn tikv_subject_fails_isolation_and_capability_gates() {
        let report = run("tikv-8.5.7", "tikv-8.5.7+tikv-client-88688d6").expect("preflight");
        assert!(report.gates.iter().any(|gate| !gate.passed));
        assert_eq!(report.result.write_skew_commits, 2);
    }

    #[test]
    fn false_label_is_detected_by_history() {
        let report = run(
            "tikv-false-serializable-label",
            "tikv-8.5.7+unsafe-serializable-label",
        )
        .expect("preflight");
        assert!(report.result.unsupported_capabilities.is_empty());
        assert!(!report.gates[0].passed);
        assert!(!report.result.eligible_for_live_spike);
    }

    #[test]
    fn backend_drift_fails_closed() {
        let error = run("foundationdb-7.4.6", "foundationdb-latest")
            .expect_err("unfrozen backend must fail");
        assert!(error.contains("requires backend"));
    }
}
