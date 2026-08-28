use crate::config::{ComparisonConstraintConfig, Direction, EvidenceClass};
use crate::program::{plan_program, LoadedProgram, ProgramGateKind, ProgramGateStatus};
use crate::result::validate_result;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct ComparisonReceipt {
    pub schema_version: u32,
    pub created_at: String,
    pub program: String,
    pub gate: String,
    pub gate_status: ProgramGateStatus,
    pub claim: String,
    pub falsifier: String,
    pub candidate: ComparisonSubject,
    pub control: ComparisonSubject,
    pub metric: ComparisonMetric,
    pub practical_improvement_fraction: f64,
    pub observed: ComparisonObservation,
    pub constraints: Vec<ComparisonConstraintObservation>,
    pub comparability_checks: Vec<ComparisonCheck>,
    pub verdict: ComparisonVerdict,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct ComparisonSubject {
    pub run_id: String,
    pub batch_id: Option<String>,
    pub suite: String,
    pub suite_hash: String,
    pub profile: String,
    pub profile_hash: String,
    pub backend: String,
    pub candidate_commit: String,
    pub value: f64,
    pub median: f64,
    pub mad: f64,
    pub samples: usize,
}

#[derive(Debug, Serialize)]
pub struct ComparisonMetric {
    pub name: String,
    pub unit: String,
    pub statistic: String,
    pub direction: Direction,
}

#[derive(Debug, Serialize)]
pub struct ComparisonObservation {
    pub candidate_value: f64,
    pub control_value: f64,
    pub directional_delta_fraction: f64,
    pub directional_delta_percent: f64,
    pub candidate_noise_fraction: f64,
    pub control_noise_fraction: f64,
    pub noise_floor_fraction: f64,
    pub decisive_effect_fraction: f64,
}

#[derive(Debug, Serialize)]
pub struct ComparisonConstraintObservation {
    pub id: String,
    pub candidate_metric: String,
    pub control_metric: String,
    pub unit: String,
    pub direction: Direction,
    pub max_regression_fraction: f64,
    pub candidate_value: f64,
    pub control_value: f64,
    pub candidate_to_control_ratio: Option<f64>,
    pub directional_delta_fraction: Option<f64>,
    pub passed: bool,
}

#[derive(Debug, Serialize)]
pub struct ComparisonCheck {
    pub id: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonVerdict {
    Better,
    Worse,
    Inconclusive,
    Invalid,
}

#[derive(Debug, Deserialize)]
struct ResultDocument {
    run_id: String,
    batch_id: Option<String>,
    lane: String,
    suite: String,
    suite_hash: String,
    profile: ResultProfile,
    candidate_commit: String,
    backend: String,
    seeds: Vec<u64>,
    hard_gates: Vec<ResultHardGate>,
    primary_metric: ResultMetric,
    #[serde(default)]
    secondary_metrics: std::collections::BTreeMap<String, f64>,
    verdict: String,
}

#[derive(Debug, Deserialize)]
struct ResultProfile {
    id: String,
    hash: String,
    #[serde(default)]
    evidence_class: Option<EvidenceClass>,
    #[serde(default)]
    workload_profile_hash: Option<String>,
    machine: String,
    rustc: String,
    lockfile_hash: String,
}

#[derive(Debug, Deserialize)]
struct ResultHardGate {
    id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ResultMetric {
    name: String,
    unit: String,
    direction: Direction,
    statistic: String,
    value: f64,
    samples: Vec<f64>,
    median: f64,
    mad: f64,
}

/// Compare one candidate and its declared program control.
///
/// # Errors
///
/// Returns an error when inputs are unreadable, schema-invalid, or the named
/// program gate is not a performance or economics gate with a paired control.
#[allow(clippy::too_many_lines)]
pub fn compare_results(
    loaded_program: &LoadedProgram,
    gate_id: &str,
    candidate_path: &Path,
    control_path: &Path,
    result_schema_path: &Path,
) -> Result<ComparisonReceipt, Box<dyn Error>> {
    let plan = plan_program(loaded_program)?;
    let gate = plan
        .phases
        .iter()
        .flat_map(|phase| &phase.gates)
        .find(|gate| gate.id == gate_id)
        .ok_or_else(|| std::io::Error::other(format!("program has no gate {gate_id}")))?;
    if !matches!(
        gate.kind,
        ProgramGateKind::Performance | ProgramGateKind::Economics
    ) {
        return Err(std::io::Error::other(format!(
            "gate {gate_id} is not a performance or economics comparison"
        ))
        .into());
    }
    let control_plan = gate
        .control
        .as_ref()
        .ok_or_else(|| std::io::Error::other(format!("gate {gate_id} has no paired control")))?;

    let candidate = read_result(candidate_path, result_schema_path)?;
    let control = read_result(control_path, result_schema_path)?;
    let candidate_minimum_samples = if gate.kind == ProgramGateKind::Performance {
        gate.configured_repeats.max(5)
    } else {
        gate.configured_repeats.max(1)
    } as usize;
    let control_minimum_samples = if gate.kind == ProgramGateKind::Performance {
        control_plan.configured_repeats.max(5)
    } else {
        control_plan.configured_repeats.max(1)
    } as usize;
    let mut checks = vec![
        boolean_check(
            "evidence.candidate_workload_profile",
            has_workload_evidence(&candidate.profile),
            format!(
                "class={:?},profile_hash={:?}",
                candidate.profile.evidence_class, candidate.profile.workload_profile_hash
            ),
        ),
        boolean_check(
            "evidence.control_workload_profile",
            has_workload_evidence(&control.profile),
            format!(
                "class={:?},profile_hash={:?}",
                control.profile.evidence_class, control.profile.workload_profile_hash
            ),
        ),
        check(
            "candidate.suite",
            candidate.suite == gate.suite,
            &candidate.suite,
            &gate.suite,
        ),
        check(
            "candidate.suite_hash",
            candidate.suite_hash == gate.suite_hash,
            &candidate.suite_hash,
            &gate.suite_hash,
        ),
        check(
            "candidate.profile",
            candidate.profile.id == gate.profile,
            &candidate.profile.id,
            &gate.profile,
        ),
        check(
            "candidate.backend",
            candidate.backend == gate.backend,
            &candidate.backend,
            &gate.backend,
        ),
        check(
            "candidate.lane",
            candidate.lane == gate.lane,
            &candidate.lane,
            &gate.lane,
        ),
        check(
            "control.suite",
            control.suite == control_plan.suite,
            &control.suite,
            &control_plan.suite,
        ),
        check(
            "control.suite_hash",
            control.suite_hash == control_plan.suite_hash,
            &control.suite_hash,
            &control_plan.suite_hash,
        ),
        check(
            "control.profile",
            control.profile.id == control_plan.profile,
            &control.profile.id,
            &control_plan.profile,
        ),
        check(
            "control.backend",
            control.backend == control_plan.backend,
            &control.backend,
            &control_plan.backend,
        ),
        check(
            "control.lane",
            control.lane == control_plan.lane,
            &control.lane,
            &control_plan.lane,
        ),
        check(
            "metric.name",
            candidate.primary_metric.name == gate.primary_metric_otel_name
                && control.primary_metric.name == gate.primary_metric_otel_name,
            &format!(
                "candidate={},control={}",
                candidate.primary_metric.name, control.primary_metric.name
            ),
            &gate.primary_metric_otel_name,
        ),
        check(
            "metric.statistic",
            candidate.primary_metric.statistic == gate.statistic
                && control.primary_metric.statistic == gate.statistic,
            &format!(
                "candidate={},control={}",
                candidate.primary_metric.statistic, control.primary_metric.statistic
            ),
            &gate.statistic,
        ),
        boolean_check(
            "metric.direction",
            same_direction(candidate.primary_metric.direction, gate.direction)
                && same_direction(control.primary_metric.direction, gate.direction),
            format!(
                "candidate={:?},control={:?},expected={:?}",
                candidate.primary_metric.direction,
                control.primary_metric.direction,
                gate.direction
            ),
        ),
        boolean_check(
            "metric.unit",
            candidate.primary_metric.unit == control.primary_metric.unit,
            format!(
                "candidate={},control={}",
                candidate.primary_metric.unit, control.primary_metric.unit
            ),
        ),
        boolean_check(
            "identity.batch",
            candidate.batch_id.is_some()
                && candidate.batch_id == control.batch_id
                && candidate
                    .batch_id
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty()),
            format!(
                "candidate={:?},control={:?}",
                candidate.batch_id, control.batch_id
            ),
        ),
        boolean_check(
            "identity.machine",
            candidate.profile.machine == control.profile.machine,
            format!(
                "candidate={},control={}",
                candidate.profile.machine, control.profile.machine
            ),
        ),
        boolean_check(
            "identity.rustc",
            candidate.profile.rustc == control.profile.rustc,
            format!(
                "candidate={},control={}",
                candidate.profile.rustc, control.profile.rustc
            ),
        ),
        boolean_check(
            "identity.lockfile",
            candidate.profile.lockfile_hash == control.profile.lockfile_hash,
            format!(
                "candidate={},control={}",
                candidate.profile.lockfile_hash, control.profile.lockfile_hash
            ),
        ),
        boolean_check(
            "identity.commit",
            candidate.candidate_commit == control.candidate_commit,
            format!(
                "candidate={},control={}",
                candidate.candidate_commit, control.candidate_commit
            ),
        ),
        boolean_check(
            "identity.seeds",
            candidate.seeds == control.seeds,
            format!(
                "candidate={:?},control={:?}",
                candidate.seeds, control.seeds
            ),
        ),
        boolean_check(
            "candidate.hard_gates",
            all_hard_gates_pass(&candidate),
            failed_hard_gates(&candidate),
        ),
        boolean_check(
            "control.hard_gates",
            all_hard_gates_pass(&control),
            failed_hard_gates(&control),
        ),
        boolean_check(
            "candidate.verdict",
            !matches!(candidate.verdict.as_str(), "crash" | "discard"),
            candidate.verdict.clone(),
        ),
        boolean_check(
            "control.verdict",
            !matches!(control.verdict.as_str(), "crash" | "discard"),
            control.verdict.clone(),
        ),
        boolean_check(
            "candidate.samples",
            candidate.primary_metric.samples.len() >= candidate_minimum_samples,
            format!(
                "observed={},required={candidate_minimum_samples}",
                candidate.primary_metric.samples.len()
            ),
        ),
        boolean_check(
            "control.samples",
            control.primary_metric.samples.len() >= control_minimum_samples,
            format!(
                "observed={},required={control_minimum_samples}",
                control.primary_metric.samples.len()
            ),
        ),
    ];
    if gate.suite == control_plan.suite {
        checks.push(boolean_check(
            "identity.suite_hash",
            candidate.suite_hash == control.suite_hash,
            format!(
                "candidate={},control={}",
                candidate.suite_hash, control.suite_hash
            ),
        ));
    }
    if gate.profile == control_plan.profile {
        checks.push(boolean_check(
            "identity.profile_hash",
            candidate.profile.hash == control.profile.hash,
            format!(
                "candidate={},control={}",
                candidate.profile.hash, control.profile.hash
            ),
        ));
    }

    let control_value = control.primary_metric.value;
    let denominator_valid = control_value.is_finite() && control_value.abs() > f64::EPSILON;
    checks.push(boolean_check(
        "metric.control_denominator",
        denominator_valid,
        format!("control_value={control_value}"),
    ));
    let directional_delta_fraction = if denominator_valid {
        match gate.direction {
            Direction::Higher => {
                (candidate.primary_metric.value - control_value) / control_value.abs()
            }
            Direction::Lower => {
                (control_value - candidate.primary_metric.value) / control_value.abs()
            }
        }
    } else {
        0.0
    };
    let candidate_noise_fraction = relative_noise(&candidate.primary_metric);
    let control_noise_fraction = relative_noise(&control.primary_metric);
    let noise_floor_fraction = candidate_noise_fraction.max(control_noise_fraction);
    let decisive_effect_fraction = gate
        .practical_improvement_fraction
        .max(noise_floor_fraction);
    let mut constraints = Vec::with_capacity(gate.comparison_constraints.len());
    for constraint in &gate.comparison_constraints {
        match evaluate_comparison_constraint(
            constraint,
            &candidate.secondary_metrics,
            &control.secondary_metrics,
        ) {
            Ok(observation) => {
                checks.push(boolean_check(
                    &format!("constraint.{}.inputs", constraint.id),
                    true,
                    format!(
                        "candidate={},control={}",
                        observation.candidate_value, observation.control_value
                    ),
                ));
                constraints.push(observation);
            }
            Err(detail) => checks.push(boolean_check(
                &format!("constraint.{}.inputs", constraint.id),
                false,
                detail,
            )),
        }
    }
    let comparable = checks.iter().all(|check| check.passed);
    let failed_constraints = constraints
        .iter()
        .filter(|constraint| !constraint.passed)
        .map(|constraint| constraint.id.as_str())
        .collect::<Vec<_>>();
    let (verdict, reason) = if !comparable {
        (
            ComparisonVerdict::Invalid,
            "one or more comparability checks failed".to_owned(),
        )
    } else if directional_delta_fraction > decisive_effect_fraction {
        (
            ComparisonVerdict::Better,
            format!(
                "candidate clears the {:.2}% practical-and-noise threshold",
                decisive_effect_fraction * 100.0
            ),
        )
    } else if directional_delta_fraction < -decisive_effect_fraction
        || !failed_constraints.is_empty()
    {
        (
            ComparisonVerdict::Worse,
            if failed_constraints.is_empty() {
                format!(
                    "candidate regresses beyond the {:.2}% practical-and-noise threshold",
                    decisive_effect_fraction * 100.0
                )
            } else {
                format!(
                    "candidate fails comparison constraints: {}",
                    failed_constraints.join(",")
                )
            },
        )
    } else {
        (
            ComparisonVerdict::Inconclusive,
            format!(
                "effect does not clear the {:.2}% practical-and-noise threshold",
                decisive_effect_fraction * 100.0
            ),
        )
    };

    Ok(ComparisonReceipt {
        schema_version: 1,
        created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        program: plan.program,
        gate: gate.id.clone(),
        gate_status: gate.status,
        claim: gate.claim.clone(),
        falsifier: gate.falsifier.clone(),
        candidate: subject(&candidate),
        control: subject(&control),
        metric: ComparisonMetric {
            name: gate.primary_metric_otel_name.clone(),
            unit: candidate.primary_metric.unit.clone(),
            statistic: gate.statistic.clone(),
            direction: gate.direction,
        },
        practical_improvement_fraction: gate.practical_improvement_fraction,
        observed: ComparisonObservation {
            candidate_value: candidate.primary_metric.value,
            control_value,
            directional_delta_fraction,
            directional_delta_percent: directional_delta_fraction * 100.0,
            candidate_noise_fraction,
            control_noise_fraction,
            noise_floor_fraction,
            decisive_effect_fraction,
        },
        constraints,
        comparability_checks: checks,
        verdict,
        reason,
    })
}

fn has_workload_evidence(profile: &ResultProfile) -> bool {
    profile.evidence_class == Some(EvidenceClass::Workload)
        && profile
            .workload_profile_hash
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn evaluate_comparison_constraint(
    constraint: &ComparisonConstraintConfig,
    candidate_metrics: &std::collections::BTreeMap<String, f64>,
    control_metrics: &std::collections::BTreeMap<String, f64>,
) -> Result<ComparisonConstraintObservation, String> {
    let candidate_value = candidate_metrics
        .get(&constraint.candidate_metric)
        .copied()
        .ok_or_else(|| format!("candidate metric {} is absent", constraint.candidate_metric))?;
    let control_value = control_metrics
        .get(&constraint.control_metric)
        .copied()
        .ok_or_else(|| format!("control metric {} is absent", constraint.control_metric))?;
    if !candidate_value.is_finite() {
        return Err(format!(
            "candidate metric {} is not finite",
            constraint.candidate_metric
        ));
    }
    if !control_value.is_finite() {
        return Err(format!(
            "control metric {} is not finite",
            constraint.control_metric
        ));
    }
    if control_value.abs() <= f64::EPSILON {
        let candidate_is_zero = candidate_value.abs() <= f64::EPSILON;
        return Ok(ComparisonConstraintObservation {
            id: constraint.id.clone(),
            candidate_metric: constraint.candidate_metric.clone(),
            control_metric: constraint.control_metric.clone(),
            unit: constraint.unit.clone(),
            direction: constraint.direction,
            max_regression_fraction: constraint.max_regression_fraction,
            candidate_value,
            control_value,
            candidate_to_control_ratio: candidate_is_zero.then_some(1.0),
            directional_delta_fraction: candidate_is_zero.then_some(0.0),
            passed: candidate_is_zero,
        });
    }
    let directional_delta_fraction = match constraint.direction {
        Direction::Higher => (candidate_value - control_value) / control_value.abs(),
        Direction::Lower => (control_value - candidate_value) / control_value.abs(),
    };
    Ok(ComparisonConstraintObservation {
        id: constraint.id.clone(),
        candidate_metric: constraint.candidate_metric.clone(),
        control_metric: constraint.control_metric.clone(),
        unit: constraint.unit.clone(),
        direction: constraint.direction,
        max_regression_fraction: constraint.max_regression_fraction,
        candidate_value,
        control_value,
        candidate_to_control_ratio: Some(candidate_value / control_value),
        directional_delta_fraction: Some(directional_delta_fraction),
        passed: directional_delta_fraction >= -constraint.max_regression_fraction,
    })
}

/// Validate a serialized comparison receipt against its JSON Schema.
///
/// # Errors
///
/// Returns an error when the schema cannot be loaded or the receipt fails it.
pub fn validate_comparison_receipt(
    schema_path: &Path,
    receipt: &Value,
) -> Result<(), Box<dyn Error>> {
    let schema: Value = serde_json::from_slice(&fs::read(schema_path)?)?;
    let validator = jsonschema::validator_for(&schema)?;
    let errors: Vec<String> = validator
        .iter_errors(receipt)
        .map(|error| error.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

fn read_result(path: &Path, schema_path: &Path) -> Result<ResultDocument, Box<dyn Error>> {
    let value: Value = serde_json::from_slice(&fs::read(path)?)?;
    validate_result(schema_path, &value)?;
    Ok(serde_json::from_value(value)?)
}

fn subject(result: &ResultDocument) -> ComparisonSubject {
    ComparisonSubject {
        run_id: result.run_id.clone(),
        batch_id: result.batch_id.clone(),
        suite: result.suite.clone(),
        suite_hash: result.suite_hash.clone(),
        profile: result.profile.id.clone(),
        profile_hash: result.profile.hash.clone(),
        backend: result.backend.clone(),
        candidate_commit: result.candidate_commit.clone(),
        value: result.primary_metric.value,
        median: result.primary_metric.median,
        mad: result.primary_metric.mad,
        samples: result.primary_metric.samples.len(),
    }
}

fn check(id: &str, passed: bool, observed: &str, expected: &str) -> ComparisonCheck {
    boolean_check(
        id,
        passed,
        format!("observed={observed},expected={expected}"),
    )
}

fn boolean_check(id: &str, passed: bool, detail: String) -> ComparisonCheck {
    ComparisonCheck {
        id: id.to_owned(),
        passed,
        detail,
    }
}

fn same_direction(left: Direction, right: Direction) -> bool {
    std::mem::discriminant(&left) == std::mem::discriminant(&right)
}

fn all_hard_gates_pass(result: &ResultDocument) -> bool {
    !result.hard_gates.is_empty() && result.hard_gates.iter().all(|gate| gate.status == "pass")
}

fn failed_hard_gates(result: &ResultDocument) -> String {
    let failed = result
        .hard_gates
        .iter()
        .filter(|gate| gate.status != "pass")
        .map(|gate| format!("{}={}", gate.id, gate.status))
        .collect::<Vec<_>>();
    if failed.is_empty() {
        "all_pass".to_owned()
    } else {
        failed.join(",")
    }
}

fn relative_noise(metric: &ResultMetric) -> f64 {
    if metric.median.abs() <= f64::EPSILON {
        0.0
    } else {
        metric.mad / metric.median.abs()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate_comparison_constraint, has_workload_evidence, relative_noise, ResultMetric,
        ResultProfile,
    };
    use crate::config::{ComparisonConstraintConfig, Direction, EvidenceClass};
    use std::collections::BTreeMap;

    #[test]
    fn relative_noise_uses_mad_over_absolute_median() {
        let metric = ResultMetric {
            name: "operation.duration".to_owned(),
            unit: "s".to_owned(),
            direction: Direction::Lower,
            statistic: "median".to_owned(),
            value: 8.0,
            samples: vec![7.0, 8.0, 9.0, 8.0, 8.0],
            median: 8.0,
            mad: 1.0,
        };
        assert!((relative_noise(&metric) - 0.125).abs() < f64::EPSILON);
    }

    #[test]
    fn lower_is_better_constraint_rejects_p99_regression() {
        let constraint = ComparisonConstraintConfig {
            id: "hot_read_p99".to_owned(),
            candidate_metric: "candidate.p99".to_owned(),
            control_metric: "control.p99".to_owned(),
            unit: "ns".to_owned(),
            direction: Direction::Lower,
            max_regression_fraction: 0.20,
        };
        let candidate = BTreeMap::from([("candidate.p99".to_owned(), 2_482.0)]);
        let control = BTreeMap::from([("control.p99".to_owned(), 1_749.0)]);
        let observed = evaluate_comparison_constraint(&constraint, &candidate, &control)
            .expect("comparable p99 metrics");
        assert!(!observed.passed);
        assert!(
            (observed
                .candidate_to_control_ratio
                .expect("non-zero control has a ratio")
                - 1.419_096_626_64)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn zero_baseline_constraint_requires_an_exact_zero_candidate() {
        let constraint = ComparisonConstraintConfig {
            id: "physical_read_bytes".to_owned(),
            candidate_metric: "candidate.physical_bytes".to_owned(),
            control_metric: "control.physical_bytes".to_owned(),
            unit: "By/{read}".to_owned(),
            direction: Direction::Lower,
            max_regression_fraction: 0.0,
        };
        let control = BTreeMap::from([("control.physical_bytes".to_owned(), 0.0)]);
        let zero_candidate = BTreeMap::from([("candidate.physical_bytes".to_owned(), 0.0)]);
        let zero_observed = evaluate_comparison_constraint(&constraint, &zero_candidate, &control)
            .expect("two exact zero values are comparable");
        assert!(zero_observed.passed);
        assert_eq!(zero_observed.candidate_to_control_ratio, Some(1.0));
        assert_eq!(zero_observed.directional_delta_fraction, Some(0.0));

        let non_zero_candidate = BTreeMap::from([("candidate.physical_bytes".to_owned(), 4096.0)]);
        let non_zero_observed =
            evaluate_comparison_constraint(&constraint, &non_zero_candidate, &control)
                .expect("a zero baseline produces an explicit failed observation");
        assert!(!non_zero_observed.passed);
        assert_eq!(non_zero_observed.candidate_to_control_ratio, None);
        assert_eq!(non_zero_observed.directional_delta_fraction, None);
    }

    #[test]
    fn comparison_constraint_requires_both_metrics() {
        let constraint = ComparisonConstraintConfig {
            id: "hot_read_p99".to_owned(),
            candidate_metric: "candidate.p99".to_owned(),
            control_metric: "control.p99".to_owned(),
            unit: "ns".to_owned(),
            direction: Direction::Lower,
            max_regression_fraction: 0.20,
        };
        let error = evaluate_comparison_constraint(
            &constraint,
            &BTreeMap::new(),
            &BTreeMap::from([("control.p99".to_owned(), 1_749.0)]),
        )
        .expect_err("missing candidate metric must invalidate comparison");
        assert!(error.contains("candidate metric candidate.p99 is absent"));
    }

    #[test]
    fn smoke_and_legacy_receipts_cannot_enter_performance_comparison() {
        let profile = |evidence_class, workload_profile_hash| ResultProfile {
            id: "gcp-r0".to_owned(),
            hash: "profile".to_owned(),
            evidence_class,
            workload_profile_hash,
            machine: "machine".to_owned(),
            rustc: "rustc".to_owned(),
            lockfile_hash: "lockfile".to_owned(),
        };
        assert!(!has_workload_evidence(&profile(
            Some(EvidenceClass::Smoke),
            Some("declared".to_owned())
        )));
        assert!(!has_workload_evidence(&profile(None, None)));
        assert!(has_workload_evidence(&profile(
            Some(EvidenceClass::Workload),
            Some("declared".to_owned())
        )));
    }
}
