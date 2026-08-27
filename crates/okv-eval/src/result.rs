use crate::config::{BudgetKind, Direction};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct EvalResult {
    pub schema_version: u32,
    pub run_id: String,
    pub batch_id: String,
    pub created_at: String,
    pub lane: String,
    pub suite: String,
    pub suite_hash: String,
    pub profile: ProfileIdentity,
    pub candidate_commit: String,
    pub parent_commit: String,
    pub backend: String,
    pub seeds: Vec<u64>,
    pub budget: BudgetResult,
    pub hard_gates: Vec<HardGateResult>,
    pub primary_metric: PrimaryMetricResult,
    pub secondary_metrics: BTreeMap<String, f64>,
    pub verdict: Verdict,
    pub reason: String,
    pub artifact_refs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ProfileIdentity {
    pub id: String,
    pub hash: String,
    pub machine: String,
    pub rustc: String,
    pub lockfile_hash: String,
}

#[derive(Debug, Serialize)]
pub struct BudgetResult {
    pub kind: BudgetKind,
    pub limit: f64,
    pub observed: f64,
}

#[derive(Debug, Serialize)]
pub struct HardGateResult {
    pub id: String,
    pub status: GateStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Error,
    Fail,
    Pass,
}

#[derive(Debug, Serialize)]
pub struct PrimaryMetricResult {
    pub name: String,
    pub unit: String,
    pub direction: Direction,
    pub statistic: String,
    pub value: f64,
    pub samples: Vec<f64>,
    pub median: f64,
    pub mad: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incumbent_median: Option<f64>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Crash,
    Discard,
    Inconclusive,
    Keep,
}

/// Validate a serialized result against the repository's JSON Schema.
///
/// # Errors
///
/// Returns an error when the schema cannot be loaded or the result fails it.
pub fn validate_result(schema_path: &Path, result: &Value) -> Result<(), Box<dyn Error>> {
    let schema: Value = serde_json::from_slice(&fs::read(schema_path)?)?;
    let validator = jsonschema::validator_for(&schema)?;
    let errors: Vec<String> = validator
        .iter_errors(result)
        .map(|error| error.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}

#[must_use]
pub fn median(samples: &[f64]) -> f64 {
    let mut values = samples.to_vec();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

#[must_use]
pub fn median_absolute_deviation(samples: &[f64], sample_median: f64) -> f64 {
    let deviations: Vec<f64> = samples
        .iter()
        .map(|sample| (sample - sample_median).abs())
        .collect();
    median(&deviations)
}

/// Reduce metric samples using the statistic declared by the eval lane.
///
/// # Errors
///
/// Returns an error for an empty sample set or unsupported statistic.
pub fn statistic_value(samples: &[f64], statistic: &str) -> Result<f64, String> {
    if samples.is_empty() {
        return Err("cannot reduce an empty metric sample set".to_owned());
    }
    match statistic {
        "median" | "per_operation" => Ok(median(samples)),
        "p99" => {
            let mut values = samples.to_vec();
            values.sort_by(f64::total_cmp);
            let rank = values.len().saturating_mul(99).div_ceil(100).max(1);
            Ok(values[rank - 1])
        }
        "total" => Ok(samples.iter().sum()),
        "minimum" | "passed" => Ok(samples.iter().copied().reduce(f64::min).unwrap_or(0.0)),
        "maximum" => Ok(samples.iter().copied().reduce(f64::max).unwrap_or(0.0)),
        _ => Err(format!("unsupported eval statistic {statistic}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{median, median_absolute_deviation, statistic_value};
    use serde_json::Value;

    #[test]
    fn calculates_median_and_mad() {
        let samples = [1.0, 2.0, 100.0, 3.0, 4.0];
        let median = median(&samples);
        assert!((median - 3.0).abs() < f64::EPSILON);
        assert!((median_absolute_deviation(&samples, median) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn applies_the_lane_statistic_without_replacing_noise_metrics() {
        let samples = [1.0, 2.0, 3.0, 4.0, 100.0];
        assert_eq!(statistic_value(&samples, "median"), Ok(3.0));
        assert_eq!(statistic_value(&samples, "p99"), Ok(100.0));
        assert_eq!(statistic_value(&samples, "total"), Ok(110.0));
        assert_eq!(statistic_value(&samples, "minimum"), Ok(1.0));
        assert_eq!(statistic_value(&samples, "maximum"), Ok(100.0));
        assert!(statistic_value(&samples, "unknown").is_err());
    }

    #[test]
    fn transaction_machine_example_matches_the_frozen_schema() {
        let schema: Value = serde_json::from_slice(include_bytes!(
            "../../../evals/schema/transaction-machine-config-v1.schema.json"
        ))
        .expect("decode schema");
        let example: Value = serde_json::from_slice(include_bytes!(
            "../../../infra/gcp/transaction-machine-config.example.json"
        ))
        .expect("decode example");
        let validator = jsonschema::validator_for(&schema).expect("compile schema");
        let errors: Vec<String> = validator
            .iter_errors(&example)
            .map(|error| error.to_string())
            .collect();
        assert!(errors.is_empty(), "{}", errors.join("; "));
    }
}
