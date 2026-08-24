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

#[cfg(test)]
mod tests {
    use super::{median, median_absolute_deviation};

    #[test]
    fn calculates_median_and_mad() {
        let samples = [1.0, 2.0, 100.0, 3.0, 4.0];
        let median = median(&samples);
        assert!((median - 3.0).abs() < f64::EPSILON);
        assert!((median_absolute_deviation(&samples, median) - 1.0).abs() < f64::EPSILON);
    }
}
