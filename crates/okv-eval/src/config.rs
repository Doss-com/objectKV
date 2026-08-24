use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize)]
pub struct Suite {
    pub schema_version: u32,
    pub id: String,
    pub status: String,
    pub metric_registry: String,
    pub result_schema: String,
    #[serde(default)]
    pub contract_files: Vec<String>,
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub dataset: BTreeMap<String, DatasetConfig>,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileConfig>,
    #[serde(default)]
    pub lanes: Vec<LaneConfig>,
    #[serde(default)]
    pub workloads: Vec<WorkloadConfig>,
    #[serde(default)]
    pub hard_gates: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TelemetryConfig {
    pub protocol: String,
    pub endpoint_env: String,
    #[serde(default)]
    pub required_signals: Vec<String>,
    #[serde(default)]
    pub required_for_profiles: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatasetConfig {
    pub logical_bytes: u64,
    pub key_count: u64,
    #[serde(default)]
    pub seeds: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProfileConfig {
    pub budget_kind: BudgetKind,
    pub budget_limit: f64,
    pub repeats: u32,
    #[serde(default)]
    pub docker_image: Option<String>,
    #[serde(default, flatten)]
    pub parameters: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    Events,
    Operations,
    Seconds,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LaneConfig {
    pub id: String,
    pub primary_metric: String,
    pub statistic: String,
    pub direction: Direction,
    pub practical_improvement_fraction: f64,
    #[serde(default)]
    pub constraints: Vec<ConstraintConfig>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Higher,
    Lower,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ConstraintConfig {
    pub metric: String,
    pub statistic: String,
    pub op: ConstraintOp,
    pub value: f64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintOp {
    Eq,
    Ge,
    Gt,
    Le,
    Lt,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkloadConfig {
    pub id: String,
    pub lane: String,
    pub operation: String,
    #[serde(default, flatten)]
    pub parameters: BTreeMap<String, toml::Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricRegistry {
    pub schema_version: u32,
    pub namespace: String,
    pub cardinality: CardinalityPolicy,
    #[serde(default)]
    pub metrics: Vec<MetricDefinition>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CardinalityPolicy {
    pub max_series_per_run: u64,
    #[serde(default)]
    pub banned_attributes: Vec<String>,
    #[serde(default)]
    pub required_resource_attributes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricDefinition {
    pub id: String,
    pub otel_name: String,
    pub kind: InstrumentKind,
    pub unit: String,
    pub description: String,
    #[serde(default)]
    pub attributes: Vec<String>,
    #[serde(default)]
    pub required_attributes: Vec<String>,
    #[serde(default)]
    pub boundaries: Vec<f64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug)]
pub struct LoadedSuite {
    pub suite_path: PathBuf,
    pub suite_bytes: Vec<u8>,
    pub suite: Suite,
    pub registry_path: PathBuf,
    pub registry: MetricRegistry,
    pub result_schema_path: PathBuf,
    pub contract_paths: Vec<PathBuf>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io { path: PathBuf, message: String },
    Parse { path: PathBuf, message: String },
    Invalid(Vec<String>),
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, message } | Self::Parse { path, message } => {
                write!(formatter, "{}: {message}", path.display())
            }
            Self::Invalid(errors) => write!(
                formatter,
                "invalid eval configuration: {}",
                errors.join("; ")
            ),
        }
    }
}

impl Error for ConfigError {}

/// Load a suite and its referenced metric registry, then validate both.
///
/// # Errors
///
/// Returns all structural validation errors together, or an I/O/parse error.
pub fn load_suite(path: &Path) -> Result<LoadedSuite, ConfigError> {
    let suite_bytes = read(path)?;
    let suite: Suite =
        toml::from_str(
            std::str::from_utf8(&suite_bytes).map_err(|error| ConfigError::Parse {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?,
        )
        .map_err(|error| ConfigError::Parse {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let registry_path = base.join(&suite.metric_registry);
    let registry_bytes = read(&registry_path)?;
    let registry: MetricRegistry =
        toml::from_str(std::str::from_utf8(&registry_bytes).map_err(|error| {
            ConfigError::Parse {
                path: registry_path.clone(),
                message: error.to_string(),
            }
        })?)
        .map_err(|error| ConfigError::Parse {
            path: registry_path.clone(),
            message: error.to_string(),
        })?;

    let contract_paths = suite
        .contract_files
        .iter()
        .map(|contract| base.join(contract))
        .collect();
    let loaded = LoadedSuite {
        suite_path: path.to_path_buf(),
        suite_bytes,
        result_schema_path: base.join(&suite.result_schema),
        contract_paths,
        suite,
        registry_path,
        registry,
    };
    let errors = validate(&loaded);
    if errors.is_empty() {
        Ok(loaded)
    } else {
        Err(ConfigError::Invalid(errors))
    }
}

fn read(path: &Path) -> Result<Vec<u8>, ConfigError> {
    fs::read(path).map_err(|error| ConfigError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

#[allow(clippy::too_many_lines)]
fn validate(loaded: &LoadedSuite) -> Vec<String> {
    let suite = &loaded.suite;
    let registry = &loaded.registry;
    let mut errors = Vec::new();

    if suite.schema_version != 1 {
        errors.push(format!(
            "unsupported suite schema version {}",
            suite.schema_version
        ));
    }
    if registry.schema_version != 1 {
        errors.push(format!(
            "unsupported metric registry schema version {}",
            registry.schema_version
        ));
    }
    if suite.id.trim().is_empty() {
        errors.push("suite id must not be empty".to_owned());
    }
    if suite.status.trim().is_empty() {
        errors.push("suite status must not be empty".to_owned());
    }
    if !loaded.result_schema_path.is_file() {
        errors.push(format!(
            "result schema does not exist: {}",
            loaded.result_schema_path.display()
        ));
    }
    for contract_path in &loaded.contract_paths {
        if !contract_path.is_file() {
            errors.push(format!(
                "contract file does not exist: {}",
                contract_path.display()
            ));
        }
    }

    validate_telemetry(suite, &mut errors);
    validate_registry(registry, &mut errors);

    let metric_ids: BTreeSet<&str> = registry
        .metrics
        .iter()
        .map(|metric| metric.id.as_str())
        .collect();
    let lane_ids = unique_ids(
        "lane",
        suite.lanes.iter().map(|lane| lane.id.as_str()),
        &mut errors,
    );
    unique_ids(
        "workload",
        suite.workloads.iter().map(|workload| workload.id.as_str()),
        &mut errors,
    );

    for (profile_id, profile) in &suite.profiles {
        if profile.budget_limit <= 0.0 || !profile.budget_limit.is_finite() {
            errors.push(format!(
                "profile {profile_id} budget_limit must be finite and positive"
            ));
        }
        if profile.repeats == 0 {
            errors.push(format!("profile {profile_id} repeats must be positive"));
        }
    }
    for required_profile in &suite.telemetry.required_for_profiles {
        if !suite.profiles.contains_key(required_profile) {
            errors.push(format!(
                "telemetry requires unknown profile {required_profile}"
            ));
        }
    }

    for lane in &suite.lanes {
        if !metric_ids.contains(lane.primary_metric.as_str()) {
            errors.push(format!(
                "lane {} references unknown primary metric {}",
                lane.id, lane.primary_metric
            ));
        }
        if !lane.practical_improvement_fraction.is_finite()
            || lane.practical_improvement_fraction < 0.0
        {
            errors.push(format!(
                "lane {} practical improvement must be finite and non-negative",
                lane.id
            ));
        }
        if lane.statistic.trim().is_empty() {
            errors.push(format!("lane {} statistic must not be empty", lane.id));
        }
        for constraint in &lane.constraints {
            if !metric_ids.contains(constraint.metric.as_str()) {
                errors.push(format!(
                    "lane {} constraint references unknown metric {}",
                    lane.id, constraint.metric
                ));
            }
            if !constraint.value.is_finite() {
                errors.push(format!("lane {} constraint value must be finite", lane.id));
            }
            if constraint.statistic.trim().is_empty() {
                errors.push(format!(
                    "lane {} has an empty constraint statistic",
                    lane.id
                ));
            }
        }
    }

    for workload in &suite.workloads {
        if !lane_ids.contains(workload.lane.as_str()) {
            errors.push(format!(
                "workload {} references unknown lane {}",
                workload.id, workload.lane
            ));
        }
        if workload.operation.trim().is_empty() {
            errors.push(format!(
                "workload {} operation must not be empty",
                workload.id
            ));
        }
    }

    if suite.workloads.is_empty() {
        errors.push("suite must define at least one workload".to_owned());
    }
    if suite.lanes.is_empty() {
        errors.push("suite must define at least one lane".to_owned());
    }
    if suite.profiles.is_empty() {
        errors.push("suite must define at least one profile".to_owned());
    }
    if suite.dataset.is_empty() {
        errors.push("suite must define at least one dataset".to_owned());
    }
    if suite.hard_gates.is_empty() {
        errors.push("suite must define at least one hard gate".to_owned());
    }

    errors
}

fn validate_telemetry(suite: &Suite, errors: &mut Vec<String>) {
    if suite.telemetry.protocol != "otlp-http" {
        errors.push(format!(
            "unsupported telemetry protocol {}",
            suite.telemetry.protocol
        ));
    }
    if suite.telemetry.endpoint_env.trim().is_empty() {
        errors.push("telemetry endpoint_env must not be empty".to_owned());
    }
    let supported: BTreeSet<&str> = ["logs", "metrics", "traces"].into_iter().collect();
    let actual: BTreeSet<&str> = suite
        .telemetry
        .required_signals
        .iter()
        .map(String::as_str)
        .collect();
    if actual != supported {
        errors.push("telemetry required_signals must contain logs, metrics, and traces".to_owned());
    }
}

fn validate_registry(registry: &MetricRegistry, errors: &mut Vec<String>) {
    validate_registry_contract(registry, errors);

    unique_ids(
        "metric",
        registry.metrics.iter().map(|metric| metric.id.as_str()),
        errors,
    );
    unique_ids(
        "OTel metric",
        registry
            .metrics
            .iter()
            .map(|metric| metric.otel_name.as_str()),
        errors,
    );
    let banned: BTreeSet<&str> = registry
        .cardinality
        .banned_attributes
        .iter()
        .map(String::as_str)
        .collect();

    for metric in &registry.metrics {
        validate_metric(registry, metric, &banned, errors);
    }
}

fn validate_registry_contract(registry: &MetricRegistry, errors: &mut Vec<String>) {
    if !registry.namespace.starts_with("okv.") {
        errors.push("metric namespace must start with okv.".to_owned());
    }
    if registry.cardinality.max_series_per_run == 0 {
        errors.push("max_series_per_run must be positive".to_owned());
    }
    if registry.cardinality.required_resource_attributes.is_empty() {
        errors.push("required_resource_attributes must not be empty".to_owned());
    }
    let supported_resource_attributes: BTreeSet<&str> = [
        "service.name",
        "service.version",
        "deployment.environment.name",
        "okv.eval.run.id",
        "okv.eval.suite.id",
        "okv.eval.suite.hash",
        "okv.eval.profile.id",
        "okv.eval.profile.hash",
        "okv.eval.candidate.commit",
        "okv.eval.backend",
    ]
    .into_iter()
    .collect();
    let required_resource_attributes: BTreeSet<&str> = registry
        .cardinality
        .required_resource_attributes
        .iter()
        .map(String::as_str)
        .collect();
    if required_resource_attributes != supported_resource_attributes {
        errors.push(
            "required_resource_attributes must exactly match the eval runner resource contract"
                .to_owned(),
        );
    }
}

fn validate_metric(
    registry: &MetricRegistry,
    metric: &MetricDefinition,
    banned: &BTreeSet<&str>,
    errors: &mut Vec<String>,
) {
    if !metric.otel_name.starts_with(&registry.namespace) {
        errors.push(format!(
            "metric {} name {} is outside namespace {}",
            metric.id, metric.otel_name, registry.namespace
        ));
    }
    if metric.unit.trim().is_empty() || metric.description.trim().is_empty() {
        errors.push(format!("metric {} needs a unit and description", metric.id));
    }
    let attributes: BTreeSet<&str> = unique_ids(
        &format!("metric {} attribute", metric.id),
        metric.attributes.iter().map(String::as_str),
        errors,
    );
    for attribute in &metric.required_attributes {
        if !attributes.contains(attribute.as_str()) {
            errors.push(format!(
                "metric {} required attribute {} is not allowlisted",
                metric.id, attribute
            ));
        }
    }
    for attribute in &metric.attributes {
        if banned.contains(attribute.as_str()) {
            errors.push(format!(
                "metric {} uses banned high-cardinality attribute {}",
                metric.id, attribute
            ));
        }
    }
    if matches!(metric.kind, InstrumentKind::Histogram) {
        if metric.boundaries.is_empty() {
            errors.push(format!("histogram {} has no boundaries", metric.id));
        }
        if metric
            .boundaries
            .windows(2)
            .any(|window| window[0] >= window[1] || !window[0].is_finite())
            || metric
                .boundaries
                .last()
                .is_some_and(|value| !value.is_finite())
        {
            errors.push(format!(
                "histogram {} boundaries must be finite and strictly increasing",
                metric.id
            ));
        }
    } else if !metric.boundaries.is_empty() {
        errors.push(format!(
            "non-histogram metric {} must not define boundaries",
            metric.id
        ));
    }
}

fn unique_ids<'a>(
    kind: &str,
    values: impl Iterator<Item = &'a str>,
    errors: &mut Vec<String>,
) -> BTreeSet<&'a str> {
    let mut unique = BTreeSet::new();
    for value in values {
        if value.trim().is_empty() {
            errors.push(format!("{kind} id must not be empty"));
        }
        if !unique.insert(value) {
            errors.push(format!("duplicate {kind} id {value}"));
        }
    }
    unique
}

#[cfg(test)]
mod tests {
    use super::load_suite;
    use std::path::Path;

    #[test]
    fn smoke_suite_and_registry_are_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let suite = load_suite(&root.join("evals/suites/smoke.toml")).expect("valid suite");
        assert_eq!(suite.suite.id, "objectkv-smoke-v1");
        assert!(suite.registry.metrics.len() >= 10);
    }
}
