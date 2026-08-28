use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
    pub evidence_class: EvidenceClass,
    #[serde(default)]
    pub workload_profile: Option<WorkloadProfileConfig>,
    #[serde(default)]
    pub docker_image: Option<String>,
    #[serde(default, flatten)]
    pub parameters: BTreeMap<String, toml::Value>,
}

/// The strongest claim one eval profile is allowed to support.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClass {
    /// Component and semantic contract evidence. This is the legacy default.
    #[default]
    Contract,
    /// Wiring, deployment, and bounded preflight evidence only.
    Smoke,
    /// A calibrated performance or economics workload against a named control.
    Workload,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkloadProfileConfig {
    pub schema_version: u32,
    pub name: String,
    pub dataset: String,
    pub access_pattern: String,
    pub cache_state: CacheState,
    pub matched_control: String,
    pub operation_mix: BTreeMap<String, f64>,
    pub concurrency: Vec<u32>,
    pub warmup: WorkloadWindowConfig,
    pub measurement: WorkloadWindowConfig,
    pub failure_schedule: Vec<String>,
    pub resource_limits: BTreeMap<String, u64>,
    pub required_metrics: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheState {
    Resident,
    WarmElastic,
    ColdElastic,
    EmptyWorker,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkloadWindowConfig {
    pub kind: BudgetKind,
    pub amount: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    #[serde(default)]
    pub comparison_constraints: Vec<ComparisonConstraintConfig>,
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

/// One candidate-to-control scalar constraint evaluated after both receipts exist.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ComparisonConstraintConfig {
    pub id: String,
    pub candidate_metric: String,
    pub control_metric: String,
    pub unit: String,
    pub direction: Direction,
    pub max_regression_fraction: f64,
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

/// Hash one complete suite contract, including the registry, result schema, and
/// every named contract file.
///
/// # Errors
///
/// Returns an error when any contract input cannot be read.
pub fn contract_hash(loaded: &LoadedSuite) -> Result<String, Box<dyn Error>> {
    let registry_bytes = fs::read(&loaded.registry_path)?;
    let schema_bytes = fs::read(&loaded.result_schema_path)?;
    let mut hasher = Sha256::new();
    for bytes in [&loaded.suite_bytes, &registry_bytes, &schema_bytes] {
        hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
        hasher.update(bytes);
    }
    for path in &loaded.contract_paths {
        let bytes = fs::read(path)?;
        hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Bind a selected workload to the hashed workload envelope before execution.
///
/// # Errors
///
/// Returns an error when a workload-class profile would execute an undeclared
/// access pattern or concurrency point.
pub fn validate_workload_selection(
    profile: &ProfileConfig,
    workload: &WorkloadConfig,
) -> Result<(), String> {
    let Some(envelope) = profile.workload_profile.as_ref() else {
        return Ok(());
    };
    let access_pattern = workload
        .parameters
        .get("distribution")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| {
            format!(
                "workload {} must declare distribution for workload evidence",
                workload.id
            )
        })?;
    if access_pattern != envelope.access_pattern {
        return Err(format!(
            "workload {} distribution {access_pattern} does not match workload profile {}",
            workload.id, envelope.access_pattern
        ));
    }
    let concurrency = workload
        .parameters
        .get("concurrent_clients")
        .and_then(toml::Value::as_integer)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            format!(
                "workload {} must declare positive concurrent_clients for workload evidence",
                workload.id
            )
        })?;
    if concurrency == 0 || !envelope.concurrency.contains(&concurrency) {
        return Err(format!(
            "workload {} concurrency {concurrency} is outside declared points {:?}",
            workload.id, envelope.concurrency
        ));
    }
    Ok(())
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
    let supported_statuses: BTreeSet<&str> = [
        "code_complete",
        "verified",
        "evaluating",
        "proposed",
        "future",
    ]
    .into_iter()
    .collect();
    if !supported_statuses.contains(suite.status.as_str()) {
        errors.push(format!(
            "suite status {} must use the canonical proof taxonomy",
            suite.status
        ));
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
        validate_workload_profile(profile_id, profile, suite, &metric_ids, &mut errors);
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
        if !supported_statistic(&lane.statistic) {
            errors.push(format!(
                "lane {} has unsupported statistic {}",
                lane.id, lane.statistic
            ));
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
            if !supported_statistic(&constraint.statistic) {
                errors.push(format!(
                    "lane {} has unsupported constraint statistic {}",
                    lane.id, constraint.statistic
                ));
            }
        }
        let mut comparison_constraint_ids = BTreeSet::new();
        for constraint in &lane.comparison_constraints {
            if !comparison_constraint_ids.insert(constraint.id.as_str()) {
                errors.push(format!(
                    "lane {} repeats comparison constraint {}",
                    lane.id, constraint.id
                ));
            }
            if constraint.id.trim().is_empty()
                || constraint.candidate_metric.trim().is_empty()
                || constraint.control_metric.trim().is_empty()
                || constraint.unit.trim().is_empty()
            {
                errors.push(format!(
                    "lane {} comparison constraints require non-empty id, metrics, and unit",
                    lane.id
                ));
            }
            if !constraint.max_regression_fraction.is_finite()
                || constraint.max_regression_fraction < 0.0
            {
                errors.push(format!(
                    "lane {} comparison constraint {} requires a finite non-negative regression fraction",
                    lane.id, constraint.id
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

#[allow(clippy::too_many_lines)]
fn validate_workload_profile(
    profile_id: &str,
    profile: &ProfileConfig,
    suite: &Suite,
    metric_ids: &BTreeSet<&str>,
    errors: &mut Vec<String>,
) {
    let Some(workload) = profile.workload_profile.as_ref() else {
        if profile.evidence_class == EvidenceClass::Workload {
            errors.push(format!(
                "profile {profile_id} declares workload evidence without a workload_profile"
            ));
        }
        return;
    };
    if profile.evidence_class != EvidenceClass::Workload {
        errors.push(format!(
            "profile {profile_id} has a workload_profile but evidence_class is not workload"
        ));
    }
    if workload.schema_version != 1 {
        errors.push(format!(
            "profile {profile_id} has unsupported workload_profile schema version {}",
            workload.schema_version
        ));
    }
    for (field, value) in [
        ("name", workload.name.as_str()),
        ("dataset", workload.dataset.as_str()),
        ("access_pattern", workload.access_pattern.as_str()),
        ("matched_control", workload.matched_control.as_str()),
    ] {
        if value.trim().is_empty() {
            errors.push(format!(
                "profile {profile_id} workload_profile {field} must not be empty"
            ));
        }
    }
    if !suite.dataset.contains_key(&workload.dataset) {
        errors.push(format!(
            "profile {profile_id} workload_profile references unknown dataset {}",
            workload.dataset
        ));
    }
    if workload.dataset != profile_id {
        errors.push(format!(
            "profile {profile_id} workload_profile dataset {} must match the runner profile id",
            workload.dataset
        ));
    }
    if profile.repeats < 5 {
        errors.push(format!(
            "profile {profile_id} workload evidence requires at least 5 repeats"
        ));
    }
    if workload.operation_mix.is_empty() {
        errors.push(format!(
            "profile {profile_id} workload_profile operation_mix must not be empty"
        ));
    } else {
        let mut total = 0.0;
        for (operation, fraction) in &workload.operation_mix {
            if operation.trim().is_empty()
                || !fraction.is_finite()
                || *fraction <= 0.0
                || *fraction > 1.0
            {
                errors.push(format!(
                    "profile {profile_id} workload_profile operation_mix entries require a non-empty operation and a fraction in (0, 1]"
                ));
            } else {
                total += fraction;
            }
        }
        if (total - 1.0).abs() > 1e-9 {
            errors.push(format!(
                "profile {profile_id} workload_profile operation_mix must sum to 1.0, observed {total}"
            ));
        }
    }
    if workload.concurrency.is_empty()
        || workload.concurrency.contains(&0)
        || workload.concurrency.iter().collect::<BTreeSet<_>>().len() != workload.concurrency.len()
    {
        errors.push(format!(
            "profile {profile_id} workload_profile concurrency must contain unique positive values"
        ));
    }
    for (name, window) in [
        ("warmup", &workload.warmup),
        ("measurement", &workload.measurement),
    ] {
        if !window.amount.is_finite() || window.amount <= 0.0 {
            errors.push(format!(
                "profile {profile_id} workload_profile {name} amount must be finite and positive"
            ));
        }
    }
    if workload.failure_schedule.is_empty()
        || workload
            .failure_schedule
            .iter()
            .any(|value| value.trim().is_empty())
    {
        errors.push(format!(
            "profile {profile_id} workload_profile failure_schedule must be explicit and non-empty"
        ));
    }
    if workload.resource_limits.is_empty()
        || workload
            .resource_limits
            .iter()
            .any(|(name, value)| name.trim().is_empty() || *value == 0)
    {
        errors.push(format!(
            "profile {profile_id} workload_profile resource_limits must contain positive named limits"
        ));
    }
    if workload.required_metrics.is_empty()
        || workload
            .required_metrics
            .iter()
            .any(|value| value.trim().is_empty())
        || workload
            .required_metrics
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != workload.required_metrics.len()
    {
        errors.push(format!(
            "profile {profile_id} workload_profile required_metrics must contain unique non-empty metrics"
        ));
    }
    for metric in &workload.required_metrics {
        if !metric_ids.contains(metric.as_str()) {
            errors.push(format!(
                "profile {profile_id} workload_profile references unknown required metric {metric}"
            ));
        }
    }
}

fn supported_statistic(statistic: &str) -> bool {
    matches!(
        statistic,
        "median" | "p99" | "total" | "minimum" | "maximum" | "per_operation" | "passed"
    )
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
        "okv.eval.batch.id",
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
    use super::{
        load_suite, validate, validate_workload_selection, BudgetKind, CacheState, EvidenceClass,
        WorkloadProfileConfig, WorkloadWindowConfig,
    };
    use std::collections::BTreeMap;
    use std::path::Path;

    #[test]
    fn smoke_suite_and_registry_are_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let suite = load_suite(&root.join("evals/suites/smoke.toml")).expect("valid suite");
        assert_eq!(suite.suite.id, "objectkv-smoke-v1");
        assert!(suite.registry.metrics.len() >= 10);
    }

    #[test]
    fn workload_evidence_fails_closed_without_a_declared_profile() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut loaded = load_suite(&root.join("evals/suites/smoke.toml")).expect("valid suite");
        loaded
            .suite
            .profiles
            .get_mut("dev")
            .expect("dev profile")
            .evidence_class = EvidenceClass::Workload;

        let errors = validate(&loaded);
        assert!(errors.iter().any(|error| error
            .contains("profile dev declares workload evidence without a workload_profile")));
    }

    #[test]
    fn complete_workload_profile_is_accepted() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut loaded = load_suite(&root.join("evals/suites/smoke.toml")).expect("valid suite");
        let profile = loaded.suite.profiles.get_mut("dev").expect("dev profile");
        profile.evidence_class = EvidenceClass::Workload;
        profile.repeats = 5;
        profile.workload_profile = Some(WorkloadProfileConfig {
            schema_version: 1,
            name: "point-read-pressure-v1".to_owned(),
            dataset: "dev".to_owned(),
            access_pattern: "zipf-0.99".to_owned(),
            cache_state: CacheState::Resident,
            matched_control: "direct-rocksdb".to_owned(),
            operation_mix: BTreeMap::from([("point_read".to_owned(), 1.0)]),
            concurrency: vec![1, 8, 32],
            warmup: WorkloadWindowConfig {
                kind: BudgetKind::Operations,
                amount: 100.0,
            },
            measurement: WorkloadWindowConfig {
                kind: BudgetKind::Operations,
                amount: 1_000.0,
            },
            failure_schedule: vec!["none-during-measurement".to_owned()],
            resource_limits: BTreeMap::from([
                ("block_cache_bytes".to_owned(), 64 * 1_024 * 1_024),
                ("local_bytes".to_owned(), 1024 * 1_024 * 1_024),
            ]),
            required_metrics: vec![
                "operation.throughput".to_owned(),
                "operation.duration".to_owned(),
            ],
        });

        assert!(validate(&loaded).is_empty());
    }

    #[test]
    fn selected_workload_must_fit_hashed_distribution_and_concurrency() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let loaded =
            load_suite(&root.join("evals/suites/single-range-native-concurrency-admission.toml"))
                .expect("valid workload suite");
        let profile = loaded
            .suite
            .profiles
            .get("gcp-r0-nvme")
            .expect("GCP workload profile");
        let declared = loaded
            .suite
            .workloads
            .iter()
            .find(|workload| workload.id == "native-snapshot-concurrency-32")
            .expect("declared concurrency workload");
        let undeclared = loaded
            .suite
            .workloads
            .iter()
            .find(|workload| workload.id == "native-snapshot-concurrency-1")
            .expect("undeclared concurrency workload");

        validate_workload_selection(profile, declared).expect("declared workload must run");
        let error = validate_workload_selection(profile, undeclared)
            .expect_err("undeclared concurrency must fail");
        assert!(error.contains("concurrency 1 is outside declared points"));
    }
}
