use crate::config::{
    contract_hash, load_suite, ComparisonConstraintConfig, Direction, LoadedSuite,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize)]
pub struct EvalProgram {
    pub schema_version: u32,
    pub id: String,
    pub status: ProgramGateStatus,
    pub spec: String,
    pub decision_log: String,
    pub comparison_schema: String,
    pub scenario: Option<String>,
    #[serde(default)]
    pub phases: Vec<ProgramPhase>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvalScenario {
    pub schema_version: u32,
    pub id: String,
    pub description: String,
    pub generator: String,
    #[serde(default)]
    pub seeds: Vec<u64>,
    #[serde(default)]
    pub initial_artifacts: Vec<String>,
    #[serde(default)]
    pub surfaces: Vec<ScenarioSurface>,
    #[serde(default)]
    pub checkpoints: Vec<ScenarioCheckpoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScenarioSurface {
    pub id: String,
    pub name: String,
    pub question: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScenarioCheckpoint {
    pub id: String,
    pub name: String,
    pub surface: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProgramPhase {
    pub id: String,
    pub name: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub gates: Vec<ProgramGate>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProgramGate {
    pub id: String,
    pub status: ProgramGateStatus,
    pub kind: ProgramGateKind,
    pub claim: String,
    pub falsifier: String,
    #[serde(default)]
    pub requirement_ids: Vec<String>,
    pub suite: String,
    pub profile: String,
    pub workload: String,
    pub backend: String,
    pub lane: String,
    pub checkpoint: Option<String>,
    #[serde(default)]
    pub negative_controls: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    pub control: Option<ProgramControl>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProgramGateStatus {
    CodeComplete,
    Verified,
    Evaluating,
    Proposed,
    Future,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProgramGateKind {
    Correctness,
    Performance,
    Compatibility,
    Operational,
    Economics,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProgramControl {
    pub id: String,
    pub suite: String,
    pub profile: String,
    pub workload: String,
    pub backend: String,
    pub lane: String,
}

#[derive(Debug)]
pub struct LoadedProgram {
    pub program_path: PathBuf,
    pub program: EvalProgram,
    pub spec_path: PathBuf,
    pub decision_log_path: PathBuf,
    pub comparison_schema_path: PathBuf,
    pub scenario_path: Option<PathBuf>,
    pub scenario: Option<EvalScenario>,
}

#[derive(Debug)]
pub enum ProgramError {
    Io { path: PathBuf, message: String },
    Parse { path: PathBuf, message: String },
    Invalid(Vec<String>),
}

impl Display for ProgramError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, message } | Self::Parse { path, message } => {
                write!(formatter, "{}: {message}", path.display())
            }
            Self::Invalid(errors) => {
                write!(formatter, "invalid eval program: {}", errors.join("; "))
            }
        }
    }
}

impl Error for ProgramError {}

#[derive(Debug, Serialize)]
pub struct ProgramPlan {
    pub program: String,
    pub status: ProgramGateStatus,
    pub spec: String,
    pub decision_log: String,
    pub comparison_schema: String,
    pub scenario: Option<EvalScenario>,
    pub phases: Vec<ProgramPhasePlan>,
}

#[derive(Debug, Serialize)]
pub struct ProgramPhasePlan {
    pub id: String,
    pub name: String,
    pub objective: String,
    pub depends_on: Vec<String>,
    pub gates: Vec<ProgramGatePlan>,
}

#[derive(Debug, Serialize)]
pub struct ProgramGatePlan {
    pub id: String,
    pub status: ProgramGateStatus,
    pub kind: ProgramGateKind,
    pub claim: String,
    pub falsifier: String,
    pub requirement_ids: Vec<String>,
    pub suite: String,
    pub suite_hash: String,
    pub profile: String,
    pub workload: String,
    pub backend: String,
    pub lane: String,
    pub checkpoint: Option<String>,
    pub surface: Option<String>,
    pub primary_metric: String,
    pub primary_metric_otel_name: String,
    pub statistic: String,
    pub direction: Direction,
    pub practical_improvement_fraction: f64,
    pub comparison_constraints: Vec<ComparisonConstraintConfig>,
    pub configured_repeats: u32,
    pub negative_controls: Vec<String>,
    pub control: Option<ProgramControlPlan>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ProgramControlPlan {
    pub id: String,
    pub suite: String,
    pub suite_hash: String,
    pub profile: String,
    pub workload: String,
    pub backend: String,
    pub lane: String,
    pub primary_metric: String,
    pub primary_metric_otel_name: String,
    pub statistic: String,
    pub direction: Direction,
    pub practical_improvement_fraction: f64,
    pub configured_repeats: u32,
}

/// Load and validate a product-level evaluation program.
///
/// # Errors
///
/// Returns all structural and cross-reference errors together when possible.
pub fn load_program(path: &Path) -> Result<LoadedProgram, ProgramError> {
    let bytes = read(path)?;
    let program: EvalProgram =
        toml::from_str(
            std::str::from_utf8(&bytes).map_err(|error| ProgramError::Parse {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?,
        )
        .map_err(|error| ProgramError::Parse {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let scenario_path = program.scenario.as_ref().map(|value| base.join(value));
    let scenario = scenario_path
        .as_ref()
        .map(|scenario_path| {
            let bytes = read(scenario_path)?;
            toml::from_str(
                std::str::from_utf8(&bytes).map_err(|error| ProgramError::Parse {
                    path: scenario_path.clone(),
                    message: error.to_string(),
                })?,
            )
            .map_err(|error| ProgramError::Parse {
                path: scenario_path.clone(),
                message: error.to_string(),
            })
        })
        .transpose()?;
    let loaded = LoadedProgram {
        program_path: path.to_path_buf(),
        spec_path: base.join(&program.spec),
        decision_log_path: base.join(&program.decision_log),
        comparison_schema_path: base.join(&program.comparison_schema),
        scenario_path,
        scenario,
        program,
    };
    let errors = validate_program(&loaded);
    if errors.is_empty() {
        Ok(loaded)
    } else {
        Err(ProgramError::Invalid(errors))
    }
}

/// Resolve a validated program to the exact suites, lanes, and metrics it names.
///
/// # Errors
///
/// Returns an error if a referenced suite changes after program validation.
pub fn plan_program(loaded: &LoadedProgram) -> Result<ProgramPlan, ProgramError> {
    let base = loaded
        .program_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let mut suite_cache = BTreeMap::new();
    let mut phases = Vec::with_capacity(loaded.program.phases.len());
    for phase in &loaded.program.phases {
        let mut gates = Vec::with_capacity(phase.gates.len());
        for gate in &phase.gates {
            let suite_path = base.join(&gate.suite);
            let (
                suite_id,
                suite_hash,
                primary_metric,
                primary_metric_otel_name,
                statistic,
                direction,
                practical_improvement_fraction,
                comparison_constraints,
                configured_repeats,
            ) = {
                let suite = cached_suite(&mut suite_cache, &suite_path)?;
                let lane = resolve_lane(suite, &gate.lane).ok_or_else(|| {
                    ProgramError::Invalid(vec![format!(
                        "gate {} references missing lane {}",
                        gate.id, gate.lane
                    )])
                })?;
                let profile = suite.suite.profiles.get(&gate.profile).ok_or_else(|| {
                    ProgramError::Invalid(vec![format!(
                        "gate {} references missing profile {}",
                        gate.id, gate.profile
                    )])
                })?;
                let primary_metric_otel_name = suite
                    .registry
                    .metrics
                    .iter()
                    .find(|metric| metric.id == lane.primary_metric)
                    .map(|metric| metric.otel_name.clone())
                    .ok_or_else(|| {
                        ProgramError::Invalid(vec![format!(
                            "gate {} lane {} references missing metric {}",
                            gate.id, gate.lane, lane.primary_metric
                        )])
                    })?;
                let suite_hash = contract_hash(suite).map_err(|error| {
                    ProgramError::Invalid(vec![format!(
                        "gate {} cannot hash suite contract: {error}",
                        gate.id
                    )])
                })?;
                (
                    suite.suite.id.clone(),
                    suite_hash,
                    lane.primary_metric.clone(),
                    primary_metric_otel_name,
                    lane.statistic.clone(),
                    lane.direction,
                    lane.practical_improvement_fraction,
                    lane.comparison_constraints.clone(),
                    profile.repeats,
                )
            };
            let control = gate
                .control
                .as_ref()
                .map(|control| {
                    let control_path = base.join(&control.suite);
                    let control_suite = cached_suite(&mut suite_cache, &control_path)?;
                    let control_lane =
                        resolve_lane(control_suite, &control.lane).ok_or_else(|| {
                            ProgramError::Invalid(vec![format!(
                                "control {} references missing lane {}",
                                control.id, control.lane
                            )])
                        })?;
                    let control_profile = control_suite
                        .suite
                        .profiles
                        .get(&control.profile)
                        .ok_or_else(|| {
                            ProgramError::Invalid(vec![format!(
                                "control {} references missing profile {}",
                                control.id, control.profile
                            )])
                        })?;
                    let control_metric_otel_name = control_suite
                        .registry
                        .metrics
                        .iter()
                        .find(|metric| metric.id == control_lane.primary_metric)
                        .map(|metric| metric.otel_name.clone())
                        .ok_or_else(|| {
                            ProgramError::Invalid(vec![format!(
                                "control {} lane {} references missing metric {}",
                                control.id, control.lane, control_lane.primary_metric
                            )])
                        })?;
                    let control_suite_hash = contract_hash(control_suite).map_err(|error| {
                        ProgramError::Invalid(vec![format!(
                            "control {} cannot hash suite contract: {error}",
                            control.id
                        )])
                    })?;
                    Ok(ProgramControlPlan {
                        id: control.id.clone(),
                        suite: control_suite.suite.id.clone(),
                        suite_hash: control_suite_hash,
                        profile: control.profile.clone(),
                        workload: control.workload.clone(),
                        backend: control.backend.clone(),
                        lane: control.lane.clone(),
                        primary_metric: control_lane.primary_metric.clone(),
                        primary_metric_otel_name: control_metric_otel_name,
                        statistic: control_lane.statistic.clone(),
                        direction: control_lane.direction,
                        practical_improvement_fraction: control_lane.practical_improvement_fraction,
                        configured_repeats: control_profile.repeats,
                    })
                })
                .transpose()?;
            gates.push(ProgramGatePlan {
                id: gate.id.clone(),
                status: gate.status,
                kind: gate.kind,
                claim: gate.claim.clone(),
                falsifier: gate.falsifier.clone(),
                requirement_ids: gate.requirement_ids.clone(),
                suite: suite_id,
                suite_hash,
                profile: gate.profile.clone(),
                workload: gate.workload.clone(),
                backend: gate.backend.clone(),
                lane: gate.lane.clone(),
                checkpoint: gate.checkpoint.clone(),
                surface: gate.checkpoint.as_ref().and_then(|checkpoint| {
                    loaded.scenario.as_ref().and_then(|scenario| {
                        scenario
                            .checkpoints
                            .iter()
                            .find(|candidate| candidate.id == *checkpoint)
                            .map(|candidate| candidate.surface.clone())
                    })
                }),
                primary_metric,
                primary_metric_otel_name,
                statistic,
                direction,
                practical_improvement_fraction,
                comparison_constraints,
                configured_repeats,
                negative_controls: gate.negative_controls.clone(),
                control,
                evidence: gate.evidence.clone(),
            });
        }
        phases.push(ProgramPhasePlan {
            id: phase.id.clone(),
            name: phase.name.clone(),
            objective: phase.objective.clone(),
            depends_on: phase.depends_on.clone(),
            gates,
        });
    }
    Ok(ProgramPlan {
        program: loaded.program.id.clone(),
        status: loaded.program.status,
        spec: loaded.spec_path.display().to_string(),
        decision_log: loaded.decision_log_path.display().to_string(),
        comparison_schema: loaded.comparison_schema_path.display().to_string(),
        scenario: loaded.scenario.clone(),
        phases,
    })
}

fn read(path: &Path) -> Result<Vec<u8>, ProgramError> {
    fs::read(path).map_err(|error| ProgramError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

#[allow(clippy::too_many_lines)]
fn validate_program(loaded: &LoadedProgram) -> Vec<String> {
    let mut errors = Vec::new();
    let program = &loaded.program;
    if program.schema_version != 1 {
        errors.push(format!(
            "unsupported program schema version {}",
            program.schema_version
        ));
    }
    require_text("program id", &program.id, &mut errors);
    if !loaded.spec_path.is_file() {
        errors.push(format!(
            "program spec does not exist: {}",
            loaded.spec_path.display()
        ));
    }
    if !loaded.decision_log_path.is_file() {
        errors.push(format!(
            "program decision log does not exist: {}",
            loaded.decision_log_path.display()
        ));
    }
    if !loaded.comparison_schema_path.is_file() {
        errors.push(format!(
            "program comparison schema does not exist: {}",
            loaded.comparison_schema_path.display()
        ));
    }
    if program.phases.is_empty() {
        errors.push("program must define at least one phase".to_owned());
    }

    let spec = fs::read_to_string(&loaded.spec_path).unwrap_or_default();
    if let Some(scenario) = &loaded.scenario {
        validate_scenario(scenario, &mut errors);
    }

    let mut phase_ids = BTreeSet::new();
    let mut prior_phase_ids = BTreeSet::new();
    let mut gate_ids = BTreeSet::new();
    let base = loaded
        .program_path
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let mut checkpoint_phases: BTreeMap<String, usize> = BTreeMap::new();
    for (phase_index, phase) in program.phases.iter().enumerate() {
        if !phase_ids.insert(phase.id.clone()) {
            errors.push(format!("duplicate phase id {}", phase.id));
        }
        require_text("phase id", &phase.id, &mut errors);
        require_text(
            &format!("phase {} name", phase.id),
            &phase.name,
            &mut errors,
        );
        require_text(
            &format!("phase {} objective", phase.id),
            &phase.objective,
            &mut errors,
        );
        for dependency in &phase.depends_on {
            if !prior_phase_ids.contains(dependency) {
                errors.push(format!(
                    "phase {} dependency {} must name an earlier phase",
                    phase.id, dependency
                ));
            }
        }
        if phase.gates.is_empty() {
            errors.push(format!("phase {} must define at least one gate", phase.id));
        }
        for gate in &phase.gates {
            validate_gate(gate, &phase.id, base, &spec, &mut gate_ids, &mut errors);
            if let Some(scenario) = &loaded.scenario {
                match &gate.checkpoint {
                    Some(checkpoint)
                        if scenario
                            .checkpoints
                            .iter()
                            .any(|candidate| candidate.id == *checkpoint) =>
                    {
                        checkpoint_phases
                            .entry(checkpoint.clone())
                            .and_modify(|prior| *prior = (*prior).min(phase_index))
                            .or_insert(phase_index);
                    }
                    Some(checkpoint) => errors.push(format!(
                        "gate {} references checkpoint {} absent from scenario {}",
                        gate.id, checkpoint, scenario.id
                    )),
                    None => errors.push(format!(
                        "gate {} must name a checkpoint when program {} has a scenario",
                        gate.id, program.id
                    )),
                }
            }
        }
        prior_phase_ids.insert(phase.id.clone());
    }
    if let Some(scenario) = &loaded.scenario {
        validate_scenario_coverage(scenario, &checkpoint_phases, &mut errors);
    }
    errors
}

#[allow(clippy::too_many_lines)]
fn validate_scenario(scenario: &EvalScenario, errors: &mut Vec<String>) {
    if scenario.schema_version != 1 {
        errors.push(format!(
            "unsupported scenario schema version {}",
            scenario.schema_version
        ));
    }
    require_text("scenario id", &scenario.id, errors);
    require_text("scenario description", &scenario.description, errors);
    require_text("scenario generator", &scenario.generator, errors);
    if scenario.seeds.is_empty() {
        errors.push(format!(
            "scenario {} must define at least one seed",
            scenario.id
        ));
    }
    if scenario.surfaces.is_empty() {
        errors.push(format!(
            "scenario {} must define at least one surface",
            scenario.id
        ));
    }
    if scenario.checkpoints.is_empty() {
        errors.push(format!(
            "scenario {} must define at least one checkpoint",
            scenario.id
        ));
    }

    let mut seed_ids = BTreeSet::new();
    for seed in &scenario.seeds {
        if !seed_ids.insert(*seed) {
            errors.push(format!("scenario {} repeats seed {seed}", scenario.id));
        }
    }
    let mut surface_ids = BTreeSet::new();
    for surface in &scenario.surfaces {
        require_text("scenario surface id", &surface.id, errors);
        require_text(
            &format!("surface {} name", surface.id),
            &surface.name,
            errors,
        );
        require_text(
            &format!("surface {} question", surface.id),
            &surface.question,
            errors,
        );
        if !surface_ids.insert(surface.id.clone()) {
            errors.push(format!("duplicate scenario surface {}", surface.id));
        }
    }

    let mut checkpoint_ids = BTreeSet::new();
    let mut produced_artifacts: BTreeMap<String, String> = BTreeMap::new();
    let initial_artifacts: BTreeSet<String> = scenario.initial_artifacts.iter().cloned().collect();
    for checkpoint in &scenario.checkpoints {
        require_text("scenario checkpoint id", &checkpoint.id, errors);
        require_text(
            &format!("checkpoint {} name", checkpoint.id),
            &checkpoint.name,
            errors,
        );
        require_text(
            &format!("checkpoint {} objective", checkpoint.id),
            &checkpoint.objective,
            errors,
        );
        if !checkpoint_ids.insert(checkpoint.id.clone()) {
            errors.push(format!("duplicate scenario checkpoint {}", checkpoint.id));
        }
        if !surface_ids.contains(&checkpoint.surface) {
            errors.push(format!(
                "checkpoint {} references unknown surface {}",
                checkpoint.id, checkpoint.surface
            ));
        }
        for dependency in &checkpoint.depends_on {
            if !checkpoint_ids.contains(dependency) {
                errors.push(format!(
                    "checkpoint {} dependency {} must name an earlier checkpoint",
                    checkpoint.id, dependency
                ));
            }
        }
        let mut available = initial_artifacts.clone();
        collect_dependency_artifacts(checkpoint, scenario, &produced_artifacts, &mut available);
        for input in &checkpoint.inputs {
            if !available.contains(input) {
                errors.push(format!(
                    "checkpoint {} input {} is not produced by its dependencies",
                    checkpoint.id, input
                ));
            }
        }
        if checkpoint.outputs.is_empty() {
            errors.push(format!(
                "checkpoint {} must produce at least one artifact",
                checkpoint.id
            ));
        }
        for output in &checkpoint.outputs {
            if initial_artifacts.contains(output) {
                errors.push(format!(
                    "checkpoint {} output {} duplicates an initial artifact",
                    checkpoint.id, output
                ));
            }
            if let Some(producer) = produced_artifacts.insert(output.clone(), checkpoint.id.clone())
            {
                errors.push(format!(
                    "artifact {} is produced by both {} and {}",
                    output, producer, checkpoint.id
                ));
            }
        }
    }
    for surface in &surface_ids {
        if !scenario
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.surface == *surface)
        {
            errors.push(format!("scenario surface {surface} has no checkpoint"));
        }
    }
}

fn collect_dependency_artifacts(
    checkpoint: &ScenarioCheckpoint,
    scenario: &EvalScenario,
    produced_artifacts: &BTreeMap<String, String>,
    available: &mut BTreeSet<String>,
) {
    let mut pending = checkpoint.depends_on.clone();
    let mut visited = BTreeSet::new();
    while let Some(dependency) = pending.pop() {
        if !visited.insert(dependency.clone()) {
            continue;
        }
        if let Some(dependency_checkpoint) = scenario
            .checkpoints
            .iter()
            .find(|candidate| candidate.id == dependency)
        {
            for output in &dependency_checkpoint.outputs {
                if produced_artifacts.contains_key(output) {
                    available.insert(output.clone());
                }
            }
            pending.extend(dependency_checkpoint.depends_on.iter().cloned());
        }
    }
}

fn validate_scenario_coverage(
    scenario: &EvalScenario,
    checkpoint_phases: &BTreeMap<String, usize>,
    errors: &mut Vec<String>,
) {
    for checkpoint in &scenario.checkpoints {
        let Some(phase_index) = checkpoint_phases.get(&checkpoint.id) else {
            errors.push(format!(
                "scenario checkpoint {} is not covered by a program gate",
                checkpoint.id
            ));
            continue;
        };
        for dependency in &checkpoint.depends_on {
            if let Some(dependency_phase) = checkpoint_phases.get(dependency) {
                if dependency_phase > phase_index {
                    errors.push(format!(
                        "checkpoint {} is gated before dependency {}",
                        checkpoint.id, dependency
                    ));
                }
            }
        }
    }
}

fn validate_gate(
    gate: &ProgramGate,
    phase_id: &str,
    base: &Path,
    spec: &str,
    gate_ids: &mut BTreeSet<String>,
    errors: &mut Vec<String>,
) {
    if !gate_ids.insert(gate.id.clone()) {
        errors.push(format!("duplicate gate id {}", gate.id));
    }
    require_text("gate id", &gate.id, errors);
    require_text(&format!("gate {} claim", gate.id), &gate.claim, errors);
    require_text(
        &format!("gate {} falsifier", gate.id),
        &gate.falsifier,
        errors,
    );
    require_text(&format!("gate {} backend", gate.id), &gate.backend, errors);
    if gate.requirement_ids.is_empty() {
        errors.push(format!(
            "gate {} in phase {phase_id} must cite at least one requirement",
            gate.id
        ));
    }
    let mut requirement_ids = BTreeSet::new();
    for requirement_id in &gate.requirement_ids {
        if !requirement_ids.insert(requirement_id.as_str()) {
            errors.push(format!(
                "gate {} repeats requirement {}",
                gate.id, requirement_id
            ));
        }
        if !spec.contains(&format!("| {requirement_id} |")) {
            errors.push(format!(
                "gate {} references requirement {} absent from the spec",
                gate.id, requirement_id
            ));
        }
    }
    if gate.status == ProgramGateStatus::Verified && gate.evidence.is_empty() {
        errors.push(format!(
            "verified gate {} must cite at least one evidence receipt",
            gate.id
        ));
    }
    if gate.kind == ProgramGateKind::Correctness && gate.negative_controls.is_empty() {
        errors.push(format!(
            "correctness gate {} must name at least one negative control",
            gate.id
        ));
    }
    if matches!(
        gate.kind,
        ProgramGateKind::Performance | ProgramGateKind::Economics
    ) && gate.control.is_none()
    {
        errors.push(format!(
            "{:?} gate {} must name a paired control",
            gate.kind, gate.id
        ));
    }

    let suite_path = base.join(&gate.suite);
    match load_suite(&suite_path) {
        Ok(suite) => validate_gate_suite_references(gate, &suite, errors),
        Err(error) => errors.push(format!(
            "gate {} suite {} is invalid: {error}",
            gate.id,
            suite_path.display()
        )),
    }
    if let Some(control) = &gate.control {
        validate_control(control, base, errors);
        validate_control_compatibility(gate, control, base, errors);
    }
}

fn validate_control_compatibility(
    gate: &ProgramGate,
    control: &ProgramControl,
    base: &Path,
    errors: &mut Vec<String>,
) {
    let Ok(candidate_suite) = load_suite(&base.join(&gate.suite)) else {
        return;
    };
    let Ok(control_suite) = load_suite(&base.join(&control.suite)) else {
        return;
    };
    let (Some(candidate_lane), Some(control_lane)) = (
        resolve_lane(&candidate_suite, &gate.lane),
        resolve_lane(&control_suite, &control.lane),
    ) else {
        return;
    };
    if candidate_lane.primary_metric != control_lane.primary_metric {
        errors.push(format!(
            "gate {} and control {} use different primary metrics: {} versus {}",
            gate.id, control.id, candidate_lane.primary_metric, control_lane.primary_metric
        ));
    }
    if candidate_lane.statistic != control_lane.statistic {
        errors.push(format!(
            "gate {} and control {} use different statistics: {} versus {}",
            gate.id, control.id, candidate_lane.statistic, control_lane.statistic
        ));
    }
    if std::mem::discriminant(&candidate_lane.direction)
        != std::mem::discriminant(&control_lane.direction)
    {
        errors.push(format!(
            "gate {} and control {} use different metric directions",
            gate.id, control.id
        ));
    }
    if gate.kind == ProgramGateKind::Performance
        && matches!(
            gate.status,
            ProgramGateStatus::Evaluating | ProgramGateStatus::Verified
        )
    {
        for (label, suite, profile_id) in [
            ("candidate", &candidate_suite, &gate.profile),
            ("control", &control_suite, &control.profile),
        ] {
            if let Some(profile) = suite.suite.profiles.get(profile_id) {
                if profile.repeats < 5 {
                    errors.push(format!(
                        "evaluating performance gate {} {label} profile {} has {} repeats; at least 5 are required",
                        gate.id, profile_id, profile.repeats
                    ));
                }
            }
        }
    }
}

fn validate_gate_suite_references(
    gate: &ProgramGate,
    suite: &LoadedSuite,
    errors: &mut Vec<String>,
) {
    if !suite.suite.profiles.contains_key(&gate.profile) {
        errors.push(format!(
            "gate {} references profile {} absent from suite {}",
            gate.id, gate.profile, suite.suite.id
        ));
    }
    let workload = suite
        .suite
        .workloads
        .iter()
        .find(|workload| workload.id == gate.workload);
    match workload {
        Some(workload) if workload.lane != gate.lane => errors.push(format!(
            "gate {} expects lane {} but workload {} belongs to {}",
            gate.id, gate.lane, gate.workload, workload.lane
        )),
        Some(_) => {}
        None => errors.push(format!(
            "gate {} references workload {} absent from suite {}",
            gate.id, gate.workload, suite.suite.id
        )),
    }
    if resolve_lane(suite, &gate.lane).is_none() {
        errors.push(format!(
            "gate {} references lane {} absent from suite {}",
            gate.id, gate.lane, suite.suite.id
        ));
    }
    for negative_control in &gate.negative_controls {
        match suite
            .suite
            .workloads
            .iter()
            .find(|workload| workload.id == *negative_control)
        {
            Some(workload)
                if workload
                    .parameters
                    .get("negative_control")
                    .and_then(toml::Value::as_str)
                    .is_some() => {}
            Some(_) => errors.push(format!(
                "gate {} negative control {} is not marked negative_control",
                gate.id, negative_control
            )),
            None => errors.push(format!(
                "gate {} references negative control {} absent from suite {}",
                gate.id, negative_control, suite.suite.id
            )),
        }
    }
}

fn validate_control(control: &ProgramControl, base: &Path, errors: &mut Vec<String>) {
    require_text("control id", &control.id, errors);
    require_text(
        &format!("control {} backend", control.id),
        &control.backend,
        errors,
    );
    let suite_path = base.join(&control.suite);
    match load_suite(&suite_path) {
        Ok(suite) => {
            if !suite.suite.profiles.contains_key(&control.profile) {
                errors.push(format!(
                    "control {} references profile {} absent from suite {}",
                    control.id, control.profile, suite.suite.id
                ));
            }
            match suite
                .suite
                .workloads
                .iter()
                .find(|workload| workload.id == control.workload)
            {
                Some(workload) if workload.lane != control.lane => errors.push(format!(
                    "control {} expects lane {} but workload {} belongs to {}",
                    control.id, control.lane, control.workload, workload.lane
                )),
                Some(_) => {}
                None => errors.push(format!(
                    "control {} references workload {} absent from suite {}",
                    control.id, control.workload, suite.suite.id
                )),
            }
            if resolve_lane(&suite, &control.lane).is_none() {
                errors.push(format!(
                    "control {} references lane {} absent from suite {}",
                    control.id, control.lane, suite.suite.id
                ));
            }
        }
        Err(error) => errors.push(format!(
            "control {} suite {} is invalid: {error}",
            control.id,
            suite_path.display()
        )),
    }
}

fn resolve_lane<'a>(
    suite: &'a LoadedSuite,
    lane_id: &str,
) -> Option<&'a crate::config::LaneConfig> {
    suite.suite.lanes.iter().find(|lane| lane.id == lane_id)
}

fn cached_suite<'a>(
    cache: &'a mut BTreeMap<PathBuf, LoadedSuite>,
    path: &Path,
) -> Result<&'a LoadedSuite, ProgramError> {
    if !cache.contains_key(path) {
        let loaded = load_suite(path).map_err(|error| {
            ProgramError::Invalid(vec![format!(
                "suite {} is invalid: {error}",
                path.display()
            )])
        })?;
        cache.insert(path.to_path_buf(), loaded);
    }
    Ok(cache.get(path).expect("suite inserted above"))
}

fn require_text(label: &str, value: &str, errors: &mut Vec<String>) {
    if value.trim().is_empty() {
        errors.push(format!("{label} must not be empty"));
    }
}

#[cfg(test)]
mod tests {
    use super::{load_program, plan_program, validate_scenario, EvalScenario};
    use std::path::Path;

    #[test]
    fn playground_v3_scenario_has_a_valid_dependency_graph() {
        let scenario: EvalScenario = toml::from_str(include_str!(
            "../../../evals/scenarios/objectkv-playground-golden-path-v3.toml"
        ))
        .expect("parse playground v3 scenario");
        let mut errors = Vec::new();
        validate_scenario(&scenario, &mut errors);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(scenario.checkpoints.len(), 6);
    }

    #[test]
    fn product_thesis_program_resolves_requirements_and_suites() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let loaded = load_program(&root.join("evals/programs/objectkv-product-thesis-v1.toml"))
            .expect("valid product thesis program");
        let plan = plan_program(&loaded).expect("resolved program plan");
        assert_eq!(plan.program, "objectkv-product-thesis-v1");
        assert!(plan.phases.len() >= 5);
        assert!(plan.phases.iter().all(|phase| !phase.gates.is_empty()));
    }

    #[test]
    fn golden_path_covers_every_scenario_checkpoint_and_surface() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let loaded = load_program(&root.join("evals/programs/objectkv-golden-path-v1.toml"))
            .expect("valid golden-path program");
        let plan = plan_program(&loaded).expect("resolved golden-path plan");
        let scenario = plan.scenario.expect("golden path has a scenario");
        let checkpoints: std::collections::BTreeSet<_> = plan
            .phases
            .iter()
            .flat_map(|phase| phase.gates.iter())
            .filter_map(|gate| gate.checkpoint.as_deref())
            .collect();
        let surfaces: std::collections::BTreeSet<_> = plan
            .phases
            .iter()
            .flat_map(|phase| phase.gates.iter())
            .filter_map(|gate| gate.surface.as_deref())
            .collect();
        assert_eq!(checkpoints.len(), scenario.checkpoints.len());
        assert_eq!(surfaces.len(), scenario.surfaces.len());
    }
}
