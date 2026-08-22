use chrono::Utc;
use clap::{Parser, Subcommand};
use okv_eval::config::{load_suite, BudgetKind, LoadedSuite, WorkloadConfig};
use okv_eval::result::{
    median, median_absolute_deviation, validate_result, BudgetResult, EvalResult, GateStatus,
    HardGateResult, PrimaryMetricResult, ProfileIdentity, Verdict,
};
use okv_eval::telemetry::{RunResource, Telemetry};
use okv_model::{ApplyOutcome, CommitBatch, Model, Mutation, Version};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Instant;
use tracing::{info, info_span};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "okv-eval", about = "objectKV evaluation runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run the in-memory model smoke check without suite orchestration.
    Smoke,
    /// Validate a suite, metric registry, and result schema as one contract.
    ValidateSuite {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
    },
    /// List the metrics exposed by a validated suite.
    ListMetrics {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
    },
    /// Print the deterministic execution plan without running a workload.
    Plan {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
        #[arg(long, default_value = "dev")]
        profile: String,
    },
    /// Run one registered workload and emit a schema-validated result.
    Run {
        #[arg(default_value = "evals/suites/smoke.toml")]
        suite: PathBuf,
        #[arg(long, default_value = "dev")]
        profile: String,
        #[arg(long, default_value = "model-smoke")]
        workload: String,
        #[arg(long, default_value = "model")]
        backend: String,
        #[arg(long)]
        output: Option<PathBuf>,
        /// Permit a diagnostic run whose source tree is not reproducible.
        #[arg(long)]
        allow_dirty: bool,
    },
}

fn main() -> ExitCode {
    match execute(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("okv-eval: {error}");
            ExitCode::FAILURE
        }
    }
}

fn execute(cli: Cli) -> Result<(), Box<dyn Error>> {
    match cli.command {
        Commands::Smoke => {
            run_model_smoke()?;
            println!("{{\"schema_version\":1,\"suite\":\"smoke\",\"status\":\"pass\",\"correctness_failures\":0}}");
        }
        Commands::ValidateSuite { suite } => {
            let loaded = load_suite(&suite)?;
            println!(
                "validated suite {} with {} metrics, {} lanes, {} workloads, and {} profiles",
                loaded.suite.id,
                loaded.registry.metrics.len(),
                loaded.suite.lanes.len(),
                loaded.suite.workloads.len(),
                loaded.suite.profiles.len()
            );
        }
        Commands::ListMetrics { suite } => list_metrics(&load_suite(&suite)?),
        Commands::Plan { suite, profile } => print_plan(&load_suite(&suite)?, &profile)?,
        Commands::Run {
            suite,
            profile,
            workload,
            backend,
            output,
            allow_dirty,
        } => run_suite(
            &suite,
            &profile,
            &workload,
            &backend,
            output.as_deref(),
            allow_dirty,
        )?,
    }
    Ok(())
}

fn list_metrics(loaded: &LoadedSuite) {
    for metric in &loaded.registry.metrics {
        println!(
            "{}\t{}\t{:?}\t{}",
            metric.id, metric.otel_name, metric.kind, metric.unit
        );
    }
}

#[derive(Serialize)]
struct ExecutionPlan<'a> {
    suite: &'a str,
    status: &'a str,
    profile: &'a str,
    repeats: u32,
    budget_kind: BudgetKind,
    budget_limit: f64,
    workloads: Vec<&'a str>,
    lanes: Vec<&'a str>,
    required_signals: &'a [String],
    telemetry_required: bool,
}

fn print_plan(loaded: &LoadedSuite, profile_id: &str) -> Result<(), Box<dyn Error>> {
    let profile = loaded
        .suite
        .profiles
        .get(profile_id)
        .ok_or_else(|| format!("unknown profile {profile_id}"))?;
    let plan = ExecutionPlan {
        suite: &loaded.suite.id,
        status: &loaded.suite.status,
        profile: profile_id,
        repeats: profile.repeats,
        budget_kind: profile.budget_kind,
        budget_limit: profile.budget_limit,
        workloads: loaded
            .suite
            .workloads
            .iter()
            .map(|workload| workload.id.as_str())
            .collect(),
        lanes: loaded
            .suite
            .lanes
            .iter()
            .map(|lane| lane.id.as_str())
            .collect(),
        required_signals: &loaded.suite.telemetry.required_signals,
        telemetry_required: loaded
            .suite
            .telemetry
            .required_for_profiles
            .iter()
            .any(|required| required == profile_id),
    };
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_suite(
    suite_path: &Path,
    profile_id: &str,
    workload_id: &str,
    backend: &str,
    output: Option<&Path>,
    allow_dirty: bool,
) -> Result<(), Box<dyn Error>> {
    let source_dirty = git_is_dirty()?;
    if !allow_dirty && source_dirty {
        return Err("source tree is dirty; commit the candidate or pass --allow-dirty for a non-comparable diagnostic run".into());
    }
    let loaded = load_suite(suite_path)?;
    let profile = loaded
        .suite
        .profiles
        .get(profile_id)
        .ok_or_else(|| format!("unknown profile {profile_id}"))?;
    let workload = loaded
        .suite
        .workloads
        .iter()
        .find(|candidate| candidate.id == workload_id)
        .ok_or_else(|| format!("unknown workload {workload_id}"))?;
    let lane = loaded
        .suite
        .lanes
        .iter()
        .find(|candidate| candidate.id == workload.lane)
        .ok_or_else(|| format!("unknown lane {}", workload.lane))?;
    let primary_definition = loaded
        .registry
        .metrics
        .iter()
        .find(|metric| metric.id == lane.primary_metric)
        .ok_or_else(|| format!("unknown primary metric {}", lane.primary_metric))?;

    let run_id = Uuid::new_v4().to_string();
    let mut candidate_commit = git_revision("HEAD")
        .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
    if source_dirty {
        candidate_commit.push_str("+dirty");
    }
    let parent_commit = git_revision("HEAD^")
        .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
    let suite_hash = contract_hash(&loaded)?;
    let profile_hash = sha256(toml::to_string(profile)?.as_bytes());
    let lockfile_hash = sha256(&fs::read("Cargo.lock")?);
    let resource = RunResource {
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        environment: profile_id.to_owned(),
        run_id: run_id.clone(),
        suite_id: loaded.suite.id.clone(),
        suite_hash: suite_hash.clone(),
        profile_id: profile_id.to_owned(),
        profile_hash: profile_hash.clone(),
        candidate_commit: candidate_commit.clone(),
        backend: backend.to_owned(),
    };
    let telemetry = Telemetry::init(&loaded.suite.telemetry, profile_id, &resource)?;
    let mut recorder = telemetry.recorder(&loaded.registry);
    let run_span = info_span!(
        "okv.eval.run",
        run.id = %run_id,
        suite.id = %loaded.suite.id,
        profile.id = %profile_id,
        workload.id = %workload.id,
        lane.id = %lane.id,
        backend = %backend
    );
    let run_guard = run_span.enter();
    info!(
        telemetry.enabled = telemetry.enabled(),
        "evaluation started"
    );

    let started = Instant::now();
    let workload_result = execute_workload(workload);
    let elapsed = started.elapsed().as_secs_f64();
    let failures = f64::from(workload_result.is_err());
    recorder.record(
        "operation.duration",
        elapsed,
        attributes(&[
            ("lane", &lane.id),
            ("workload", &workload.id),
            ("operation", &workload.operation),
            ("backend", backend),
            ("result", if failures == 0.0 { "pass" } else { "fail" }),
        ]),
    )?;
    recorder.record(
        "correctness.failures",
        failures,
        attributes(&[("lane", &lane.id), ("workload", &workload.id)]),
    )?;

    let samples = recorder.samples(&lane.primary_metric).to_vec();
    let samples = if samples.is_empty() && lane.primary_metric == "correctness.failures" {
        vec![failures]
    } else {
        samples
    };
    if samples.is_empty() {
        return Err(format!(
            "workload {} did not record primary metric {}",
            workload.id, lane.primary_metric
        )
        .into());
    }
    let sample_median = median(&samples);
    let budget_observed = match profile.budget_kind {
        BudgetKind::Seconds => elapsed,
        BudgetKind::Events | BudgetKind::Operations => 1.0,
    };
    let budget_passed = budget_observed <= profile.budget_limit;
    let correctness_passed = workload_result.is_ok();
    let verdict = if !correctness_passed || !budget_passed {
        Verdict::Discard
    } else if source_dirty {
        Verdict::Inconclusive
    } else {
        Verdict::Keep
    };
    let reason = workload_result.err().unwrap_or_else(|| {
        if source_dirty {
            "diagnostic dirty-tree run; hard gates passed but the result is not comparable"
                .to_owned()
        } else if budget_passed {
            "all configured hard gates passed".to_owned()
        } else {
            format!(
                "budget exceeded: observed {budget_observed}, limit {}",
                profile.budget_limit
            )
        }
    });
    let result = EvalResult {
        schema_version: 1,
        run_id,
        created_at: Utc::now().to_rfc3339(),
        lane: lane.id.clone(),
        suite: loaded.suite.id.clone(),
        suite_hash,
        profile: ProfileIdentity {
            id: profile_id.to_owned(),
            hash: profile_hash,
            machine: command_output("uname", &["-m"]).unwrap_or_else(|| "unknown".to_owned()),
            rustc: command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_owned()),
            lockfile_hash,
        },
        candidate_commit,
        parent_commit,
        backend: backend.to_owned(),
        seeds: dataset_seeds(&loaded, profile_id),
        budget: BudgetResult {
            kind: profile.budget_kind,
            limit: profile.budget_limit,
            observed: budget_observed,
        },
        hard_gates: vec![
            HardGateResult {
                id: "correctness_failures".to_owned(),
                status: if correctness_passed {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "budget_must_hold".to_owned(),
                status: if budget_passed {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "schema_valid".to_owned(),
                status: GateStatus::Pass,
                detail: None,
            },
        ],
        primary_metric: PrimaryMetricResult {
            name: primary_definition.otel_name.clone(),
            unit: primary_definition.unit.clone(),
            direction: lane.direction,
            mad: median_absolute_deviation(&samples, sample_median),
            median: sample_median,
            samples,
            incumbent_median: None,
        },
        secondary_metrics: BTreeMap::from([("operation.duration.median".to_owned(), elapsed)]),
        verdict,
        reason,
        artifact_refs: output
            .map(|path| vec![path.display().to_string()])
            .unwrap_or_default(),
    };
    let value = serde_json::to_value(&result)?;
    validate_result(&loaded.result_schema_path, &value)?;
    let rendered = serde_json::to_string_pretty(&value)?;
    if let Some(path) = output {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, format!("{rendered}\n"))?;
    }
    println!("{rendered}");
    info!(verdict = ?result.verdict, "evaluation finished");
    drop(run_guard);
    drop(run_span);
    telemetry.shutdown();
    Ok(())
}

fn execute_workload(workload: &WorkloadConfig) -> Result<(), String> {
    match workload.operation.as_str() {
        "model_smoke" => run_model_smoke(),
        operation => Err(format!(
            "operation {operation} is declared but has no runner implementation"
        )),
    }
}

fn run_model_smoke() -> Result<(), String> {
    let mut model = Model::default();
    let batch = CommitBatch {
        version: Version::new(1),
        mutations: vec![Mutation::Set {
            key: b"inventory/sku-1".to_vec(),
            value: b"10".to_vec(),
        }],
    };
    if model
        .apply(batch.clone())
        .map_err(|error| error.to_string())?
        != ApplyOutcome::Applied
    {
        return Err("initial commit was not applied".to_owned());
    }
    if model.apply(batch).map_err(|error| error.to_string())? != ApplyOutcome::AlreadyApplied {
        return Err("exact replay was not idempotent".to_owned());
    }
    if model
        .get(b"inventory/sku-1", Version::new(1))
        .map_err(|error| error.to_string())?
        != Some(&b"10"[..])
    {
        return Err("snapshot read returned the wrong value".to_owned());
    }
    Ok(())
}

fn attributes(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

fn dataset_seeds(loaded: &LoadedSuite, profile_id: &str) -> Vec<u64> {
    loaded
        .suite
        .dataset
        .get(profile_id)
        .or_else(|| loaded.suite.dataset.values().next())
        .map(|dataset| dataset.seeds.clone())
        .unwrap_or_default()
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn contract_hash(loaded: &LoadedSuite) -> Result<String, Box<dyn Error>> {
    let registry_bytes = fs::read(&loaded.registry_path)?;
    let schema_bytes = fs::read(&loaded.result_schema_path)?;
    let mut hasher = Sha256::new();
    for bytes in [&loaded.suite_bytes, &registry_bytes, &schema_bytes] {
        hasher.update(u64::try_from(bytes.len())?.to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn git_is_dirty() -> Result<bool, Box<dyn Error>> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=all"])
        .output()?;
    if !output.status.success() {
        return Err("git status failed while establishing candidate identity".into());
    }
    Ok(!output.stdout.is_empty())
}

fn git_revision(revision: &str) -> Option<String> {
    command_output("git", &["rev-parse", revision])
}

fn command_output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program).args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
