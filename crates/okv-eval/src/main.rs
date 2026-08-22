use chrono::Utc;
use clap::{Parser, Subcommand};
use okv_eval::config::{load_suite, BudgetKind, LoadedSuite, WorkloadConfig};
use okv_eval::result::{
    median, median_absolute_deviation, validate_result, BudgetResult, EvalResult, GateStatus,
    HardGateResult, PrimaryMetricResult, ProfileIdentity, Verdict,
};
use okv_eval::telemetry::{RunResource, Telemetry};
use okv_model::{
    run_differential_history, ApplyOutcome, CommitBatch, CommitIdentity, DifferentialMode, Model,
    Mutation, Version,
};
use okv_object::{
    filesystem_backend, gcs_backend_from_env, memory_backend, minio_backend_from_env,
    run_conformance, validate_conformance_report, CaseStatus, ConformanceOptions,
    ConformanceProfile,
};
use okv_sim::run_generation_fencing;
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

struct Measurement {
    metric: &'static str,
    value: f64,
    attributes: BTreeMap<String, String>,
}

struct WorkloadExecution {
    error: Option<String>,
    measurements: Vec<Measurement>,
    hard_gates: Vec<HardGateResult>,
    budget_units: f64,
    artifact_refs: Vec<String>,
    secondary_metrics: BTreeMap<String, f64>,
}

impl WorkloadExecution {
    fn passed(&self) -> bool {
        self.error.is_none()
    }
}

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
    if let Some(expected_backend) = profile
        .parameters
        .get("backend")
        .and_then(toml::Value::as_str)
    {
        if expected_backend != backend {
            return Err(format!(
                "profile {profile_id} requires backend {expected_backend}, received {backend}"
            )
            .into());
        }
    }
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

    let seeds = dataset_seeds(&loaded, profile_id);
    let started = Instant::now();
    let workload_execution = execute_workload(workload, &candidate_commit, &seeds, backend);
    let elapsed = started.elapsed().as_secs_f64();
    for measurement in &workload_execution.measurements {
        recorder.record(
            measurement.metric,
            measurement.value,
            measurement.attributes.clone(),
        )?;
    }
    let failures = f64::from(!workload_execution.passed());
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
        BudgetKind::Events | BudgetKind::Operations => workload_execution.budget_units,
    };
    let budget_passed = budget_observed <= profile.budget_limit;
    let correctness_passed = workload_execution.passed();
    let verdict = if !correctness_passed || !budget_passed {
        Verdict::Discard
    } else if source_dirty {
        Verdict::Inconclusive
    } else {
        Verdict::Keep
    };
    let reason = workload_execution.error.unwrap_or_else(|| {
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
        seeds,
        budget: BudgetResult {
            kind: profile.budget_kind,
            limit: profile.budget_limit,
            observed: budget_observed,
        },
        hard_gates: {
            let mut gates = vec![
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
            ];
            gates.extend(workload_execution.hard_gates);
            gates
        },
        primary_metric: PrimaryMetricResult {
            name: primary_definition.otel_name.clone(),
            unit: primary_definition.unit.clone(),
            direction: lane.direction,
            mad: median_absolute_deviation(&samples, sample_median),
            median: sample_median,
            samples,
            incumbent_median: None,
        },
        secondary_metrics: {
            let mut metrics = workload_execution.secondary_metrics;
            metrics.insert("operation.duration.median".to_owned(), elapsed);
            metrics
        },
        verdict,
        reason,
        artifact_refs: {
            let mut refs = workload_execution.artifact_refs;
            if let Some(path) = output {
                refs.push(path.display().to_string());
            }
            refs
        },
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

fn execute_workload(
    workload: &WorkloadConfig,
    candidate_commit: &str,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    match workload.operation.as_str() {
        "model_smoke" => execution_from_result(run_model_smoke()),
        "deterministic_generation_recovery" => {
            run_generation_recovery(workload, candidate_commit, seeds)
        }
        "model_differential_history" => run_model_differential(workload, seeds),
        "object_store_conformance" => run_object_store_conformance(workload, backend),
        operation => execution_from_result(Err(format!(
            "operation {operation} is declared but has no runner implementation"
        ))),
    }
}

#[allow(clippy::too_many_lines)]
fn run_model_differential(workload: &WorkloadConfig, seeds: &[u64]) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "model differential workload requires at least one seed".to_owned(),
        ));
    }
    let steps = workload
        .parameters
        .get("steps")
        .and_then(toml::Value::as_integer)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(1_000);
    let legacy_range_bug = workload
        .parameters
        .get("inject_ignore_range_clears")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match (control, legacy_range_bug) {
        (None | Some("none"), false) => DifferentialMode::Correct,
        (None | Some("ignore_range_clears"), true) | (Some("ignore_range_clears"), false) => {
            DifferentialMode::IgnoreRangeClears
        }
        (Some("mutation_order_affects_replay"), false) => {
            DifferentialMode::MutationOrderAffectsReplay
        }
        (Some("accept_conflicting_replay"), false) => DifferentialMode::AcceptConflictingReplay,
        (Some("future_read_falls_back"), false) => DifferentialMode::FutureReadFallsBack,
        (Some("reject_retention_boundary"), false) => DifferentialMode::RejectRetentionBoundary,
        (Some("serve_expired_read"), false) => DifferentialMode::ServeExpiredRead,
        (Some("accept_stale_generation"), false) => DifferentialMode::AcceptStaleGeneration,
        (Some(other), _) => {
            return execution_from_result(Err(format!("unknown model negative control {other}")));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut range_clear_count = 0_u64;
    let mut exact_replay_count = 0_u64;
    let mut conflicting_replay_count = 0_u64;
    let mut future_read_count = 0_u64;
    let mut retention_count = 0_u64;
    let mut too_old_read_count = 0_u64;
    let mut historical_read_count = 0_u64;
    let mut stale_generation_count = 0_u64;
    let mut read_count = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = run_differential_history(*seed, steps, mode);
        let second = run_differential_history(*seed, steps, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        range_clear_count = range_clear_count.saturating_add(first.range_clear_count);
        exact_replay_count = exact_replay_count.saturating_add(first.exact_replay_count);
        conflicting_replay_count =
            conflicting_replay_count.saturating_add(first.conflicting_replay_count);
        future_read_count = future_read_count.saturating_add(first.future_read_count);
        retention_count = retention_count.saturating_add(first.retention_count);
        too_old_read_count = too_old_read_count.saturating_add(first.too_old_read_count);
        historical_read_count = historical_read_count.saturating_add(first.historical_read_count);
        stale_generation_count =
            stale_generation_count.saturating_add(first.stale_generation_count);
        read_count = read_count.saturating_add(first.read_count);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        measurements.push(Measurement {
            metric: "correctness.anomalies",
            value: bounded_count(first.anomaly_count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("oracle", "okv-model-independent-snapshot-v2"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        "mvcc_semantics"
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-model-history://{}/{seed}/{steps}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let range_clear_exercised = range_clear_count > 0;
    let semantic_operations_exercised = range_clear_exercised
        && exact_replay_count > 0
        && conflicting_replay_count > 0
        && future_read_count > 0
        && retention_count > 0
        && too_old_read_count > 0
        && historical_read_count > 0
        && stale_generation_count > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "MVCC differential gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "model.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "model.range_clear_exercised".to_owned(),
                status: if range_clear_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(range_clear_count.to_string()),
            },
            HardGateResult {
                id: "model.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "exact_replay={exact_replay_count}, conflicting_replay={conflicting_replay_count}, future_read={future_read_count}, retention={retention_count}, too_old={too_old_read_count}, historical={historical_read_count}, stale_generation={stale_generation_count}"
                )),
            },
            HardGateResult {
                id: "model.oracle_agreement".to_owned(),
                status: if anomaly_count == 0 {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(event_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            (
                "model.history.events".to_owned(),
                bounded_count(event_count),
            ),
            (
                "model.history.range_clears".to_owned(),
                bounded_count(range_clear_count),
            ),
            (
                "model.history.exact_replays".to_owned(),
                bounded_count(exact_replay_count),
            ),
            (
                "model.history.conflicting_replays".to_owned(),
                bounded_count(conflicting_replay_count),
            ),
            (
                "model.history.future_reads".to_owned(),
                bounded_count(future_read_count),
            ),
            (
                "model.history.retentions".to_owned(),
                bounded_count(retention_count),
            ),
            (
                "model.history.too_old_reads".to_owned(),
                bounded_count(too_old_read_count),
            ),
            (
                "model.history.historical_reads".to_owned(),
                bounded_count(historical_read_count),
            ),
            (
                "model.history.stale_generations".to_owned(),
                bounded_count(stale_generation_count),
            ),
            ("model.history.reads".to_owned(), bounded_count(read_count)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_object_store_conformance(workload: &WorkloadConfig, backend_id: &str) -> WorkloadExecution {
    let profile = match workload
        .parameters
        .get("contract_profile")
        .and_then(toml::Value::as_str)
    {
        Some("segment") => ConformanceProfile::Segment,
        Some("authority") => ConformanceProfile::Authority,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unsupported object-store contract profile {other}"
            )));
        }
        None => {
            return execution_from_result(Err(
                "object-store workload requires contract_profile".to_owned()
            ));
        }
    };

    let filesystem_root = std::env::temp_dir().join(format!("okv-object-eval-{}", Uuid::new_v4()));
    let backend = match backend_id {
        "memory" => Ok(memory_backend()),
        "filesystem" => filesystem_backend(&filesystem_root),
        "minio" => minio_backend_from_env(),
        "gcs" => gcs_backend_from_env(),
        other => {
            return execution_from_result(Err(format!(
                "object-store conformance has no backend adapter for {other}"
            )));
        }
    };
    let backend = match backend {
        Ok(backend) => backend,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return execution_from_result(Err(format!(
                "failed to create object-store evaluation runtime: {error}"
            )));
        }
    };
    let report = runtime.block_on(run_conformance(
        backend,
        profile,
        &ConformanceOptions::default(),
    ));
    if let Err(error) = validate_conformance_report(&report) {
        return execution_from_result(Err(error.to_string()));
    }

    if backend_id == "filesystem" {
        let _ = fs::remove_dir_all(&filesystem_root);
    }

    let mut measurements = vec![Measurement {
        metric: "correctness.anomalies",
        value: bounded_count(report.failure_count),
        attributes: attributes(&[
            ("lane", &workload.lane),
            ("workload", &workload.id),
            ("oracle", "okv-object-store-v1"),
            (
                "anomaly.class",
                if report.passed() {
                    "none"
                } else {
                    "backend_contract"
                },
            ),
        ]),
    }];
    for case in &report.cases {
        measurements.push(Measurement {
            metric: "compatibility.cases",
            value: 1.0,
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("compat.suite", "okv-object-store-v1"),
                ("feature.class", &case.id),
                (
                    "status",
                    match case.status {
                        CaseStatus::Pass => "pass",
                        CaseStatus::Fail => "fail",
                        CaseStatus::Unsupported => "unsupported",
                    },
                ),
            ]),
        });
    }
    let mut request_count = 0_u64;
    let mut request_bytes = 0_u64;
    let mut response_bytes = 0_u64;
    for stat in &report.stats.requests {
        request_count = request_count.saturating_add(stat.count);
        request_bytes = request_bytes.saturating_add(stat.request_bytes);
        response_bytes = response_bytes.saturating_add(stat.response_bytes);
        measurements.push(Measurement {
            metric: "object_store.requests",
            value: bounded_count(stat.count),
            attributes: attributes(&[
                ("lane", &workload.lane),
                ("workload", &workload.id),
                ("backend", backend_id),
                ("store", &report.backend.id),
                ("api", &stat.api),
                ("result", &stat.result),
            ]),
        });
        if stat.request_bytes > 0 {
            measurements.push(Measurement {
                metric: "object_store.bytes",
                value: bounded_count(stat.request_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend_id),
                    ("store", &report.backend.id),
                    ("direction", "request"),
                    ("api", &stat.api),
                ]),
            });
        }
        if stat.response_bytes > 0 {
            measurements.push(Measurement {
                metric: "object_store.bytes",
                value: bounded_count(stat.response_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("backend", backend_id),
                    ("store", &report.backend.id),
                    ("direction", "response"),
                    ("api", &stat.api),
                ]),
            });
        }
    }

    let hard_gates = report
        .cases
        .iter()
        .map(|case| HardGateResult {
            id: format!("object_store.{}", case.id),
            status: match case.status {
                CaseStatus::Pass => GateStatus::Pass,
                CaseStatus::Fail => GateStatus::Fail,
                CaseStatus::Unsupported => GateStatus::Error,
            },
            detail: Some(case.detail.clone()),
        })
        .collect();
    WorkloadExecution {
        error: (!report.passed()).then(|| {
            let failed: Vec<&str> = report
                .cases
                .iter()
                .filter(|case| case.required && case.status != CaseStatus::Pass)
                .map(|case| case.id.as_str())
                .collect();
            format!("object-store contract failed: {}", failed.join(", "))
        }),
        measurements,
        hard_gates,
        budget_units: bounded_count(request_count),
        artifact_refs: vec![format!(
            "okv-object-conformance://{}/{}/{}/{}",
            report.backend.id,
            profile,
            report.backend.driver_version,
            report.backend.server_version
        )],
        secondary_metrics: BTreeMap::from([
            (
                "object_store.conformance_failures".to_owned(),
                bounded_count(report.failure_count),
            ),
            (
                "object_store.request_count".to_owned(),
                bounded_count(request_count),
            ),
            (
                "object_store.request_bytes".to_owned(),
                bounded_count(request_bytes),
            ),
            (
                "object_store.response_bytes".to_owned(),
                bounded_count(response_bytes),
            ),
        ]),
    }
}

fn bounded_count(value: u64) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn execution_from_result(result: Result<(), String>) -> WorkloadExecution {
    WorkloadExecution {
        error: result.err(),
        measurements: Vec::new(),
        hard_gates: Vec::new(),
        budget_units: 1.0,
        artifact_refs: Vec::new(),
        secondary_metrics: BTreeMap::new(),
    }
}

fn run_generation_recovery(
    workload: &WorkloadConfig,
    candidate_commit: &str,
    seeds: &[u64],
) -> WorkloadExecution {
    let Some(seed) = seeds.first().copied() else {
        return execution_from_result(Err(
            "deterministic generation recovery requires at least one dataset seed".to_owned(),
        ));
    };
    let first = run_generation_fencing(seed, candidate_commit, false);
    let second = run_generation_fencing(seed, candidate_commit, false);

    match (first, second) {
        (Ok(first), Ok(second)) => {
            let exact_replay = first == second;
            let Ok(anomaly_count) = u32::try_from(first.invariant_failures.len()) else {
                return execution_from_result(Err(
                    "simulation anomaly count exceeds the result contract".to_owned(),
                ));
            };
            let Ok(event_count) = u32::try_from(first.events.len()) else {
                return execution_from_result(Err(
                    "simulation event count exceeds the result contract".to_owned(),
                ));
            };
            let anomaly_count = f64::from(anomaly_count);
            let event_count = f64::from(event_count);
            let mut errors = Vec::new();
            if !exact_replay {
                errors.push("same-seed simulation traces diverged".to_owned());
            }
            errors.extend(first.invariant_failures.clone());
            WorkloadExecution {
                error: (!errors.is_empty()).then(|| errors.join("; ")),
                measurements: vec![Measurement {
                    metric: "correctness.anomalies",
                    value: anomaly_count + f64::from(!exact_replay),
                    attributes: attributes(&[
                        ("lane", &workload.lane),
                        ("workload", &workload.id),
                        ("oracle", "generation-fencing-v1"),
                        (
                            "anomaly.class",
                            if exact_replay && anomaly_count == 0.0 {
                                "none"
                            } else {
                                "simulation_invariant"
                            },
                        ),
                    ]),
                }],
                hard_gates: vec![
                    HardGateResult {
                        id: "exact_seed_replay".to_owned(),
                        status: if exact_replay {
                            GateStatus::Pass
                        } else {
                            GateStatus::Fail
                        },
                        detail: Some(first.trace_sha256.clone()),
                    },
                    HardGateResult {
                        id: "stale_generation_fenced".to_owned(),
                        status: if anomaly_count == 0.0 {
                            GateStatus::Pass
                        } else {
                            GateStatus::Fail
                        },
                        detail: None,
                    },
                ],
                budget_units: event_count,
                artifact_refs: vec![format!(
                    "okv-sim://generation-fencing-v1/{seed}/{}",
                    first.trace_sha256
                )],
                secondary_metrics: BTreeMap::from([("simulation.events".to_owned(), event_count)]),
            }
        }
        (Err(first), Err(second)) => execution_from_result(Err(format!(
            "both simulation replays failed: first={first}; second={second}"
        ))),
        (Err(error), _) | (_, Err(error)) => {
            execution_from_result(Err(format!("simulation replay failed: {error}")))
        }
    }
}

fn run_model_smoke() -> Result<(), String> {
    let mut model = Model::default();
    let batch = CommitBatch {
        version: Version::new(1),
        identity: CommitIdentity::for_test(1),
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
    for path in &loaded.contract_paths {
        let bytes = fs::read(path)?;
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
