use chrono::Utc;
use clap::{Parser, Subcommand};
use okv_consensus::{
    run_generation_process_contract, run_process_node, run_raft_cluster_contract,
    run_raft_process_contract, run_raft_storage_contract, GenerationProcessMode, ProcessNodeConfig,
    RaftClusterMode, RaftProcessMode, RaftStorageMode,
};
use okv_eval::config::{load_suite, BudgetKind, LoadedSuite, WorkloadConfig};
use okv_eval::result::{
    median, median_absolute_deviation, validate_result, BudgetResult, EvalResult, GateStatus,
    HardGateResult, PrimaryMetricResult, ProfileIdentity, Verdict,
};
use okv_eval::telemetry::{RunResource, Telemetry};
use okv_model::{
    run_differential_history, run_htap_contract, ApplyOutcome, CommitBatch, CommitIdentity,
    DifferentialMode, HtapContractMode, Model, Mutation, Version,
};
use okv_object::{
    filesystem_backend, gcs_backend_from_env, memory_backend, minio_backend_from_env,
    run_conformance, validate_conformance_report, CaseStatus, ConformanceOptions,
    ConformanceProfile,
};
use okv_sim::{
    run_commit_contract, run_generation_fencing, run_persisted_wal_contract, CommitContractMode,
    PersistedWalMode,
};
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
    /// Emit one canonical real-process trace without suite orchestration.
    RaftProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Emit one canonical cell-generation takeover trace without suite orchestration.
    GenerationProcessTrace {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "correct")]
        mode: String,
    },
    /// Internal entrypoint used by the real-process consensus controller.
    #[command(hide = true)]
    ConsensusNode {
        #[arg(long)]
        config_json: String,
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
        Commands::RaftProcessTrace { seed, mode } => {
            let mode = parse_raft_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_raft_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::GenerationProcessTrace { seed, mode } => {
            let mode = parse_generation_process_mode(&mode).map_err(std::io::Error::other)?;
            let executable = std::env::current_exe()?;
            let report = run_generation_process_contract(seed, mode, &executable)?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Commands::ConsensusNode { config_json } => {
            let config = serde_json::from_str::<ProcessNodeConfig>(&config_json)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_process_node(config))?;
        }
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
        "commit_envelope_contract" => run_commit_envelope_contract(workload, seeds),
        "persisted_wal_contract" => run_persisted_wal(workload, seeds, backend),
        "raft_storage_contract" => run_raft_storage(workload, seeds, backend),
        "raft_cluster_contract" => run_raft_cluster(workload, seeds, backend),
        "raft_process_contract" => run_raft_process(workload, seeds, backend),
        "generation_process_contract" => run_generation_process(workload, seeds, backend),
        "htap_exactness_contract" => run_htap_exactness_contract(workload, seeds, backend),
        "model_differential_history" => run_model_differential(workload, seeds),
        "object_store_conformance" => run_object_store_conformance(workload, backend),
        operation => execution_from_result(Err(format!(
            "operation {operation} is declared but has no runner implementation"
        ))),
    }
}

#[allow(clippy::too_many_lines)]
fn run_persisted_wal(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "persisted WAL workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => PersistedWalMode::Correct,
        Some("ram_only_dedup") => PersistedWalMode::RamOnlyDedup,
        Some("ack_before_quorum") => PersistedWalMode::AckBeforeQuorum,
        Some("trust_single_replica") => PersistedWalMode::TrustSingleReplica,
        Some("accept_torn_as_commit") => PersistedWalMode::AcceptTornAsCommit,
        Some("skip_log_chain_validation") => PersistedWalMode::SkipLogChainValidation,
        Some("ignore_complete_corruption") => PersistedWalMode::IgnoreCompleteCorruption,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown persisted WAL negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut quorum_appends = 0_u64;
    let mut recovered_records = 0_u64;
    let mut reopened_wals = 0_u64;
    let mut recovered_outcomes = 0_u64;
    let mut leader_only_attempts = 0_u64;
    let mut torn_tail_replicas = 0_u64;
    let mut corruption_failures = 0_u64;
    let mut physical_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_persisted_wal_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_persisted_wal_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        quorum_appends = quorum_appends.saturating_add(first.quorum_appends);
        recovered_records = recovered_records.saturating_add(first.recovered_records);
        reopened_wals = reopened_wals.saturating_add(first.reopened_wals);
        recovered_outcomes = recovered_outcomes.saturating_add(first.recovered_outcomes);
        leader_only_attempts = leader_only_attempts.saturating_add(first.leader_only_attempts);
        torn_tail_replicas = torn_tail_replicas.saturating_add(first.torn_tail_replicas);
        corruption_failures = corruption_failures.saturating_add(first.corruption_failures);
        physical_bytes = physical_bytes.saturating_add(first.physical_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "okv-persisted-wal-contract-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "persisted_wal" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.quorum_appends),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "cell-commit-contract"),
                    ("result", "quorum-fsynced"),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: bounded_count(first.physical_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "local-three-file"),
                    ("fault", mode.id()),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "fresh-open-recovery"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-persisted-wal://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(7);
    let semantic_operations_exercised = event_count == expected_events
        && quorum_appends > 0
        && recovered_records > 0
        && reopened_wals > 0
        && recovered_outcomes > 0
        && leader_only_attempts > 0
        && torn_tail_replicas > 0
        && corruption_failures > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "persisted WAL gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "persisted_wal.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "persisted_wal.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "events={event_count}, quorum_appends={quorum_appends}, recovered_records={recovered_records}, reopened={reopened_wals}, outcomes={recovered_outcomes}, leader_only={leader_only_attempts}, torn={torn_tail_replicas}, corruption_failures={corruption_failures}"
                )),
            },
            HardGateResult {
                id: "persisted_wal.contract_agreement".to_owned(),
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
            ("wal.contract.events".to_owned(), bounded_count(event_count)),
            (
                "wal.contract.quorum_appends".to_owned(),
                bounded_count(quorum_appends),
            ),
            (
                "wal.contract.recovered_records".to_owned(),
                bounded_count(recovered_records),
            ),
            (
                "wal.contract.reopened".to_owned(),
                bounded_count(reopened_wals),
            ),
            (
                "wal.contract.recovered_outcomes".to_owned(),
                bounded_count(recovered_outcomes),
            ),
            (
                "wal.contract.leader_only_attempts".to_owned(),
                bounded_count(leader_only_attempts),
            ),
            (
                "wal.contract.torn_tail_replicas".to_owned(),
                bounded_count(torn_tail_replicas),
            ),
            (
                "wal.contract.corruption_failures".to_owned(),
                bounded_count(corruption_failures),
            ),
            (
                "wal.contract.physical_bytes".to_owned(),
                bounded_count(physical_bytes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_raft_storage(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "Raft storage workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => RaftStorageMode::Correct,
        Some("ram_only_vote") => RaftStorageMode::RamOnlyVote,
        Some("ram_only_committed") => RaftStorageMode::RamOnlyCommitted,
        Some("ignore_conflict_truncate") => RaftStorageMode::IgnoreConflictTruncate,
        Some("ignore_purge") => RaftStorageMode::IgnorePurge,
        Some("accept_log_gap") => RaftStorageMode::AcceptLogGap,
        Some("ignore_complete_corruption") => RaftStorageMode::IgnoreCompleteCorruption,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown Raft storage negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut reopened_stores = 0_u64;
    let mut durable_votes = 0_u64;
    let mut durable_committed_positions = 0_u64;
    let mut appended_entries = 0_u64;
    let mut conflict_truncations = 0_u64;
    let mut purged_prefixes = 0_u64;
    let mut rejected_log_gaps = 0_u64;
    let mut torn_tail_repairs = 0_u64;
    let mut corruption_failures = 0_u64;
    let mut physical_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_raft_storage_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_raft_storage_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        reopened_stores = reopened_stores.saturating_add(first.reopened_stores);
        durable_votes = durable_votes.saturating_add(first.durable_votes);
        durable_committed_positions =
            durable_committed_positions.saturating_add(first.durable_committed_positions);
        appended_entries = appended_entries.saturating_add(first.appended_entries);
        conflict_truncations = conflict_truncations.saturating_add(first.conflict_truncations);
        purged_prefixes = purged_prefixes.saturating_add(first.purged_prefixes);
        rejected_log_gaps = rejected_log_gaps.saturating_add(first.rejected_log_gaps);
        torn_tail_repairs = torn_tail_repairs.saturating_add(first.torn_tail_repairs);
        corruption_failures = corruption_failures.saturating_add(first.corruption_failures);
        physical_bytes = physical_bytes.saturating_add(first.physical_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "openraft-storage-contract-v1"),
                    ("anomaly.class", if exact { "none" } else { "raft_storage" }),
                ]),
            },
            Measurement {
                metric: "wal.retained_bytes",
                value: bounded_count(first.physical_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("topology", "per-node-journal"),
                    ("fault", mode.id()),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "stable-log-reopen"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-openraft-storage://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(8);
    let semantic_operations_exercised = event_count == expected_events
        && reopened_stores > 0
        && durable_votes > 0
        && durable_committed_positions > 0
        && appended_entries > 0
        && conflict_truncations > 0
        && purged_prefixes > 0
        && torn_tail_repairs > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "Raft storage gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "raft_storage.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "raft_storage.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "events={event_count}, reopened={reopened_stores}, votes={durable_votes}, committed={durable_committed_positions}, appended={appended_entries}, truncations={conflict_truncations}, purges={purged_prefixes}, gap_rejections={rejected_log_gaps}, torn_repairs={torn_tail_repairs}, corruption_failures={corruption_failures}"
                )),
            },
            HardGateResult {
                id: "raft_storage.contract_agreement".to_owned(),
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
            ("raft_storage.events".to_owned(), bounded_count(event_count)),
            (
                "raft_storage.reopened".to_owned(),
                bounded_count(reopened_stores),
            ),
            (
                "raft_storage.durable_votes".to_owned(),
                bounded_count(durable_votes),
            ),
            (
                "raft_storage.durable_committed".to_owned(),
                bounded_count(durable_committed_positions),
            ),
            (
                "raft_storage.appended_entries".to_owned(),
                bounded_count(appended_entries),
            ),
            (
                "raft_storage.conflict_truncations".to_owned(),
                bounded_count(conflict_truncations),
            ),
            (
                "raft_storage.purged_prefixes".to_owned(),
                bounded_count(purged_prefixes),
            ),
            (
                "raft_storage.rejected_log_gaps".to_owned(),
                bounded_count(rejected_log_gaps),
            ),
            (
                "raft_storage.torn_tail_repairs".to_owned(),
                bounded_count(torn_tail_repairs),
            ),
            (
                "raft_storage.corruption_failures".to_owned(),
                bounded_count(corruption_failures),
            ),
            (
                "raft_storage.physical_bytes".to_owned(),
                bounded_count(physical_bytes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_raft_cluster(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "Raft cluster workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => RaftClusterMode::Correct,
        Some("acknowledge_before_quorum") => RaftClusterMode::AcknowledgeBeforeQuorum,
        Some("skip_successor_election") => RaftClusterMode::SkipSuccessorElection,
        Some("skip_restart_catchup") => RaftClusterMode::SkipRestartCatchup,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown Raft cluster negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut committed_writes = 0_u64;
    let mut elections = 0_u64;
    let mut stale_write_attempts = 0_u64;
    let mut stale_write_acks = 0_u64;
    let mut partitions = 0_u64;
    let mut repairs = 0_u64;
    let mut simulated_crashes = 0_u64;
    let mut simulated_bounces = 0_u64;
    let mut caught_up_nodes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_raft_cluster_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_raft_cluster_contract(*seed, mode) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        committed_writes = committed_writes.saturating_add(first.committed_writes);
        elections = elections.saturating_add(first.elections);
        stale_write_attempts = stale_write_attempts.saturating_add(first.stale_write_attempts);
        stale_write_acks = stale_write_acks.saturating_add(first.stale_write_acks);
        partitions = partitions.saturating_add(first.partitions);
        repairs = repairs.saturating_add(first.repairs);
        simulated_crashes = simulated_crashes.saturating_add(first.simulated_crashes);
        simulated_bounces = simulated_bounces.saturating_add(first.simulated_bounces);
        caught_up_nodes = caught_up_nodes.saturating_add(first.caught_up_nodes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "openraft-cluster-contract-v1"),
                    ("anomaly.class", if exact { "none" } else { "raft_cluster" }),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_writes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "openraft-cell-v0"),
                    ("result", if exact { "quorum-applied" } else { "rejected" }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "leader-failover-and-bounce"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-openraft-cluster://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(8)
        && partitions == seed_count.saturating_mul(2)
        && repairs == seed_count.saturating_mul(2)
        && simulated_crashes == seed_count
        && simulated_bounces == seed_count
        && stale_write_attempts == seed_count;
    let expected_success_path = mode != RaftClusterMode::Correct
        || (committed_writes == seed_count.saturating_mul(3)
            && elections == seed_count.saturating_mul(3)
            && stale_write_acks == 0
            && caught_up_nodes == seed_count.saturating_mul(3));
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && expected_success_path;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "Raft cluster gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_success_path={expected_success_path}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "raft_cluster.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "raft_cluster.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, commits={committed_writes}, elections={elections}, stale_attempts={stale_write_attempts}, stale_acks={stale_write_acks}, partitions={partitions}, repairs={repairs}, crashes={simulated_crashes}, bounces={simulated_bounces}, caught_up={caught_up_nodes}"
                )),
            },
            HardGateResult {
                id: "raft_cluster.contract_agreement".to_owned(),
                status: if anomaly_count == 0 && expected_success_path {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("raft_cluster.checks".to_owned(), bounded_count(check_count)),
            (
                "raft_cluster.committed_writes".to_owned(),
                bounded_count(committed_writes),
            ),
            (
                "raft_cluster.elections".to_owned(),
                bounded_count(elections),
            ),
            (
                "raft_cluster.stale_write_attempts".to_owned(),
                bounded_count(stale_write_attempts),
            ),
            (
                "raft_cluster.stale_write_acks".to_owned(),
                bounded_count(stale_write_acks),
            ),
            (
                "raft_cluster.partitions".to_owned(),
                bounded_count(partitions),
            ),
            ("raft_cluster.repairs".to_owned(), bounded_count(repairs)),
            (
                "raft_cluster.simulated_crashes".to_owned(),
                bounded_count(simulated_crashes),
            ),
            (
                "raft_cluster.simulated_bounces".to_owned(),
                bounded_count(simulated_bounces),
            ),
            (
                "raft_cluster.caught_up_nodes".to_owned(),
                bounded_count(caught_up_nodes),
            ),
        ]),
    }
}

fn parse_raft_process_mode(value: &str) -> Result<RaftProcessMode, String> {
    match value {
        "correct" | "none" => Ok(RaftProcessMode::Correct),
        "disable_dedup" => Ok(RaftProcessMode::DisableDedup),
        "acknowledge_before_quorum" => Ok(RaftProcessMode::AcknowledgeBeforeQuorum),
        "skip_killed_node_restart" => Ok(RaftProcessMode::SkipKilledNodeRestart),
        other => Err(format!("unknown Raft process mode {other}")),
    }
}

fn parse_generation_process_mode(value: &str) -> Result<GenerationProcessMode, String> {
    match value {
        "correct" | "none" => Ok(GenerationProcessMode::Correct),
        "bypass_stale_commit_fence" => Ok(GenerationProcessMode::BypassStaleCommitFence),
        "accept_write_during_recovery" => Ok(GenerationProcessMode::AcceptWriteDuringRecovery),
        "accept_competing_recovery" => Ok(GenerationProcessMode::AcceptCompetingRecovery),
        "activate_without_recovery_proof" => {
            Ok(GenerationProcessMode::ActivateWithoutRecoveryProof)
        }
        "accept_single_signer_fence" => Ok(GenerationProcessMode::AcceptSingleSignerFence),
        "accept_tampered_fence_position" => Ok(GenerationProcessMode::AcceptTamperedFencePosition),
        "accept_duplicate_recovery_signer" => {
            Ok(GenerationProcessMode::AcceptDuplicateRecoverySigner)
        }
        "accept_stale_recovery_certificate" => {
            Ok(GenerationProcessMode::AcceptStaleRecoveryCertificate)
        }
        "accept_wrong_recovery_membership" => {
            Ok(GenerationProcessMode::AcceptWrongRecoveryMembership)
        }
        other => Err(format!("unknown generation process mode {other}")),
    }
}

#[allow(clippy::too_many_lines)]
fn run_generation_process(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "generation process workload requires at least one seed".to_owned(),
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_generation_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut authority_process_starts = 0_u64;
    let mut data_process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut authority_failovers = 0_u64;
    let mut learner_additions = 0_u64;
    let mut membership_changes = 0_u64;
    let mut generation_preparations = 0_u64;
    let mut generation_reservations = 0_u64;
    let mut generation_activations = 0_u64;
    let mut committed_data_writes = 0_u64;
    let mut fenced_commit_attempts = 0_u64;
    let mut fenced_commit_rejections = 0_u64;
    let mut caught_up_nodes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();

    for seed in seeds {
        let first = match run_generation_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_generation_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        authority_process_starts =
            authority_process_starts.saturating_add(first.authority_process_starts);
        data_process_starts = data_process_starts.saturating_add(first.data_process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        authority_failovers = authority_failovers.saturating_add(first.authority_failovers);
        learner_additions = learner_additions.saturating_add(first.learner_additions);
        membership_changes = membership_changes.saturating_add(first.membership_changes);
        generation_preparations =
            generation_preparations.saturating_add(first.generation_preparations);
        generation_reservations =
            generation_reservations.saturating_add(first.generation_reservations);
        generation_activations =
            generation_activations.saturating_add(first.generation_activations);
        committed_data_writes = committed_data_writes.saturating_add(first.committed_data_writes);
        fenced_commit_attempts =
            fenced_commit_attempts.saturating_add(first.fenced_commit_attempts);
        fenced_commit_rejections =
            fenced_commit_rejections.saturating_add(first.fenced_commit_rejections);
        caught_up_nodes = caught_up_nodes.saturating_add(first.caught_up_generation_two_nodes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "generation-takeover-process-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "generation_takeover" },
                    ),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_data_writes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "strict-serializable-cell-generation"),
                    ("result", if exact { "committed" } else { "unsafe-control" }),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "cell-generation-takeover"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-generation-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_preparations = if mode == GenerationProcessMode::AcceptCompetingRecovery {
        2
    } else {
        1
    };
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(16)
        && authority_process_starts == seed_count.saturating_mul(3)
        && data_process_starts == seed_count.saturating_mul(6)
        && process_kills == seed_count
        && authority_failovers == seed_count
        && learner_additions == seed_count.saturating_mul(3)
        && membership_changes == seed_count
        && generation_preparations == seed_count.saturating_mul(expected_preparations)
        && generation_reservations == seed_count
        && generation_activations == seed_count
        && fenced_commit_attempts == seed_count.saturating_mul(4);
    let expected_success_path = mode != GenerationProcessMode::Correct
        || (committed_data_writes == seed_count.saturating_mul(2)
            && fenced_commit_rejections == seed_count.saturating_mul(4)
            && caught_up_nodes == seed_count.saturating_mul(3));
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && expected_success_path;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "generation takeover gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_success_path={expected_success_path}; {detail}",
            mode.id()
        )
    });

    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "generation_process.exact_fresh_process_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "generation_process.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, authority_starts={authority_process_starts}, data_starts={data_process_starts}, kills={process_kills}, authority_failovers={authority_failovers}, learners={learner_additions}, membership_changes={membership_changes}, preparations={generation_preparations}, reservations={generation_reservations}, activations={generation_activations}, data_commits={committed_data_writes}, fence_attempts={fenced_commit_attempts}, fence_rejections={fenced_commit_rejections}, caught_up={caught_up_nodes}"
                )),
            },
            HardGateResult {
                id: "generation_process.contract_agreement".to_owned(),
                status: if anomaly_count == 0 && expected_success_path {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("generation_process.checks".to_owned(), bounded_count(check_count)),
            (
                "generation_process.authority_failovers".to_owned(),
                bounded_count(authority_failovers),
            ),
            (
                "generation_process.membership_changes".to_owned(),
                bounded_count(membership_changes),
            ),
            (
                "generation_process.committed_data_writes".to_owned(),
                bounded_count(committed_data_writes),
            ),
            (
                "generation_process.fenced_commit_rejections".to_owned(),
                bounded_count(fenced_commit_rejections),
            ),
            (
                "generation_process.caught_up_nodes".to_owned(),
                bounded_count(caught_up_nodes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_raft_process(workload: &WorkloadConfig, seeds: &[u64], backend: &str) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "Raft process workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str)
        .unwrap_or("none");
    let mode = match parse_raft_process_mode(control) {
        Ok(mode) => mode,
        Err(error) => return execution_from_result(Err(error)),
    };
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return execution_from_result(Err(error.to_string())),
    };

    let mut anomaly_count = 0_u64;
    let mut check_count = 0_u64;
    let mut committed_writes = 0_u64;
    let mut elections = 0_u64;
    let mut process_starts = 0_u64;
    let mut process_kills = 0_u64;
    let mut dropped_replies = 0_u64;
    let mut duplicate_retries = 0_u64;
    let mut recovered_outcomes = 0_u64;
    let mut caught_up_nodes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = match run_raft_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        let second = match run_raft_process_contract(*seed, mode, &executable) {
            Ok(report) => report,
            Err(error) => return execution_from_result(Err(error)),
        };
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        check_count = check_count.saturating_add(first.executed_checks);
        committed_writes = committed_writes.saturating_add(first.committed_writes);
        elections = elections.saturating_add(first.elections);
        process_starts = process_starts.saturating_add(first.process_starts);
        process_kills = process_kills.saturating_add(first.process_kills);
        dropped_replies = dropped_replies.saturating_add(first.dropped_replies);
        duplicate_retries = duplicate_retries.saturating_add(first.duplicate_retries);
        recovered_outcomes = recovered_outcomes.saturating_add(first.recovered_outcomes);
        caught_up_nodes = caught_up_nodes.saturating_add(first.caught_up_nodes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, check {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "openraft-process-contract-v1"),
                    ("anomaly.class", if exact { "none" } else { "raft_process" }),
                ]),
            },
            Measurement {
                metric: "transaction.commits",
                value: bounded_count(first.committed_writes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("isolation", "openraft-cell-v0"),
                    (
                        "result",
                        if exact {
                            "replay-deduplicated"
                        } else {
                            "rejected"
                        },
                    ),
                ]),
            },
            Measurement {
                metric: "availability.success_ratio",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("operation", "lost-reply-process-failover-restart"),
                    ("fault", mode.id()),
                    ("backend", backend),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-openraft-process://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let seed_count = u64::try_from(seeds.len()).unwrap_or(u64::MAX);
    let expected_counts = match mode {
        RaftProcessMode::Correct | RaftProcessMode::DisableDedup => (4, 1, 1, 1),
        RaftProcessMode::AcknowledgeBeforeQuorum => (6, 3, 0, 0),
        RaftProcessMode::SkipKilledNodeRestart => (3, 1, 1, 1),
    };
    let semantic_operations_exercised = check_count == seed_count.saturating_mul(8)
        && process_starts == seed_count.saturating_mul(expected_counts.0)
        && process_kills == seed_count.saturating_mul(expected_counts.1)
        && dropped_replies == seed_count.saturating_mul(expected_counts.2)
        && duplicate_retries == seed_count.saturating_mul(expected_counts.3);
    let expected_success_path = mode != RaftProcessMode::Correct
        || (committed_writes == seed_count.saturating_mul(3)
            && elections == seed_count.saturating_mul(2)
            && recovered_outcomes == seed_count.saturating_mul(3)
            && caught_up_nodes == seed_count.saturating_mul(3));
    let passed = anomaly_count == 0
        && exact_replay
        && semantic_operations_exercised
        && expected_success_path;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "Raft process gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}, expected_success_path={expected_success_path}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "raft_process.exact_fresh_process_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "raft_process.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "checks={check_count}, commits={committed_writes}, elections={elections}, starts={process_starts}, kills={process_kills}, dropped_replies={dropped_replies}, retries={duplicate_retries}, recovered_outcomes={recovered_outcomes}, caught_up={caught_up_nodes}"
                )),
            },
            HardGateResult {
                id: "raft_process.contract_agreement".to_owned(),
                status: if anomaly_count == 0 && expected_success_path {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: mismatch_details.first().cloned(),
            },
        ],
        budget_units: bounded_count(check_count),
        artifact_refs,
        secondary_metrics: BTreeMap::from([
            ("raft_process.checks".to_owned(), bounded_count(check_count)),
            (
                "raft_process.committed_writes".to_owned(),
                bounded_count(committed_writes),
            ),
            (
                "raft_process.elections".to_owned(),
                bounded_count(elections),
            ),
            (
                "raft_process.process_starts".to_owned(),
                bounded_count(process_starts),
            ),
            (
                "raft_process.process_kills".to_owned(),
                bounded_count(process_kills),
            ),
            (
                "raft_process.dropped_replies".to_owned(),
                bounded_count(dropped_replies),
            ),
            (
                "raft_process.duplicate_retries".to_owned(),
                bounded_count(duplicate_retries),
            ),
            (
                "raft_process.recovered_outcomes".to_owned(),
                bounded_count(recovered_outcomes),
            ),
            (
                "raft_process.caught_up_nodes".to_owned(),
                bounded_count(caught_up_nodes),
            ),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_htap_exactness_contract(
    workload: &WorkloadConfig,
    seeds: &[u64],
    backend: &str,
) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "HTAP exactness workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => HtapContractMode::Correct,
        Some("pushdown_poison") => HtapContractMode::PushdownPoison,
        Some("schema_partition_move") => HtapContractMode::SchemaPartitionMove,
        Some("wal_pop_conflation") => HtapContractMode::WalPopConflation,
        Some("lease_gc_race") => HtapContractMode::LeaseGcRace,
        Some("certificate_toctou") => HtapContractMode::CertificateToctou,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown HTAP exactness negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut exact_checks = 0_u64;
    let mut tail_rows = 0_u64;
    let mut tail_bytes = 0_u64;
    let mut peak_memory_bytes = 0_u64;
    let mut spill_bytes = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = run_htap_contract(*seed, mode);
        let second = run_htap_contract(*seed, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        exact_checks = exact_checks.saturating_add(first.exact_checks);
        tail_rows = tail_rows.saturating_add(first.tail_rows);
        tail_bytes = tail_bytes.saturating_add(first.tail_bytes);
        peak_memory_bytes = peak_memory_bytes.max(first.peak_memory_bytes);
        spill_bytes = spill_bytes.saturating_add(first.spill_bytes);
        if let Some(detail) = &first.first_mismatch {
            mismatch_details.push(format!(
                "seed {seed}, step {:?}: {detail}",
                first.first_mismatch_step
            ));
        }
        let exact = first.anomaly_count == 0;
        measurements.extend([
            Measurement {
                metric: "correctness.anomalies",
                value: bounded_count(first.anomaly_count),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("oracle", "okv-htap-row-oracle-v1"),
                    (
                        "anomaly.class",
                        if exact { "none" } else { "htap_exactness" },
                    ),
                ]),
            },
            Measurement {
                metric: "query.result_exact",
                value: if exact { 1.0 } else { 0.0 },
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("oracle", "okv-htap-row-oracle-v1"),
                ]),
            },
            Measurement {
                metric: "htap.tail_rows",
                value: bounded_count(first.tail_rows),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("base.format", "model-row"),
                ]),
            },
            Measurement {
                metric: "htap.tail_bytes",
                value: bounded_count(first.tail_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("base.format", "model-row"),
                ]),
            },
            Measurement {
                metric: "htap.peak_memory",
                value: bounded_count(first.peak_memory_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("merge.kind", "ordered-model"),
                ]),
            },
            Measurement {
                metric: "htap.spill_bytes",
                value: bounded_count(first.spill_bytes),
                attributes: attributes(&[
                    ("lane", &workload.lane),
                    ("workload", &workload.id),
                    ("query.class", "base_plus_tail_contract"),
                    ("backend", backend),
                    ("merge.kind", "ordered-model"),
                ]),
            },
        ]);
        artifact_refs.push(format!(
            "okv-htap-contract://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let expected_events = u64::try_from(seeds.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(6);
    let semantic_operations_exercised = event_count == expected_events
        && exact_checks > 0
        && tail_rows > 0
        && tail_bytes > 0
        && peak_memory_bytes > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "HTAP exactness gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "htap.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "htap.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "events={event_count}, exact_checks={exact_checks}, tail_rows={tail_rows}, tail_bytes={tail_bytes}, peak_memory_bytes={peak_memory_bytes}, spill_bytes={spill_bytes}"
                )),
            },
            HardGateResult {
                id: "htap.exact_result".to_owned(),
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
            ("htap.contract.events".to_owned(), bounded_count(event_count)),
            (
                "htap.contract.exact_checks".to_owned(),
                bounded_count(exact_checks),
            ),
            ("htap.tail_rows".to_owned(), bounded_count(tail_rows)),
            ("htap.tail_bytes".to_owned(), bounded_count(tail_bytes)),
            (
                "htap.peak_memory_bytes".to_owned(),
                bounded_count(peak_memory_bytes),
            ),
            ("htap.spill_bytes".to_owned(), bounded_count(spill_bytes)),
        ]),
    }
}

#[allow(clippy::too_many_lines)]
fn run_commit_envelope_contract(workload: &WorkloadConfig, seeds: &[u64]) -> WorkloadExecution {
    if seeds.is_empty() {
        return execution_from_result(Err(
            "commit envelope workload requires at least one seed".to_owned()
        ));
    }
    let control = workload
        .parameters
        .get("negative_control")
        .and_then(toml::Value::as_str);
    let mode = match control {
        None | Some("none") => CommitContractMode::Correct,
        Some("ram_only_dedup") => CommitContractMode::RamOnlyDedup,
        Some("accept_conflicting_retry") => CommitContractMode::AcceptConflictingRetry,
        Some("accept_partial_resolver") => CommitContractMode::AcceptPartialResolver,
        Some("omit_required_log_tag") => CommitContractMode::OmitRequiredLogTag,
        Some("accept_stale_generation") => CommitContractMode::AcceptStaleGeneration,
        Some("ack_before_quorum") => CommitContractMode::AckBeforeQuorum,
        Some(other) => {
            return execution_from_result(Err(format!(
                "unknown commit contract negative control {other}"
            )));
        }
    };

    let mut anomaly_count = 0_u64;
    let mut event_count = 0_u64;
    let mut acknowledged_commits = 0_u64;
    let mut recovered_commits = 0_u64;
    let mut retry_count = 0_u64;
    let mut leader_only_attempts = 0_u64;
    let mut exact_replay = true;
    let mut mismatch_details = Vec::new();
    let mut measurements = Vec::new();
    let mut artifact_refs = Vec::new();
    for seed in seeds {
        let first = run_commit_contract(*seed, mode);
        let second = run_commit_contract(*seed, mode);
        exact_replay &= first == second;
        anomaly_count = anomaly_count.saturating_add(first.anomaly_count);
        event_count = event_count.saturating_add(first.executed_steps);
        acknowledged_commits = acknowledged_commits.saturating_add(first.acknowledged_commits);
        recovered_commits = recovered_commits.saturating_add(first.recovered_commits);
        retry_count = retry_count.saturating_add(first.retry_count);
        leader_only_attempts = leader_only_attempts.saturating_add(first.leader_only_attempts);
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
                ("oracle", "okv-cell-commit-contract-v1"),
                (
                    "anomaly.class",
                    if first.anomaly_count == 0 {
                        "none"
                    } else {
                        "commit_contract"
                    },
                ),
            ]),
        });
        artifact_refs.push(format!(
            "okv-commit-contract://{}/{seed}/{}",
            mode.id(),
            first.trace_sha256
        ));
    }

    let semantic_operations_exercised = acknowledged_commits > 0
        && recovered_commits > 0
        && retry_count > 0
        && leader_only_attempts > 0;
    let passed = anomaly_count == 0 && exact_replay && semantic_operations_exercised;
    let error = (!passed).then(|| {
        let detail = if mismatch_details.is_empty() {
            "no semantic mismatch detail".to_owned()
        } else {
            mismatch_details.join("; ")
        };
        format!(
            "commit contract gate failed: mode={}, anomalies={anomaly_count}, exact_replay={exact_replay}, semantic_operations={semantic_operations_exercised}; {detail}",
            mode.id()
        )
    });
    WorkloadExecution {
        error,
        measurements,
        hard_gates: vec![
            HardGateResult {
                id: "commit.exact_seed_replay".to_owned(),
                status: if exact_replay {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: None,
            },
            HardGateResult {
                id: "commit.semantic_operations_exercised".to_owned(),
                status: if semantic_operations_exercised {
                    GateStatus::Pass
                } else {
                    GateStatus::Fail
                },
                detail: Some(format!(
                    "acknowledged={acknowledged_commits}, recovered={recovered_commits}, retries={retry_count}, leader_only={leader_only_attempts}"
                )),
            },
            HardGateResult {
                id: "commit.contract_agreement".to_owned(),
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
                "commit.contract.events".to_owned(),
                bounded_count(event_count),
            ),
            (
                "commit.contract.acknowledged".to_owned(),
                bounded_count(acknowledged_commits),
            ),
            (
                "commit.contract.recovered".to_owned(),
                bounded_count(recovered_commits),
            ),
            (
                "commit.contract.retries".to_owned(),
                bounded_count(retry_count),
            ),
            (
                "commit.contract.leader_only_attempts".to_owned(),
                bounded_count(leader_only_attempts),
            ),
        ]),
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
