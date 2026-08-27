mod api;
mod chess;
mod delta;
mod durable;
mod object_history;
mod store;
mod web;

use okv_model::Version;
use std::env;
use store::PrototypeKernel;

fn main() -> Result<(), String> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments
        .first()
        .is_some_and(|argument| argument == "consensus-node")
    {
        return run_consensus_node(&arguments);
    }
    if arguments
        .iter()
        .any(|argument| argument == "--delta-golden")
    {
        return delta::run_golden();
    }
    if arguments
        .iter()
        .any(|argument| argument == "--consensus-golden")
    {
        return durable::run_consensus_golden();
    }
    if arguments
        .iter()
        .any(|argument| argument == "--object-golden")
    {
        return object_history::run_object_golden();
    }
    if arguments
        .iter()
        .any(|argument| argument == "--branch-gc-golden")
    {
        return object_history::run_branch_gc_golden();
    }
    if arguments
        .iter()
        .any(|argument| argument == "--transactional-golden")
    {
        return run_transactional_golden();
    }
    if arguments.iter().any(|argument| argument == "--golden") {
        return run_golden();
    }
    if arguments
        .iter()
        .any(|argument| argument == "--serving-golden")
    {
        return run_serving_golden();
    }
    let kernel = PrototypeKernel::bootstrap()?;
    if arguments.iter().any(|argument| argument == "--web") {
        let port = argument_value(&arguments, "--port").map_or(Ok(4268), |value| {
            value.parse::<u16>().map_err(|error| error.to_string())
        })?;
        return web::serve(kernel, port);
    }
    println!("objectKV Chess state lab");
    println!("run with --web [--port 4268] or --golden");
    Ok(())
}

fn run_transactional_golden() -> Result<(), String> {
    let mut kernel = PrototypeKernel::bootstrap()?;
    kernel.apply_move("e2e4")?;
    kernel.apply_move("e7e5")?;
    kernel.apply_move("g1f3")?;
    kernel.apply_move("b8c6")?;
    let main_fingerprint = kernel.read_state(None)?.fingerprint();
    let branch = kernel.fork_from(Version::new(3))?;
    kernel.apply_move("c2c4")?;
    let materialized = kernel.read_state(None)?;
    let branch_fingerprint = materialized.fingerprint();
    kernel.switch_branch("main")?;
    let main_switch_exact = kernel.read_state(None)?.fingerprint() == main_fingerprint;
    kernel.switch_branch(&branch)?;
    let branch_switch_exact = kernel.read_state(None)?.fingerprint() == branch_fingerprint;
    let application_replay = kernel.replay_application_history()?;
    let application_replay_exact = application_replay == materialized;
    let atomic_abort = kernel.verify_atomic_abort()?;
    let stats = kernel.stats()?;
    let records_aligned = stats.txlog_entries.saturating_mul(4) == stats.application_record_bytes;
    if !main_switch_exact
        || !branch_switch_exact
        || !application_replay_exact
        || !atomic_abort
        || !records_aligned
    {
        return Err("Chess G2 transactional application history failed".to_owned());
    }
    println!(
        "{{\"workload\":\"chess-transactional-history-v1\",\"rung\":\"G2\",\"status\":\"VERIFIED\",\"scope\":\"single-process-transaction-model\",\"commits\":{},\"application_record_bytes\":{},\"application_record_bytes_per_commit\":4,\"txlog_bytes\":{},\"record_and_mutations_atomic\":true,\"atomic_abort_no_effect\":{},\"records_aligned\":{},\"application_replay_exact\":{},\"main_switch_exact\":{},\"branch_switch_exact\":{},\"fingerprint\":\"{}\"}}",
        stats.txlog_entries,
        stats.application_record_bytes,
        stats.txlog_bytes,
        atomic_abort,
        records_aligned,
        application_replay_exact,
        main_switch_exact,
        branch_switch_exact,
        application_replay.fingerprint()
    );
    Ok(())
}

fn argument_value<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    let position = arguments.iter().position(|argument| argument == name)?;
    arguments.get(position + 1).map(String::as_str)
}

fn run_consensus_node(arguments: &[String]) -> Result<(), String> {
    let encoded = argument_value(arguments, "--config-json")
        .ok_or_else(|| "consensus-node requires --config-json".to_owned())?;
    let config = serde_json::from_str(encoded).map_err(|error| error.to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(okv_consensus::run_process_node(config))
}

fn run_golden() -> Result<(), String> {
    let mut kernel = PrototypeKernel::bootstrap()?;
    kernel.apply_move("e2e4")?;
    kernel.apply_move("e7e5")?;
    kernel.apply_move("g1f3")?;
    kernel.apply_move("b8c6")?;

    let main_version = kernel.latest_version();
    let main_fingerprint = kernel.read_state(None)?.fingerprint();
    let historical_fingerprint = kernel.read_state(Some(Version::new(3)))?.fingerprint();
    let forward_fingerprint = kernel.read_state(Some(main_version))?.fingerprint();
    let snapshot_round_trip = main_fingerprint == forward_fingerprint;

    let branch = kernel.fork_from(Version::new(3))?;
    kernel.apply_move("c2c4")?;
    let branch_fingerprint = kernel.read_state(None)?.fingerprint();
    let branch_diverged =
        branch_fingerprint != main_fingerprint && branch_fingerprint != historical_fingerprint;

    kernel.switch_branch("main")?;
    let main_switch_exact = kernel.read_state(None)?.fingerprint() == main_fingerprint;
    kernel.switch_branch(&branch)?;
    let branch_switch_exact = kernel.read_state(None)?.fingerprint() == branch_fingerprint;
    kernel.recover_from_txlog()?;
    let replay_exact = kernel.read_state(None)?.fingerprint() == branch_fingerprint;
    let stats = kernel.stats()?;
    let passed = snapshot_round_trip
        && branch_diverged
        && main_switch_exact
        && branch_switch_exact
        && replay_exact;
    if !passed {
        return Err("Chess golden path failed".to_owned());
    }
    let bytes_per_commit = if stats.txlog_entries == 0 {
        0
    } else {
        stats.txlog_bytes / stats.txlog_entries
    };
    println!(
        "{{\"workload\":\"chess-history-v1\",\"record_kind\":\"materialized-kv\",\"api_version\":\"{}\",\"status\":\"VERIFIED\",\"scope\":\"single-process-volatile\",\"snapshot_round_trip\":{},\"branch_diverged\":{},\"main_switch_exact\":{},\"branch_switch_exact\":{},\"replay_exact\":{},\"active_branch\":\"{}\",\"branches\":{},\"txlog_entries\":{},\"txlog_bytes\":{},\"txlog_bytes_per_commit\":{},\"fingerprint\":\"{}\"}}",
        api::API_VERSION,
        snapshot_round_trip,
        branch_diverged,
        main_switch_exact,
        branch_switch_exact,
        replay_exact,
        kernel.active_branch(),
        stats.branches.len(),
        stats.txlog_entries,
        stats.txlog_bytes,
        bytes_per_commit,
        branch_fingerprint
    );
    Ok(())
}

fn run_serving_golden() -> Result<(), String> {
    let mut kernel = PrototypeKernel::bootstrap()?;
    kernel.apply_move("e2e4")?;
    kernel.apply_move("e7e5")?;
    kernel.apply_move("g1f3")?;
    kernel.apply_move("b8c6")?;
    let main_fingerprint = kernel.read_state(None)?.fingerprint();
    let branch = kernel.fork_from(Version::new(3))?;
    kernel.apply_move("c2c4")?;
    let latest = kernel.latest_version();
    let expected = kernel.read_state(Some(latest))?;
    let historical = kernel.read_state(Some(Version::new(2)))?;
    for _ in 0..1_000 {
        let _ = kernel.read_state(Some(latest))?;
    }
    let measured_reads = 20_000_u32;
    let measured_started = std::time::Instant::now();
    let mut read_latencies_nanos = Vec::with_capacity(measured_reads as usize);
    for _ in 0..measured_reads {
        let read_started = std::time::Instant::now();
        let state = kernel.read_state(Some(latest))?;
        read_latencies_nanos.push(read_started.elapsed().as_nanos());
        if state != expected {
            return Err("Chess RAM serving read diverged".to_owned());
        }
    }
    let measured_elapsed = measured_started.elapsed();
    read_latencies_nanos.sort_unstable();
    let rebuild_started = std::time::Instant::now();
    kernel.discard_and_rebuild_serving_image()?;
    let rebuild_micros = rebuild_started.elapsed().as_micros();
    let rebuild_exact = kernel.read_state(None)? == expected;
    let historical_after_rebuild = kernel.read_state(Some(Version::new(2)))? == historical;
    kernel.switch_branch("main")?;
    let main_switch_exact = kernel.read_state(None)?.fingerprint() == main_fingerprint;
    kernel.switch_branch(&branch)?;
    let branch_switch_exact = kernel.read_state(None)? == expected;
    let stats = kernel.stats()?;
    if !rebuild_exact || !historical_after_rebuild || !main_switch_exact || !branch_switch_exact {
        return Err("Chess RAM serving lifecycle diverged".to_owned());
    }
    println!(
        "{{\"workload\":\"chess-ram-serving-v1\",\"rung\":\"G4\",\"status\":\"VERIFIED\",\"scope\":\"single-process-ram-serving-image\",\"serving_profile\":\"ram\",\"ssd_profile_status\":\"PROPOSED\",\"measured_reads\":{},\"reads_per_second\":{:.2},\"point_read_latency_nanos_p50\":{},\"point_read_latency_nanos_p95\":{},\"point_read_latency_nanos_p99\":{},\"rebuild_micros\":{},\"discarded_image_rebuild_exact\":true,\"historical_read_after_rebuild_exact\":true,\"main_switch_exact\":true,\"branch_switch_exact\":true,\"txlog_bytes\":{},\"visible_rows\":{},\"local_data_files\":0,\"fingerprint\":\"{}\"}}",
        measured_reads,
        f64::from(measured_reads) / measured_elapsed.as_secs_f64(),
        latency_percentile(&read_latencies_nanos, 50),
        latency_percentile(&read_latencies_nanos, 95),
        latency_percentile(&read_latencies_nanos, 99),
        rebuild_micros,
        stats.txlog_bytes,
        stats.visible_rows,
        expected.fingerprint()
    );
    Ok(())
}

fn latency_percentile(sorted_values: &[u128], percentile: usize) -> u128 {
    let index = sorted_values
        .len()
        .saturating_sub(1)
        .saturating_mul(percentile)
        / 100;
    sorted_values.get(index).copied().unwrap_or_default()
}
