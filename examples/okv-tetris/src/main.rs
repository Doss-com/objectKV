mod api;
mod delta;
mod durable;
mod game;
mod object_history;
mod store;
mod trace;
mod web;

use game::{Action, GameState, HEIGHT, WIDTH};
use okv_model::Version;
use std::env;
use std::io::{self, Write};
use std::time::Instant;
use store::{KernelStats, PrototypeKernel};

fn main() -> Result<(), String> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments
        .first()
        .is_some_and(|argument| argument == "consensus-node")
    {
        return run_consensus_node(&arguments);
    }
    if let Some(position) = arguments
        .iter()
        .position(|argument| argument == "--delta-golden")
    {
        let actions = arguments.get(position + 1).map_or(Ok(2_000), |value| {
            value.parse::<usize>().map_err(|error| error.to_string())
        })?;
        return delta::run_golden(actions);
    }
    if arguments
        .iter()
        .any(|argument| argument == "--consensus-golden")
    {
        return durable::run_consensus_golden();
    }
    if let Some(position) = arguments
        .iter()
        .position(|argument| argument == "--object-golden")
    {
        let actions = arguments.get(position + 1).map_or(Ok(512), |value| {
            value.parse::<usize>().map_err(|error| error.to_string())
        })?;
        return object_history::run_object_golden(actions);
    }
    if let Some(position) = arguments
        .iter()
        .position(|argument| argument == "--branch-gc-golden")
    {
        let actions = arguments.get(position + 1).map_or(Ok(512), |value| {
            value.parse::<usize>().map_err(|error| error.to_string())
        })?;
        return object_history::run_branch_gc_golden(actions);
    }
    let mut kernel = PrototypeKernel::bootstrap()?;
    if let Some(position) = arguments
        .iter()
        .position(|argument| argument == "--serving-golden")
    {
        let actions = arguments.get(position + 1).map_or(Ok(2_000), |value| {
            value.parse::<usize>().map_err(|error| error.to_string())
        })?;
        return run_serving_golden(&mut kernel, actions);
    }
    if let Some(position) = arguments
        .iter()
        .position(|argument| argument == "--transactional-golden")
    {
        let actions = arguments.get(position + 1).map_or(Ok(2_000), |value| {
            value.parse::<usize>().map_err(|error| error.to_string())
        })?;
        return run_transactional_golden(&mut kernel, actions);
    }
    if let Some(position) = arguments.iter().position(|argument| argument == "--golden") {
        let actions = arguments.get(position + 1).map_or(Ok(2_000), |value| {
            value.parse::<usize>().map_err(|error| error.to_string())
        })?;
        return run_golden(&mut kernel, actions);
    }
    if arguments.iter().any(|argument| argument == "--web") {
        let port = argument_value(&arguments, "--port").map_or(Ok(4267), |value| {
            value.parse::<u16>().map_err(|error| error.to_string())
        })?;
        return web::serve(kernel, port);
    }
    if let Some(position) = arguments.iter().position(|argument| argument == "--script") {
        let script = arguments
            .get(position + 1)
            .ok_or_else(|| "--script requires a comma-separated command list".to_owned())?;
        return run_script(&mut kernel, script);
    }
    run_interactive(&mut kernel)
}

fn run_transactional_golden(kernel: &mut PrototypeKernel, actions: usize) -> Result<(), String> {
    if actions < 2 {
        return Err("--transactional-golden requires at least two actions".to_owned());
    }
    let action_trace = trace::deterministic_actions(actions);
    let trace_sha256 = trace::trace_sha256(&action_trace);
    let started = Instant::now();
    for (index, action) in action_trace.into_iter().enumerate() {
        kernel.apply_action(action)?;
        if index == actions / 2 {
            kernel.fork()?;
        }
    }
    let elapsed = started.elapsed();
    let materialized = kernel.read_game(None)?;
    let application_replay = kernel.replay_application_history()?;
    let atomic_abort = kernel.verify_atomic_abort()?;
    let stats = kernel.stats()?;
    let records_aligned = stats.txlog_entries.saturating_mul(2) == stats.application_record_bytes;
    let replay_exact = materialized == application_replay;
    if !atomic_abort || !records_aligned || !replay_exact {
        return Err("Tetris G2 transactional application history failed".to_owned());
    }
    let action_count = u32::try_from(actions)
        .map_err(|_| "Tetris transactional action count exceeds u32".to_owned())?;
    let elapsed_seconds = elapsed.as_secs_f64();
    let actions_per_second = if elapsed_seconds == 0.0 {
        0.0
    } else {
        f64::from(action_count) / elapsed_seconds
    };
    println!(
        "{{\"workload\":\"tetris-transactional-history-v1\",\"rung\":\"G2\",\"status\":\"VERIFIED\",\"scope\":\"single-process-transaction-model\",\"trace_sha256\":\"{}\",\"actions\":{},\"actions_per_second\":{:.2},\"commits\":{},\"application_record_bytes\":{},\"application_record_bytes_per_commit\":2,\"txlog_bytes\":{},\"record_and_mutations_atomic\":true,\"atomic_abort_no_effect\":{},\"records_aligned\":{},\"application_replay_exact\":{},\"fingerprint\":\"{}\"}}",
        trace_sha256,
        actions,
        actions_per_second,
        stats.txlog_entries,
        stats.application_record_bytes,
        stats.txlog_bytes,
        atomic_abort,
        records_aligned,
        replay_exact,
        application_replay.fingerprint()
    );
    Ok(())
}

fn run_golden(kernel: &mut PrototypeKernel, actions: usize) -> Result<(), String> {
    if actions < 2 {
        return Err("--golden requires at least two actions".to_owned());
    }
    let action_trace = trace::deterministic_actions(actions);
    let trace_sha256 = trace::trace_sha256(&action_trace);
    let started = Instant::now();
    for (index, action) in action_trace.into_iter().enumerate() {
        kernel.apply_action(action)?;
        if index == actions / 2 {
            kernel.fork()?;
        }
    }
    let elapsed = started.elapsed();
    let latest = kernel.latest_version();
    let latest_state = kernel.read_game(Some(latest))?;
    let midpoint = Version::new((latest.sequence() / 2).max(1));
    let _historical = kernel.read_game(Some(midpoint))?;
    let forward_state = kernel.read_game(Some(latest))?;
    let snapshot_round_trip = latest_state == forward_state;
    kernel.recover_from_txlog()?;
    let replay_exact = kernel.read_game(None)? == latest_state;
    let stats = kernel.stats()?;
    let passed = snapshot_round_trip && replay_exact;
    if !passed {
        return Err("Tetris golden path failed".to_owned());
    }
    let elapsed_seconds = elapsed.as_secs_f64();
    let actions_per_second = if elapsed_seconds == 0.0 {
        0.0
    } else {
        let action_count = u32::try_from(actions)
            .map_err(|_| "Tetris golden action count exceeds u32".to_owned())?;
        f64::from(action_count) / elapsed_seconds
    };
    let bytes_per_commit = if stats.txlog_entries == 0 {
        0
    } else {
        stats.txlog_bytes / stats.txlog_entries
    };
    println!(
        "{{\"workload\":\"tetris-state-rate-v1\",\"record_kind\":\"materialized-kv\",\"api_version\":\"{}\",\"status\":\"VERIFIED\",\"scope\":\"single-process-volatile\",\"trace_sha256\":\"{}\",\"actions\":{},\"actions_per_second\":{:.2},\"snapshot_round_trip\":{},\"replay_exact\":{},\"active_branch\":\"{}\",\"branches\":{},\"txlog_entries\":{},\"txlog_bytes\":{},\"txlog_bytes_per_commit\":{},\"recoveries\":{},\"fingerprint\":\"{}\"}}",
        api::API_VERSION,
        trace_sha256,
        actions,
        actions_per_second,
        snapshot_round_trip,
        replay_exact,
        stats.branch,
        stats.branches.len(),
        stats.txlog_entries,
        stats.txlog_bytes,
        bytes_per_commit,
        stats.recoveries,
        latest_state.fingerprint()
    );
    Ok(())
}

fn run_serving_golden(kernel: &mut PrototypeKernel, actions: usize) -> Result<(), String> {
    if actions < 2 {
        return Err("--serving-golden requires at least two actions".to_owned());
    }
    let action_trace = trace::deterministic_actions(actions);
    let trace_sha256 = trace::trace_sha256(&action_trace);
    for (index, action) in action_trace.into_iter().enumerate() {
        kernel.apply_action(action)?;
        if index == actions / 2 {
            kernel.fork()?;
        }
    }
    let latest = kernel.latest_version();
    let expected = kernel.read_game(Some(latest))?;
    let historical_version = Version::new((latest.sequence() / 2).max(1));
    let historical = kernel.read_game(Some(historical_version))?;
    for _ in 0..1_000 {
        let _ = kernel.read_game(Some(latest))?;
    }
    let measured_reads = 20_000_u32;
    let measured_started = Instant::now();
    let mut read_latencies_nanos = Vec::with_capacity(measured_reads as usize);
    for _ in 0..measured_reads {
        let read_started = Instant::now();
        let state = kernel.read_game(Some(latest))?;
        read_latencies_nanos.push(read_started.elapsed().as_nanos());
        if state != expected {
            return Err("Tetris RAM serving read diverged".to_owned());
        }
    }
    let measured_elapsed = measured_started.elapsed();
    read_latencies_nanos.sort_unstable();
    let rebuild_started = Instant::now();
    kernel.discard_and_rebuild_serving_image()?;
    let rebuild_micros = rebuild_started.elapsed().as_micros();
    let rebuild_exact = kernel.read_game(None)? == expected;
    let historical_after_rebuild = kernel.read_game(Some(historical_version))? == historical;
    let stats = kernel.stats()?;
    if !rebuild_exact || !historical_after_rebuild {
        return Err("Tetris RAM serving rebuild diverged".to_owned());
    }
    println!(
        "{{\"workload\":\"tetris-ram-serving-v1\",\"rung\":\"G4\",\"status\":\"VERIFIED\",\"scope\":\"single-process-ram-serving-image\",\"serving_profile\":\"ram\",\"ssd_profile_status\":\"PROPOSED\",\"trace_sha256\":\"{}\",\"actions\":{},\"measured_reads\":{},\"reads_per_second\":{:.2},\"point_read_latency_nanos_p50\":{},\"point_read_latency_nanos_p95\":{},\"point_read_latency_nanos_p99\":{},\"rebuild_micros\":{},\"discarded_image_rebuild_exact\":true,\"historical_read_after_rebuild_exact\":true,\"txlog_bytes\":{},\"visible_rows\":{},\"local_data_files\":0,\"fingerprint\":\"{}\"}}",
        trace_sha256,
        actions,
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

fn run_interactive(kernel: &mut PrototypeKernel) -> Result<(), String> {
    let mut viewed_version = kernel.latest_version();
    loop {
        render(kernel, viewed_version, true)?;
        print!("command> ");
        io::stdout().flush().map_err(|error| error.to_string())?;
        let mut command = String::new();
        if io::stdin()
            .read_line(&mut command)
            .map_err(|error| error.to_string())?
            == 0
        {
            break;
        }
        match execute(kernel, command.trim(), &mut viewed_version)? {
            Control::Continue => {}
            Control::Quit => break,
        }
    }
    Ok(())
}

fn run_script(kernel: &mut PrototypeKernel, script: &str) -> Result<(), String> {
    let mut viewed_version = kernel.latest_version();
    for command in script
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        if execute(kernel, command, &mut viewed_version)? == Control::Quit {
            break;
        }
    }
    render(kernel, viewed_version, false)?;
    let game = kernel.read_game(Some(viewed_version))?;
    let kernel_stats = kernel.stats()?;
    println!(
        "SCRIPT_OK api={} branch={} version={} score={} lines={} txlog_entries={} recoveries={} branches={}",
        api::API_VERSION,
        kernel_stats.branch,
        viewed_version,
        game.score,
        game.lines,
        kernel_stats.txlog_entries,
        kernel_stats.recoveries,
        kernel_stats.branches.len()
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Control {
    Continue,
    Quit,
}

fn execute(
    kernel: &mut PrototypeKernel,
    command: &str,
    viewed_version: &mut Version,
) -> Result<Control, String> {
    let action = match command {
        "a" | "left" => Some(Action::Left),
        "d" | "right" => Some(Action::Right),
        "w" | "rotate" => Some(Action::Rotate),
        "s" | "tick" => Some(Action::Tick),
        "f" | "drop" => Some(Action::Drop),
        "n" | "reset" => Some(Action::Reset),
        "x" | "recover" => {
            kernel.recover_from_txlog()?;
            *viewed_version = kernel.latest_version();
            None
        }
        "b" | "branch" => {
            kernel.fork()?;
            *viewed_version = kernel.latest_version();
            None
        }
        "[" | "older" => {
            let current = viewed_version.sequence();
            if current > 1 {
                *viewed_version = Version::new(current - 1);
            }
            None
        }
        "]" | "newer" => {
            let latest = kernel.latest_version().sequence();
            if viewed_version.sequence() < latest {
                *viewed_version = Version::new(viewed_version.sequence() + 1);
            }
            None
        }
        "q" | "quit" => return Ok(Control::Quit),
        "" => None,
        other => return Err(format!("unknown command {other:?}")),
    };
    if let Some(action) = action {
        kernel.apply_action(action)?;
        *viewed_version = kernel.latest_version();
    }
    Ok(Control::Continue)
}

fn render(
    kernel: &mut PrototypeKernel,
    viewed_version: Version,
    clear: bool,
) -> Result<(), String> {
    if clear {
        print!("\x1b[2J\x1b[H");
    }
    let game = kernel.read_game(Some(viewed_version))?;
    let kernel_stats = kernel.stats()?;
    println!("objectKV TETRIS boundary playground");
    println!("===================================");
    println!(
        "BACKEND  okv-model + okv-log (real crates)\nTOPOLOGY one process, in memory\nDURABILITY none, crash is simulated replay\nAPI      {}",
        api::API_VERSION
    );
    println!();
    render_board(&game);
    println!(
        "score={}  lines={}  ticks={}  piece={}  game_over={}",
        game.score,
        game.lines,
        game.ticks,
        game.active.kind.glyph(),
        game.game_over
    );
    println!();
    render_storage(&kernel_stats, viewed_version);
    println!();
    println!("[a] left  [d] right  [w] rotate  [s] tick  [f] drop  [n] reset");
    println!("[[] older snapshot  []] newer snapshot  [x] crash/replay  [b] fork  [q] quit");
    Ok(())
}

fn render_board(state: &GameState) {
    let board = state.visible_board();
    println!("┌{}┐", "──".repeat(WIDTH));
    for row in board.iter().take(HEIGHT) {
        print!("│");
        for cell in row.iter().take(WIDTH) {
            if *cell == 0 {
                print!("  ");
            } else {
                print!("{} ", piece_glyph(*cell));
            }
        }
        println!("│");
    }
    println!("└{}┘", "──".repeat(WIDTH));
}

fn render_storage(stats: &KernelStats, viewed_version: Version) {
    println!("objectKV state");
    println!(
        "  branch          {} of {}",
        stats.branch,
        stats.branches.len()
    );
    println!(
        "  snapshot        {} (latest {})",
        viewed_version, stats.latest_version
    );
    println!(
        "  reads           {} point, {} range",
        stats.point_reads, stats.range_reads
    );
    println!(
        "  visible rows    {} view, {} application-log events",
        stats.visible_rows, stats.event_rows
    );
    println!(
        "  txLog           {} entries, {} bytes, {} recoveries",
        stats.txlog_entries, stats.txlog_bytes, stats.recoveries
    );
    println!("  last operation  {}", stats.last_action);
    if let Some(receipt) = &stats.last_receipt {
        println!(
            "  receipt         version={} request={} mutations={} log_index={} replayed={}",
            receipt.commit_version,
            receipt.request_id,
            receipt.mutation_count,
            receipt.txlog_index,
            receipt.replayed
        );
    }
    println!("  object requests 0 (object publication is not wired into this example)");
}

fn piece_glyph(value: u8) -> char {
    match value {
        1 => 'I',
        2 => 'O',
        3 => 'T',
        4 => 'L',
        5 => 'S',
        6 => 'J',
        7 => 'Z',
        _ => '?',
    }
}
