use crate::api::CommittedEnvelope;
use crate::chess::ChessState;
use crate::delta::{encode_reset, record_label};
use crate::store::PrototypeKernel;
use okv_consensus::{run_raft_process_payload_contract, RaftProcessMode, RaftProcessPayloads};
use std::env;
use std::time::Instant;

pub fn run_consensus_golden() -> Result<(), String> {
    let mut kernel = PrototypeKernel::bootstrap()?;
    kernel.apply_move("e2e4")?;
    kernel.apply_move("e7e5")?;
    let expected = kernel.read_state(None)?;
    let payloads = kernel.committed_envelope_bytes();
    let [initial, uncertain, final_payload] = payloads.as_slice() else {
        return Err("Chess G3 requires exactly three canonical envelopes".to_owned());
    };
    let canonical_envelope_atomic = payloads.iter().all(|payload| {
        CommittedEnvelope::decode(payload).is_ok_and(|envelope| {
            !envelope.application_record.is_empty() && !envelope.mutations.is_empty()
        })
    });
    if !canonical_envelope_atomic {
        return Err("Chess G3 envelope omitted an atomic component".to_owned());
    }
    let executable = env::current_exe().map_err(|error| error.to_string())?;
    let started = Instant::now();
    let report = run_raft_process_payload_contract(
        22_071,
        RaftProcessMode::Correct,
        &executable,
        RaftProcessPayloads {
            initial: initial.clone(),
            uncertain: uncertain.clone(),
            final_payload: final_payload.clone(),
        },
    )?;
    let elapsed = started.elapsed();
    let mut replayed = ChessState::default();
    for payload in &payloads {
        let envelope = CommittedEnvelope::decode(payload)?;
        let record = envelope.application_record;
        if record == encode_reset() {
            replayed = ChessState::default();
        } else {
            replayed = replayed.apply_move(&record_label(&record)?)?.0;
        }
    }
    let exact_game_replay = replayed == expected;
    if report.anomaly_count != 0 || !exact_game_replay || report.caught_up_nodes != 3 {
        return Err("Chess G3 process consensus failed".to_owned());
    }
    let envelope_bytes = payloads.iter().map(Vec::len).sum::<usize>();
    println!(
        "{{\"workload\":\"chess-process-consensus-v1\",\"rung\":\"G3\",\"status\":\"VERIFIED\",\"scope\":\"three-process-openraft-one-host\",\"independent_host_durability\":false,\"canonical_envelope_atomic\":true,\"commits\":3,\"canonical_envelope_bytes\":{},\"scenario_duration_millis\":{},\"actions_per_second\":{:.2},\"anomalies\":{},\"elections\":{},\"process_starts\":{},\"process_kills\":{},\"dropped_replies\":{},\"duplicate_retries\":{},\"recovered_outcomes\":{},\"caught_up_nodes\":{},\"exact_game_replay\":true,\"trace_sha256\":\"{}\",\"fingerprint\":\"{}\"}}",
        envelope_bytes,
        elapsed.as_millis(),
        3.0 / elapsed.as_secs_f64(),
        report.anomaly_count,
        report.elections,
        report.process_starts,
        report.process_kills,
        report.dropped_replies,
        report.duplicate_retries,
        report.recovered_outcomes,
        report.caught_up_nodes,
        report.trace_sha256,
        replayed.fingerprint()
    );
    Ok(())
}
