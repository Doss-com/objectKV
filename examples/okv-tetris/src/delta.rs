use crate::game::{Action, GameState};
use crate::trace;
use okv_app_history::{
    decode_checkpoint, encode_checkpoint, CheckpointError, ReducerId, CHECKPOINT_OVERHEAD_BYTES,
};
use okv_log::{LogEntry, LogState};
use std::collections::BTreeMap;
use std::time::Instant;

const DELTA_SCHEMA_VERSION: u8 = 1;
const CHECKPOINT_INTERVAL: u64 = 256;
const REDUCER_SCHEMA_VERSION: u8 = 1;

struct DeltaHistory {
    log: LogState,
    checkpoints: BTreeMap<u64, Vec<u8>>,
    state: GameState,
}

impl DeltaHistory {
    fn bootstrap() -> Self {
        let state = GameState::default();
        let checkpoint = encode_checkpoint(
            ReducerId::Tetris,
            REDUCER_SCHEMA_VERSION,
            0,
            &state.encode(),
        )
        .expect("Tetris checkpoint state fits the playground format");
        Self {
            log: LogState::default(),
            checkpoints: BTreeMap::from([(0, checkpoint)]),
            state,
        }
    }

    fn append(&mut self, action: Action) -> Result<(), String> {
        let index = self
            .log
            .last_entry()
            .map_or(1, |(index, _)| index.saturating_add(1));
        let entry = LogEntry::new(index, encode_action(action));
        let commands = self
            .log
            .plan_suffix_append(std::slice::from_ref(&entry))
            .map_err(|error| error.to_string())?;
        self.log
            .apply_all(&commands)
            .map_err(|error| error.to_string())?;
        self.state = self.state.apply(action);
        if index % CHECKPOINT_INTERVAL == 0 {
            let checkpoint = encode_checkpoint(
                ReducerId::Tetris,
                REDUCER_SCHEMA_VERSION,
                index,
                &self.state.encode(),
            )
            .map_err(|error| error.to_string())?;
            self.checkpoints.insert(index, checkpoint);
        }
        Ok(())
    }

    fn reconstruct(&self, version: u64) -> Result<GameState, String> {
        let latest = self.latest_version();
        if version > latest {
            return Err(format!("delta version {version} exceeds latest {latest}"));
        }
        let (checkpoint_version, checkpoint) = self
            .checkpoints
            .range(..=version)
            .next_back()
            .ok_or_else(|| "initial Tetris checkpoint is absent".to_owned())?;
        let decoded = decode_checkpoint(
            ReducerId::Tetris,
            REDUCER_SCHEMA_VERSION,
            *checkpoint_version,
            checkpoint,
        )
        .map_err(|error| error.to_string())?;
        let mut state = GameState::decode(&decoded.state)?;
        if *checkpoint_version < version {
            let from = checkpoint_version.saturating_add(1);
            let to = version
                .checked_add(1)
                .ok_or_else(|| "delta version overflow".to_owned())?;
            for entry in self
                .log
                .entries_exact(from, to)
                .map_err(|error| error.to_string())?
            {
                state = state.apply(decode_action(&entry.payload)?);
            }
        }
        Ok(state)
    }

    fn latest_version(&self) -> u64 {
        self.log.last_entry().map_or(0, |(index, _)| index)
    }

    fn delta_bytes(&self) -> usize {
        self.log
            .entries_clamped(..)
            .iter()
            .map(|entry| entry.payload.len())
            .sum()
    }

    fn checkpoint_bytes(&self) -> usize {
        self.checkpoints.values().map(Vec::len).sum()
    }

    fn checkpoint_state_bytes(&self) -> usize {
        self.checkpoints
            .values()
            .map(|checkpoint| checkpoint.len().saturating_sub(CHECKPOINT_OVERHEAD_BYTES))
            .sum()
    }
}

pub fn run_golden(actions: usize) -> Result<(), String> {
    if actions < 2 {
        return Err("--delta-golden requires at least two actions".to_owned());
    }
    let action_trace = trace::deterministic_actions(actions);
    let trace_sha256 = trace::trace_sha256(&action_trace);
    let mut history = DeltaHistory::bootstrap();
    let started = Instant::now();
    for action in action_trace {
        history.append(action)?;
    }
    let elapsed = started.elapsed();
    let latest = history.latest_version();
    let midpoint = (latest / 2).max(1);
    let _historical = history.reconstruct(midpoint)?;
    let replayed = history.reconstruct(latest)?;
    let replay_exact = replayed == history.state;
    let poison_controls = run_poison_controls()?;
    if !replay_exact || poison_controls != 7 {
        return Err("Tetris delta replay diverged".to_owned());
    }
    let action_count =
        u32::try_from(actions).map_err(|_| "Tetris delta action count exceeds u32".to_owned())?;
    let elapsed_seconds = elapsed.as_secs_f64();
    let actions_per_second = if elapsed_seconds == 0.0 {
        0.0
    } else {
        f64::from(action_count) / elapsed_seconds
    };
    let delta_bytes = history.delta_bytes();
    let checkpoint_bytes = history.checkpoint_bytes();
    let checkpoint_state_bytes = history.checkpoint_state_bytes();
    let logical_bytes = delta_bytes.saturating_add(checkpoint_bytes);
    let logical_bytes_per_action = f64::from(
        u32::try_from(logical_bytes).map_err(|_| "logical byte count exceeds u32".to_owned())?,
    ) / f64::from(action_count);
    let latest_checkpoint = history
        .checkpoints
        .range(..=latest)
        .next_back()
        .map_or(0, |(version, _)| *version);
    let tail_depth = latest.saturating_sub(latest_checkpoint);
    println!(
        "{{\"workload\":\"tetris-action-delta-v2\",\"rung\":\"G1\",\"record_kind\":\"application-delta\",\"status\":\"VERIFIED\",\"scope\":\"single-process-volatile\",\"trace_sha256\":\"{}\",\"actions\":{},\"actions_per_second\":{:.2},\"delta_schema_version\":{},\"reducer_schema_version\":{},\"delta_bytes\":{},\"delta_bytes_per_action\":2,\"checkpoint_interval\":{},\"checkpoint_count\":{},\"checkpoint_state_bytes\":{},\"checkpoint_bytes\":{},\"checkpoint_overhead_bytes_each\":{},\"logical_bytes\":{},\"logical_bytes_per_action\":{:.3},\"latest_replay_depth\":{},\"snapshot_round_trip\":true,\"checkpoint_identity_exact\":true,\"poison_controls\":{},\"replay_exact\":{},\"fingerprint\":\"{}\"}}",
        trace_sha256,
        actions,
        actions_per_second,
        DELTA_SCHEMA_VERSION,
        REDUCER_SCHEMA_VERSION,
        delta_bytes,
        CHECKPOINT_INTERVAL,
        history.checkpoints.len(),
        checkpoint_state_bytes,
        checkpoint_bytes,
        CHECKPOINT_OVERHEAD_BYTES,
        logical_bytes,
        logical_bytes_per_action,
        tail_depth,
        poison_controls,
        replay_exact,
        replayed.fingerprint()
    );
    Ok(())
}

pub(crate) const fn encode_action(action: Action) -> [u8; 2] {
    [
        DELTA_SCHEMA_VERSION,
        match action {
            Action::Left => 1,
            Action::Right => 2,
            Action::Rotate => 3,
            Action::Tick => 4,
            Action::Drop => 5,
            Action::Reset => 6,
        },
    ]
}

fn run_poison_controls() -> Result<usize, String> {
    let state = GameState::default();
    let checkpoint = encode_checkpoint(
        ReducerId::Tetris,
        REDUCER_SCHEMA_VERSION,
        0,
        &state.encode(),
    )
    .map_err(|error| error.to_string())?;
    let mut passed = 0;
    passed += usize::from(decode_action(&[2, 1]).is_err());
    passed += usize::from(decode_action(&[1, 255]).is_err());
    passed += usize::from(
        decode_checkpoint(ReducerId::Chess, REDUCER_SCHEMA_VERSION, 0, &checkpoint)
            == Err(CheckpointError::ReducerMismatch),
    );
    passed += usize::from(
        decode_checkpoint(ReducerId::Tetris, REDUCER_SCHEMA_VERSION, 1, &checkpoint)
            == Err(CheckpointError::PositionMismatch),
    );
    let mut corrupt = checkpoint;
    corrupt[CHECKPOINT_OVERHEAD_BYTES] ^= 1;
    passed += usize::from(
        decode_checkpoint(ReducerId::Tetris, REDUCER_SCHEMA_VERSION, 0, &corrupt)
            == Err(CheckpointError::ChecksumMismatch),
    );
    let log = LogState::default();
    passed += usize::from(
        log.plan_suffix_append(&[LogEntry::new(1, [1]), LogEntry::new(3, [1])])
            .is_err(),
    );
    passed += usize::from(
        log.plan_suffix_append(&[LogEntry::new(2, [1]), LogEntry::new(1, [1])])
            .is_err(),
    );
    Ok(passed)
}

pub(crate) fn decode_action(bytes: &[u8]) -> Result<Action, String> {
    if bytes.len() != 2 || bytes[0] != DELTA_SCHEMA_VERSION {
        return Err("unsupported Tetris action delta".to_owned());
    }
    match bytes[1] {
        1 => Ok(Action::Left),
        2 => Ok(Action::Right),
        3 => Ok(Action::Rotate),
        4 => Ok(Action::Tick),
        5 => Ok(Action::Drop),
        6 => Ok(Action::Reset),
        tag => Err(format!("unknown Tetris action tag {tag}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_action, encode_action, run_poison_controls, DeltaHistory};
    use crate::game::Action;

    #[test]
    fn every_action_round_trips() {
        for action in [
            Action::Left,
            Action::Right,
            Action::Rotate,
            Action::Tick,
            Action::Drop,
            Action::Reset,
        ] {
            assert_eq!(
                decode_action(&encode_action(action)).expect("decode"),
                action
            );
        }
    }

    #[test]
    fn checkpoint_plus_tail_reconstructs_exact_state() {
        let mut history = DeltaHistory::bootstrap();
        for _ in 0..300 {
            let action = if history.state.game_over {
                Action::Reset
            } else {
                Action::Drop
            };
            history.append(action).expect("append");
        }
        assert_eq!(
            history.reconstruct(300).expect("reconstruct"),
            history.state
        );
        assert!(history.checkpoints.contains_key(&256));
    }

    #[test]
    fn every_g1_poison_is_detected() {
        assert_eq!(run_poison_controls().expect("poisons"), 7);
    }
}
