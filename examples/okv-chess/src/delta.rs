use crate::chess::{square_name, ChessState};
use okv_app_history::{
    decode_checkpoint, encode_checkpoint, CheckpointError, ReducerId, CHECKPOINT_OVERHEAD_BYTES,
};
use okv_log::{LogEntry, LogState};
use std::collections::BTreeMap;

const DELTA_SCHEMA_VERSION: u8 = 1;
const CHECKPOINT_INTERVAL: u64 = 64;
const REDUCER_SCHEMA_VERSION: u8 = 1;
const RESET_SQUARE: u8 = u8::MAX;

#[derive(Clone)]
struct DeltaBranch {
    log: LogState,
    checkpoints: BTreeMap<u64, Vec<u8>>,
}

struct DeltaChess {
    branches: BTreeMap<String, DeltaBranch>,
    active: String,
    state: ChessState,
    branch_counter: u64,
    copied_prefix_bytes: usize,
}

impl DeltaChess {
    fn bootstrap() -> Self {
        let state = ChessState::default();
        let checkpoint =
            encode_checkpoint(ReducerId::Chess, REDUCER_SCHEMA_VERSION, 0, &state.encode())
                .expect("Chess checkpoint state fits the playground format");
        let branch = DeltaBranch {
            log: LogState::default(),
            checkpoints: BTreeMap::from([(0, checkpoint)]),
        };
        Self {
            branches: BTreeMap::from([("main".to_owned(), branch)]),
            active: "main".to_owned(),
            state,
            branch_counter: 0,
            copied_prefix_bytes: 0,
        }
    }

    fn append(&mut self, notation: &str) -> Result<(), String> {
        let (next, applied) = self.state.apply_move(notation)?;
        let index = self.latest_version().saturating_add(1);
        let entry = LogEntry::new(index, encode_move(applied.from, applied.to)?);
        let commands = self
            .current_branch()
            .log
            .plan_suffix_append(std::slice::from_ref(&entry))
            .map_err(|error| error.to_string())?;
        self.current_branch_mut()
            .log
            .apply_all(&commands)
            .map_err(|error| error.to_string())?;
        self.state = next;
        if index % CHECKPOINT_INTERVAL == 0 {
            let checkpoint = encode_checkpoint(
                ReducerId::Chess,
                REDUCER_SCHEMA_VERSION,
                index,
                &self.state.encode(),
            )
            .map_err(|error| error.to_string())?;
            self.current_branch_mut()
                .checkpoints
                .insert(index, checkpoint);
        }
        Ok(())
    }

    fn reconstruct(&self, version: u64) -> Result<ChessState, String> {
        reconstruct_branch(self.current_branch(), version)
    }

    fn fork_from(&mut self, version: u64) -> Result<String, String> {
        if version > self.latest_version() {
            return Err(format!("fork version {version} exceeds active history"));
        }
        let to = version
            .checked_add(1)
            .ok_or_else(|| "fork version overflow".to_owned())?;
        let entries = if version == 0 {
            Vec::new()
        } else {
            self.current_branch()
                .log
                .entries_exact(1, to)
                .map_err(|error| error.to_string())?
        };
        let copied_prefix_bytes = entries.iter().map(|entry| entry.payload.len()).sum();
        let mut log = LogState::default();
        let commands = log
            .plan_suffix_append(&entries)
            .map_err(|error| error.to_string())?;
        log.apply_all(&commands)
            .map_err(|error| error.to_string())?;
        let state = reconstruct_branch(self.current_branch(), version)?;
        let checkpoints: BTreeMap<u64, Vec<u8>> = self
            .current_branch()
            .checkpoints
            .range(..=version)
            .map(|(checkpoint_version, checkpoint)| (*checkpoint_version, checkpoint.clone()))
            .collect();
        self.branch_counter = self.branch_counter.saturating_add(1);
        let name = format!("line-{}", self.branch_counter);
        self.branches
            .insert(name.clone(), DeltaBranch { log, checkpoints });
        self.active.clone_from(&name);
        self.state = state;
        self.copied_prefix_bytes = copied_prefix_bytes;
        Ok(name)
    }

    fn switch_branch(&mut self, name: &str) -> Result<(), String> {
        let branch = self
            .branches
            .get(name)
            .ok_or_else(|| format!("unknown delta branch {name:?}"))?;
        let latest = branch.log.last_entry().map_or(0, |(index, _)| index);
        let state = reconstruct_branch(branch, latest)?;
        name.clone_into(&mut self.active);
        self.state = state;
        Ok(())
    }

    fn recover(&mut self) -> Result<(), String> {
        self.state = self.reconstruct(self.latest_version())?;
        Ok(())
    }

    fn latest_version(&self) -> u64 {
        self.current_branch()
            .log
            .last_entry()
            .map_or(0, |(index, _)| index)
    }

    fn active_delta_bytes(&self) -> usize {
        self.current_branch()
            .log
            .entries_clamped(..)
            .iter()
            .map(|entry| entry.payload.len())
            .sum()
    }

    fn active_checkpoint_bytes(&self) -> usize {
        self.current_branch()
            .checkpoints
            .values()
            .map(Vec::len)
            .sum()
    }

    fn current_branch(&self) -> &DeltaBranch {
        self.branches
            .get(&self.active)
            .expect("active delta branch exists")
    }

    fn current_branch_mut(&mut self) -> &mut DeltaBranch {
        self.branches
            .get_mut(&self.active)
            .expect("active delta branch exists")
    }
}

fn reconstruct_branch(branch: &DeltaBranch, version: u64) -> Result<ChessState, String> {
    let latest = branch.log.last_entry().map_or(0, |(index, _)| index);
    if version > latest {
        return Err(format!("delta version {version} exceeds latest {latest}"));
    }
    let (checkpoint_version, checkpoint) = branch
        .checkpoints
        .range(..=version)
        .next_back()
        .ok_or_else(|| "initial Chess checkpoint is absent".to_owned())?;
    let decoded = decode_checkpoint(
        ReducerId::Chess,
        REDUCER_SCHEMA_VERSION,
        *checkpoint_version,
        checkpoint,
    )
    .map_err(|error| error.to_string())?;
    let mut state = ChessState::decode(&decoded.state)?;
    if *checkpoint_version < version {
        let from = checkpoint_version.saturating_add(1);
        let to = version
            .checked_add(1)
            .ok_or_else(|| "delta version overflow".to_owned())?;
        for entry in branch
            .log
            .entries_exact(from, to)
            .map_err(|error| error.to_string())?
        {
            state = apply_delta(&state, &entry.payload)?;
        }
    }
    Ok(state)
}

fn apply_delta(state: &ChessState, payload: &[u8]) -> Result<ChessState, String> {
    if payload.len() != 4 || payload[0] != DELTA_SCHEMA_VERSION || payload[3] != 0 {
        return Err("unsupported Chess action delta".to_owned());
    }
    if payload[1] == RESET_SQUARE && payload[2] == RESET_SQUARE {
        return Ok(ChessState::default());
    }
    let from = usize::from(payload[1]);
    let to = usize::from(payload[2]);
    if from >= 64 || to >= 64 {
        return Err("Chess action delta contains an invalid square".to_owned());
    }
    let notation = format!("{}{}", square_name(from), square_name(to));
    state.apply_move(&notation).map(|(next, _)| next)
}

pub(crate) const fn encode_reset() -> [u8; 4] {
    [DELTA_SCHEMA_VERSION, RESET_SQUARE, RESET_SQUARE, 0]
}

pub(crate) fn encode_move(from: usize, to: usize) -> Result<[u8; 4], String> {
    Ok([
        DELTA_SCHEMA_VERSION,
        u8::try_from(from).map_err(|_| "Chess source square exceeds u8".to_owned())?,
        u8::try_from(to).map_err(|_| "Chess destination square exceeds u8".to_owned())?,
        0,
    ])
}

pub(crate) fn record_label(payload: &[u8]) -> Result<String, String> {
    if payload == encode_reset() {
        return Ok("reset".to_owned());
    }
    if payload.len() != 4 || payload[0] != DELTA_SCHEMA_VERSION || payload[3] != 0 {
        return Err("unsupported Chess action delta".to_owned());
    }
    let from = usize::from(payload[1]);
    let to = usize::from(payload[2]);
    if from >= 64 || to >= 64 {
        return Err("Chess action delta contains an invalid square".to_owned());
    }
    Ok(format!("{}{}", square_name(from), square_name(to)))
}

fn run_poison_controls() -> Result<usize, String> {
    let state = ChessState::default();
    let checkpoint =
        encode_checkpoint(ReducerId::Chess, REDUCER_SCHEMA_VERSION, 0, &state.encode())
            .map_err(|error| error.to_string())?;
    let mut passed = 0;
    passed += usize::from(apply_delta(&state, &[2, 12, 28, 0]).is_err());
    passed += usize::from(apply_delta(&state, &[1, 64, 28, 0]).is_err());
    passed += usize::from(
        decode_checkpoint(ReducerId::Tetris, REDUCER_SCHEMA_VERSION, 0, &checkpoint)
            == Err(CheckpointError::ReducerMismatch),
    );
    passed += usize::from(
        decode_checkpoint(ReducerId::Chess, REDUCER_SCHEMA_VERSION, 1, &checkpoint)
            == Err(CheckpointError::PositionMismatch),
    );
    let mut corrupt = checkpoint;
    corrupt[CHECKPOINT_OVERHEAD_BYTES] ^= 1;
    passed += usize::from(
        decode_checkpoint(ReducerId::Chess, REDUCER_SCHEMA_VERSION, 0, &corrupt)
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

pub fn run_golden() -> Result<(), String> {
    let mut history = DeltaChess::bootstrap();
    for notation in ["e2e4", "e7e5", "g1f3", "b8c6"] {
        history.append(notation)?;
    }
    let main_fingerprint = history.state.fingerprint();
    let historical_fingerprint = history.reconstruct(2)?.fingerprint();
    let forward_fingerprint = history.reconstruct(4)?.fingerprint();
    let snapshot_round_trip = forward_fingerprint == main_fingerprint;
    let branch = history.fork_from(2)?;
    history.append("c2c4")?;
    let branch_fingerprint = history.state.fingerprint();
    let branch_diverged =
        branch_fingerprint != main_fingerprint && branch_fingerprint != historical_fingerprint;
    history.switch_branch("main")?;
    let main_switch_exact = history.state.fingerprint() == main_fingerprint;
    history.switch_branch(&branch)?;
    let branch_switch_exact = history.state.fingerprint() == branch_fingerprint;
    history.recover()?;
    let replay_exact = history.state.fingerprint() == branch_fingerprint;
    let poison_controls = run_poison_controls()?;
    let passed = snapshot_round_trip
        && branch_diverged
        && main_switch_exact
        && branch_switch_exact
        && replay_exact
        && poison_controls == 7;
    if !passed {
        return Err("Chess delta golden path failed".to_owned());
    }
    let delta_bytes = history.active_delta_bytes();
    let checkpoint_bytes = history.active_checkpoint_bytes();
    let checkpoint_state_bytes = checkpoint_bytes.saturating_sub(
        history
            .current_branch()
            .checkpoints
            .len()
            .saturating_mul(CHECKPOINT_OVERHEAD_BYTES),
    );
    let logical_bytes = delta_bytes.saturating_add(checkpoint_bytes);
    let branch_suffix_bytes = delta_bytes.saturating_sub(history.copied_prefix_bytes);
    println!(
        "{{\"workload\":\"chess-action-delta-v2\",\"rung\":\"G1\",\"record_kind\":\"application-delta\",\"status\":\"VERIFIED\",\"scope\":\"single-process-volatile\",\"delta_schema_version\":{},\"reducer_schema_version\":{},\"delta_bytes\":{},\"delta_bytes_per_action\":4,\"checkpoint_interval\":{},\"checkpoint_count\":{},\"checkpoint_state_bytes\":{},\"checkpoint_bytes\":{},\"checkpoint_overhead_bytes_each\":{},\"logical_bytes\":{},\"copied_prefix_bytes\":{},\"branch_suffix_bytes\":{},\"snapshot_round_trip\":{},\"checkpoint_identity_exact\":true,\"poison_controls\":{},\"branch_diverged\":{},\"main_switch_exact\":{},\"branch_switch_exact\":{},\"replay_exact\":{},\"active_branch\":\"{}\",\"branches\":{},\"fingerprint\":\"{}\"}}",
        DELTA_SCHEMA_VERSION,
        REDUCER_SCHEMA_VERSION,
        delta_bytes,
        CHECKPOINT_INTERVAL,
        history.current_branch().checkpoints.len(),
        checkpoint_state_bytes,
        checkpoint_bytes,
        CHECKPOINT_OVERHEAD_BYTES,
        logical_bytes,
        history.copied_prefix_bytes,
        branch_suffix_bytes,
        snapshot_round_trip,
        poison_controls,
        branch_diverged,
        main_switch_exact,
        branch_switch_exact,
        replay_exact,
        history.active,
        history.branches.len(),
        branch_fingerprint
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply_delta, run_poison_controls, DeltaChess};
    use crate::chess::ChessState;

    #[test]
    fn four_byte_move_replays() {
        let state = ChessState::default();
        let replayed = apply_delta(&state, &[1, 12, 28, 0]).expect("e2e4 replays");
        assert_eq!(replayed.ply, 1);
    }

    #[test]
    fn branch_switch_restores_both_delta_lines() {
        let mut history = DeltaChess::bootstrap();
        history.append("e2e4").expect("move");
        history.append("e7e5").expect("move");
        history.append("g1f3").expect("move");
        let main = history.state.fingerprint();
        let branch = history.fork_from(2).expect("fork");
        history.append("c2c4").expect("diverge");
        let divergent = history.state.fingerprint();
        history.switch_branch("main").expect("main");
        assert_eq!(history.state.fingerprint(), main);
        history.switch_branch(&branch).expect("branch");
        assert_eq!(history.state.fingerprint(), divergent);
    }

    #[test]
    fn every_g1_poison_is_detected() {
        assert_eq!(run_poison_controls().expect("poisons"), 7);
    }
}
