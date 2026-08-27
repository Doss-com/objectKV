use crate::game::{Action, GameState};
use sha2::{Digest, Sha256};

const ACTION_CYCLE: [Action; 5] = [
    Action::Left,
    Action::Right,
    Action::Rotate,
    Action::Tick,
    Action::Drop,
];

/// Build the frozen deterministic workload used by every Tetris golden rung.
#[must_use]
pub fn deterministic_actions(count: usize) -> Vec<Action> {
    let mut state = GameState::default();
    (0..count)
        .map(|index| {
            let action = if state.game_over {
                Action::Reset
            } else {
                ACTION_CYCLE[index % ACTION_CYCLE.len()]
            };
            state = state.apply(action);
            action
        })
        .collect()
}

/// Stable SHA-256 identity for an exact ordered Tetris action trace.
#[must_use]
pub fn trace_sha256(actions: &[Action]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"okv-tetris-trace-v1");
    for action in actions {
        digest.update(action.label().as_bytes());
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_is_deterministic_and_state_complete() {
        let left = deterministic_actions(2_000);
        let right = deterministic_actions(2_000);
        assert_eq!(left, right);
        assert_eq!(trace_sha256(&left), trace_sha256(&right));
        assert_eq!(left.len(), 2_000);
    }
}
