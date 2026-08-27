use crate::api::{
    CommitReceipt, CommittedEnvelope, KvMutation, PointReadRequest, RangeReadRequest,
    TransactRequest, API_VERSION,
};
use crate::delta::{decode_action, encode_action};
use crate::game::{Action, GameState, HEIGHT, WIDTH};
use okv_log::{LogEntry, LogState};
use okv_model::{
    ApplyOutcome, CommitBatch, CommitIdentity, KeyRange, Model, Mutation, Row, Version,
};
use std::collections::BTreeMap;

const TENANT: &str = "tetris-demo";
const STATE_KEY: &[u8] = b"apps/tetris/games/demo/view/state";
const VIEW_START: &[u8] = b"apps/tetris/games/demo/view/";
const VIEW_END: &[u8] = b"apps/tetris/games/demo/view0";
const EVENT_START: &[u8] = b"apps/tetris/games/demo/events/";
const EVENT_END: &[u8] = b"apps/tetris/games/demo/events0";
type KvRows = Vec<Row>;

#[derive(Clone, Debug)]
pub struct BranchStats {
    pub name: String,
    pub parent: Option<String>,
    pub fork_version: u64,
    pub latest_version: u64,
    pub txlog_entries: usize,
    pub txlog_bytes: usize,
    pub active: bool,
}

#[derive(Clone, Debug)]
struct BranchOrigin {
    parent: Option<String>,
    fork_version: u64,
}

#[derive(Debug)]
pub struct KernelStats {
    pub branch: String,
    pub branches: Vec<BranchStats>,
    pub latest_version: Version,
    pub txlog_entries: usize,
    pub txlog_bytes: usize,
    pub application_record_bytes: usize,
    pub point_reads: u64,
    pub range_reads: u64,
    pub recoveries: u64,
    pub visible_rows: usize,
    pub event_rows: usize,
    pub last_receipt: Option<CommitReceipt>,
    pub last_action: String,
}

pub struct PrototypeKernel {
    branches: BTreeMap<String, LogState>,
    branch_origins: BTreeMap<String, BranchOrigin>,
    branch: String,
    model: Model,
    next_request_id: u64,
    point_reads: u64,
    range_reads: u64,
    recoveries: u64,
    branch_counter: u64,
    last_receipt: Option<CommitReceipt>,
    last_action: String,
}

impl PrototypeKernel {
    pub fn bootstrap() -> Result<Self, String> {
        let mut kernel = Self {
            branches: BTreeMap::from([("main".to_owned(), LogState::default())]),
            branch_origins: BTreeMap::from([(
                "main".to_owned(),
                BranchOrigin {
                    parent: None,
                    fork_version: 0,
                },
            )]),
            branch: "main".to_owned(),
            model: Model::default(),
            next_request_id: 1,
            point_reads: 0,
            range_reads: 0,
            recoveries: 0,
            branch_counter: 0,
            last_receipt: None,
            last_action: "bootstrap".to_owned(),
        };
        kernel.commit_state(Action::Reset, &GameState::default())?;
        Ok(kernel)
    }

    pub fn apply_action(&mut self, action: Action) -> Result<CommitReceipt, String> {
        let current = self.read_game(None)?;
        let next = current.apply(action);
        self.commit_state(action, &next)
    }

    pub fn read_game(&mut self, version: Option<Version>) -> Result<GameState, String> {
        let read_version = version.unwrap_or_else(|| self.model.latest_version());
        let request = PointReadRequest {
            tenant: TENANT.to_owned(),
            key: STATE_KEY.to_vec(),
            read_version,
        };
        if request.tenant != TENANT {
            return Err(format!("unknown tenant {}", request.tenant));
        }
        self.point_reads += 1;
        let value = self
            .model
            .get(&request.key, request.read_version)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("state absent at version {}", request.read_version))?;
        GameState::decode(value)
    }

    pub fn read_view_rows(&mut self, version: Version) -> Result<KvRows, String> {
        self.range_read(RangeReadRequest {
            tenant: TENANT.to_owned(),
            start: VIEW_START.to_vec(),
            end: VIEW_END.to_vec(),
            read_version: version,
        })
    }

    pub fn read_event_rows(&mut self, version: Version) -> Result<KvRows, String> {
        self.range_read(RangeReadRequest {
            tenant: TENANT.to_owned(),
            start: EVENT_START.to_vec(),
            end: EVENT_END.to_vec(),
            read_version: version,
        })
    }

    pub fn recover_from_txlog(&mut self) -> Result<(), String> {
        let version = self.model.latest_version();
        let before = self
            .model
            .get(STATE_KEY, version)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "state absent before recovery".to_owned())?
            .to_vec();
        let recovered = replay(self.current_log())?;
        let after = recovered
            .get(STATE_KEY, version)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "state absent after recovery".to_owned())?;
        if before != after {
            return Err("replayed state differs from the pre-crash snapshot".to_owned());
        }
        self.model = recovered;
        self.recoveries += 1;
        "crash-recover from okv-log, exact state verified".clone_into(&mut self.last_action);
        Ok(())
    }

    pub fn discard_and_rebuild_serving_image(&mut self) -> Result<(), String> {
        let log = self.current_log().clone();
        self.model = Model::default();
        self.model = replay(&log)?;
        self.recoveries = self.recoveries.saturating_add(1);
        "discarded RAM image and rebuilt from canonical txLog".clone_into(&mut self.last_action);
        Ok(())
    }

    pub fn replay_application_history(&self) -> Result<GameState, String> {
        let mut state = GameState::default();
        for (_, record) in self.application_records()? {
            state = state.apply(decode_action(&record)?);
        }
        Ok(state)
    }

    pub fn committed_envelope_bytes(&self) -> Vec<Vec<u8>> {
        self.current_log()
            .entries_clamped(..)
            .into_iter()
            .map(|entry| entry.payload)
            .collect()
    }

    pub fn verify_atomic_abort(&mut self) -> Result<bool, String> {
        let before_version = self.model.latest_version();
        let before_log = self.current_log().entries_clamped(..).len();
        let before_state = self
            .model
            .get(STATE_KEY, before_version)
            .map_err(|error| error.to_string())?
            .map(ToOwned::to_owned);
        let rejected = self.transact(TransactRequest {
            tenant: TENANT.to_owned(),
            read_version: Version::ZERO,
            request_id: self.next_request_id,
            mutations: vec![KvMutation::Set {
                key: STATE_KEY.to_vec(),
                value: b"must-not-commit".to_vec(),
            }],
            application_record: encode_action(Action::Left).to_vec(),
        });
        let after_state = self
            .model
            .get(STATE_KEY, before_version)
            .map_err(|error| error.to_string())?
            .map(ToOwned::to_owned);
        Ok(rejected.is_err()
            && self.model.latest_version() == before_version
            && self.current_log().entries_clamped(..).len() == before_log
            && after_state == before_state)
    }

    pub fn fork(&mut self) -> Result<String, String> {
        self.fork_from(self.latest_version())
    }

    pub fn fork_from(&mut self, version: Version) -> Result<String, String> {
        let latest = self.latest_version();
        if version.sequence() == 0 || version > latest {
            return Err(format!("fork version {version} is outside 1..={latest}"));
        }
        let to = version
            .sequence()
            .checked_add(1)
            .ok_or_else(|| "fork version overflow".to_owned())?;
        let entries = self
            .current_log()
            .entries_exact(1, to)
            .map_err(|error| error.to_string())?;
        let mut log = LogState::default();
        let commands = log
            .plan_suffix_append(&entries)
            .map_err(|error| error.to_string())?;
        log.apply_all(&commands)
            .map_err(|error| error.to_string())?;

        self.branch_counter += 1;
        let name = format!("branch-{}", self.branch_counter);
        let parent = self.branch.clone();
        self.branches.insert(name.clone(), log);
        self.branch_origins.insert(
            name.clone(),
            BranchOrigin {
                parent: Some(parent),
                fork_version: version.sequence(),
            },
        );
        self.branch.clone_from(&name);
        self.model = replay(self.current_log())?;
        self.last_action = format!("forked {name} from version {}", version.sequence());
        Ok(name)
    }

    pub fn switch_branch(&mut self, name: &str) -> Result<(), String> {
        let log = self
            .branches
            .get(name)
            .ok_or_else(|| format!("unknown branch {name:?}"))?;
        let model = replay(log)?;
        name.clone_into(&mut self.branch);
        self.model = model;
        self.last_receipt = None;
        self.last_action = format!("switched to {name}");
        Ok(())
    }

    pub fn latest_version(&self) -> Version {
        self.model.latest_version()
    }

    pub fn stats(&mut self) -> Result<KernelStats, String> {
        let latest_version = self.latest_version();
        let visible_rows = self.read_view_rows(latest_version)?.len();
        let event_rows = self.read_event_rows(latest_version)?.len();
        let entries = self.current_log().entries_clamped(..);
        let branches = self
            .branches
            .iter()
            .map(|(name, log)| {
                let entries = log.entries_clamped(..);
                let origin = self
                    .branch_origins
                    .get(name)
                    .expect("every branch has an origin");
                BranchStats {
                    name: name.clone(),
                    parent: origin.parent.clone(),
                    fork_version: origin.fork_version,
                    latest_version: log.last_entry().map_or(0, |(index, _)| index),
                    txlog_entries: entries.len(),
                    txlog_bytes: entries.iter().map(|entry| entry.payload.len()).sum(),
                    active: *name == self.branch,
                }
            })
            .collect();
        Ok(KernelStats {
            branch: self.branch.clone(),
            branches,
            latest_version,
            txlog_entries: entries.len(),
            txlog_bytes: entries.iter().map(|entry| entry.payload.len()).sum(),
            application_record_bytes: self
                .application_records()?
                .iter()
                .map(|(_, record)| record.len())
                .sum(),
            point_reads: self.point_reads,
            range_reads: self.range_reads,
            recoveries: self.recoveries,
            visible_rows,
            event_rows,
            last_receipt: self.last_receipt.clone(),
            last_action: self.last_action.clone(),
        })
    }

    fn commit_state(&mut self, action: Action, state: &GameState) -> Result<CommitReceipt, String> {
        let sequence = self
            .current_log()
            .last_entry()
            .map_or(1, |(index, _)| index.saturating_add(1));
        let commit_version = Version::new(sequence);
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let request = TransactRequest {
            tenant: TENANT.to_owned(),
            read_version: self.model.latest_version(),
            request_id,
            mutations: state_mutations(commit_version, action, state),
            application_record: encode_action(action).to_vec(),
        };
        self.transact(request)
    }

    fn transact(&mut self, request: TransactRequest) -> Result<CommitReceipt, String> {
        if request.tenant != TENANT {
            return Err(format!("unknown tenant {}", request.tenant));
        }
        if request.read_version != self.model.latest_version() {
            return Err(format!(
                "stale read version {}, latest is {}",
                request.read_version,
                self.model.latest_version()
            ));
        }
        let commit_version = Version::new(
            self.current_log()
                .last_entry()
                .map_or(1, |(index, _)| index.saturating_add(1)),
        );
        let envelope = CommittedEnvelope {
            commit_version,
            request_id: request.request_id,
            mutations: request.mutations,
            application_record: request.application_record,
        };
        let payload = envelope.encode();
        let txlog_index = commit_version.sequence();
        let entry = LogEntry::new(txlog_index, &payload);
        let commands = self
            .current_log()
            .plan_suffix_append(std::slice::from_ref(&entry))
            .map_err(|error| error.to_string())?;
        let outcome = self
            .model
            .apply(to_commit_batch(&envelope))
            .map_err(|error| error.to_string())?;
        self.current_log_mut()
            .apply_all(&commands)
            .map_err(|error| error.to_string())?;
        let receipt = CommitReceipt {
            api_version: API_VERSION,
            commit_version,
            request_id: request.request_id,
            replayed: outcome == ApplyOutcome::AlreadyApplied,
            mutation_count: envelope.mutations.len(),
            txlog_index,
        };
        decode_action(&envelope.application_record)?
            .label()
            .clone_into(&mut self.last_action);
        self.last_receipt = Some(receipt.clone());
        Ok(receipt)
    }

    fn range_read(&mut self, request: RangeReadRequest) -> Result<KvRows, String> {
        if request.tenant != TENANT {
            return Err(format!("unknown tenant {}", request.tenant));
        }
        self.range_reads += 1;
        let range = KeyRange::new(request.start, request.end).map_err(|error| error.to_string())?;
        self.model
            .scan(&range, request.read_version)
            .map_err(|error| error.to_string())
    }

    fn current_log(&self) -> &LogState {
        self.branches
            .get(&self.branch)
            .expect("current branch exists")
    }

    fn current_log_mut(&mut self) -> &mut LogState {
        self.branches
            .get_mut(&self.branch)
            .expect("current branch exists")
    }

    fn application_records(&self) -> Result<Vec<(u64, Vec<u8>)>, String> {
        self.current_log()
            .entries_clamped(..)
            .into_iter()
            .map(|entry| {
                let envelope = CommittedEnvelope::decode(&entry.payload)?;
                Ok((entry.index, envelope.application_record))
            })
            .collect()
    }
}

fn replay(log: &LogState) -> Result<Model, String> {
    let mut model = Model::default();
    for entry in log.entries_clamped(..) {
        let envelope = CommittedEnvelope::decode(&entry.payload)?;
        if envelope.commit_version.sequence() != entry.index {
            return Err(format!(
                "txLog index {} disagrees with commit version {}",
                entry.index, envelope.commit_version
            ));
        }
        model
            .apply(to_commit_batch(&envelope))
            .map_err(|error| error.to_string())?;
    }
    Ok(model)
}

fn to_commit_batch(envelope: &CommittedEnvelope) -> CommitBatch {
    CommitBatch {
        version: envelope.commit_version,
        identity: CommitIdentity::for_test(envelope.request_id),
        mutations: envelope
            .mutations
            .iter()
            .map(|mutation| match mutation {
                KvMutation::Set { key, value } => Mutation::Set {
                    key: key.clone(),
                    value: value.clone(),
                },
                KvMutation::Clear { key } => Mutation::Clear { key: key.clone() },
                KvMutation::ClearRange { start, end } => Mutation::ClearRange {
                    range: KeyRange::new(start.clone(), end.clone())
                        .expect("frozen API contains valid ranges"),
                },
            })
            .collect(),
    }
}

fn state_mutations(version: Version, action: Action, state: &GameState) -> Vec<KvMutation> {
    let mut mutations = vec![
        KvMutation::ClearRange {
            start: VIEW_START.to_vec(),
            end: VIEW_END.to_vec(),
        },
        KvMutation::Set {
            key: STATE_KEY.to_vec(),
            value: state.encode(),
        },
        KvMutation::Set {
            key: b"apps/tetris/games/demo/view/meta/score".to_vec(),
            value: state.score.to_be_bytes().to_vec(),
        },
        KvMutation::Set {
            key: b"apps/tetris/games/demo/view/meta/lines".to_vec(),
            value: state.lines.to_be_bytes().to_vec(),
        },
        KvMutation::Clear {
            key: b"apps/tetris/games/demo/view/meta/transient".to_vec(),
        },
    ];
    let board = state.visible_board();
    for (y, row) in board.iter().enumerate().take(HEIGHT) {
        for (x, cell) in row.iter().enumerate().take(WIDTH) {
            if *cell != 0 {
                mutations.push(KvMutation::Set {
                    key: format!("apps/tetris/games/demo/view/cells/{y:02}/{x:02}").into_bytes(),
                    value: vec![*cell],
                });
            }
        }
    }
    mutations.push(KvMutation::Set {
        key: format!("apps/tetris/games/demo/events/{:020}", version.sequence()).into_bytes(),
        value: action.label().as_bytes().to_vec(),
    });
    mutations
}

#[cfg(test)]
mod tests {
    use super::PrototypeKernel;
    use crate::game::Action;
    use okv_model::Version;

    #[test]
    fn branch_lineage_tracks_parent_and_exact_fork_version() {
        let mut kernel = PrototypeKernel::bootstrap().expect("bootstrap");
        kernel.apply_action(Action::Tick).expect("tick");
        kernel.apply_action(Action::Left).expect("left");
        let main = kernel.read_game(None).expect("main state").fingerprint();

        let branch = kernel.fork_from(Version::new(2)).expect("fork");
        kernel.apply_action(Action::Right).expect("branch move");
        let divergent = kernel.read_game(None).expect("branch state").fingerprint();
        assert_ne!(main, divergent);

        let stats = kernel.stats().expect("stats");
        let line = stats
            .branches
            .iter()
            .find(|candidate| candidate.name == branch)
            .expect("branch stats");
        assert_eq!(line.parent.as_deref(), Some("main"));
        assert_eq!(line.fork_version, 2);

        kernel.switch_branch("main").expect("switch main");
        assert_eq!(
            kernel.read_game(None).expect("restored main").fingerprint(),
            main
        );
        kernel.switch_branch(&branch).expect("switch branch");
        assert_eq!(
            kernel
                .read_game(None)
                .expect("restored branch")
                .fingerprint(),
            divergent
        );
    }
}
