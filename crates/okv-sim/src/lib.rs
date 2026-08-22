//! Deterministic simulation contract and bootstrap recovery scenario.

#[cfg(not(tokio_unstable))]
compile_error!("okv-sim requires --cfg tokio_unstable to seed Tokio runtime scheduling");

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use turmoil::fs::shim::std::fs::{create_dir_all, sync_dir, OpenOptions};
use turmoil::net::{TcpListener, TcpStream};

const AUTHORITY: &str = "authority";
const CLIENT: &str = "client";
const PORT: u16 = 7401;
const STATE_PATH: &str = "/control/authority.state";
const PROFILE: &str =
    "generation-fencing-v1;tick_ms=1;latency_ms=1..5;random_node_order=true;fs_sync_probability=0";
const TRACE_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TraceEvent {
    pub index: usize,
    pub virtual_time_ms: u128,
    pub actor: String,
    pub action: String,
    pub result: String,
    pub generation: u64,
    pub root_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct TraceBody {
    contract_version: u32,
    framework: String,
    framework_version: String,
    scenario: String,
    source_revision: String,
    lockfile_sha256: String,
    profile_sha256: String,
    seed: u64,
    verdict: String,
    invariant_failures: Vec<String>,
    events: Vec<TraceEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SimulationTrace {
    pub contract_version: u32,
    pub framework: String,
    pub framework_version: String,
    pub scenario: String,
    pub source_revision: String,
    pub lockfile_sha256: String,
    pub profile_sha256: String,
    pub seed: u64,
    pub verdict: String,
    pub invariant_failures: Vec<String>,
    pub events: Vec<TraceEvent>,
    pub trace_sha256: String,
}

impl SimulationTrace {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.invariant_failures.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AuthorityState {
    generation: u64,
    root_version: u64,
}

impl Default for AuthorityState {
    fn default() -> Self {
        Self {
            generation: 1,
            root_version: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Activate = 1,
    Publish = 2,
    Read = 3,
}

impl TryFrom<u8> for Operation {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Activate),
            2 => Ok(Self::Publish),
            3 => Ok(Self::Read),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown authority operation",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    Accepted = 1,
    StaleGeneration = 2,
    CompareFailed = 3,
    Current = 4,
}

impl TryFrom<u8> for Status {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Accepted),
            2 => Ok(Self::StaleGeneration),
            3 => Ok(Self::CompareFailed),
            4 => Ok(Self::Current),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown authority response",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Request {
    operation: Operation,
    generation: u64,
    expected_root: u64,
    next_root: u64,
}

#[derive(Clone, Copy, Debug)]
struct Response {
    status: Status,
    state: AuthorityState,
}

/// Run the first deterministic generation-fencing scenario.
///
/// # Errors
///
/// Returns an error when the simulation infrastructure or wire probe fails.
pub fn run_generation_fencing(
    seed: u64,
    source_revision: &str,
    inject_stale_publication_bug: bool,
) -> Result<SimulationTrace, String> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let phase = Arc::new(AtomicU8::new(0));

    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(30))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(5))
        .enable_random_order();
    builder.fs().sync_probability(0.0);
    let mut sim = builder.build();

    sim.host(AUTHORITY, move || {
        authority_server(inject_stale_publication_bug)
    });

    let client_events = Arc::clone(&events);
    let client_failures = Arc::clone(&failures);
    let client_phase = Arc::clone(&phase);
    sim.client(CLIENT, async move {
        client_scenario(client_events, client_failures, client_phase).await
    });

    let mut elapsed_ticks = 0_u128;
    while phase.load(Ordering::SeqCst) < 1 {
        if sim.step().map_err(|error| error.to_string())? {
            return Err("simulation completed before requesting the crash".to_owned());
        }
        elapsed_ticks += 1;
    }

    record(
        &events,
        elapsed_ticks,
        "harness",
        "process_crash",
        "injected",
        1,
        1,
    );
    sim.crash(AUTHORITY);
    sim.bounce(AUTHORITY);
    record(
        &events,
        elapsed_ticks,
        "harness",
        "process_restart",
        "injected",
        1,
        1,
    );
    phase.store(2, Ordering::SeqCst);
    sim.run().map_err(|error| error.to_string())?;

    let events = take_mutex(events)?;
    let invariant_failures = take_mutex(failures)?;
    Ok(finalize_trace(
        seed,
        source_revision,
        events,
        invariant_failures,
    ))
}

async fn authority_server(inject_stale_publication_bug: bool) -> turmoil::Result<()> {
    let mut state = load_state()?;
    persist_state(state)?;
    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let request = read_request(&mut stream).await?;
        let status = match request.operation {
            Operation::Read => Status::Current,
            Operation::Activate => {
                if request.generation == state.generation + 1
                    && request.expected_root == state.root_version
                {
                    state.generation = request.generation;
                    persist_state(state)?;
                    Status::Accepted
                } else if request.generation <= state.generation {
                    Status::StaleGeneration
                } else {
                    Status::CompareFailed
                }
            }
            Operation::Publish => {
                if !inject_stale_publication_bug && request.generation != state.generation {
                    Status::StaleGeneration
                } else if request.expected_root != state.root_version {
                    Status::CompareFailed
                } else {
                    state.root_version = request.next_root;
                    persist_state(state)?;
                    Status::Accepted
                }
            }
        };
        write_response(&mut stream, Response { status, state }).await?;
    }
}

async fn client_scenario(
    events: Arc<Mutex<Vec<TraceEvent>>>,
    failures: Arc<Mutex<Vec<String>>>,
    phase: Arc<AtomicU8>,
) -> turmoil::Result<()> {
    let first = request(Request {
        operation: Operation::Publish,
        generation: 1,
        expected_root: 0,
        next_root: 1,
    })
    .await?;
    record_response(&events, "publish_g1_root1", first);
    require_status(&failures, "initial publication", first, Status::Accepted);

    phase.store(1, Ordering::SeqCst);
    while phase.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    verify_recovered_state(&events, &failures).await?;
    inject_network_partition(&events, &failures).await;
    exercise_generation_fencing(&events, &failures).await?;

    Ok(())
}

async fn verify_recovered_state(
    events: &Arc<Mutex<Vec<TraceEvent>>>,
    failures: &Arc<Mutex<Vec<String>>>,
) -> turmoil::Result<()> {
    let recovered = request(Request {
        operation: Operation::Read,
        generation: 0,
        expected_root: 0,
        next_root: 0,
    })
    .await?;
    record_response(events, "read_after_restart", recovered);
    if recovered.state
        != (AuthorityState {
            generation: 1,
            root_version: 1,
        })
    {
        push_failure(
            failures,
            format!("synced root did not survive crash: {recovered:?}"),
        );
    }
    Ok(())
}

async fn inject_network_partition(
    events: &Arc<Mutex<Vec<TraceEvent>>>,
    failures: &Arc<Mutex<Vec<String>>>,
) {
    turmoil::partition(CLIENT, AUTHORITY);
    record_now(events, "client", "network_partition", "injected", 1, 1);
    let partitioned = timeout(
        Duration::from_secs(1),
        TcpStream::connect((AUTHORITY, PORT)),
    )
    .await;
    let partition_result = match partitioned {
        Ok(Ok(_)) => {
            push_failure(
                failures,
                "connection succeeded across explicit partition".to_owned(),
            );
            "unexpected_connection"
        }
        Ok(Err(_)) => "connection_refused",
        Err(_) => "connection_timeout",
    };
    record_now(
        events,
        "client",
        "connect_while_partitioned",
        partition_result,
        1,
        1,
    );
    turmoil::repair(CLIENT, AUTHORITY);
    record_now(events, "client", "network_repair", "injected", 1, 1);
}

async fn exercise_generation_fencing(
    events: &Arc<Mutex<Vec<TraceEvent>>>,
    failures: &Arc<Mutex<Vec<String>>>,
) -> turmoil::Result<()> {
    let activate = request(Request {
        operation: Operation::Activate,
        generation: 2,
        expected_root: 1,
        next_root: 1,
    })
    .await?;
    record_response(events, "activate_g2", activate);
    require_status(
        failures,
        "generation activation",
        activate,
        Status::Accepted,
    );

    let stale = request(Request {
        operation: Operation::Publish,
        generation: 1,
        expected_root: 1,
        next_root: 2,
    })
    .await?;
    record_response(events, "stale_g1_publish", stale);
    require_status(
        failures,
        "stale generation publication",
        stale,
        Status::StaleGeneration,
    );

    let fresh = request(Request {
        operation: Operation::Publish,
        generation: 2,
        expected_root: 1,
        next_root: 2,
    })
    .await?;
    record_response(events, "publish_g2_root2", fresh);
    require_status(failures, "fresh publication", fresh, Status::Accepted);

    let final_state = request(Request {
        operation: Operation::Read,
        generation: 0,
        expected_root: 0,
        next_root: 0,
    })
    .await?;
    record_response(events, "read_final", final_state);
    if final_state.state
        != (AuthorityState {
            generation: 2,
            root_version: 2,
        })
    {
        push_failure(
            failures,
            format!("unexpected final authority state: {final_state:?}"),
        );
    }

    Ok(())
}

async fn request(request: Request) -> io::Result<Response> {
    let mut stream = timeout(
        Duration::from_secs(5),
        TcpStream::connect((AUTHORITY, PORT)),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "authority connect timed out"))??;
    write_request(&mut stream, request).await?;
    read_response(&mut stream).await
}

async fn write_request(stream: &mut TcpStream, request: Request) -> io::Result<()> {
    let mut bytes = [0_u8; 25];
    bytes[0] = request.operation as u8;
    bytes[1..9].copy_from_slice(&request.generation.to_be_bytes());
    bytes[9..17].copy_from_slice(&request.expected_root.to_be_bytes());
    bytes[17..25].copy_from_slice(&request.next_root.to_be_bytes());
    stream.write_all(&bytes).await
}

async fn read_request(stream: &mut TcpStream) -> io::Result<Request> {
    let mut bytes = [0_u8; 25];
    stream.read_exact(&mut bytes).await?;
    Ok(Request {
        operation: Operation::try_from(bytes[0])?,
        generation: read_u64(&bytes[1..9]),
        expected_root: read_u64(&bytes[9..17]),
        next_root: read_u64(&bytes[17..25]),
    })
}

async fn write_response(stream: &mut TcpStream, response: Response) -> io::Result<()> {
    let mut bytes = [0_u8; 17];
    bytes[0] = response.status as u8;
    bytes[1..9].copy_from_slice(&response.state.generation.to_be_bytes());
    bytes[9..17].copy_from_slice(&response.state.root_version.to_be_bytes());
    stream.write_all(&bytes).await
}

async fn read_response(stream: &mut TcpStream) -> io::Result<Response> {
    let mut bytes = [0_u8; 17];
    stream.read_exact(&mut bytes).await?;
    Ok(Response {
        status: Status::try_from(bytes[0])?,
        state: AuthorityState {
            generation: read_u64(&bytes[1..9]),
            root_version: read_u64(&bytes[9..17]),
        },
    })
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(bytes);
    u64::from_be_bytes(value)
}

fn load_state() -> io::Result<AuthorityState> {
    match OpenOptions::new().read(true).write(true).open(STATE_PATH) {
        Ok(file) => {
            let mut bytes = [0_u8; 16];
            let read = file.read_at(&mut bytes, 0)?;
            if read == bytes.len() {
                Ok(AuthorityState {
                    generation: read_u64(&bytes[..8]),
                    root_version: read_u64(&bytes[8..]),
                })
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "authority state is truncated",
                ))
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(AuthorityState::default()),
        Err(error) => Err(error),
    }
}

fn persist_state(state: AuthorityState) -> io::Result<()> {
    create_dir_all("/control")?;
    sync_dir("/")?;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(STATE_PATH)?;
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&state.generation.to_be_bytes());
    bytes[8..].copy_from_slice(&state.root_version.to_be_bytes());
    file.write_all_at(&bytes, 0)?;
    file.sync_all()?;
    sync_dir("/control")
}

fn record_response(events: &Arc<Mutex<Vec<TraceEvent>>>, action: &str, response: Response) {
    record_now(
        events,
        "client",
        action,
        status_name(response.status),
        response.state.generation,
        response.state.root_version,
    );
}

fn record_now(
    events: &Arc<Mutex<Vec<TraceEvent>>>,
    actor: &str,
    action: &str,
    result: &str,
    generation: u64,
    root_version: u64,
) {
    let virtual_time_ms = turmoil::sim_elapsed().map_or(0, |value| value.as_millis());
    record(
        events,
        virtual_time_ms,
        actor,
        action,
        result,
        generation,
        root_version,
    );
}

fn record(
    events: &Arc<Mutex<Vec<TraceEvent>>>,
    virtual_time_ms: u128,
    actor: &str,
    action: &str,
    result: &str,
    generation: u64,
    root_version: u64,
) {
    let mut guard = events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let index = guard.len();
    guard.push(TraceEvent {
        index,
        virtual_time_ms,
        actor: actor.to_owned(),
        action: action.to_owned(),
        result: result.to_owned(),
        generation,
        root_version,
    });
}

fn require_status(
    failures: &Arc<Mutex<Vec<String>>>,
    invariant: &str,
    response: Response,
    expected: Status,
) {
    if response.status != expected {
        push_failure(
            failures,
            format!(
                "{invariant}: expected {}, observed {} at generation {} root {}",
                status_name(expected),
                status_name(response.status),
                response.state.generation,
                response.state.root_version
            ),
        );
    }
}

fn push_failure(failures: &Arc<Mutex<Vec<String>>>, failure: String) {
    failures
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(failure);
}

fn status_name(status: Status) -> &'static str {
    match status {
        Status::Accepted => "accepted",
        Status::StaleGeneration => "stale_generation",
        Status::CompareFailed => "compare_failed",
        Status::Current => "current",
    }
}

fn take_mutex<T: Clone>(value: Arc<Mutex<T>>) -> Result<T, String> {
    match Arc::try_unwrap(value) {
        Ok(mutex) => mutex.into_inner().map_err(|error| error.to_string()),
        Err(shared) => Ok(shared.lock().map_err(|error| error.to_string())?.clone()),
    }
}

fn finalize_trace(
    seed: u64,
    source_revision: &str,
    events: Vec<TraceEvent>,
    invariant_failures: Vec<String>,
) -> SimulationTrace {
    let verdict = if invariant_failures.is_empty() {
        "pass"
    } else {
        "fail"
    };
    let body = TraceBody {
        contract_version: TRACE_CONTRACT_VERSION,
        framework: "turmoil".to_owned(),
        framework_version: "0.7.2".to_owned(),
        scenario: "generation-fencing-v1".to_owned(),
        source_revision: source_revision.to_owned(),
        lockfile_sha256: sha256(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../Cargo.lock"
        ))),
        profile_sha256: sha256(PROFILE.as_bytes()),
        seed,
        verdict: verdict.to_owned(),
        invariant_failures,
        events,
    };
    let trace_sha256 = sha256(&serde_json::to_vec(&body).expect("trace body serializes"));
    SimulationTrace {
        contract_version: body.contract_version,
        framework: body.framework,
        framework_version: body.framework_version,
        scenario: body.scenario,
        source_revision: body.source_revision,
        lockfile_sha256: body.lockfile_sha256,
        profile_sha256: body.profile_sha256,
        seed: body.seed,
        verdict: body.verdict,
        invariant_failures: body.invariant_failures,
        events: body.events,
        trace_sha256,
    }
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::run_generation_fencing;

    #[test]
    fn same_seed_replays_exactly() {
        let first = run_generation_fencing(1103, "test-revision", false).unwrap();
        let second = run_generation_fencing(1103, "test-revision", false).unwrap();
        assert_eq!(first, second);
        assert!(first.passed());
    }

    #[test]
    fn adjacent_seed_changes_the_explored_trace() {
        let first = run_generation_fencing(1103, "test-revision", false).unwrap();
        let second = run_generation_fencing(1104, "test-revision", false).unwrap();
        assert_ne!(first.trace_sha256, second.trace_sha256);
    }

    #[test]
    fn stale_publication_negative_control_fails() {
        let trace = run_generation_fencing(1103, "test-revision", true).unwrap();
        assert!(!trace.passed());
        assert!(trace
            .invariant_failures
            .iter()
            .any(|failure| failure.contains("stale generation publication")));
    }
}
