use clap::{Parser, Subcommand, ValueEnum};
use okv_object::{
    write_nvme_range_image_stream, NvmeIoMode, NvmeRangeImageConfig, NvmeRangeImageIdentity,
    NvmeRangeImageReader,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const TRACE_MAGIC: &[u8; 8] = b"OKVTRC01";
const TRACE_HEADER_BYTES: usize = 40;
const KEY_PREFIX: &str = "k/";

#[derive(Debug, Parser)]
#[command(name = "range-image-nvme-probe")]
#[command(about = "Disposable RFC 0071 objectKV NVMe benchmark probe")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Trace {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        seed: u64,
        #[arg(long)]
        key_count: usize,
        #[arg(long)]
        warmup_points: usize,
        #[arg(long)]
        measured_points: usize,
    },
    Run {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        trace: PathBuf,
        #[arg(long)]
        block_payload_bytes: usize,
        #[arg(long)]
        value_bytes: usize,
        #[arg(long, default_value_t = 67_108_864)]
        reader_memory_budget_bytes: usize,
        #[arg(long, value_delimiter = ',', default_value = "1,8,32")]
        concurrencies: Vec<usize>,
        #[arg(long, value_enum, default_value_t = IoModeArg::Direct)]
        io_mode: IoModeArg,
    },
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum IoModeArg {
    Buffered,
    #[default]
    Direct,
}

impl From<IoModeArg> for NvmeIoMode {
    fn from(value: IoModeArg) -> Self {
        match value {
            IoModeArg::Buffered => Self::Buffered,
            IoModeArg::Direct => Self::Direct,
        }
    }
}

#[derive(Clone, Debug)]
struct Trace {
    seed: u64,
    key_count: usize,
    warmup: Vec<u32>,
    measured: Vec<u32>,
    sha256: String,
}

#[derive(Clone, Copy, Debug, Default)]
struct ProcessSnapshot {
    accumulated_cpu_millis: u64,
    peak_rss_bytes: u64,
    minor_faults: u64,
    major_faults: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
}

#[derive(Clone, Debug, Serialize)]
struct PointCurve {
    concurrency: usize,
    samples: usize,
    duration_seconds: f64,
    iops: f64,
    latency_p50_seconds: f64,
    latency_p95_seconds: f64,
    latency_p99_seconds: f64,
    latency_p999_seconds: f64,
    physical_bytes_per_point_p50: u64,
    physical_bytes_per_point_p99: u64,
    physical_bytes: u64,
    physical_bytes_per_second: f64,
    logical_bytes_per_second: f64,
    cache_hit_ratio: f64,
    cpu_seconds_per_million_points: f64,
    minor_faults: u64,
    major_faults: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
    exact: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ScanCurve {
    rows: usize,
    logical_bytes: u64,
    physical_bytes: u64,
    duration_seconds: f64,
    logical_bytes_per_second: f64,
    rows_per_second: f64,
    digest_sha256: String,
    exact: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ProbeReceipt {
    schema_version: u32,
    engine: &'static str,
    io_mode: &'static str,
    seed: u64,
    key_count: usize,
    value_bytes: usize,
    logical_value_bytes: u64,
    trace_sha256: String,
    fixture_sha256: String,
    block_payload_bytes: usize,
    image_identity_sha256: String,
    image_bytes: u64,
    image_amplification: f64,
    index_logical_bytes: u64,
    index_physical_bytes: u64,
    block_count: u32,
    open_operations: u64,
    open_bytes: u64,
    direct_io_active: bool,
    alignment_violations: u64,
    base_resident_bytes: u64,
    maximum_cache_bytes: u64,
    maximum_inflight_buffer_bytes: u64,
    accounted_resident_bytes: u64,
    peak_worker_rss_bytes: u64,
    point_curves: Vec<PointCurve>,
    scan: ScanCurve,
    semantic_replay_sha256: String,
}

#[derive(Debug)]
struct PointWorkerReceipt {
    latencies_ns: Vec<u64>,
    physical_bytes: Vec<u64>,
    exact: bool,
}

fn main() {
    if let Err(error) = execute(Cli::parse()) {
        eprintln!("range-image-nvme-probe: {error}");
        std::process::exit(1);
    }
}

fn execute(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Trace {
            path,
            seed,
            key_count,
            warmup_points,
            measured_points,
        } => {
            let trace = write_trace(&path, seed, key_count, warmup_points, measured_points)?;
            println!("{}", trace.sha256);
        }
        Command::Run {
            root,
            trace,
            block_payload_bytes,
            value_bytes,
            reader_memory_budget_bytes,
            concurrencies,
            io_mode,
        } => {
            let receipt = run_probe(
                &root,
                &trace,
                block_payload_bytes,
                value_bytes,
                reader_memory_budget_bytes,
                &concurrencies,
                io_mode.into(),
            )?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|error| error.to_string())?
            );
        }
    }
    Ok(())
}

fn write_trace(
    path: &Path,
    seed: u64,
    key_count: usize,
    warmup_points: usize,
    measured_points: usize,
) -> Result<Trace, String> {
    if key_count == 0 || key_count > usize::try_from(u32::MAX).unwrap_or(usize::MAX) {
        return Err("trace key count is outside u32".to_owned());
    }
    let key_count_u64 = u64::try_from(key_count).map_err(|error| error.to_string())?;
    let warmup_u64 = u64::try_from(warmup_points).map_err(|error| error.to_string())?;
    let measured_u64 = u64::try_from(measured_points).map_err(|error| error.to_string())?;
    let mut bytes = Vec::with_capacity(
        TRACE_HEADER_BYTES.saturating_add(
            warmup_points
                .saturating_add(measured_points)
                .saturating_mul(size_of::<u32>()),
        ),
    );
    bytes.extend_from_slice(TRACE_MAGIC);
    bytes.extend_from_slice(&seed.to_be_bytes());
    bytes.extend_from_slice(&key_count_u64.to_be_bytes());
    bytes.extend_from_slice(&warmup_u64.to_be_bytes());
    bytes.extend_from_slice(&measured_u64.to_be_bytes());
    let mut state = seed ^ 0x517c_c1b7_2722_0a95;
    for _ in 0..warmup_points.saturating_add(measured_points) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let ordinal = u32::try_from(state % key_count_u64).map_err(|error| error.to_string())?;
        bytes.extend_from_slice(&ordinal.to_be_bytes());
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    load_trace(path)
}

fn load_trace(path: &Path) -> Result<Trace, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    if bytes.len() < TRACE_HEADER_BYTES || &bytes[..8] != TRACE_MAGIC {
        return Err("trace header is invalid".to_owned());
    }
    let seed = take_u64(&bytes, 8)?;
    let key_count = usize::try_from(take_u64(&bytes, 16)?)
        .map_err(|_| "trace key count exceeds usize".to_owned())?;
    let warmup_count = usize::try_from(take_u64(&bytes, 24)?)
        .map_err(|_| "trace warmup count exceeds usize".to_owned())?;
    let measured_count = usize::try_from(take_u64(&bytes, 32)?)
        .map_err(|_| "trace measured count exceeds usize".to_owned())?;
    let count = warmup_count
        .checked_add(measured_count)
        .ok_or_else(|| "trace count overflow".to_owned())?;
    let expected_bytes = TRACE_HEADER_BYTES
        .checked_add(
            count
                .checked_mul(size_of::<u32>())
                .ok_or_else(|| "trace size overflow".to_owned())?,
        )
        .ok_or_else(|| "trace size overflow".to_owned())?;
    if key_count == 0 || bytes.len() != expected_bytes {
        return Err("trace physical length is invalid".to_owned());
    }
    let mut ordinals = Vec::with_capacity(count);
    for chunk in bytes[TRACE_HEADER_BYTES..].chunks_exact(size_of::<u32>()) {
        let ordinal = u32::from_be_bytes(
            chunk
                .try_into()
                .map_err(|_| "trace ordinal is invalid".to_owned())?,
        );
        if usize::try_from(ordinal).unwrap_or(usize::MAX) >= key_count {
            return Err("trace ordinal is outside the fixture".to_owned());
        }
        ordinals.push(ordinal);
    }
    let measured = ordinals.split_off(warmup_count);
    Ok(Trace {
        seed,
        key_count,
        warmup: ordinals,
        measured,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    })
}

#[allow(clippy::too_many_lines)]
fn run_probe(
    root: &Path,
    trace_path: &Path,
    block_payload_bytes: usize,
    value_bytes: usize,
    reader_memory_budget_bytes: usize,
    concurrencies: &[usize],
    io_mode: NvmeIoMode,
) -> Result<ProbeReceipt, String> {
    if value_bytes == 0 || concurrencies.is_empty() || concurrencies.contains(&0) {
        return Err("probe dimensions are invalid".to_owned());
    }
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let trace = load_trace(trace_path)?;
    let image_path = root.join("objectkv-range-image.okv");
    let fixture_sha256 = fixture_sha256(trace.seed, trace.key_count, value_bytes);
    let range_begin = key_for(0);
    let range_end = b"k0".to_vec();
    let root_identity_digest: [u8; 32] =
        Sha256::digest([b"okv-rfc0071-root-v1".as_slice(), fixture_sha256.as_bytes()].concat())
            .into();
    let identity = NvmeRangeImageIdentity {
        target_version: 1,
        range_begin: &range_begin,
        range_end: &range_end,
        row_count: u64::try_from(trace.key_count).map_err(|error| error.to_string())?,
        root_identity_digest,
        image_identity_sha256: None,
    };
    let rows = (0..trace.key_count).map(|ordinal| {
        (
            key_for(ordinal),
            deterministic_value(trace.seed, ordinal, value_bytes),
        )
    });
    let write_receipt =
        write_nvme_range_image_stream(&image_path, &identity, block_payload_bytes, rows)?;

    let logical_value_bytes = u64::try_from(trace.key_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(value_bytes).unwrap_or(u64::MAX));
    let mut point_curves = Vec::with_capacity(concurrencies.len());
    let mut maximum_accounted = 0_u64;
    let mut maximum_peak_rss = 0_u64;
    let mut representative_open = None;
    for &concurrency in concurrencies {
        let config = NvmeRangeImageConfig {
            block_payload_bytes,
            reader_memory_budget_bytes,
            maximum_concurrency: concurrency,
            io_mode,
        };
        let expected = NvmeRangeImageIdentity {
            image_identity_sha256: Some(&write_receipt.image_identity_sha256),
            ..identity.clone()
        };
        let (reader, open) = NvmeRangeImageReader::open(&image_path, &expected, config)?;
        if representative_open.is_none() {
            representative_open = Some(open.clone());
        }
        run_points(
            &reader,
            trace.seed,
            value_bytes,
            &trace.warmup,
            concurrency,
            false,
        )?;
        let before = process_snapshot();
        let started = Instant::now();
        let workers = run_points(
            &reader,
            trace.seed,
            value_bytes,
            &trace.measured,
            concurrency,
            true,
        )?;
        let duration = started.elapsed().as_secs_f64();
        let after = process_snapshot();
        let mut latencies = Vec::with_capacity(trace.measured.len());
        let mut physical = Vec::with_capacity(trace.measured.len());
        let mut exact = true;
        for worker in workers {
            latencies.extend(worker.latencies_ns);
            physical.extend(worker.physical_bytes);
            exact &= worker.exact;
        }
        latencies.sort_unstable();
        physical.sort_unstable();
        let physical_bytes = physical.iter().copied().sum::<u64>();
        let samples_u64 = u64::try_from(trace.measured.len()).unwrap_or(u64::MAX);
        let misses = physical.iter().filter(|bytes| **bytes > 0).count();
        let cache_hit_ratio =
            1.0 - f64_from_usize(misses) / f64_from_usize(trace.measured.len()).max(1.0);
        let cpu_millis = after
            .accumulated_cpu_millis
            .saturating_sub(before.accumulated_cpu_millis);
        let point_curve = PointCurve {
            concurrency,
            samples: trace.measured.len(),
            duration_seconds: duration,
            iops: f64_from_usize(trace.measured.len()) / duration.max(f64::EPSILON),
            latency_p50_seconds: nanos_to_seconds(percentile(&latencies, 500)),
            latency_p95_seconds: nanos_to_seconds(percentile(&latencies, 950)),
            latency_p99_seconds: nanos_to_seconds(percentile(&latencies, 990)),
            latency_p999_seconds: nanos_to_seconds(percentile(&latencies, 999)),
            physical_bytes_per_point_p50: percentile(&physical, 500),
            physical_bytes_per_point_p99: percentile(&physical, 990),
            physical_bytes,
            physical_bytes_per_second: f64_from_u64(physical_bytes) / duration.max(f64::EPSILON),
            logical_bytes_per_second: f64_from_u64(
                samples_u64.saturating_mul(u64::try_from(value_bytes).unwrap_or(u64::MAX)),
            ) / duration.max(f64::EPSILON),
            cache_hit_ratio,
            cpu_seconds_per_million_points: f64_from_u64(cpu_millis)
                / 1_000.0
                / (f64_from_usize(trace.measured.len()) / 1_000_000.0).max(f64::EPSILON),
            minor_faults: after.minor_faults.saturating_sub(before.minor_faults),
            major_faults: after.major_faults.saturating_sub(before.major_faults),
            voluntary_context_switches: after
                .voluntary_context_switches
                .saturating_sub(before.voluntary_context_switches),
            involuntary_context_switches: after
                .involuntary_context_switches
                .saturating_sub(before.involuntary_context_switches),
            exact,
        };
        maximum_accounted = maximum_accounted.max(reader.accounted_resident_bytes());
        maximum_peak_rss = maximum_peak_rss.max(after.peak_rss_bytes);
        point_curves.push(point_curve);
    }

    let scan_config = NvmeRangeImageConfig {
        block_payload_bytes,
        reader_memory_budget_bytes,
        maximum_concurrency: 1,
        io_mode,
    };
    let expected = NvmeRangeImageIdentity {
        image_identity_sha256: Some(&write_receipt.image_identity_sha256),
        ..identity
    };
    let (scan_reader, _) = NvmeRangeImageReader::open(&image_path, &expected, scan_config)?;
    let io_before = scan_reader.file_io();
    let mut scan_hasher = Sha256::new();
    let mut scan_ordinal = 0_usize;
    let mut scan_exact = true;
    let scan_started = Instant::now();
    let scan_rows =
        scan_reader.scan_batches(&range_begin, &range_end, trace.key_count, 256, |batch| {
            for (key, value) in batch {
                scan_exact &= key.as_slice() == key_for(scan_ordinal).as_slice()
                    && value_matches(trace.seed, scan_ordinal, value_bytes, value);
                update_digest(&mut scan_hasher, key, value);
                scan_ordinal = scan_ordinal.saturating_add(1);
            }
            Ok(())
        })?;
    let scan_duration = scan_started.elapsed().as_secs_f64();
    let scan_io = scan_reader.file_io().difference_since(io_before);
    scan_exact &= scan_rows == trace.key_count && scan_ordinal == trace.key_count;
    let scan_digest_sha256 = format!("{:x}", scan_hasher.finalize());
    let scan = ScanCurve {
        rows: scan_rows,
        logical_bytes: logical_value_bytes,
        physical_bytes: scan_io.bytes,
        duration_seconds: scan_duration,
        logical_bytes_per_second: f64_from_u64(logical_value_bytes)
            / scan_duration.max(f64::EPSILON),
        rows_per_second: f64_from_usize(scan_rows) / scan_duration.max(f64::EPSILON),
        digest_sha256: scan_digest_sha256.clone(),
        exact: scan_exact,
    };
    maximum_accounted = maximum_accounted.max(scan_reader.accounted_resident_bytes());
    maximum_peak_rss = maximum_peak_rss.max(process_snapshot().peak_rss_bytes);
    let open = representative_open.ok_or_else(|| "probe has no open receipt".to_owned())?;
    let semantic_replay_sha256 = semantic_replay_digest(
        &trace.sha256,
        &fixture_sha256,
        &write_receipt.image_identity_sha256,
        &scan_digest_sha256,
        &point_curves,
    );
    Ok(ProbeReceipt {
        schema_version: 1,
        engine: "objectkv-range-image-v3",
        io_mode: io_mode.id(),
        seed: trace.seed,
        key_count: trace.key_count,
        value_bytes,
        logical_value_bytes,
        trace_sha256: trace.sha256,
        fixture_sha256,
        block_payload_bytes,
        image_identity_sha256: write_receipt.image_identity_sha256,
        image_bytes: write_receipt.image_bytes,
        image_amplification: f64_from_u64(write_receipt.image_bytes)
            / f64_from_u64(logical_value_bytes).max(1.0),
        index_logical_bytes: write_receipt.index_logical_bytes,
        index_physical_bytes: write_receipt.index_physical_bytes,
        block_count: write_receipt.block_count,
        open_operations: open.open_file_io.operations,
        open_bytes: open.open_file_io.bytes,
        direct_io_active: open.direct_io_active,
        alignment_violations: open.alignment_violations,
        base_resident_bytes: open.base_resident_bytes,
        maximum_cache_bytes: open.maximum_cache_bytes,
        maximum_inflight_buffer_bytes: open.maximum_inflight_buffer_bytes,
        accounted_resident_bytes: maximum_accounted,
        peak_worker_rss_bytes: maximum_peak_rss,
        point_curves,
        scan,
        semantic_replay_sha256,
    })
}

fn run_points(
    reader: &NvmeRangeImageReader,
    seed: u64,
    value_bytes: usize,
    trace: &[u32],
    concurrency: usize,
    record: bool,
) -> Result<Vec<PointWorkerReceipt>, String> {
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(concurrency);
        for worker in 0..concurrency {
            let start = trace.len().saturating_mul(worker) / concurrency;
            let end = trace.len().saturating_mul(worker.saturating_add(1)) / concurrency;
            let slice = &trace[start..end];
            handles.push(scope.spawn(move || {
                let mut receipt = PointWorkerReceipt {
                    latencies_ns: Vec::with_capacity(if record { slice.len() } else { 0 }),
                    physical_bytes: Vec::with_capacity(if record { slice.len() } else { 0 }),
                    exact: true,
                };
                for ordinal in slice {
                    let ordinal = usize::try_from(*ordinal).unwrap_or(usize::MAX);
                    let key = key_for(ordinal);
                    let started = Instant::now();
                    let read = reader.get_with_io(&key)?;
                    let latency = started.elapsed().as_nanos();
                    let value = read
                        .value
                        .ok_or_else(|| "point key is absent from range image".to_owned())?;
                    receipt.exact &= value_matches(seed, ordinal, value_bytes, &value);
                    if record {
                        receipt
                            .latencies_ns
                            .push(u64::try_from(latency).unwrap_or(u64::MAX));
                        receipt.physical_bytes.push(read.physical_bytes);
                    }
                }
                Ok::<PointWorkerReceipt, String>(receipt)
            }));
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "point worker panicked".to_owned())?
            })
            .collect()
    })
}

fn fixture_sha256(seed: u64, key_count: usize, value_bytes: usize) -> String {
    let mut digest = Sha256::new();
    for ordinal in 0..key_count {
        let key = key_for(ordinal);
        let value = deterministic_value(seed, ordinal, value_bytes);
        update_digest(&mut digest, &key, &value);
    }
    format!("{:x}", digest.finalize())
}

fn key_for(ordinal: usize) -> Vec<u8> {
    format!("{KEY_PREFIX}{ordinal:016x}").into_bytes()
}

fn deterministic_value(seed: u64, ordinal: usize, bytes: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(bytes);
    let mut state = initial_value_state(seed, ordinal);
    while value.len() < bytes {
        value.extend_from_slice(&next_value_word(&mut state).to_be_bytes());
    }
    value.truncate(bytes);
    value
}

fn value_matches(seed: u64, ordinal: usize, expected_bytes: usize, value: &[u8]) -> bool {
    if value.len() != expected_bytes {
        return false;
    }
    let mut state = initial_value_state(seed, ordinal);
    let mut offset = 0_usize;
    while offset < value.len() {
        let word = next_value_word(&mut state).to_be_bytes();
        let remaining = value.len().saturating_sub(offset).min(word.len());
        if value[offset..offset + remaining] != word[..remaining] {
            return false;
        }
        offset = offset.saturating_add(remaining);
    }
    true
}

fn initial_value_state(seed: u64, ordinal: usize) -> u64 {
    seed ^ u64::try_from(ordinal)
        .unwrap_or(u64::MAX)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

fn next_value_word(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut mixed = *state;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

fn update_digest(hasher: &mut Sha256, key: &[u8], value: &[u8]) {
    hasher.update(u64::try_from(key.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(key);
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn semantic_replay_digest(
    trace: &str,
    fixture: &str,
    image: &str,
    scan: &str,
    points: &[PointCurve],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-rfc0071-semantic-replay-v1");
    hasher.update(trace.as_bytes());
    hasher.update(fixture.as_bytes());
    hasher.update(image.as_bytes());
    hasher.update(scan.as_bytes());
    for point in points {
        hasher.update(point.concurrency.to_be_bytes());
        hasher.update([u8::from(point.exact)]);
    }
    format!("{:x}", hasher.finalize())
}

fn percentile(sorted: &[u64], per_thousand: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let numerator = sorted.len().saturating_mul(per_thousand);
    let index = numerator
        .saturating_add(999)
        .checked_div(1_000)
        .unwrap_or(0)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted[index]
}

fn process_snapshot() -> ProcessSnapshot {
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory().with_cpu(),
    );
    let (accumulated_cpu_millis, current_rss_bytes) =
        system.process(pid).map_or((0, 0), |process| {
            (process.accumulated_cpu_time(), process.memory())
        });
    let peak_rss_bytes = linux_peak_rss_bytes().unwrap_or(current_rss_bytes);
    let (minor_faults, major_faults) = linux_faults();
    let (voluntary_context_switches, involuntary_context_switches) = linux_context_switches();
    ProcessSnapshot {
        accumulated_cpu_millis,
        peak_rss_bytes,
        minor_faults,
        major_faults,
        voluntary_context_switches,
        involuntary_context_switches,
    }
}

fn linux_peak_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let kibibytes = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    Some(kibibytes.saturating_mul(1_024))
}

fn linux_faults() -> (u64, u64) {
    let Ok(stat) = fs::read_to_string("/proc/self/stat") else {
        return (0, 0);
    };
    let Some(close) = stat.rfind(')') else {
        return (0, 0);
    };
    let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
    let minor = fields
        .get(7)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let major = fields
        .get(9)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    (minor, major)
}

fn linux_context_switches() -> (u64, u64) {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return (0, 0);
    };
    let value = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0)
    };
    (
        value("voluntary_ctxt_switches:"),
        value("nonvoluntary_ctxt_switches:"),
    )
}

fn take_u64(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let end = offset.saturating_add(size_of::<u64>());
    let encoded = bytes
        .get(offset..end)
        .ok_or_else(|| "trace integer is truncated".to_owned())?;
    Ok(u64::from_be_bytes(
        encoded
            .try_into()
            .map_err(|_| "trace integer is invalid".to_owned())?,
    ))
}

fn nanos_to_seconds(value: u64) -> f64 {
    f64_from_u64(value) / 1_000_000_000.0
}

fn f64_from_u64(value: u64) -> f64 {
    value.to_string().parse().unwrap_or(f64::MAX)
}

fn f64_from_usize(value: usize) -> f64 {
    value.to_string().parse().unwrap_or(f64::MAX)
}

const fn size_of<T>() -> usize {
    std::mem::size_of::<T>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn trace_round_trip_is_exact() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("trace.bin");
        let written = write_trace(&path, 724_851, 96, 32, 64).expect("write trace");
        let loaded = load_trace(&path).expect("load trace");
        assert_eq!(loaded.seed, 724_851);
        assert_eq!(loaded.key_count, 96);
        assert_eq!(loaded.warmup.len(), 32);
        assert_eq!(loaded.measured.len(), 64);
        assert_eq!(loaded.sha256, written.sha256);
    }

    #[test]
    fn value_verifier_matches_generator() {
        let value = deterministic_value(724_851, 17, 8_193);
        assert!(value_matches(724_851, 17, 8_193, &value));
        assert!(!value_matches(724_851, 18, 8_193, &value));
    }
}
