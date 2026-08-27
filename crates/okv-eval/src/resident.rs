//! Bounded resident `ServingWorker` versus direct `RocksDB` evaluation profile.

use rocksdb::{Options, WriteBatch, WriteOptions, DB};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::{Builder as TempDirBuilder, TempDir};

const ACTIVE_GENERATION: u64 = 7;
const READ_VERSION: u64 = 41;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidentMode {
    Candidate,
    DirectControl,
    DirectOwnedControl,
    ObjectFallbackPoison,
}

#[derive(Clone, Debug)]
pub struct ResidentProfile {
    pub key_count: u64,
    pub value_bytes: usize,
    pub operations_per_repeat: usize,
    pub warmup_operations: usize,
    pub repeats: u32,
    pub seeds: Vec<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResidentSample {
    pub seed: u64,
    pub repeat: u32,
    pub operations: usize,
    pub elapsed_seconds: f64,
    pub operations_per_second: f64,
    pub latency_ns_p50: u64,
    pub latency_ns_p95: u64,
    pub latency_ns_p99: u64,
    pub latency_ns_p999: u64,
    pub correctness_failures: u64,
    pub digest: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResidentReport {
    pub samples: Vec<ResidentSample>,
    pub operations: u64,
    pub correctness_failures: u64,
    pub object_fallbacks: u64,
    pub measured_reads: u64,
    pub max_local_bytes: u64,
}

impl ResidentReport {
    #[must_use]
    pub fn cache_hit_ratio(&self) -> f64 {
        if self.measured_reads == 0 {
            return 0.0;
        }
        count_as_f64(self.measured_reads.saturating_sub(self.object_fallbacks))
            / count_as_f64(self.measured_reads)
    }
}

/// Run one bounded resident read profile.
///
/// # Errors
///
/// Returns an error when the profile is invalid, `RocksDB` cannot be prepared, or
/// a warmup read cannot be satisfied.
pub fn run_resident_profile(
    mode: ResidentMode,
    profile: &ResidentProfile,
) -> Result<ResidentReport, String> {
    validate_profile(profile)?;
    let mut report = ResidentReport {
        samples: Vec::new(),
        operations: 0,
        correctness_failures: 0,
        object_fallbacks: 0,
        measured_reads: 0,
        max_local_bytes: 0,
    };

    for seed in &profile.seeds {
        let directory = resident_tempdir()?;
        let database = open_database(directory.path())?;
        populate(&database, profile)?;
        report.max_local_bytes = report
            .max_local_bytes
            .max(directory_bytes(directory.path())?);
        let keys = operation_keys(profile.key_count, profile.operations_per_repeat, *seed);
        let mut resident = ResidentRange {
            db: &database,
            begin: 0,
            end: profile.key_count,
            generation: ACTIVE_GENERATION,
            complete_through: READ_VERSION,
            recent_overlay: BTreeMap::new(),
            object_fallbacks: 0,
        };

        warm(
            mode,
            &database,
            &mut resident,
            &keys,
            profile.warmup_operations,
        )?;
        if mode == ResidentMode::ObjectFallbackPoison {
            resident.complete_through = READ_VERSION - 1;
        }

        for repeat in 0..profile.repeats {
            let sample = match mode {
                ResidentMode::DirectControl => measure_direct(&database, &keys, *seed, repeat),
                ResidentMode::DirectOwnedControl => {
                    measure_direct_owned(&database, &keys, *seed, repeat)
                }
                ResidentMode::Candidate | ResidentMode::ObjectFallbackPoison => {
                    measure_candidate(&mut resident, &keys, *seed, repeat)
                }
            };
            report.operations = report
                .operations
                .saturating_add(u64::try_from(sample.operations).unwrap_or(u64::MAX));
            report.correctness_failures = report
                .correctness_failures
                .saturating_add(sample.correctness_failures);
            report.measured_reads = report
                .measured_reads
                .saturating_add(u64::try_from(sample.operations).unwrap_or(u64::MAX));
            report.samples.push(sample);
        }
        report.object_fallbacks = report
            .object_fallbacks
            .saturating_add(resident.object_fallbacks);
    }

    Ok(report)
}

fn validate_profile(profile: &ResidentProfile) -> Result<(), String> {
    if profile.key_count < 10 {
        return Err("resident profile requires at least 10 keys".to_owned());
    }
    if profile.value_bytes < 16 {
        return Err("resident profile requires values of at least 16 bytes".to_owned());
    }
    if profile.operations_per_repeat == 0 || profile.repeats == 0 {
        return Err("resident profile requires positive operations and repeats".to_owned());
    }
    if profile.seeds.is_empty() || profile.seeds.contains(&0) {
        return Err("resident profile requires at least one non-zero seed".to_owned());
    }
    Ok(())
}

fn open_database(path: &Path) -> Result<DB, String> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.optimize_for_point_lookup(128);
    options.set_max_open_files(256);
    DB::open(&options, path).map_err(|error| format!("open resident DB: {error}"))
}

fn populate(database: &DB, profile: &ResidentProfile) -> Result<(), String> {
    let mut write_options = WriteOptions::default();
    write_options.disable_wal(true);
    let mut batch = WriteBatch::default();
    for key_id in 0..profile.key_count {
        batch.put(key_bytes(key_id), value_bytes(key_id, profile.value_bytes));
        if key_id % 4_096 == 4_095 {
            database
                .write_opt(batch, &write_options)
                .map_err(|error| format!("populate resident DB: {error}"))?;
            batch = WriteBatch::default();
        }
    }
    if !batch.is_empty() {
        database
            .write_opt(batch, &write_options)
            .map_err(|error| format!("populate resident DB: {error}"))?;
    }
    database
        .flush()
        .map_err(|error| format!("flush resident DB: {error}"))
}

fn operation_keys(key_count: u64, operations: usize, seed: u64) -> Vec<u64> {
    let mut random = XorShift64(seed);
    let hot_keys = (key_count / 5).max(1);
    let cold_keys = key_count - hot_keys;
    (0..operations)
        .map(|_| {
            if random.next() % 100 < 80 || cold_keys == 0 {
                random.next() % hot_keys
            } else {
                hot_keys + random.next() % cold_keys
            }
        })
        .collect()
}

fn warm(
    mode: ResidentMode,
    database: &DB,
    resident: &mut ResidentRange<'_>,
    keys: &[u64],
    operations: usize,
) -> Result<(), String> {
    for key_id in keys.iter().cycle().take(operations) {
        let value = match mode {
            ResidentMode::DirectControl => direct_get(database, *key_id),
            ResidentMode::DirectOwnedControl => direct_get_owned(database, *key_id),
            ResidentMode::Candidate | ResidentMode::ObjectFallbackPoison => {
                resident.get_at(*key_id, READ_VERSION, ACTIVE_GENERATION)
            }
        }?;
        if value.is_none() {
            return Err("resident warmup key missing".to_owned());
        }
    }
    Ok(())
}

fn measure_direct(database: &DB, keys: &[u64], seed: u64, repeat: u32) -> ResidentSample {
    measure(keys, seed, repeat, |key_id| direct_get(database, key_id))
}

fn measure_direct_owned(database: &DB, keys: &[u64], seed: u64, repeat: u32) -> ResidentSample {
    measure(keys, seed, repeat, |key_id| {
        direct_get_owned(database, key_id)
    })
}

fn measure_candidate(
    resident: &mut ResidentRange<'_>,
    keys: &[u64],
    seed: u64,
    repeat: u32,
) -> ResidentSample {
    measure(keys, seed, repeat, |key_id| {
        resident.get_at(key_id, READ_VERSION, ACTIVE_GENERATION)
    })
}

fn measure<F>(keys: &[u64], seed: u64, repeat: u32, mut read: F) -> ResidentSample
where
    F: FnMut(u64) -> Result<Option<ValueDigest>, String>,
{
    let run_started = Instant::now();
    let mut failures = 0;
    let mut digest = 0_u64;
    let mut latencies = Vec::with_capacity(keys.len());
    for key_id in keys {
        let started = Instant::now();
        match read(*key_id) {
            Ok(Some(value)) => digest = digest.wrapping_add(value.0),
            _ => failures += 1,
        }
        latencies.push(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
    }
    let elapsed_seconds = run_started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    ResidentSample {
        seed,
        repeat,
        operations: keys.len(),
        elapsed_seconds,
        operations_per_second: count_as_f64(u64::try_from(keys.len()).unwrap_or(u64::MAX))
            / elapsed_seconds,
        latency_ns_p50: percentile(&latencies, 50, 100),
        latency_ns_p95: percentile(&latencies, 95, 100),
        latency_ns_p99: percentile(&latencies, 99, 100),
        latency_ns_p999: percentile(&latencies, 999, 1_000),
        correctness_failures: failures,
        digest,
    }
}

struct VersionedValue {
    version: u64,
    value: Vec<u8>,
}

struct ResidentRange<'a> {
    db: &'a DB,
    begin: u64,
    end: u64,
    generation: u64,
    complete_through: u64,
    recent_overlay: BTreeMap<u64, VersionedValue>,
    object_fallbacks: u64,
}

impl ResidentRange<'_> {
    fn get_at(
        &mut self,
        key_id: u64,
        read_version: u64,
        generation: u64,
    ) -> Result<Option<ValueDigest>, String> {
        if generation != self.generation {
            return Err("stale resident generation".to_owned());
        }
        if key_id < self.begin || key_id >= self.end {
            return Err("key outside resident range".to_owned());
        }
        if let Some(versioned) = self.recent_overlay.get(&key_id) {
            if versioned.version <= read_version {
                return validate_value(key_id, &versioned.value).map(Some);
            }
        }
        if read_version > self.complete_through {
            self.object_fallbacks = self.object_fallbacks.saturating_add(1);
        }
        direct_get(self.db, key_id)
    }
}

#[derive(Clone, Copy)]
struct ValueDigest(u64);

fn direct_get(database: &DB, key_id: u64) -> Result<Option<ValueDigest>, String> {
    database
        .get_pinned(key_bytes(key_id))
        .map_err(|error| format!("resident point read: {error}"))?
        .map(|value| validate_value(key_id, value.as_ref()))
        .transpose()
}

fn direct_get_owned(database: &DB, key_id: u64) -> Result<Option<ValueDigest>, String> {
    database
        .get(key_bytes(key_id))
        .map_err(|error| format!("resident owned point read: {error}"))?
        .map(|value| validate_value(key_id, &value))
        .transpose()
}

fn key_bytes(key_id: u64) -> [u8; 8] {
    key_id.to_be_bytes()
}

fn value_bytes(key_id: u64, length: usize) -> Vec<u8> {
    let mut value = vec![0_u8; length];
    let mut state = key_id ^ 0x9e37_79b9_7f4a_7c15;
    for chunk in value.chunks_mut(8) {
        state = splitmix64(state);
        let encoded = state.to_be_bytes();
        chunk.copy_from_slice(&encoded[..chunk.len()]);
    }
    value[..8].copy_from_slice(&key_id.to_be_bytes());
    let tail = key_id.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
    value[length - 8..].copy_from_slice(&tail.to_be_bytes());
    value
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate_value(key_id: u64, value: &[u8]) -> Result<ValueDigest, String> {
    if value.len() < 16 || value[..8] != key_id.to_be_bytes() {
        return Err("resident value identity mismatch".to_owned());
    }
    let tail: [u8; 8] = value[value.len() - 8..]
        .try_into()
        .map_err(|error| format!("resident value tail: {error}"))?;
    let tail = u64::from_be_bytes(tail);
    let expected = key_id.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
    if tail != expected {
        return Err("resident value checksum mismatch".to_owned());
    }
    Ok(ValueDigest(key_id ^ tail ^ value.len() as u64))
}

fn percentile(values: &[u64], numerator: usize, denominator: usize) -> u64 {
    let index = (values.len() - 1)
        .saturating_mul(numerator)
        .div_ceil(denominator);
    values[index]
}

#[allow(clippy::cast_precision_loss)]
fn count_as_f64(value: u64) -> f64 {
    value as f64
}

fn directory_bytes(path: &Path) -> Result<u64, String> {
    let mut bytes = 0_u64;
    let entries = fs::read_dir(path).map_err(|error| format!("read resident DB: {error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("read resident DB entry: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("stat resident DB entry: {error}"))?;
        if metadata.is_file() {
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok(bytes)
}

fn resident_tempdir() -> Result<TempDir, String> {
    match std::env::var_os("OKV_EVAL_SERVING_SCRATCH_ROOT") {
        Some(root) => {
            let root = std::path::PathBuf::from(root);
            fs::create_dir_all(&root).map_err(|error| {
                format!(
                    "create configured resident scratch root {}: {error}",
                    root.display()
                )
            })?;
            TempDirBuilder::new()
                .prefix("okv-direct-rocksdb-")
                .tempdir_in(&root)
                .map_err(|error| format!("create resident DB below {}: {error}", root.display()))
        }
        None => TempDir::new().map_err(|error| format!("create resident DB: {error}")),
    }
}

struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}

#[cfg(test)]
mod tests {
    use super::{run_resident_profile, ResidentMode, ResidentProfile};

    fn smoke_profile() -> ResidentProfile {
        ResidentProfile {
            key_count: 1_024,
            value_bytes: 128,
            operations_per_repeat: 1_000,
            warmup_operations: 500,
            repeats: 1,
            seeds: vec![1_103],
        }
    }

    #[test]
    fn candidate_stays_resident() {
        let report = run_resident_profile(ResidentMode::Candidate, &smoke_profile())
            .expect("resident candidate should run");
        assert_eq!(report.correctness_failures, 0);
        assert_eq!(report.object_fallbacks, 0);
        assert!((report.cache_hit_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn poison_is_detected_after_warmup() {
        let report = run_resident_profile(ResidentMode::ObjectFallbackPoison, &smoke_profile())
            .expect("resident poison should run");
        assert_eq!(report.correctness_failures, 0);
        assert_eq!(report.object_fallbacks, report.measured_reads);
        assert!(report.cache_hit_ratio().abs() < f64::EPSILON);
    }

    #[test]
    fn owned_control_reads_the_same_complete_workload() {
        let report = run_resident_profile(ResidentMode::DirectOwnedControl, &smoke_profile())
            .expect("owned direct control should run");
        assert_eq!(report.correctness_failures, 0);
        assert_eq!(report.object_fallbacks, 0);
        assert_eq!(report.measured_reads, 1_000);
    }
}
