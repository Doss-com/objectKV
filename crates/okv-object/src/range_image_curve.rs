//! Frozen RFC-0070 bounded-memory range-image experiment.

use crate::range_image::{
    corrupt_first_block_value, corrupt_index_checksum, root_identity_digest, write_range_image,
    RangeImageIdentity, RangeImageOpenMode, RangeImageReader, RangeRow,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const IMAGE_NAME: &str = "range-image.okv";
const IMAGE_FORMAT: &str = "okv-derived-sorted-range-v2";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeImageCurveMode {
    #[default]
    Correct,
    DecodeWholeImage,
    LinearPointScan,
    AcceptCorruptIndex,
    SkipBlockChecksum,
}

impl RangeImageCurveMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::DecodeWholeImage => "decode_whole_image",
            Self::LinearPointScan => "linear_point_scan",
            Self::AcceptCorruptIndex => "accept_corrupt_index",
            Self::SkipBlockChecksum => "skip_block_checksum",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeImageDistribution {
    #[default]
    Uniform,
    Zipf099,
    Sequential,
}

impl RangeImageDistribution {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Zipf099 => "zipf_0_99",
            Self::Sequential => "sequential",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeImageCurveConfig {
    pub key_count: usize,
    pub value_bytes: usize,
    pub logical_range_count: usize,
    pub assigned_range_index: usize,
    pub reader_memory_budget_bytes: usize,
    pub warmup_point_reads: usize,
    pub measured_point_reads: usize,
    pub distribution: RangeImageDistribution,
    pub process_reopen: bool,
    pub scan: bool,
    pub mode: RangeImageCurveMode,
    pub seed: u64,
    #[serde(default)]
    pub process_probe_executable: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeImageCurveProbeConfig {
    pub curve: RangeImageCurveConfig,
    pub image_path: PathBuf,
    pub image_identity_sha256: String,
    pub image_bytes: u64,
    pub index_bytes: u64,
    pub block_count: u32,
    pub root_identity_digest: [u8; 32],
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RangeImageCurveReceipt {
    pub contract_version: u16,
    pub mode: RangeImageCurveMode,
    pub distribution: RangeImageDistribution,
    pub seed: u64,
    pub logical_range_count: usize,
    pub assigned_range_index: usize,
    pub reader_memory_budget_bytes: u64,
    pub image_format: String,
    pub image_identity_sha256: String,
    pub image_bytes: u64,
    pub index_bytes: u64,
    pub block_count: u32,
    pub accounted_resident_bytes: u64,
    pub peak_rss_delta_bytes: u64,
    pub open_duration_seconds: f64,
    pub open_file_read_operations: u64,
    pub open_file_read_bytes: u64,
    pub point_file_read_operations_p99: u64,
    pub point_file_read_bytes_p99: u64,
    pub point_p99_seconds: f64,
    pub application_cache_hit_ratio: f64,
    pub scan_rows: u64,
    pub scan_rows_per_second: f64,
    pub exact_points: bool,
    pub exact_scan: bool,
    pub outside_range_refused: bool,
    pub root_bound_receipt_exact: bool,
    pub index_checksum_verified: bool,
    pub block_checksums_verified: bool,
    pub post_ready_provider_requests: u64,
    pub post_ready_provider_bytes: u64,
    pub os_page_cache_controlled: bool,
    pub process_reopen_requested: bool,
    pub process_reopen_executed: bool,
    pub scratch_cleanup_complete: bool,
    pub semantic_receipt_sha256: String,
}

/// Build one disposable image and measure it through the configured process boundary.
///
/// # Errors
///
/// Returns an error when fixture generation, image creation, probing, or guarded
/// scratch cleanup fails.
pub fn run_range_image_curve_worker(
    config: &RangeImageCurveConfig,
) -> Result<RangeImageCurveReceipt, String> {
    validate_config(config)?;
    let scratch_parent =
        std::env::var_os("OKV_EVAL_SCRATCH_ROOT").map_or_else(std::env::temp_dir, PathBuf::from);
    fs::create_dir_all(&scratch_parent).map_err(|error| error.to_string())?;
    let scratch = tempfile::Builder::new()
        .prefix(&format!("okv-range-image-io-{}-", config.seed))
        .tempdir_in(&scratch_parent)
        .map_err(|error| error.to_string())?;
    let scratch_path = scratch.path().to_path_buf();
    let image_path = scratch.path().join(IMAGE_NAME);
    let rows = assigned_rows(config);
    let (range_begin, range_end) = assigned_bounds(config);
    let root_identity = curve_root_identity(config, &range_begin, &range_end);
    let write = write_range_image(
        &image_path,
        &RangeImageIdentity {
            target_version: 1,
            range_begin: &range_begin,
            range_end: &range_end,
            row_count: u64::try_from(rows.len()).unwrap_or(u64::MAX),
            root_identity_digest: root_identity,
            image_identity_sha256: None,
        },
        &rows,
    )?;
    match config.mode {
        RangeImageCurveMode::AcceptCorruptIndex => corrupt_index_checksum(&image_path)?,
        RangeImageCurveMode::SkipBlockChecksum => corrupt_first_block_value(&image_path)?,
        RangeImageCurveMode::Correct
        | RangeImageCurveMode::DecodeWholeImage
        | RangeImageCurveMode::LinearPointScan => {}
    }
    let mut curve = config.clone();
    curve.process_probe_executable = None;
    let probe = RangeImageCurveProbeConfig {
        curve,
        image_path,
        image_identity_sha256: write.image_identity_sha256,
        image_bytes: write.image_bytes,
        index_bytes: write.index_bytes,
        block_count: write.block_count,
        root_identity_digest: root_identity,
    };
    let mut receipt = if config.process_reopen {
        let executable = config
            .process_probe_executable
            .as_deref()
            .ok_or_else(|| "range-image process reopen requires a probe executable".to_owned())?;
        let mut receipt = run_probe_child(executable, &probe)?;
        receipt.process_reopen_executed = true;
        receipt
    } else {
        run_range_image_curve_probe(&probe)?
    };
    scratch.close().map_err(|error| error.to_string())?;
    receipt.scratch_cleanup_complete = !scratch_path.exists();
    Ok(receipt)
}

/// Open and measure a retained image without constructing an object-store client.
///
/// # Errors
///
/// Returns an error when the image cannot be authenticated or the deterministic
/// workload cannot complete.
#[allow(clippy::too_many_lines)]
pub fn run_range_image_curve_probe(
    config: &RangeImageCurveProbeConfig,
) -> Result<RangeImageCurveReceipt, String> {
    validate_config(&config.curve)?;
    let expected = assigned_rows(&config.curve);
    let (range_begin, range_end) = assigned_bounds(&config.curve);
    let resident_before = resident_memory_bytes();
    let open_mode = match config.curve.mode {
        RangeImageCurveMode::AcceptCorruptIndex => RangeImageOpenMode::AcceptCorruptIndexChecksum,
        RangeImageCurveMode::SkipBlockChecksum => RangeImageOpenMode::SkipBlockChecksum,
        RangeImageCurveMode::Correct
        | RangeImageCurveMode::DecodeWholeImage
        | RangeImageCurveMode::LinearPointScan => RangeImageOpenMode::Correct,
    };
    let open_started = Instant::now();
    let (reader, open) = RangeImageReader::open_with_mode(
        &config.image_path,
        &RangeImageIdentity {
            target_version: 1,
            range_begin: &range_begin,
            range_end: &range_end,
            row_count: u64::try_from(expected.len()).unwrap_or(u64::MAX),
            root_identity_digest: config.root_identity_digest,
            image_identity_sha256: Some(&config.image_identity_sha256),
        },
        config.curve.reader_memory_budget_bytes,
        open_mode,
    )?;
    let open_duration_seconds = open_started.elapsed().as_secs_f64();
    let retained_whole_image = if config.curve.mode == RangeImageCurveMode::DecodeWholeImage {
        fs::read(&config.image_path).map_err(|error| error.to_string())?
    } else {
        Vec::new()
    };
    let trace = point_trace(
        config.curve.distribution,
        expected.len(),
        config
            .curve
            .warmup_point_reads
            .saturating_add(config.curve.measured_point_reads),
        config.curve.seed,
    );
    let point = |ordinal: usize| {
        if config.curve.mode == RangeImageCurveMode::LinearPointScan {
            reader.get_linear_uncached(&expected[ordinal].0)
        } else {
            reader.get(&expected[ordinal].0)
        }
    };
    let first_exact = point(0)? == Some(expected[0].1.clone());
    for ordinal in trace.iter().take(config.curve.warmup_point_reads) {
        let _ = point(*ordinal)?;
    }
    let mut exact_points = first_exact;
    let mut durations = Vec::with_capacity(config.curve.measured_point_reads);
    let mut operations = Vec::with_capacity(config.curve.measured_point_reads);
    let mut bytes = Vec::with_capacity(config.curve.measured_point_reads);
    let mut cache_hits = 0_u64;
    for ordinal in trace
        .iter()
        .skip(config.curve.warmup_point_reads)
        .take(config.curve.measured_point_reads)
    {
        let before = reader.file_io();
        let started = Instant::now();
        let observed = point(*ordinal)?;
        durations.push(started.elapsed().as_secs_f64());
        let io = reader.file_io().difference_since(before);
        operations.push(io.operations);
        bytes.push(io.bytes);
        cache_hits = cache_hits.saturating_add(u64::from(io.operations == 0));
        exact_points &= observed.as_ref() == Some(&expected[*ordinal].1);
    }
    durations.sort_by(f64::total_cmp);
    operations.sort_unstable();
    bytes.sort_unstable();
    let mut exact_scan = true;
    let mut scan_rows = 0_usize;
    let mut scan_rows_per_second = 0.0;
    if config.curve.scan {
        let scan_started = Instant::now();
        let mut observed_scan_rows = 0_usize;
        let returned_scan_rows =
            reader.scan_batches(&range_begin, &range_end, expected.len(), 32, |batch| {
                let start = observed_scan_rows;
                let end = start.saturating_add(batch.len());
                exact_scan &= expected.get(start..end).is_some_and(|rows| rows == batch);
                observed_scan_rows = end;
                Ok(())
            })?;
        scan_rows = returned_scan_rows;
        let scan_seconds = scan_started.elapsed().as_secs_f64();
        scan_rows_per_second = bounded_f64_usize(scan_rows) / scan_seconds.max(f64::EPSILON);
        exact_scan &= scan_rows == expected.len() && observed_scan_rows == scan_rows;
    }
    let outside_range_refused = reader.get(b"outside-range-image").is_err();
    let accounted_resident_bytes = reader
        .accounted_resident_bytes()
        .saturating_add(u64::try_from(retained_whole_image.capacity()).unwrap_or(u64::MAX));
    let resident_after = resident_memory_bytes();
    let peak_rss_delta_bytes = resident_after.saturating_sub(resident_before);
    let index_checksum_verified = config.curve.mode != RangeImageCurveMode::AcceptCorruptIndex;
    let block_checksums_verified = config.curve.mode != RangeImageCurveMode::SkipBlockChecksum;
    let root_bound_receipt_exact =
        index_checksum_verified && reader.image_identity_sha256() == config.image_identity_sha256;
    let semantic_receipt_sha256 = semantic_receipt(
        config,
        exact_points,
        exact_scan,
        outside_range_refused,
        root_bound_receipt_exact,
        index_checksum_verified,
        block_checksums_verified,
    );
    Ok(RangeImageCurveReceipt {
        contract_version: 1,
        mode: config.curve.mode,
        distribution: config.curve.distribution,
        seed: config.curve.seed,
        logical_range_count: config.curve.logical_range_count,
        assigned_range_index: config.curve.assigned_range_index,
        reader_memory_budget_bytes: u64::try_from(config.curve.reader_memory_budget_bytes)
            .unwrap_or(u64::MAX),
        image_format: IMAGE_FORMAT.to_owned(),
        image_identity_sha256: config.image_identity_sha256.clone(),
        image_bytes: config.image_bytes,
        index_bytes: config.index_bytes,
        block_count: config.block_count,
        accounted_resident_bytes,
        peak_rss_delta_bytes,
        open_duration_seconds,
        open_file_read_operations: open.open_file_io.operations,
        open_file_read_bytes: open.open_file_io.bytes,
        point_file_read_operations_p99: percentile_u64(&operations, 99),
        point_file_read_bytes_p99: percentile_u64(&bytes, 99),
        point_p99_seconds: percentile_f64(&durations, 99),
        application_cache_hit_ratio: ratio_u64(
            cache_hits,
            u64::try_from(config.curve.measured_point_reads).unwrap_or(u64::MAX),
        ),
        scan_rows: u64::try_from(scan_rows).unwrap_or(u64::MAX),
        scan_rows_per_second,
        exact_points,
        exact_scan,
        outside_range_refused,
        root_bound_receipt_exact,
        index_checksum_verified,
        block_checksums_verified,
        post_ready_provider_requests: 0,
        post_ready_provider_bytes: 0,
        os_page_cache_controlled: false,
        process_reopen_requested: config.curve.process_reopen,
        process_reopen_executed: false,
        scratch_cleanup_complete: false,
        semantic_receipt_sha256,
    })
}

fn run_probe_child(
    executable: &Path,
    config: &RangeImageCurveProbeConfig,
) -> Result<RangeImageCurveReceipt, String> {
    let output = Command::new(executable)
        .arg("range-image-curve-probe")
        .arg("--config-json")
        .arg(serde_json::to_string(config).map_err(|error| error.to_string())?)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "range-image curve probe failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

fn validate_config(config: &RangeImageCurveConfig) -> Result<(), String> {
    if config.key_count < 16
        || config.value_bytes < 1_024
        || config.logical_range_count == 0
        || !config.key_count.is_multiple_of(config.logical_range_count)
        || config.assigned_range_index >= config.logical_range_count
        || config.reader_memory_budget_bytes == 0
        || config.measured_point_reads == 0
    {
        return Err("range-image curve dimensions or budgets are invalid".to_owned());
    }
    Ok(())
}

fn assigned_bounds(config: &RangeImageCurveConfig) -> (Vec<u8>, Vec<u8>) {
    let keys_per_range = config.key_count / config.logical_range_count;
    let first = config.assigned_range_index.saturating_mul(keys_per_range);
    let end = first.saturating_add(keys_per_range);
    (
        key_for(first),
        if end == config.key_count {
            b"k0".to_vec()
        } else {
            key_for(end)
        },
    )
}

fn assigned_rows(config: &RangeImageCurveConfig) -> Vec<RangeRow> {
    let keys_per_range = config.key_count / config.logical_range_count;
    let first = config.assigned_range_index.saturating_mul(keys_per_range);
    let end = first.saturating_add(keys_per_range);
    (first..end)
        .map(|ordinal| {
            (
                key_for(ordinal),
                deterministic_value(config.seed, ordinal, config.value_bytes),
            )
        })
        .collect()
}

fn key_for(ordinal: usize) -> Vec<u8> {
    format!("k/{ordinal:016x}").into_bytes()
}

fn deterministic_value(seed: u64, ordinal: usize, bytes: usize) -> Vec<u8> {
    let mut state = seed
        ^ u64::try_from(ordinal)
            .unwrap_or(u64::MAX)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut value = Vec::with_capacity(bytes);
    while value.len() < bytes {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        value.extend_from_slice(&mixed.to_be_bytes());
    }
    value.truncate(bytes);
    value
}

fn curve_root_identity(
    config: &RangeImageCurveConfig,
    range_begin: &[u8],
    range_end: &[u8],
) -> [u8; 32] {
    let seed = config.seed.to_be_bytes();
    let range_count = config.logical_range_count.to_be_bytes();
    let assignment = config.assigned_range_index.to_be_bytes();
    root_identity_digest(&[
        b"range-image-curve-cell",
        &seed,
        &range_count,
        &assignment,
        range_begin,
        range_end,
    ])
}

fn point_trace(
    distribution: RangeImageDistribution,
    key_count: usize,
    count: usize,
    seed: u64,
) -> Vec<usize> {
    if distribution == RangeImageDistribution::Sequential {
        return (0..count).map(|index| index % key_count).collect();
    }
    let mut state = seed ^ 0x517c_c1b7_2722_0a95;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    if distribution == RangeImageDistribution::Uniform {
        let key_count = u64::try_from(key_count).unwrap_or(u64::MAX);
        return (0..count)
            .map(|_| usize::try_from(next() % key_count).unwrap_or(0))
            .collect();
    }
    let mut cumulative = Vec::with_capacity(key_count);
    let mut total = 0.0_f64;
    for rank in 1..=key_count {
        total += 1.0 / bounded_f64_usize(rank).powf(0.99);
        cumulative.push(total);
    }
    (0..count)
        .map(|_| {
            let sample =
                f64::from(u32::try_from(next() >> 32).unwrap_or(u32::MAX)) / f64::from(u32::MAX);
            let target = sample * total;
            cumulative.partition_point(|value| *value < target)
        })
        .collect()
}

#[allow(clippy::fn_params_excessive_bools)]
fn semantic_receipt(
    config: &RangeImageCurveProbeConfig,
    exact_points: bool,
    exact_scan: bool,
    outside_range_refused: bool,
    root_bound_receipt_exact: bool,
    index_checksum_verified: bool,
    block_checksums_verified: bool,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-range-image-curve-receipt-v1");
    hasher.update(config.curve.seed.to_be_bytes());
    hasher.update(config.curve.mode.id().as_bytes());
    hasher.update(config.curve.distribution.id().as_bytes());
    hasher.update(config.curve.logical_range_count.to_be_bytes());
    hasher.update(config.curve.assigned_range_index.to_be_bytes());
    hasher.update(config.curve.reader_memory_budget_bytes.to_be_bytes());
    hasher.update(config.image_identity_sha256.as_bytes());
    hasher.update([
        u8::from(exact_points),
        u8::from(exact_scan),
        u8::from(outside_range_refused),
        u8::from(root_bound_receipt_exact),
        u8::from(index_checksum_verified),
        u8::from(block_checksums_verified),
    ]);
    format!("{:x}", hasher.finalize())
}

fn percentile_u64(sorted: &[u64], percentile: usize) -> u64 {
    sorted
        .get(sorted.len().saturating_sub(1).saturating_mul(percentile) / 100)
        .copied()
        .unwrap_or(0)
}

fn percentile_f64(sorted: &[f64], percentile: usize) -> f64 {
    sorted
        .get(sorted.len().saturating_sub(1).saturating_mul(percentile) / 100)
        .copied()
        .unwrap_or(0.0)
}

fn ratio_u64(numerator: u64, denominator: u64) -> f64 {
    f64::from(u32::try_from(numerator.min(u64::from(u32::MAX))).unwrap_or(u32::MAX))
        / f64::from(
            u32::try_from(denominator.min(u64::from(u32::MAX)))
                .unwrap_or(u32::MAX)
                .max(1),
        )
}

fn bounded_f64_usize(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn resident_memory_bytes() -> u64 {
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map_or(0, sysinfo::Process::memory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarter_range_obeys_one_mebibyte_budget() {
        let config = RangeImageCurveConfig {
            key_count: 1_024,
            value_bytes: 1_024,
            logical_range_count: 4,
            assigned_range_index: 1,
            reader_memory_budget_bytes: 65_536,
            warmup_point_reads: 16,
            measured_point_reads: 64,
            distribution: RangeImageDistribution::Uniform,
            process_reopen: false,
            scan: true,
            mode: RangeImageCurveMode::Correct,
            seed: 724_851,
            process_probe_executable: None,
        };
        let receipt = run_range_image_curve_worker(&config).unwrap();
        assert!(receipt.exact_points);
        assert!(receipt.exact_scan);
        assert!(receipt.accounted_resident_bytes <= 65_536);
        assert!(receipt.scratch_cleanup_complete);
    }
}
