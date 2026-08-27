//! Indexed cold point-read evaluation over the portable row-object format.

use okv_object::{
    content_sha256, encode_row_object_set, filesystem_backend, read_indexed_point,
    read_point_from_full_object, scan_full_object_for_point, Backend, EncodedRowSegment,
    ObservedBackend, PointReadOutcome, RequestStats, RevisionToken, RowObjectManifestV1,
    RowObjectReference, RowRecord, RowSegmentIndex, WriteCondition,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

const GENERATION: u64 = 7;
const READ_VERSION: u64 = 41;
const OBJECT_PREFIX: &str = "row-objects/v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColdReadMode {
    Candidate,
    DirectControl,
    ScanObjectPoison,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmptyWorkerMode {
    LazyCandidate,
    FullHydrationControl,
    FullHydrationPoison,
}

#[derive(Clone, Debug)]
pub struct ColdReadProfile {
    pub key_count: u64,
    pub value_bytes: usize,
    pub operations_per_repeat: usize,
    pub repeats: u32,
    pub seeds: Vec<u64>,
    pub target_object_bytes: usize,
    pub target_block_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ColdReadSample {
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
pub struct ColdReadReport {
    pub samples: Vec<ColdReadSample>,
    pub operations: u64,
    pub correctness_failures: u64,
    pub data_range_requests: u64,
    pub full_data_requests: u64,
    pub list_requests: u64,
    pub data_response_bytes: u64,
    pub index_warmup_requests: u64,
    pub index_warmup_bytes: u64,
    pub manifest_warmup_requests: u64,
    pub manifest_warmup_bytes: u64,
    pub manifest_bytes: u64,
    pub index_bytes: u64,
    pub data_object_bytes: u64,
    pub segment_count: u64,
    pub max_data_object_bytes: u64,
    pub max_index_bytes: u64,
    pub block_count: u64,
    pub max_block_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct EmptyWorkerSample {
    pub seed: u64,
    pub repeat: u32,
    pub first_read_seconds: f64,
    pub correctness_failures: u64,
    pub manifest_requests: u64,
    pub index_requests: u64,
    pub data_range_requests: u64,
    pub data_full_requests: u64,
    pub list_requests: u64,
    pub manifest_response_bytes: u64,
    pub index_response_bytes: u64,
    pub data_response_bytes: u64,
    pub total_response_bytes: u64,
    pub hydrated_data_objects: u64,
    pub digest: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct EmptyWorkerReport {
    pub samples: Vec<EmptyWorkerSample>,
    pub correctness_failures: u64,
    pub manifest_bytes: u64,
    pub index_bytes: u64,
    pub data_object_bytes: u64,
    pub segment_count: u64,
    pub max_index_bytes: u64,
    pub max_block_bytes: u64,
}

impl ColdReadReport {
    #[must_use]
    pub fn requests_per_operation(&self) -> f64 {
        if self.operations == 0 {
            return 0.0;
        }
        count_as_f64(
            self.data_range_requests
                .saturating_add(self.full_data_requests),
        ) / count_as_f64(self.operations)
    }

    #[must_use]
    pub fn bytes_per_operation(&self) -> f64 {
        if self.operations == 0 {
            return 0.0;
        }
        count_as_f64(self.data_response_bytes) / count_as_f64(self.operations)
    }
}

/// Run one physical local-filesystem cold point-read profile.
///
/// # Errors
///
/// Returns an error for invalid configuration, object publication or reads,
/// index corruption, or runtime construction failure.
pub fn run_cold_read_profile(
    mode: ColdReadMode,
    profile: &ColdReadProfile,
) -> Result<ColdReadReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create cold-read runtime: {error}"))?;
    runtime.block_on(run_profile(mode, profile))
}

/// Run independent first reads from empty process-local state over one durable
/// row-object fixture.
///
/// # Errors
///
/// Returns an error for invalid configuration, publication, manifest or index
/// corruption, object reads, or runtime construction.
pub fn run_empty_worker_profile(
    mode: EmptyWorkerMode,
    profile: &ColdReadProfile,
) -> Result<EmptyWorkerReport, String> {
    validate_profile(profile)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create empty-worker runtime: {error}"))?;
    runtime.block_on(run_empty_worker(mode, profile))
}

async fn run_profile(
    mode: ColdReadMode,
    profile: &ColdReadProfile,
) -> Result<ColdReadReport, String> {
    let prepared = prepare_profile(profile).await?;
    let serving = &prepared.serving;
    let mut samples = Vec::new();
    let mut operations = 0_u64;
    let mut correctness_failures = 0_u64;
    for seed in &profile.seeds {
        let sample_repeats = if mode == ColdReadMode::ScanObjectPoison {
            1
        } else {
            profile.repeats
        };
        let operation_count = if mode == ColdReadMode::ScanObjectPoison {
            1
        } else {
            profile.operations_per_repeat
        };
        let keys = operation_keys(profile.key_count, operation_count, *seed);
        for repeat in 0..sample_repeats {
            let sample = measure(mode, serving, &keys, *seed, repeat).await;
            operations =
                operations.saturating_add(u64::try_from(sample.operations).unwrap_or(u64::MAX));
            correctness_failures = correctness_failures.saturating_add(sample.correctness_failures);
            samples.push(sample);
        }
    }
    let measured_stats = serving.backend.stats();
    Ok(ColdReadReport {
        samples,
        operations,
        correctness_failures,
        data_range_requests: request_count(&measured_stats, "get.range"),
        full_data_requests: request_count(&measured_stats, "get"),
        list_requests: request_count(&measured_stats, "list"),
        data_response_bytes: response_bytes(&measured_stats),
        index_warmup_requests: request_count(&prepared.index_warmup_stats, "get"),
        index_warmup_bytes: response_bytes(&prepared.index_warmup_stats),
        manifest_warmup_requests: request_count(&prepared.manifest_warmup_stats, "get"),
        manifest_warmup_bytes: response_bytes(&prepared.manifest_warmup_stats),
        manifest_bytes: prepared.manifest_bytes,
        index_bytes: prepared.index_bytes,
        data_object_bytes: prepared.data_object_bytes,
        segment_count: u64::try_from(serving.segments.len()).unwrap_or(u64::MAX),
        max_data_object_bytes: prepared.max_data_object_bytes,
        max_index_bytes: prepared.max_index_bytes,
        block_count: serving
            .segments
            .iter()
            .map(|segment| u64::try_from(segment.index.block_count()).unwrap_or(u64::MAX))
            .sum(),
        max_block_bytes: serving
            .segments
            .iter()
            .map(|segment| segment.index.max_block_bytes())
            .max()
            .unwrap_or(0),
    })
}

struct PreparedColdRead {
    _root: TempDir,
    serving: ObjectBaseServingRange,
    manifest_warmup_stats: RequestStats,
    index_warmup_stats: RequestStats,
    manifest_bytes: u64,
    index_bytes: u64,
    data_object_bytes: u64,
    max_data_object_bytes: u64,
    max_index_bytes: u64,
}

async fn prepare_profile(profile: &ColdReadProfile) -> Result<PreparedColdRead, String> {
    let encoded = encode_profile(profile)?;
    let root = TempDir::new().map_err(|error| format!("create cold-read root: {error}"))?;
    let backend = filesystem_backend(root.path()).map_err(|error| error.to_string())?;
    let observed = Arc::new(ObservedBackend::new(backend));
    let publication = publish_segments(observed.as_ref(), encoded).await?;
    let manifest = RowObjectManifestV1::new(GENERATION, READ_VERSION, publication.references)?;
    let manifest_bytes = manifest.encode()?;
    let manifest_length = u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX);
    let manifest_key = format!(
        "{OBJECT_PREFIX}/manifest/sha256/{}",
        content_sha256(&manifest_bytes)
    );
    observed
        .put(&manifest_key, manifest_bytes.into(), WriteCondition::Create)
        .await
        .map_err(|error| error.to_string())?;

    observed.clear_stats();
    let manifest_read = observed
        .get(&manifest_key, None, None)
        .await
        .map_err(|error| error.to_string())?;
    let manifest = RowObjectManifestV1::decode(&manifest_read.bytes)?;
    let manifest_warmup_stats = observed.stats();
    observed.clear_stats();
    let mut warmed_segments = Vec::with_capacity(manifest.segments.len());
    for reference in &manifest.segments {
        let index_read = observed
            .get(&reference.index_key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        let index = RowSegmentIndex::decode(&index_read.bytes)?;
        reference.validate_index(&index_read.bytes, &index)?;
        let data_revision = publication
            .data_revisions
            .get(&reference.data_key)
            .cloned()
            .ok_or_else(|| "missing row data revision after publication".to_owned())?;
        warmed_segments.push(WarmedSegment {
            reference: reference.clone(),
            data_revision,
            index,
        });
    }
    let index_warmup_stats = observed.stats();
    observed.clear_stats();
    Ok(PreparedColdRead {
        _root: root,
        serving: ObjectBaseServingRange {
            backend: observed,
            manifest,
            segments: warmed_segments,
        },
        manifest_warmup_stats,
        index_warmup_stats,
        manifest_bytes: manifest_length,
        index_bytes: publication.index_bytes,
        data_object_bytes: publication.data_object_bytes,
        max_data_object_bytes: publication.max_data_object_bytes,
        max_index_bytes: publication.max_index_bytes,
    })
}

fn encode_profile(profile: &ColdReadProfile) -> Result<Vec<EncodedRowSegment>, String> {
    let records = (0..profile.key_count)
        .map(|key_id| {
            RowRecord::value(
                key_bytes(key_id),
                READ_VERSION,
                value_bytes(key_id, profile.value_bytes),
            )
        })
        .collect::<Vec<_>>();
    encode_row_object_set(
        GENERATION,
        &records,
        profile.target_object_bytes,
        profile.target_block_bytes,
    )
}

struct PublishedSegments {
    references: Vec<RowObjectReference>,
    data_revisions: BTreeMap<String, RevisionToken>,
    index_bytes: u64,
    data_object_bytes: u64,
    max_data_object_bytes: u64,
    max_index_bytes: u64,
}

async fn publish_segments(
    backend: &ObservedBackend,
    encoded: Vec<EncodedRowSegment>,
) -> Result<PublishedSegments, String> {
    let mut references = Vec::with_capacity(encoded.len());
    let mut data_revisions = BTreeMap::new();
    let mut data_object_bytes = 0_u64;
    let mut index_bytes = 0_u64;
    let mut max_data_object_bytes = 0_u64;
    let mut max_index_bytes = 0_u64;
    for segment in encoded {
        let reference = RowObjectReference::from_encoded(OBJECT_PREFIX, &segment)?;
        let data_revision = backend
            .put(&reference.data_key, segment.data, WriteCondition::Create)
            .await
            .map_err(|error| error.to_string())?;
        backend
            .put(&reference.index_key, segment.index, WriteCondition::Create)
            .await
            .map_err(|error| error.to_string())?;
        data_object_bytes = data_object_bytes.saturating_add(reference.data_bytes);
        index_bytes = index_bytes.saturating_add(reference.index_bytes);
        max_data_object_bytes = max_data_object_bytes.max(reference.data_bytes);
        max_index_bytes = max_index_bytes.max(reference.index_bytes);
        data_revisions.insert(reference.data_key.clone(), data_revision);
        references.push(reference);
    }
    Ok(PublishedSegments {
        references,
        data_revisions,
        index_bytes,
        data_object_bytes,
        max_data_object_bytes,
        max_index_bytes,
    })
}

struct EmptyWorkerFixture {
    root: TempDir,
    manifest_key: String,
    manifest_bytes: u64,
    publication: PublishedSegments,
}

async fn run_empty_worker(
    mode: EmptyWorkerMode,
    profile: &ColdReadProfile,
) -> Result<EmptyWorkerReport, String> {
    let fixture = publish_empty_worker_fixture(profile).await?;
    let mut samples = Vec::new();
    for seed in &profile.seeds {
        for repeat in 0..profile.repeats {
            let operation_seed = seed.wrapping_add(u64::from(repeat).wrapping_mul(7_919));
            let key_id = operation_keys(profile.key_count, 1, operation_seed)[0];
            samples.push(measure_empty_worker(mode, &fixture, key_id, *seed, repeat).await?);
        }
    }
    let correctness_failures = samples
        .iter()
        .map(|sample| sample.correctness_failures)
        .sum();
    let manifest = read_fixture_manifest(&fixture).await?;
    let max_block_bytes = load_max_block_bytes(&fixture, &manifest).await?;
    Ok(EmptyWorkerReport {
        samples,
        correctness_failures,
        manifest_bytes: fixture.manifest_bytes,
        index_bytes: fixture.publication.index_bytes,
        data_object_bytes: fixture.publication.data_object_bytes,
        segment_count: u64::try_from(manifest.segments.len()).unwrap_or(u64::MAX),
        max_index_bytes: fixture.publication.max_index_bytes,
        max_block_bytes,
    })
}

async fn publish_empty_worker_fixture(
    profile: &ColdReadProfile,
) -> Result<EmptyWorkerFixture, String> {
    let encoded = encode_profile(profile)?;
    let root = TempDir::new().map_err(|error| format!("create empty-worker root: {error}"))?;
    let backend = filesystem_backend(root.path()).map_err(|error| error.to_string())?;
    let observed = ObservedBackend::new(backend);
    let publication = publish_segments(&observed, encoded).await?;
    let manifest =
        RowObjectManifestV1::new(GENERATION, READ_VERSION, publication.references.clone())?;
    let manifest = manifest.encode()?;
    let manifest_bytes = u64::try_from(manifest.len()).unwrap_or(u64::MAX);
    let manifest_key = format!(
        "{OBJECT_PREFIX}/manifest/sha256/{}",
        content_sha256(&manifest)
    );
    observed
        .put(&manifest_key, manifest.into(), WriteCondition::Create)
        .await
        .map_err(|error| error.to_string())?;
    Ok(EmptyWorkerFixture {
        root,
        manifest_key,
        manifest_bytes,
        publication,
    })
}

async fn read_fixture_manifest(
    fixture: &EmptyWorkerFixture,
) -> Result<RowObjectManifestV1, String> {
    let backend = filesystem_backend(fixture.root.path()).map_err(|error| error.to_string())?;
    let read = backend
        .get(&fixture.manifest_key, None, None)
        .await
        .map_err(|error| error.to_string())?;
    if !fixture.manifest_key.ends_with(&content_sha256(&read.bytes)) {
        return Err("row manifest content identity mismatch".to_owned());
    }
    RowObjectManifestV1::decode(&read.bytes)
}

async fn load_max_block_bytes(
    fixture: &EmptyWorkerFixture,
    manifest: &RowObjectManifestV1,
) -> Result<u64, String> {
    let backend = filesystem_backend(fixture.root.path()).map_err(|error| error.to_string())?;
    let mut maximum = 0_u64;
    for reference in &manifest.segments {
        let read = backend
            .get(&reference.index_key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        let index = RowSegmentIndex::decode(&read.bytes)?;
        reference.validate_index(&read.bytes, &index)?;
        maximum = maximum.max(index.max_block_bytes());
    }
    Ok(maximum)
}

async fn measure_empty_worker(
    mode: EmptyWorkerMode,
    fixture: &EmptyWorkerFixture,
    key_id: u64,
    seed: u64,
    repeat: u32,
) -> Result<EmptyWorkerSample, String> {
    let started = Instant::now();
    let backend = filesystem_backend(fixture.root.path()).map_err(|error| error.to_string())?;
    let observed = ObservedBackend::new(backend);
    let manifest_read = observed
        .get(&fixture.manifest_key, None, None)
        .await
        .map_err(|error| error.to_string())?;
    if !fixture
        .manifest_key
        .ends_with(&content_sha256(&manifest_read.bytes))
    {
        return Err("row manifest content identity mismatch".to_owned());
    }
    let manifest = RowObjectManifestV1::decode(&manifest_read.bytes)?;
    if manifest.generation != GENERATION || manifest.covered_through < READ_VERSION {
        return Err("empty worker opened an inadmissible row manifest".to_owned());
    }
    let key = key_bytes(key_id);
    let reference = manifest
        .locate(&key)
        .ok_or_else(|| "empty-worker key is outside the manifest".to_owned())?;
    let (
        point,
        index_requests,
        index_response_bytes,
        data_range_requests,
        data_full_requests,
        data_response_bytes,
        hydrated_data_objects,
    ) = match mode {
        EmptyWorkerMode::LazyCandidate => read_lazy_point(&observed, reference, &key).await?,
        EmptyWorkerMode::FullHydrationControl | EmptyWorkerMode::FullHydrationPoison => {
            read_after_full_hydration(&observed, &manifest, reference, &key).await?
        }
    };
    let correctness_failures = match point.outcome {
        PointReadOutcome::Value(value) if validate_value(key_id, &value) => 0,
        PointReadOutcome::Value(_) | PointReadOutcome::Tombstone | PointReadOutcome::Absent => 1,
    };
    let stats = observed.stats();
    Ok(EmptyWorkerSample {
        seed,
        repeat,
        first_read_seconds: started.elapsed().as_secs_f64(),
        correctness_failures,
        manifest_requests: 1,
        index_requests,
        data_range_requests,
        data_full_requests,
        list_requests: request_count(&stats, "list"),
        manifest_response_bytes: u64::try_from(manifest_read.bytes.len()).unwrap_or(u64::MAX),
        index_response_bytes,
        data_response_bytes,
        total_response_bytes: response_bytes(&stats),
        hydrated_data_objects,
        digest: key_id ^ point.data_bytes,
    })
}

async fn read_lazy_point(
    backend: &ObservedBackend,
    reference: &RowObjectReference,
    key: &[u8],
) -> Result<(okv_object::PointRead, u64, u64, u64, u64, u64, u64), String> {
    let index_read = backend
        .get(&reference.index_key, None, None)
        .await
        .map_err(|error| error.to_string())?;
    let index = RowSegmentIndex::decode(&index_read.bytes)?;
    reference.validate_index(&index_read.bytes, &index)?;
    let point = read_indexed_point(
        backend,
        &reference.data_key,
        None,
        &index,
        key,
        READ_VERSION,
    )
    .await?;
    let index_bytes = u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX);
    let data_bytes = point.data_bytes;
    Ok((point, 1, index_bytes, 1, 0, data_bytes, 0))
}

async fn read_after_full_hydration(
    backend: &ObservedBackend,
    manifest: &RowObjectManifestV1,
    selected: &RowObjectReference,
    key: &[u8],
) -> Result<(okv_object::PointRead, u64, u64, u64, u64, u64, u64), String> {
    let mut selected_data = None;
    let mut selected_index = None;
    let mut index_response_bytes = 0_u64;
    let mut data_response_bytes = 0_u64;
    for reference in &manifest.segments {
        let index_read = backend
            .get(&reference.index_key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        let index = RowSegmentIndex::decode(&index_read.bytes)?;
        reference.validate_index(&index_read.bytes, &index)?;
        index_response_bytes = index_response_bytes
            .saturating_add(u64::try_from(index_read.bytes.len()).unwrap_or(u64::MAX));
        let data_read = backend
            .get(&reference.data_key, None, None)
            .await
            .map_err(|error| error.to_string())?;
        if u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX) != reference.data_bytes
            || content_sha256(&data_read.bytes) != reference.data_sha256
        {
            return Err("hydrated row object does not match the manifest".to_owned());
        }
        data_response_bytes = data_response_bytes
            .saturating_add(u64::try_from(data_read.bytes.len()).unwrap_or(u64::MAX));
        if reference.data_key == selected.data_key {
            selected_data = Some(data_read.bytes);
            selected_index = Some(index);
        }
    }
    let data = selected_data.ok_or_else(|| "selected row object was not hydrated".to_owned())?;
    let index = selected_index.ok_or_else(|| "selected row index was not hydrated".to_owned())?;
    let point = read_point_from_full_object(&data, &index, key, READ_VERSION)?;
    let segment_count = u64::try_from(manifest.segments.len()).unwrap_or(u64::MAX);
    Ok((
        point,
        segment_count,
        index_response_bytes,
        0,
        segment_count,
        data_response_bytes,
        segment_count,
    ))
}

fn validate_profile(profile: &ColdReadProfile) -> Result<(), String> {
    if profile.key_count < 10
        || profile.value_bytes < 16
        || profile.operations_per_repeat == 0
        || profile.repeats == 0
        || profile.seeds.is_empty()
        || profile.seeds.contains(&0)
        || profile.target_object_bytes < profile.target_block_bytes
        || profile.target_block_bytes < 4_096
    {
        return Err("invalid cold point-read profile".to_owned());
    }
    Ok(())
}

struct ObjectBaseServingRange {
    backend: Arc<ObservedBackend>,
    manifest: RowObjectManifestV1,
    segments: Vec<WarmedSegment>,
}

struct WarmedSegment {
    reference: RowObjectReference,
    data_revision: RevisionToken,
    index: RowSegmentIndex,
}

impl ObjectBaseServingRange {
    fn locate(&self, key: &[u8]) -> Option<&WarmedSegment> {
        let mut lower = 0_usize;
        let mut upper = self.segments.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            if self.segments[middle].reference.first_key.as_slice() <= key {
                lower = middle + 1;
            } else {
                upper = middle;
            }
        }
        let candidate = lower
            .checked_sub(1)
            .and_then(|index| self.segments.get(index))?;
        (key <= candidate.reference.last_key.as_slice()).then_some(candidate)
    }

    async fn get_at(
        &self,
        mode: ColdReadMode,
        key_id: u64,
        read_version: u64,
        generation: u64,
    ) -> Result<okv_object::PointRead, String> {
        let key = key_bytes(key_id);
        if mode == ColdReadMode::Candidate {
            if generation != self.manifest.generation {
                return Err("stale object-base serving generation".to_owned());
            }
            if read_version > self.manifest.covered_through {
                return Err("object-base coverage is behind read version".to_owned());
            }
        }
        let segment = self
            .locate(&key)
            .ok_or_else(|| "key outside object-base serving range".to_owned())?;
        match mode {
            ColdReadMode::Candidate | ColdReadMode::DirectControl => {
                read_indexed_point(
                    self.backend.as_ref(),
                    &segment.reference.data_key,
                    Some(&segment.data_revision),
                    &segment.index,
                    &key,
                    read_version,
                )
                .await
            }
            ColdReadMode::ScanObjectPoison => {
                scan_full_object_for_point(
                    self.backend.as_ref(),
                    &segment.reference.data_key,
                    Some(&segment.data_revision),
                    &segment.index,
                    &key,
                    read_version,
                )
                .await
            }
        }
    }
}

async fn measure(
    mode: ColdReadMode,
    serving: &ObjectBaseServingRange,
    keys: &[u64],
    seed: u64,
    repeat: u32,
) -> ColdReadSample {
    let run_started = Instant::now();
    let mut failures = 0_u64;
    let mut digest = 0_u64;
    let mut latencies = Vec::with_capacity(keys.len());
    for key_id in keys {
        let started = Instant::now();
        match serving
            .get_at(mode, *key_id, READ_VERSION, GENERATION)
            .await
        {
            Ok(read) => match read.outcome {
                PointReadOutcome::Value(value) if validate_value(*key_id, &value) => {
                    digest = digest.wrapping_add(*key_id ^ read.data_bytes);
                }
                PointReadOutcome::Value(_)
                | PointReadOutcome::Tombstone
                | PointReadOutcome::Absent => {
                    failures = failures.saturating_add(1);
                }
            },
            Err(_) => failures = failures.saturating_add(1),
        }
        latencies.push(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
    }
    let elapsed_seconds = run_started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    ColdReadSample {
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

fn operation_keys(key_count: u64, operations: usize, seed: u64) -> Vec<u64> {
    let mut random = XorShift64(seed);
    (0..operations).map(|_| random.next() % key_count).collect()
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

fn validate_value(key_id: u64, value: &[u8]) -> bool {
    if value.len() < 16 || value[..8] != key_id.to_be_bytes() {
        return false;
    }
    let Ok(tail) = <[u8; 8]>::try_from(&value[value.len() - 8..]) else {
        return false;
    };
    u64::from_be_bytes(tail) == (key_id.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15)
}

fn request_count(stats: &RequestStats, api: &str) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| request.api == api)
        .map(|request| request.count)
        .sum()
}

fn response_bytes(stats: &RequestStats) -> u64 {
    stats
        .requests
        .iter()
        .map(|request| request.response_bytes)
        .sum()
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

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
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
    use super::{
        run_cold_read_profile, run_empty_worker_profile, ColdReadMode, ColdReadProfile,
        EmptyWorkerMode,
    };

    fn smoke_profile() -> ColdReadProfile {
        ColdReadProfile {
            key_count: 1_024,
            value_bytes: 128,
            operations_per_repeat: 100,
            repeats: 2,
            seeds: vec![1_103, 2_207],
            target_object_bytes: 16_384,
            target_block_bytes: 4_096,
        }
    }

    #[test]
    fn candidate_uses_exactly_one_data_range_get() {
        let report = run_cold_read_profile(ColdReadMode::Candidate, &smoke_profile())
            .expect("cold candidate should run");
        assert_eq!(report.correctness_failures, 0);
        assert_eq!(report.data_range_requests, report.operations);
        assert_eq!(report.full_data_requests, 0);
        assert_eq!(report.list_requests, 0);
        assert!(report.segment_count > 1);
        assert_eq!(report.manifest_warmup_requests, 1);
        assert_eq!(report.manifest_warmup_bytes, report.manifest_bytes);
        assert_eq!(report.index_warmup_requests, report.segment_count);
        assert_eq!(report.index_warmup_bytes, report.index_bytes);
        assert!(report.max_data_object_bytes <= 16_384);
        assert!(report.data_response_bytes <= report.operations * report.max_block_bytes);
    }

    #[test]
    fn scan_poison_reads_the_complete_object() {
        let report = run_cold_read_profile(ColdReadMode::ScanObjectPoison, &smoke_profile())
            .expect("scan poison should execute");
        assert_eq!(report.correctness_failures, 0);
        assert_eq!(report.data_range_requests, 0);
        assert_eq!(report.full_data_requests, 2);
        assert_eq!(report.operations, 2);
        assert!(report.data_response_bytes > report.max_block_bytes * report.operations);
        assert!(report.data_response_bytes <= report.max_data_object_bytes * report.operations);
    }

    #[test]
    fn empty_worker_loads_only_selected_metadata_and_block() {
        let report = run_empty_worker_profile(EmptyWorkerMode::LazyCandidate, &smoke_profile())
            .expect("lazy empty worker should run");
        assert_eq!(report.correctness_failures, 0);
        assert!(report.segment_count > 1);
        for sample in report.samples {
            assert_eq!(sample.manifest_requests, 1);
            assert_eq!(sample.index_requests, 1);
            assert_eq!(sample.data_range_requests, 1);
            assert_eq!(sample.data_full_requests, 0);
            assert_eq!(sample.hydrated_data_objects, 0);
            assert_eq!(sample.list_requests, 0);
            assert!(
                sample.total_response_bytes
                    <= report.manifest_bytes + report.max_index_bytes + report.max_block_bytes
            );
        }
    }

    #[test]
    fn full_hydration_control_reads_the_complete_closure() {
        let report =
            run_empty_worker_profile(EmptyWorkerMode::FullHydrationControl, &smoke_profile())
                .expect("full hydration control should run");
        assert_eq!(report.correctness_failures, 0);
        for sample in report.samples {
            assert_eq!(sample.manifest_requests, 1);
            assert_eq!(sample.index_requests, report.segment_count);
            assert_eq!(sample.data_range_requests, 0);
            assert_eq!(sample.data_full_requests, report.segment_count);
            assert_eq!(sample.hydrated_data_objects, report.segment_count);
            assert_eq!(sample.index_response_bytes, report.index_bytes);
            assert_eq!(sample.data_response_bytes, report.data_object_bytes);
            assert_eq!(
                sample.total_response_bytes,
                report.manifest_bytes + report.index_bytes + report.data_object_bytes
            );
        }
    }
}
