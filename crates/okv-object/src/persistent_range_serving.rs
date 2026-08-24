//! Persistent immutable range base shared by disposable serving processes.

use crate::{AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use okv_consensus::{
    CellLogSetPolicy, CellMutation, PublicationObjectKind, PublicationObjectReference,
};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_sim::CommitEnvelope;
use okv_slate::{
    inspect_latest_physical_manifest, verify_physical_manifest_on_local_root,
    AuthorityManifestReference, MvccGcPhysicalManifestReceipt, SlateEngine,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::config::Settings;
use slatedb::Db;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

const FORMAT_VERSION: u16 = 1;
const DELTA_FORMAT_VERSION: u16 = 1;

/// Stable inputs for one persistent immutable range base.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistentRangeBaseConfig {
    pub object_root: PathBuf,
    pub descriptor_path: PathBuf,
    pub database_path: String,
    pub seed: u64,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub base_version: u64,
    pub minimum_readable_version: u64,
    pub log_chain_sha256: [u8; 32],
}

/// Local authority input naming one exact immutable object closure.
///
/// This descriptor is not a replicated publication-authority receipt. It is a
/// durable bootstrap root whose complete physical closure is authenticated on
/// every process reopen.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistentRangeBaseDescriptor {
    pub format_version: u16,
    pub database_path: String,
    pub root: AuthorityRangeRoot,
    pub physical: MvccGcPhysicalManifestReceipt,
}

/// Immutable object segment containing one ordered certified commit suffix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistentRangeDeltaDescriptor {
    pub format_version: u16,
    pub database_path: String,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub after_version: u64,
    pub through_version: u64,
    pub prior_log_chain_sha256: [u8; 32],
    pub final_log_chain_sha256: [u8; 32],
    pub previous_delta_sha256: [u8; 32],
    pub record_count: u64,
    pub mutation_bytes: u64,
    pub object: PublicationObjectReference,
}

/// Stable inputs for one incremental persistent-range delta.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistentRangeDeltaConfig {
    pub object_root: PathBuf,
    pub database_path: String,
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub after_version: u64,
    pub prior_log_chain_sha256: [u8; 32],
    pub previous_delta_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PersistentRangeDeltaPayload {
    format_version: u16,
    database_path: String,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    after_version: u64,
    through_version: u64,
    prior_log_chain_sha256: [u8; 32],
    final_log_chain_sha256: [u8; 32],
    previous_delta_sha256: [u8; 32],
    mutation_bytes: u64,
    records: Vec<CertifiedTxLogRecord>,
}

/// Materialize one content-addressed certified delta segment.
///
/// # Errors
///
/// Returns an error for invalid identity, ordering, commit-chain, path, or
/// immutable-object bytes.
pub fn materialize_persistent_range_delta(
    config: &PersistentRangeDeltaConfig,
    records: &[CertifiedTxLogRecord],
) -> Result<PersistentRangeDeltaDescriptor, String> {
    let (through_version, final_log_chain_sha256, mutation_bytes) =
        validate_delta_records(config, records)?;
    let payload = PersistentRangeDeltaPayload {
        format_version: DELTA_FORMAT_VERSION,
        database_path: config.database_path.clone(),
        cell_id: config.cell_id,
        tenant_id: config.tenant_id,
        generation: config.generation,
        after_version: config.after_version,
        through_version,
        prior_log_chain_sha256: config.prior_log_chain_sha256,
        final_log_chain_sha256,
        previous_delta_sha256: config.previous_delta_sha256,
        mutation_bytes,
        records: records.to_vec(),
    };
    let bytes = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let key = format!(
        "{}/deltas/{:020}-{:020}-{}.segment",
        config.database_path,
        config.after_version,
        through_version,
        &sha256[..16]
    );
    let path = delta_object_path(&config.object_root, &key)?;
    persist_immutable_bytes(&path, &bytes)?;
    Ok(PersistentRangeDeltaDescriptor {
        format_version: DELTA_FORMAT_VERSION,
        database_path: config.database_path.clone(),
        cell_id: config.cell_id,
        tenant_id: config.tenant_id,
        generation: config.generation,
        after_version: config.after_version,
        through_version,
        prior_log_chain_sha256: config.prior_log_chain_sha256,
        final_log_chain_sha256,
        previous_delta_sha256: config.previous_delta_sha256,
        record_count: u64::try_from(records.len()).unwrap_or(u64::MAX),
        mutation_bytes,
        object: PublicationObjectReference {
            kind: PublicationObjectKind::Data,
            key,
            length: u64::try_from(bytes.len()).map_err(|error| error.to_string())?,
            sha256,
        },
    })
}

/// Load and authenticate one immutable certified delta segment.
///
/// # Errors
///
/// Returns an error for missing or changed bytes, invalid metadata, or a
/// payload that no longer matches its descriptor.
pub fn load_persistent_range_delta(
    object_root: &Path,
    descriptor: &PersistentRangeDeltaDescriptor,
) -> Result<Vec<CertifiedTxLogRecord>, String> {
    validate_delta_descriptor(descriptor)?;
    let path = delta_object_path(object_root, &descriptor.object.key)?;
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    if u64::try_from(bytes.len()).map_err(|error| error.to_string())? != descriptor.object.length
        || format!("{:x}", Sha256::digest(&bytes)) != descriptor.object.sha256
    {
        return Err("persistent range delta object failed identity verification".to_owned());
    }
    let payload: PersistentRangeDeltaPayload =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let config = PersistentRangeDeltaConfig {
        object_root: object_root.to_path_buf(),
        database_path: descriptor.database_path.clone(),
        cell_id: descriptor.cell_id,
        tenant_id: descriptor.tenant_id,
        generation: descriptor.generation,
        after_version: descriptor.after_version,
        prior_log_chain_sha256: descriptor.prior_log_chain_sha256,
        previous_delta_sha256: descriptor.previous_delta_sha256,
    };
    let (through_version, final_log_chain_sha256, mutation_bytes) =
        validate_delta_records(&config, &payload.records)?;
    if payload.format_version != descriptor.format_version
        || payload.database_path != descriptor.database_path
        || payload.cell_id != descriptor.cell_id
        || payload.tenant_id != descriptor.tenant_id
        || payload.generation != descriptor.generation
        || payload.after_version != descriptor.after_version
        || payload.through_version != descriptor.through_version
        || payload.prior_log_chain_sha256 != descriptor.prior_log_chain_sha256
        || payload.final_log_chain_sha256 != descriptor.final_log_chain_sha256
        || payload.previous_delta_sha256 != descriptor.previous_delta_sha256
        || payload.mutation_bytes != descriptor.mutation_bytes
        || through_version != descriptor.through_version
        || final_log_chain_sha256 != descriptor.final_log_chain_sha256
        || mutation_bytes != descriptor.mutation_bytes
        || u64::try_from(payload.records.len()).unwrap_or(u64::MAX) != descriptor.record_count
    {
        return Err("persistent range delta payload differs from its descriptor".to_owned());
    }
    Ok(payload.records)
}

/// Authenticate an ordered delta lineage above one full immutable base.
///
/// # Errors
///
/// Returns an error when any descriptor, object, frontier, identity, or chain
/// link is missing or inconsistent.
pub fn load_persistent_range_delta_lineage(
    object_root: &Path,
    base: &PersistentRangeBaseDescriptor,
    deltas: &[PersistentRangeDeltaDescriptor],
) -> Result<Vec<CertifiedTxLogRecord>, String> {
    let mut frontier = base.root.covered_through;
    let mut log_chain_sha256 = base.root.log_chain_sha256;
    let mut previous_delta_sha256 = [0; 32];
    let mut records = Vec::new();
    for delta in deltas {
        if delta.database_path != base.database_path
            || delta.cell_id != base.root.cell_id
            || delta.tenant_id != base.root.tenant_id
            || delta.generation != base.root.generation
            || delta.after_version != frontier
            || delta.prior_log_chain_sha256 != log_chain_sha256
            || delta.previous_delta_sha256 != previous_delta_sha256
        {
            return Err("persistent range delta lineage is not contiguous".to_owned());
        }
        records.extend(load_persistent_range_delta(object_root, delta)?);
        frontier = delta.through_version;
        log_chain_sha256 = delta.final_log_chain_sha256;
        previous_delta_sha256 = persistent_range_delta_descriptor_sha256(delta)?;
    }
    Ok(records)
}

/// Return the canonical digest linking the next delta descriptor to this one.
///
/// # Errors
///
/// Returns an error only when descriptor serialization fails.
pub fn persistent_range_delta_descriptor_sha256(
    descriptor: &PersistentRangeDeltaDescriptor,
) -> Result<[u8; 32], String> {
    Ok(Sha256::digest(serde_json::to_vec(descriptor).map_err(|error| error.to_string())?).into())
}

/// Materialize or reopen one exact `SlateDB` base on a local object-store root.
///
/// # Errors
///
/// Returns an error for invalid identity or version inputs, incomplete mutation
/// history, object construction failure, descriptor disagreement, or physical
/// closure corruption.
pub async fn materialize_persistent_range_base(
    config: &PersistentRangeBaseConfig,
    mutations: &BTreeMap<u64, Vec<CellMutation>>,
) -> Result<PersistentRangeBaseDescriptor, String> {
    validate_config(config, mutations)?;
    if config.descriptor_path.exists() {
        let descriptor = load_persistent_range_base(&config.descriptor_path)?;
        validate_descriptor(config, &descriptor)?;
        verify_physical_manifest_on_local_root(&config.object_root, &descriptor.physical).await?;
        return Ok(descriptor);
    }

    fs::create_dir_all(&config.object_root).map_err(|error| error.to_string())?;
    if let Some(parent) = config.descriptor_path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(&config.object_root).map_err(|error| error.to_string())?,
    );
    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };
    let database = Db::builder(config.database_path.as_str(), Arc::clone(&store))
        .with_settings(settings)
        .with_seed(config.seed)
        .build()
        .await
        .map_err(|error| error.to_string())?;
    let engine = SlateEngine::new(database);
    for sequence in 1..=config.base_version {
        engine
            .apply(model_batch(sequence, mutations)?)
            .await
            .map_err(|error| error.to_string())?;
    }
    engine.flush().await.map_err(|error| error.to_string())?;
    let physical = inspect_latest_physical_manifest(
        Arc::clone(&store),
        &config.database_path,
        config.seed ^ 0x5045_5253_4953_5400,
    )
    .await?;
    engine.close().await.map_err(|error| error.to_string())?;
    let descriptor = PersistentRangeBaseDescriptor {
        format_version: FORMAT_VERSION,
        database_path: config.database_path.clone(),
        root: AuthorityRangeRoot {
            cell_id: config.cell_id,
            tenant_id: config.tenant_id,
            generation: config.generation,
            manifest: AuthorityManifestReference {
                key: physical.manifest.key.clone(),
                length: physical.manifest.length,
                sha256: physical.manifest.sha256.clone(),
            },
            covered_through: config.base_version,
            minimum_readable_version: config.minimum_readable_version,
            log_chain_sha256: config.log_chain_sha256,
        },
        physical,
    };
    persist_descriptor(&config.descriptor_path, &descriptor)?;
    verify_physical_manifest_on_local_root(&config.object_root, &descriptor.physical).await?;
    Ok(descriptor)
}

/// Load one durable base descriptor without opening any object.
///
/// # Errors
///
/// Returns an error when the descriptor is absent, malformed, or unsupported.
pub fn load_persistent_range_base(path: &Path) -> Result<PersistentRangeBaseDescriptor, String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    let descriptor: PersistentRangeBaseDescriptor =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if descriptor.format_version != FORMAT_VERSION {
        return Err(format!(
            "persistent range base format {} is unsupported",
            descriptor.format_version
        ));
    }
    Ok(descriptor)
}

/// Open one fresh serving view after authenticating the descriptor's complete
/// object closure and every supplied txLog certificate.
///
/// # Errors
///
/// Returns an error for physical corruption, invalid tail order or identity,
/// incomplete certificate coverage, or a target outside the retained view.
pub async fn open_persistent_range_view(
    object_root: &Path,
    descriptor: &PersistentRangeBaseDescriptor,
    target_version: u64,
    records: Vec<CertifiedTxLogRecord>,
    policies: &BTreeMap<u16, CellLogSetPolicy>,
    seed: u64,
) -> Result<AuthorityBoundRangeView, String> {
    audit_persistent_range_physical_closure(object_root, descriptor).await?;
    open_manifest_bound_persistent_range_view(
        object_root,
        descriptor,
        target_version,
        records,
        policies,
        seed,
    )
    .await
}

/// Open one experimental manifest-bound view without rereading every live SST.
///
/// The selected manifest bytes and visible frontier remain authenticated by
/// [`AuthorityBoundRangeView`]. Callers must separately prove the backing
/// object's touched-byte integrity and eventually run
/// [`audit_persistent_range_physical_closure`] before treating this as a
/// production serving policy.
///
/// # Errors
///
/// Returns an error for a changed manifest, invalid tail, incomplete
/// certificate coverage, or a target outside the retained view.
pub async fn open_manifest_bound_persistent_range_view(
    object_root: &Path,
    descriptor: &PersistentRangeBaseDescriptor,
    target_version: u64,
    records: Vec<CertifiedTxLogRecord>,
    policies: &BTreeMap<u16, CellLogSetPolicy>,
    seed: u64,
) -> Result<AuthorityBoundRangeView, String> {
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(object_root).map_err(|error| error.to_string())?);
    AuthorityBoundRangeView::open(
        &descriptor.database_path,
        store,
        descriptor.root.clone(),
        target_version,
        records,
        policies,
        seed,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Reread and authenticate the complete manifest and live-SST closure.
///
/// # Errors
///
/// Returns an error for a malformed receipt, missing object, or changed bytes.
pub async fn audit_persistent_range_physical_closure(
    object_root: &Path,
    descriptor: &PersistentRangeBaseDescriptor,
) -> Result<(), String> {
    verify_physical_manifest_on_local_root(object_root, &descriptor.physical).await
}

fn validate_config(
    config: &PersistentRangeBaseConfig,
    mutations: &BTreeMap<u64, Vec<CellMutation>>,
) -> Result<(), String> {
    if config.object_root.as_os_str().is_empty()
        || config.descriptor_path.as_os_str().is_empty()
        || config.database_path.is_empty()
        || config.database_path.starts_with('/')
        || config.database_path.split('/').any(|part| part == "..")
        || config.generation == 0
        || config.base_version == 0
        || config.minimum_readable_version == 0
        || config.minimum_readable_version > config.base_version
        || config.log_chain_sha256 == [0; 32]
    {
        return Err("persistent range base configuration is invalid".to_owned());
    }
    for sequence in 1..=config.base_version {
        if !mutations.contains_key(&sequence) {
            return Err(format!(
                "persistent range base has no mutation batch at version {sequence}"
            ));
        }
    }
    Ok(())
}

fn validate_descriptor(
    config: &PersistentRangeBaseConfig,
    descriptor: &PersistentRangeBaseDescriptor,
) -> Result<(), String> {
    if descriptor.format_version != FORMAT_VERSION
        || descriptor.database_path != config.database_path
        || descriptor.root.cell_id != config.cell_id
        || descriptor.root.tenant_id != config.tenant_id
        || descriptor.root.generation != config.generation
        || descriptor.root.covered_through != config.base_version
        || descriptor.root.minimum_readable_version != config.minimum_readable_version
        || descriptor.root.log_chain_sha256 != config.log_chain_sha256
        || descriptor.root.manifest.key != descriptor.physical.manifest.key
        || descriptor.root.manifest.length != descriptor.physical.manifest.length
        || descriptor.root.manifest.sha256 != descriptor.physical.manifest.sha256
        || !descriptor.physical.is_valid()
    {
        return Err("persistent range base descriptor differs from requested identity".to_owned());
    }
    Ok(())
}

fn validate_delta_records(
    config: &PersistentRangeDeltaConfig,
    records: &[CertifiedTxLogRecord],
) -> Result<(u64, [u8; 32], u64), String> {
    if config.object_root.as_os_str().is_empty()
        || !valid_database_path(&config.database_path)
        || config.generation == 0
        || records.is_empty()
    {
        return Err("persistent range delta configuration is invalid".to_owned());
    }

    let mut previous_version = config.after_version;
    let mut previous_log_chain_sha256 = config.prior_log_chain_sha256;
    let mut mutation_bytes = 0_u64;
    for record in records {
        let envelope =
            CommitEnvelope::decode(&record.envelope).map_err(|error| error.to_string())?;
        let sequence = envelope.version().sequence();
        if sequence <= previous_version {
            return Err("persistent range delta records are not strictly ordered".to_owned());
        }
        if envelope.cell_id() != config.cell_id
            || envelope.tenant_id() != config.tenant_id
            || envelope.generation() != config.generation
        {
            return Err("persistent range delta record names another range domain".to_owned());
        }
        if envelope.previous_log_chain() != previous_log_chain_sha256 {
            return Err("persistent range delta commit chain is broken".to_owned());
        }

        let required = envelope
            .required_log_tags()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let supplied = record
            .certificates
            .iter()
            .map(|certificate| certificate.statement.log_set_id)
            .collect::<BTreeSet<_>>();
        let envelope_sha256: [u8; 32] = Sha256::digest(&record.envelope).into();
        if required.len() != record.certificates.len()
            || required != supplied
            || record.certificates.iter().any(|certificate| {
                let statement = &certificate.statement;
                statement.format_version != 1
                    || statement.cell_id != config.cell_id
                    || statement.tenant_id != config.tenant_id
                    || statement.generation != config.generation
                    || statement.commit_sequence != sequence
                    || statement.envelope_sha256 != envelope_sha256
            })
        {
            return Err(
                "persistent range delta certificates do not cover the exact record".to_owned(),
            );
        }

        mutation_bytes = mutation_bytes
            .checked_add(
                u64::try_from(envelope.canonical_mutations().len())
                    .map_err(|error| error.to_string())?,
            )
            .ok_or_else(|| "persistent range delta mutation bytes overflowed".to_owned())?;
        previous_version = sequence;
        previous_log_chain_sha256 = envelope_sha256;
    }
    Ok((previous_version, previous_log_chain_sha256, mutation_bytes))
}

fn validate_delta_descriptor(descriptor: &PersistentRangeDeltaDescriptor) -> Result<(), String> {
    let valid_sha256 = descriptor.object.sha256.len() == 64
        && descriptor
            .object
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    let expected_key = if valid_sha256 {
        format!(
            "{}/deltas/{:020}-{:020}-{}.segment",
            descriptor.database_path,
            descriptor.after_version,
            descriptor.through_version,
            &descriptor.object.sha256[..16]
        )
    } else {
        String::new()
    };
    if descriptor.format_version != DELTA_FORMAT_VERSION
        || !valid_database_path(&descriptor.database_path)
        || descriptor.generation == 0
        || descriptor.after_version >= descriptor.through_version
        || descriptor.record_count == 0
        || descriptor.mutation_bytes == 0
        || descriptor.record_count > descriptor.through_version - descriptor.after_version
        || descriptor.object.kind != PublicationObjectKind::Data
        || descriptor.object.length == 0
        || !valid_sha256
        || descriptor.object.key != expected_key
    {
        return Err("persistent range delta descriptor is invalid".to_owned());
    }
    Ok(())
}

fn valid_database_path(database_path: &str) -> bool {
    !database_path.is_empty()
        && !database_path.starts_with('/')
        && Path::new(database_path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn delta_object_path(object_root: &Path, key: &str) -> Result<PathBuf, String> {
    if object_root.as_os_str().is_empty()
        || key.is_empty()
        || Path::new(key)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err("persistent range delta object path is invalid".to_owned());
    }
    Ok(object_root.join(key))
}

fn persist_immutable_bytes(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "persistent range delta object has no parent".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    if path.exists() {
        return verify_immutable_bytes(path, bytes);
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "persistent range delta object name is invalid".to_owned())?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    drop(file);

    match fs::hard_link(&temporary, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_immutable_bytes(path, bytes)?;
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(error.to_string());
        }
    }
    fs::remove_file(&temporary).map_err(|error| error.to_string())?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn verify_immutable_bytes(path: &Path, expected: &[u8]) -> Result<(), String> {
    let observed = fs::read(path).map_err(|error| error.to_string())?;
    if observed != expected {
        return Err("persistent range delta immutable object already differs".to_owned());
    }
    Ok(())
}

fn model_batch(
    sequence: u64,
    mutations: &BTreeMap<u64, Vec<CellMutation>>,
) -> Result<CommitBatch, String> {
    let batch = mutations
        .get(&sequence)
        .ok_or_else(|| format!("missing mutation batch at version {sequence}"))?;
    Ok(CommitBatch {
        version: Version::new(sequence),
        identity: CommitIdentity::for_test(sequence),
        mutations: batch
            .iter()
            .map(|mutation| match mutation {
                CellMutation::Clear { key } => Mutation::Clear { key: key.clone() },
                CellMutation::Set { key, value } => Mutation::Set {
                    key: key.clone(),
                    value: value.clone(),
                },
            })
            .collect(),
    })
}

fn persist_descriptor(
    path: &Path,
    descriptor: &PersistentRangeBaseDescriptor,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(descriptor).map_err(|error| error.to_string())?;
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary).map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    fs::rename(&temporary, path).map_err(|error| error.to_string())?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use okv_consensus::{CellTaggedLogCertificate, CellTaggedLogStatement, RequestIdentity};
    use okv_sim::CommitEnvelopeParts;

    #[test]
    fn delta_descriptor_v1_fixture_remains_exact() {
        let fixture = include_str!("../fixtures/persistent-range-delta-v1.json").trim();
        let descriptor: PersistentRangeDeltaDescriptor = serde_json::from_str(fixture).unwrap();
        validate_delta_descriptor(&descriptor).unwrap();
        assert_eq!(serde_json::to_string(&descriptor).unwrap(), fixture);
    }

    #[test]
    fn materializes_one_content_addressed_delta_and_rejects_changed_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let prior_log_chain_sha256 = [0x33; 32];
        let first = certified_record(41, prior_log_chain_sha256);
        let second = certified_record(42, Sha256::digest(&first.envelope).into());
        let config = PersistentRangeDeltaConfig {
            object_root: temporary.path().join("objects"),
            database_path: "persistent-test".to_owned(),
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 1,
            after_version: 40,
            prior_log_chain_sha256,
            previous_delta_sha256: [0; 32],
        };

        let descriptor =
            materialize_persistent_range_delta(&config, &[first.clone(), second.clone()]).unwrap();
        let repeated =
            materialize_persistent_range_delta(&config, &[first.clone(), second.clone()]).unwrap();
        assert_eq!(repeated, descriptor);
        assert_eq!(
            load_persistent_range_delta(&config.object_root, &descriptor).unwrap(),
            vec![first, second]
        );

        let path = config.object_root.join(&descriptor.object.key);
        let mut changed = fs::read(&path).unwrap();
        changed[0] ^= 0xff;
        fs::write(path, changed).unwrap();
        assert!(load_persistent_range_delta(&config.object_root, &descriptor).is_err());
    }

    #[tokio::test]
    async fn reopens_one_authenticated_object_base_without_source_rows() {
        let temporary = tempfile::tempdir().unwrap();
        let mutations = BTreeMap::from([
            (
                1,
                vec![CellMutation::Set {
                    key: b"a".to_vec(),
                    value: b"a1".to_vec(),
                }],
            ),
            (
                2,
                vec![CellMutation::Set {
                    key: b"b".to_vec(),
                    value: b"b2".to_vec(),
                }],
            ),
        ]);
        let config = PersistentRangeBaseConfig {
            object_root: temporary.path().join("objects"),
            descriptor_path: temporary.path().join("root.json"),
            database_path: "persistent-test".to_owned(),
            seed: 71,
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 1,
            base_version: 2,
            minimum_readable_version: 1,
            log_chain_sha256: [0x33; 32],
        };
        let first = materialize_persistent_range_base(&config, &mutations)
            .await
            .unwrap();
        let reopened = materialize_persistent_range_base(&config, &mutations)
            .await
            .unwrap();
        assert_eq!(first, reopened);
        let view = open_persistent_range_view(
            &config.object_root,
            &reopened,
            2,
            Vec::new(),
            &BTreeMap::new(),
            72,
        )
        .await
        .unwrap();
        assert_eq!(
            view.scan_at(&[], &[0xff], 2, 10).await.unwrap(),
            vec![
                (b"a".to_vec(), b"a1".to_vec()),
                (b"b".to_vec(), b"b2".to_vec())
            ]
        );

        let delta = certified_record(3, config.log_chain_sha256);
        let delta_config = PersistentRangeDeltaConfig {
            object_root: config.object_root.clone(),
            database_path: config.database_path.clone(),
            cell_id: config.cell_id,
            tenant_id: config.tenant_id,
            generation: config.generation,
            after_version: config.base_version,
            prior_log_chain_sha256: config.log_chain_sha256,
            previous_delta_sha256: [0; 32],
        };
        let delta_descriptor =
            materialize_persistent_range_delta(&delta_config, std::slice::from_ref(&delta))
                .unwrap();
        assert_eq!(
            load_persistent_range_delta_lineage(
                &config.object_root,
                &reopened,
                &[delta_descriptor.clone()]
            )
            .unwrap(),
            vec![delta]
        );
        let mut broken_lineage = delta_descriptor.clone();
        broken_lineage.previous_delta_sha256 = [0xff; 32];
        assert!(load_persistent_range_delta_lineage(
            &config.object_root,
            &reopened,
            &[broken_lineage]
        )
        .is_err());
        fs::remove_file(config.object_root.join(&delta_descriptor.object.key)).unwrap();
        assert!(load_persistent_range_delta(&config.object_root, &delta_descriptor).is_err());

        let sst_path = config.object_root.join(&reopened.physical.live_ssts[0].key);
        let mut changed_sst = fs::read(&sst_path).unwrap();
        changed_sst[0] ^= 0xff;
        fs::write(sst_path, changed_sst).unwrap();
        assert!(open_persistent_range_view(
            &config.object_root,
            &reopened,
            2,
            Vec::new(),
            &BTreeMap::new(),
            73,
        )
        .await
        .is_err());
    }

    fn certified_record(
        sequence: u64,
        previous_log_chain_sha256: [u8; 32],
    ) -> CertifiedTxLogRecord {
        let envelope = CommitEnvelope::from_parts(CommitEnvelopeParts {
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 1,
            version: Version::from_parts(1, sequence),
            log_index: sequence,
            client_id: [0x44; 16],
            request_id: sequence,
            resolver_set_id: [0x55; 16],
            read_conflicts: vec![0x01],
            write_conflicts: vec![0x02],
            canonical_mutations: serde_json::to_vec(&vec![CellMutation::Set {
                key: format!("key-{sequence}").into_bytes(),
                value: format!("value-{sequence}").into_bytes(),
            }])
            .unwrap(),
            required_resolvers: vec![1],
            required_log_tags: vec![10],
            previous_log_chain: previous_log_chain_sha256,
        });
        let envelope = envelope.encode();
        CertifiedTxLogRecord {
            certificates: vec![CellTaggedLogCertificate {
                statement: CellTaggedLogStatement {
                    format_version: 1,
                    cell_id: [0x11; 16],
                    tenant_id: [0x22; 16],
                    generation: 1,
                    transaction_identity: RequestIdentity {
                        client_id: 44,
                        request_id: sequence,
                    },
                    commit_sequence: sequence,
                    log_set_id: 10,
                    policy_epoch: 1,
                    envelope_sha256: Sha256::digest(&envelope).into(),
                    durable_position: sequence,
                },
                attestations: Vec::new(),
            }],
            envelope,
        }
    }
}
