//! Replicated stable-root selection for the `PostgreSQL` page-store bridge.

use crate::smgr_durable::{DurablePostgresRange, PostgresDurableFrontier, PostgresTxLogPopReceipt};
use crate::PostgresRelationForkIdentity;
use okv_consensus::{
    CellStateSnapshot, GenerationCredential, PublicationAction, PublicationAuthorityProcessFixture,
    PublicationClient, PublicationCommand, PublicationCommandStatus, PublicationIntent,
    PublicationObjectKind, PublicationObjectReference, PublicationPopCapabilityStatement,
    RequestIdentity,
};
use okv_object::PublicationPopPolicy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const PROTOCOL_FORMAT_VERSION: u16 = 1;
const LEGACY_STABLE_ROOT_FORMAT_VERSION: u16 = 1;
const STABLE_ROOT_FORMAT_VERSION: u16 = 2;

/// Replicated publication authority used by `PostgreSQL` stable-sync requests.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresPublicationAuthorityConfig {
    pub endpoints: Vec<String>,
    pub generation: u64,
    pub transaction_system_id: String,
    pub destination_root: String,
    #[serde(default)]
    pub txlog_pop: Option<PostgresPublicationPopConfig>,
}

/// Pinned signer policy used to authorize deletion from `PostgreSQL` txLogs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresPublicationPopConfig {
    pub authority_cell_id: u64,
    pub quorum_size: u16,
    pub members: BTreeMap<u64, Vec<u8>>,
    pub pop_epoch: u64,
}

/// Configuration for the bounded three-process publication-authority harness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresStableAuthorityConfig {
    pub seed: u64,
    pub status_file: PathBuf,
    pub process_executable: PathBuf,
    pub authority_cell_id: u64,
    pub generation: u64,
    pub transaction_system_id: String,
}

/// Machine-readable endpoint receipt from the stable-authority harness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresStableAuthorityStatus {
    pub authority_cell_id: u64,
    pub generation: u64,
    pub transaction_system_id: String,
    pub endpoints: Vec<String>,
    pub process_count: usize,
    pub pop_capability_members: BTreeMap<u64, Vec<u8>>,
    pub pop_quorum_size: u16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PostgresStableRoot {
    #[serde(default, flatten)]
    object_snapshot: CellStateSnapshot,
    format_version: u16,
    durable: PostgresDurableFrontier,
    postgres_wal_flush_lsn: u64,
}

pub(crate) struct PostgresStableReceipt {
    pub objectkv_version: u64,
    pub object_frontier: u64,
    pub maximum_page_lsn: u64,
    pub postgres_wal_flush_lsn: u64,
    pub authority_term: u64,
    pub authority_index: u64,
    pub manifest_sha256: String,
    manifest: PublicationObjectReference,
}

pub(crate) struct PostgresStablePublisher {
    object_root: PathBuf,
    relation: PostgresRelationForkIdentity,
    config: PostgresPublicationAuthorityConfig,
    client: PublicationClient,
    current: Option<PostgresStableReceipt>,
}

impl PostgresStablePublisher {
    pub async fn open(
        object_root: PathBuf,
        relation: PostgresRelationForkIdentity,
        cell_generation: u64,
        config: PostgresPublicationAuthorityConfig,
        durable: &DurablePostgresRange,
    ) -> Result<Self, String> {
        validate_publication_config(&config, cell_generation)?;
        let client = PublicationClient::new(config.endpoints.clone())?;
        let state = client.read().await?;
        let current = if let Some(reference) = state.roots.get(&config.destination_root) {
            Some(
                load_and_validate_root(
                    &object_root,
                    relation,
                    durable,
                    reference,
                    state.revision.term,
                    state.revision.index,
                )
                .await?,
            )
        } else {
            None
        };
        Ok(Self {
            object_root,
            relation,
            config,
            client,
            current,
        })
    }

    pub fn current(&self) -> Option<&PostgresStableReceipt> {
        self.current.as_ref()
    }

    #[allow(clippy::too_many_lines)]
    pub async fn publish(
        &mut self,
        durable: &DurablePostgresRange,
        target_version: u64,
        postgres_wal_flush_lsn: u64,
        object_snapshot: CellStateSnapshot,
    ) -> Result<&PostgresStableReceipt, String> {
        let frontier = durable.recoverable_frontier(target_version)?;
        if frontier.relation != self.relation || postgres_wal_flush_lsn < frontier.maximum_page_lsn
        {
            return Err(
                "PostgreSQL stable root is not covered by its WAL flush frontier".to_owned(),
            );
        }
        validate_object_snapshot(&object_snapshot, &frontier)?;
        let state = self.client.read().await?;
        let expected_prior_root = state.roots.get(&self.config.destination_root).cloned();
        if let Some(reference) = &expected_prior_root {
            let (current_version, current_object_frontier) = if let Some(current) = self
                .current
                .as_ref()
                .filter(|current| &current.manifest == reference)
            {
                (current.objectkv_version, current.object_frontier)
            } else {
                let current = load_and_validate_root(
                    &self.object_root,
                    self.relation,
                    durable,
                    reference,
                    state.revision.term,
                    state.revision.index,
                )
                .await?;
                let position = (current.objectkv_version, current.object_frontier);
                self.current = Some(current);
                position
            };
            if current_version > target_version
                || current_version == target_version
                    && current_object_frontier >= frontier_object_version(&frontier)
            {
                return self.current.as_ref().ok_or_else(|| {
                    "PostgreSQL stable root disappeared after validation".to_owned()
                });
            }
        }

        let root = PostgresStableRoot {
            object_snapshot,
            format_version: STABLE_ROOT_FORMAT_VERSION,
            durable: frontier,
            postgres_wal_flush_lsn,
        };
        let (manifest, object_keys) = persist_stable_root(&self.object_root, &root)?;
        let publication_id = format!(
            "postgres-smgr-{}-{}-{}",
            self.relation.relation_number,
            target_version,
            &manifest.sha256[..16]
        );
        let credential = GenerationCredential {
            generation: self.config.generation,
            transaction_system_id: self.config.transaction_system_id.clone(),
        };
        let prepared = self
            .client
            .commit(&PublicationCommand {
                identity: publication_identity(&manifest.sha256, b"prepare"),
                credential: credential.clone(),
                action: PublicationAction::Prepare {
                    publication_id: publication_id.clone(),
                    intent: PublicationIntent {
                        object_keys,
                        manifest: manifest.clone(),
                        destination_root: self.config.destination_root.clone(),
                        expected_prior_root: expected_prior_root.clone(),
                    },
                },
            })
            .await?;
        if prepared.status != PublicationCommandStatus::Accepted {
            return Err(format!(
                "PostgreSQL stable-root prepare returned {:?}",
                prepared.status
            ));
        }
        let published = self
            .client
            .commit(&PublicationCommand {
                identity: publication_identity(&manifest.sha256, b"publish"),
                credential,
                action: PublicationAction::Publish {
                    publication_id,
                    destination_root: self.config.destination_root.clone(),
                    expected_prior_root,
                    manifest: manifest.clone(),
                },
            })
            .await?;
        if published.status != PublicationCommandStatus::Accepted {
            return Err(format!(
                "PostgreSQL stable-root publish returned {:?}",
                published.status
            ));
        }
        let observed = self.client.read().await?;
        if observed.roots.get(&self.config.destination_root) != Some(&manifest) {
            return Err(
                "PostgreSQL stable root differs after linearizable publication read".to_owned(),
            );
        }
        let receipt = load_and_validate_root(
            &self.object_root,
            self.relation,
            durable,
            &manifest,
            observed.revision.term,
            observed.revision.index,
        )
        .await?;
        self.current = Some(receipt);
        self.current
            .as_ref()
            .ok_or_else(|| "PostgreSQL stable receipt was not retained".to_owned())
    }

    /// Use the replicated publication root to authorize deletion through its base.
    pub async fn pop_published_prefix(
        &self,
        durable: &mut DurablePostgresRange,
    ) -> Result<Option<PostgresTxLogPopReceipt>, String> {
        let Some(pop) = &self.config.txlog_pop else {
            return Ok(None);
        };
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| "PostgreSQL txLog pop has no published stable root".to_owned())?;
        let manifest_bytes = fs::read(local_object_path(&self.object_root, &current.manifest.key)?)
            .map_err(|error| error.to_string())?;
        let statement = PublicationPopCapabilityStatement {
            format_version: PROTOCOL_FORMAT_VERSION,
            authority_cell_id: pop.authority_cell_id,
            generation: self.config.generation,
            transaction_system_id: self.config.transaction_system_id.clone(),
            destination_root: self.config.destination_root.clone(),
            manifest: current.manifest.clone(),
            object_frontier: current.object_frontier,
            pop_epoch: pop.pop_epoch,
        };
        let capability = self
            .client
            .pop_capability(&statement, pop.quorum_size)
            .await?;
        let publication_root_sha256: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&current.manifest).map_err(|error| error.to_string())?,
        )
        .into();
        durable
            .pop_published_prefix(
                publication_root_sha256,
                current.object_frontier,
                pop.pop_epoch,
                &capability,
                &manifest_bytes,
            )
            .map(Some)
    }
}

/// Run a bounded three-process publication authority until the harness exits.
///
/// # Errors
///
/// Returns an error for invalid configuration, authority bootstrap failure, or
/// status publication failure.
pub fn run_postgres_stable_authority(config: PostgresStableAuthorityConfig) -> Result<(), String> {
    validate_authority_config(&config)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async move {
        let fixture = PublicationAuthorityProcessFixture::start_for_generation(
            &config.process_executable,
            config.seed,
            config.authority_cell_id,
            config.generation,
            &config.transaction_system_id,
        )
        .await?;
        let status = PostgresStableAuthorityStatus {
            authority_cell_id: config.authority_cell_id,
            generation: config.generation,
            transaction_system_id: config.transaction_system_id,
            endpoints: fixture.endpoints(),
            process_count: fixture.process_count(),
            pop_capability_members: PublicationAuthorityProcessFixture::pop_capability_members()?,
            pop_quorum_size: 2,
        };
        persist_json(&config.status_file, &status)?;
        println!(
            "{}",
            serde_json::to_string(&status).map_err(|error| error.to_string())?
        );
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), String>(())
    })
}

fn validate_publication_config(
    config: &PostgresPublicationAuthorityConfig,
    cell_generation: u64,
) -> Result<(), String> {
    if config.endpoints.is_empty()
        || config.endpoints.iter().any(String::is_empty)
        || config.generation == 0
        || config.generation != cell_generation
        || config.transaction_system_id.is_empty()
        || config.destination_root.is_empty()
        || config.txlog_pop.as_ref().is_some_and(|pop| {
            pop.authority_cell_id == 0
                || pop.quorum_size == 0
                || usize::from(pop.quorum_size) > pop.members.len()
                || pop.members.values().any(Vec::is_empty)
                || pop.pop_epoch == 0
        })
    {
        return Err("PostgreSQL publication authority configuration is invalid".to_owned());
    }
    Ok(())
}

pub(crate) fn publication_pop_policy(
    config: &PostgresPublicationAuthorityConfig,
) -> Option<PublicationPopPolicy> {
    config.txlog_pop.as_ref().map(|pop| PublicationPopPolicy {
        members: pop.members.clone(),
        quorum_size: pop.quorum_size,
    })
}

fn validate_authority_config(config: &PostgresStableAuthorityConfig) -> Result<(), String> {
    if config.seed == 0
        || config.status_file.as_os_str().is_empty()
        || !config.process_executable.is_file()
        || config.authority_cell_id == 0
        || config.generation == 0
        || config.transaction_system_id.is_empty()
    {
        return Err("PostgreSQL stable-authority configuration is invalid".to_owned());
    }
    Ok(())
}

fn persist_stable_root(
    object_root: &Path,
    root: &PostgresStableRoot,
) -> Result<(PublicationObjectReference, BTreeSet<String>), String> {
    let bytes = serde_json::to_vec(root).map_err(|error| error.to_string())?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let key = format!(
        "{}/postgres-checkpoints/{:020}-{digest}.manifest",
        root.durable.base.database_path, root.durable.target_version
    );
    let path = local_object_path(object_root, &key)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    match OpenOptions::new().create_new(true).write(true).open(&path) {
        Ok(mut file) => {
            file.write_all(&bytes).map_err(|error| error.to_string())?;
            file.sync_all().map_err(|error| error.to_string())?;
            File::open(
                path.parent()
                    .ok_or_else(|| "PostgreSQL stable root has no parent".to_owned())?,
            )
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(&path).map_err(|read_error| read_error.to_string())? != bytes {
                return Err("content-addressed PostgreSQL stable root changed bytes".to_owned());
            }
        }
        Err(error) => return Err(error.to_string()),
    }
    let manifest = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: key.clone(),
        length: u64::try_from(bytes.len()).map_err(|error| error.to_string())?,
        sha256: digest,
    };
    let object_keys = std::iter::once(key)
        .chain(std::iter::once(
            root.durable.base.physical.manifest.key.clone(),
        ))
        .chain(
            root.durable
                .base
                .physical
                .live_ssts
                .iter()
                .map(|object| object.key.clone()),
        )
        .chain(
            root.durable
                .object_deltas
                .iter()
                .map(|delta| delta.object.key.clone()),
        )
        .collect();
    Ok((manifest, object_keys))
}

async fn load_and_validate_root(
    object_root: &Path,
    relation: PostgresRelationForkIdentity,
    durable: &DurablePostgresRange,
    reference: &PublicationObjectReference,
    authority_term: u64,
    authority_index: u64,
) -> Result<PostgresStableReceipt, String> {
    if reference.kind != PublicationObjectKind::Manifest
        || authority_index == 0
        || reference.sha256.len() != 64
    {
        return Err("replicated PostgreSQL stable-root reference is invalid".to_owned());
    }
    let bytes = fs::read(local_object_path(object_root, &reference.key)?)
        .map_err(|error| error.to_string())?;
    if u64::try_from(bytes.len()).map_err(|error| error.to_string())? != reference.length
        || format!("{:x}", Sha256::digest(&bytes)) != reference.sha256
    {
        return Err("replicated PostgreSQL stable-root object failed verification".to_owned());
    }
    let root: PostgresStableRoot =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if !matches!(
        root.format_version,
        LEGACY_STABLE_ROOT_FORMAT_VERSION | STABLE_ROOT_FORMAT_VERSION
    ) || root.durable.relation != relation
        || root.postgres_wal_flush_lsn < root.durable.maximum_page_lsn
    {
        return Err("replicated PostgreSQL stable root differs from durable state".to_owned());
    }
    let current_frontier = durable.recoverable_frontier(root.durable.target_version);
    if !matches!(current_frontier, Ok(current) if current == root.durable) {
        durable.validate_archived_frontier(&root.durable).await?;
    }
    validate_object_snapshot(&root.object_snapshot, &root.durable)?;
    Ok(PostgresStableReceipt {
        objectkv_version: root.durable.target_version,
        object_frontier: frontier_object_version(&root.durable),
        maximum_page_lsn: root.durable.maximum_page_lsn,
        postgres_wal_flush_lsn: root.postgres_wal_flush_lsn,
        authority_term,
        authority_index,
        manifest_sha256: reference.sha256.clone(),
        manifest: reference.clone(),
    })
}

fn validate_object_snapshot(
    snapshot: &CellStateSnapshot,
    frontier: &PostgresDurableFrontier,
) -> Result<(), String> {
    validate_object_snapshot_identity(
        snapshot,
        frontier.base.root.cell_id,
        frontier.base.root.tenant_id,
        frontier.base.root.generation,
        frontier_object_version(frontier),
    )?;
    if frontier.visible_rows_sha256 == [0; 32] {
        return Ok(());
    }
    if !snapshot.rows.is_empty() {
        let mut rows = snapshot.rows.clone();
        rows.sort();
        let digest: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&rows).map_err(|error| error.to_string())?).into();
        if rows.windows(2).any(|pair| pair[0].0 == pair[1].0)
            || digest != frontier.visible_rows_sha256
        {
            return Err("PostgreSQL stable object snapshot differs from its base".to_owned());
        }
    }
    Ok(())
}

fn frontier_object_version(frontier: &PostgresDurableFrontier) -> u64 {
    frontier
        .object_deltas
        .last()
        .map_or(frontier.base.root.covered_through, |delta| {
            delta.through_version
        })
}

fn validate_object_snapshot_identity(
    snapshot: &CellStateSnapshot,
    cell_id: [u8; 16],
    tenant_id: [u8; 16],
    generation: u64,
    object_frontier: u64,
) -> Result<(), String> {
    if snapshot.cell_id != cell_id
        || snapshot.tenant_id != tenant_id
        || snapshot.generation != generation
        || snapshot.latest_sequence != object_frontier
    {
        return Err("PostgreSQL stable object snapshot differs from its base".to_owned());
    }
    Ok(())
}

fn publication_identity(manifest_sha256: &str, phase: &[u8]) -> RequestIdentity {
    let mut hasher = Sha256::new();
    hasher.update(b"objectkv/postgres/stable-publication/v1\0");
    hasher.update(manifest_sha256.as_bytes());
    hasher.update(phase);
    let digest: [u8; 32] = hasher.finalize().into();
    let mut client = [0_u8; 8];
    client.copy_from_slice(&digest[..8]);
    let mut request = [0_u8; 8];
    request.copy_from_slice(&digest[8..16]);
    let client_id = u64::from_be_bytes(client).max(1);
    let request_id = u64::from_be_bytes(request).max(1);
    RequestIdentity {
        client_id,
        request_id,
    }
}

fn local_object_path(root: &Path, key: &str) -> Result<PathBuf, String> {
    if key.is_empty()
        || key.starts_with('/')
        || key.split('/').any(|part| part.is_empty() || part == "..")
    {
        return Err("PostgreSQL stable root contains an invalid object key".to_owned());
    }
    Ok(root.join(key))
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
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
    use super::{
        local_object_path, publication_identity, validate_object_snapshot_identity,
        CellStateSnapshot,
    };
    use std::path::Path;

    #[test]
    fn publication_identities_are_deterministic_and_phase_separated() {
        let prepare = publication_identity(&"a".repeat(64), b"prepare");
        let replay = publication_identity(&"a".repeat(64), b"prepare");
        let publish = publication_identity(&"a".repeat(64), b"publish");

        assert_eq!(prepare, replay);
        assert_ne!(prepare, publish);
        assert_ne!(prepare.client_id, 0);
        assert_ne!(prepare.request_id, 0);
    }

    #[test]
    fn stable_object_paths_refuse_escape_or_ambiguous_keys() {
        let root = Path::new("/objects");
        assert_eq!(
            local_object_path(root, "postgres/checkpoint.manifest").unwrap(),
            root.join("postgres/checkpoint.manifest")
        );
        for invalid in ["", "/absolute", "postgres//root", "postgres/../root"] {
            assert!(local_object_path(root, invalid).is_err());
        }
    }

    #[test]
    fn object_snapshot_identity_is_required_without_a_row_digest() {
        let snapshot = CellStateSnapshot {
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 7,
            latest_sequence: 41,
            ..CellStateSnapshot::default()
        };
        validate_object_snapshot_identity(&snapshot, [0x11; 16], [0x22; 16], 7, 41).unwrap();
        assert!(
            validate_object_snapshot_identity(&snapshot, [0x10; 16], [0x22; 16], 7, 41).is_err()
        );
        assert!(
            validate_object_snapshot_identity(&snapshot, [0x11; 16], [0x22; 16], 7, 40).is_err()
        );
    }
}
