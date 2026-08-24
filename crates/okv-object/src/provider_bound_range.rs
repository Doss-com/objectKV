//! Provider-revision-bound immutable range roots and read-only object views.

use crate::{AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord, RevisionToken};
use async_trait::async_trait;
use chrono::DateTime;
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, Extensions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result as ObjectStoreResult,
};
use okv_consensus::CellLogSetPolicy;
use okv_slate::MvccGcPhysicalManifestReceipt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const PROVIDER_BOUND_ROOT_FORMAT_VERSION: u16 = 2;
const PROVIDER_IDENTITY_VERSION: u16 = 1;

/// Provider family whose exact immutable revision is part of a range root.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    Gcs,
    S3,
    VersionedTest,
}

impl Display for ProviderKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Gcs => "gcs",
            Self::S3 => "s3",
            Self::VersionedTest => "versioned-test",
        })
    }
}

/// One immutable object's portable digest plus provider-selected revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderBoundPhysicalObjectReceipt {
    pub key: String,
    pub revision: RevisionToken,
    pub length: u64,
    pub sha256: String,
}

/// Exact provider identity for a manifest and every live SST it names.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderBoundPhysicalManifestReceipt {
    pub identity_version: u16,
    pub provider: ProviderKind,
    pub namespace: String,
    pub manifest_id: u64,
    pub manifest: ProviderBoundPhysicalObjectReceipt,
    pub live_ssts: Vec<ProviderBoundPhysicalObjectReceipt>,
    pub closure_sha256: String,
}

/// Replicated range identity extended with one provider closure digest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderBoundAuthorityRangeRoot {
    pub format_version: u16,
    pub logical: AuthorityRangeRoot,
    pub provider_closure_sha256: String,
}

/// Version-2 persistent base descriptor admitted for lazy provider-bound reads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProviderBoundPersistentRangeBaseDescriptor {
    pub format_version: u16,
    pub database_path: String,
    pub root: ProviderBoundAuthorityRangeRoot,
    pub physical: MvccGcPhysicalManifestReceipt,
    pub provider: ProviderBoundPhysicalManifestReceipt,
}

/// Read-path counters that exclude fixture construction and background audit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ProviderBoundReadStats {
    pub get_requests: u64,
    pub revision_checks: u64,
    pub refused_requests: u64,
}

/// Read-only object-store facade that refuses any object or revision not in the
/// authority-selected provider closure.
#[derive(Debug)]
pub struct ProviderBoundObjectStore {
    inner: Arc<dyn ObjectStore>,
    objects: BTreeMap<String, ProviderBoundPhysicalObjectReceipt>,
    get_requests: AtomicU64,
    revision_checks: AtomicU64,
    refused_requests: AtomicU64,
}

impl Display for ProviderBoundObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("provider-bound-object-store")
    }
}

impl ProviderBoundObjectStore {
    /// Construct a read-only exact-revision view for one active provider scope.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid receipt or active provider mismatch.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        active_provider: ProviderKind,
        active_namespace: &str,
        receipt: &ProviderBoundPhysicalManifestReceipt,
    ) -> Result<Self, String> {
        validate_provider_physical_manifest(receipt)?;
        if receipt.provider != active_provider || receipt.namespace != active_namespace {
            return Err("active object-store scope differs from provider-bound root".to_owned());
        }
        let objects = std::iter::once(&receipt.manifest)
            .chain(&receipt.live_ssts)
            .map(|object| (object.key.clone(), object.clone()))
            .collect();
        Ok(Self {
            inner,
            objects,
            get_requests: AtomicU64::new(0),
            revision_checks: AtomicU64::new(0),
            refused_requests: AtomicU64::new(0),
        })
    }

    /// Return read-path request and identity-check counts.
    #[must_use]
    pub fn stats(&self) -> ProviderBoundReadStats {
        ProviderBoundReadStats {
            get_requests: self.get_requests.load(Ordering::Relaxed),
            revision_checks: self.revision_checks.load(Ordering::Relaxed),
            refused_requests: self.refused_requests.load(Ordering::Relaxed),
        }
    }

    fn refused(&self, detail: impl Into<String>) -> object_store::Error {
        self.refused_requests.fetch_add(1, Ordering::Relaxed);
        object_store::Error::Precondition {
            path: "provider-bound-view".to_owned(),
            source: std::io::Error::other(detail.into()).into(),
        }
    }

    fn listed_objects(
        &self,
        prefix: Option<&ObjectPath>,
        offset: Option<&ObjectPath>,
    ) -> Vec<ObjectMeta> {
        let prefix = prefix.map(ObjectPath::to_string);
        let offset = offset.map(ObjectPath::to_string);
        self.objects
            .values()
            .filter(|object| {
                prefix.as_ref().is_none_or(|prefix| {
                    object.key == *prefix
                        || object
                            .key
                            .strip_prefix(prefix)
                            .is_some_and(|suffix| suffix.starts_with('/'))
                }) && offset
                    .as_ref()
                    .is_none_or(|offset| object.key.as_str() > offset.as_str())
            })
            .map(|object| ObjectMeta {
                location: ObjectPath::from(object.key.clone()),
                last_modified: DateTime::UNIX_EPOCH,
                size: object.length,
                e_tag: object.revision.e_tag.clone(),
                version: object.revision.version.clone(),
            })
            .collect()
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ProviderBoundObjectStore {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        Err(not_implemented("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart_opts"))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        mut options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let key = location.to_string();
        // SlateDB checks this mutable GC discovery hint while opening the exact
        // manifest selected by authority. It is not part of the immutable
        // serving closure, so the facade must not allow it to change that view.
        // Synthesize the same absence SlateDB accepts for a database that has
        // not advanced its manifest GC boundary.
        if key.ends_with("/gc/manifest.boundary") {
            return Err(object_store::Error::NotFound {
                path: key,
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "manifest boundary is outside the authority-selected closure",
                )
                .into(),
            });
        }
        self.get_requests.fetch_add(1, Ordering::Relaxed);
        let expected = self
            .objects
            .get(&key)
            .ok_or_else(|| self.refused(format!("object {key} is outside selected closure")))?;
        if let Some(if_match) = &options.if_match {
            if expected.revision.e_tag.as_ref() != Some(if_match) {
                return Err(self.refused(format!("caller supplied another ETag for {key}")));
            }
        }
        if let Some(version) = &options.version {
            if expected.revision.version.as_ref() != Some(version) {
                return Err(self.refused(format!("caller supplied another version for {key}")));
            }
        }
        if expected.revision.e_tag.is_none() && expected.revision.version.is_none() {
            return Err(self.refused(format!("object {key} has no exact revision")));
        }
        options.if_match.clone_from(&expected.revision.e_tag);
        options.version.clone_from(&expected.revision.version);
        self.revision_checks.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get_opts(location, options).await?;
        let observed = RevisionToken {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        if !expected.revision.matches(&observed) || result.meta.size != expected.length {
            return Err(self.refused(format!("object {key} returned another identity")));
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<bytes::Bytes>> {
        let mut output = Vec::with_capacity(ranges.len());
        for range in ranges {
            let result = self
                .get_opts(location, GetOptions::new().with_range(Some(range.clone())))
                .await?;
            output.push(result.bytes().await?);
        }
        Ok(output)
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        stream::once(async { Err(not_implemented("delete_stream")) }).boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        stream::iter(self.listed_objects(prefix, None).into_iter().map(Ok)).boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        stream::iter(
            self.listed_objects(prefix, Some(offset))
                .into_iter()
                .map(Ok),
        )
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        Ok(ListResult {
            common_prefixes: Vec::new(),
            objects: self.listed_objects(prefix, None),
            extensions: Extensions::default(),
        })
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        Err(not_implemented("copy_opts"))
    }

    async fn rename_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        Err(not_implemented("rename_opts"))
    }
}

/// Read and hash the exact selected closure while capturing provider revisions.
///
/// This is publication work, not replacement-worker work. It prevents a later
/// lazy reader from binding a stale application digest to an unrelated object
/// generation.
///
/// # Errors
///
/// Returns an error for changed bytes, lengths, missing revisions, or reads.
pub async fn bind_provider_physical_manifest(
    store: Arc<dyn ObjectStore>,
    provider: ProviderKind,
    namespace: &str,
    physical: &MvccGcPhysicalManifestReceipt,
) -> Result<ProviderBoundPhysicalManifestReceipt, String> {
    if namespace.is_empty() || !physical.is_valid() {
        return Err("provider binding input is invalid".to_owned());
    }
    let manifest = bind_object(Arc::clone(&store), &physical.manifest).await?;
    let mut live_ssts = Vec::with_capacity(physical.live_ssts.len());
    for object in &physical.live_ssts {
        live_ssts.push(bind_object(Arc::clone(&store), object).await?);
    }
    live_ssts.sort_by(|left, right| left.key.cmp(&right.key));
    let mut receipt = ProviderBoundPhysicalManifestReceipt {
        identity_version: PROVIDER_IDENTITY_VERSION,
        provider,
        namespace: namespace.to_owned(),
        manifest_id: physical.manifest_id,
        manifest,
        live_ssts,
        closure_sha256: String::new(),
    };
    receipt.closure_sha256 = provider_closure_digest(&receipt);
    validate_provider_physical_manifest(&receipt)?;
    Ok(receipt)
}

/// Wrap a version-1 eager descriptor in an authority-selectable version-2 root.
///
/// # Errors
///
/// Returns an error when the logical and provider closures differ.
pub fn promote_provider_bound_persistent_range_base(
    base: &crate::PersistentRangeBaseDescriptor,
    provider: ProviderBoundPhysicalManifestReceipt,
) -> Result<ProviderBoundPersistentRangeBaseDescriptor, String> {
    if base.format_version != 1 {
        return Err("only persistent range base version 1 can be promoted".to_owned());
    }
    let descriptor = ProviderBoundPersistentRangeBaseDescriptor {
        format_version: PROVIDER_BOUND_ROOT_FORMAT_VERSION,
        database_path: base.database_path.clone(),
        root: ProviderBoundAuthorityRangeRoot {
            format_version: PROVIDER_BOUND_ROOT_FORMAT_VERSION,
            logical: base.root.clone(),
            provider_closure_sha256: provider.closure_sha256.clone(),
        },
        physical: base.physical.clone(),
        provider,
    };
    validate_provider_bound_persistent_range_base(&descriptor)?;
    Ok(descriptor)
}

/// Load one version-2 provider-bound descriptor without opening objects.
///
/// # Errors
///
/// Returns an error when the file is absent, malformed, unsupported, or
/// internally inconsistent.
pub fn load_provider_bound_persistent_range_base(
    path: &Path,
) -> Result<ProviderBoundPersistentRangeBaseDescriptor, String> {
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let descriptor: ProviderBoundPersistentRangeBaseDescriptor =
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    validate_provider_bound_persistent_range_base(&descriptor)?;
    Ok(descriptor)
}

/// Validate one provider-bound descriptor and its portable physical closure.
///
/// # Errors
///
/// Returns an error for any format, identity, digest, or closure mismatch.
pub fn validate_provider_bound_persistent_range_base(
    descriptor: &ProviderBoundPersistentRangeBaseDescriptor,
) -> Result<(), String> {
    if descriptor.format_version != PROVIDER_BOUND_ROOT_FORMAT_VERSION
        || descriptor.root.format_version != PROVIDER_BOUND_ROOT_FORMAT_VERSION
        || descriptor.database_path.is_empty()
        || !descriptor.physical.is_valid()
    {
        return Err("provider-bound persistent range descriptor is invalid".to_owned());
    }
    validate_provider_physical_manifest(&descriptor.provider)?;
    if descriptor.root.provider_closure_sha256 != descriptor.provider.closure_sha256
        || descriptor.root.logical.manifest.key != descriptor.physical.manifest.key
        || descriptor.root.logical.manifest.length != descriptor.physical.manifest.length
        || descriptor.root.logical.manifest.sha256 != descriptor.physical.manifest.sha256
        || descriptor.provider.manifest_id != descriptor.physical.manifest_id
        || !closures_match(&descriptor.physical, &descriptor.provider)
    {
        return Err("provider-bound root differs from selected physical closure".to_owned());
    }
    Ok(())
}

/// Open a lazy immutable view whose every touched object read is revision-bound.
///
/// # Errors
///
/// Returns an error for descriptor, provider scope, object revision, base, or
/// certified txLog disagreement.
#[allow(clippy::too_many_arguments)]
pub async fn open_provider_bound_persistent_range_view(
    store: Arc<dyn ObjectStore>,
    active_provider: ProviderKind,
    active_namespace: &str,
    descriptor: &ProviderBoundPersistentRangeBaseDescriptor,
    target_version: u64,
    records: Vec<CertifiedTxLogRecord>,
    policies: &BTreeMap<u16, CellLogSetPolicy>,
    seed: u64,
) -> Result<(AuthorityBoundRangeView, Arc<ProviderBoundObjectStore>), String> {
    validate_provider_bound_persistent_range_base(descriptor)?;
    let bound = Arc::new(ProviderBoundObjectStore::new(
        store,
        active_provider,
        active_namespace,
        &descriptor.provider,
    )?);
    let object_store: Arc<dyn ObjectStore> = bound.clone();
    let view = AuthorityBoundRangeView::open(
        &descriptor.database_path,
        object_store,
        descriptor.root.logical.clone(),
        target_version,
        records,
        policies,
        seed,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok((view, bound))
}

/// Canonical provider-closure digest selected by the range root.
#[must_use]
pub fn provider_closure_digest(receipt: &ProviderBoundPhysicalManifestReceipt) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"okv-provider-bound-physical-closure-v1");
    hasher.update(receipt.identity_version.to_be_bytes());
    hash_string(&mut hasher, &receipt.provider.to_string());
    hash_string(&mut hasher, &receipt.namespace);
    hasher.update(receipt.manifest_id.to_be_bytes());
    hash_provider_object(&mut hasher, &receipt.manifest);
    for object in &receipt.live_ssts {
        hash_provider_object(&mut hasher, object);
    }
    format!("{:x}", hasher.finalize())
}

fn validate_provider_physical_manifest(
    receipt: &ProviderBoundPhysicalManifestReceipt,
) -> Result<(), String> {
    let mut keys = BTreeSet::new();
    if receipt.identity_version != PROVIDER_IDENTITY_VERSION
        || receipt.namespace.is_empty()
        || receipt.manifest_id == 0
        || !valid_provider_object(receipt.provider, &receipt.manifest)
        || receipt.live_ssts.is_empty()
        || !keys.insert(receipt.manifest.key.as_str())
    {
        return Err("provider physical manifest receipt is invalid".to_owned());
    }
    let mut prior = None;
    for object in &receipt.live_ssts {
        if !valid_provider_object(receipt.provider, object)
            || !keys.insert(object.key.as_str())
            || prior.is_some_and(|key: &str| key >= object.key.as_str())
        {
            return Err("provider physical manifest object list is invalid".to_owned());
        }
        prior = Some(object.key.as_str());
    }
    if receipt.closure_sha256 != provider_closure_digest(receipt) {
        return Err("provider physical closure digest is invalid".to_owned());
    }
    Ok(())
}

fn valid_provider_object(
    provider: ProviderKind,
    object: &ProviderBoundPhysicalObjectReceipt,
) -> bool {
    let revision_present = object.revision.e_tag.is_some() || object.revision.version.is_some();
    let provider_revision_valid =
        provider != ProviderKind::Gcs || object.revision.version.is_some();
    !object.key.is_empty()
        && object.length > 0
        && revision_present
        && provider_revision_valid
        && valid_sha256(&object.sha256)
}

fn valid_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn closures_match(
    physical: &MvccGcPhysicalManifestReceipt,
    provider: &ProviderBoundPhysicalManifestReceipt,
) -> bool {
    let physical_objects = std::iter::once(&physical.manifest)
        .chain(&physical.live_ssts)
        .map(|object| (object.key.as_str(), (object.length, object.sha256.as_str())))
        .collect::<BTreeMap<_, _>>();
    let provider_objects = std::iter::once(&provider.manifest)
        .chain(&provider.live_ssts)
        .map(|object| (object.key.as_str(), (object.length, object.sha256.as_str())))
        .collect::<BTreeMap<_, _>>();
    physical_objects == provider_objects
}

async fn bind_object(
    store: Arc<dyn ObjectStore>,
    expected: &okv_slate::MvccGcPhysicalObjectReceipt,
) -> Result<ProviderBoundPhysicalObjectReceipt, String> {
    let location = ObjectPath::from(expected.key.clone());
    let result = store
        .get(&location)
        .await
        .map_err(|error| format!("read provider object {}: {error}", expected.key))?;
    let revision = RevisionToken {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let object_length = result.meta.size;
    let bytes = result
        .bytes()
        .await
        .map_err(|error| format!("read provider object body {}: {error}", expected.key))?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    if revision.e_tag.is_none() && revision.version.is_none()
        || object_length != expected.length
        || u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected.length
        || digest != expected.sha256
    {
        return Err(format!(
            "provider object {} differs from publication receipt",
            expected.key
        ));
    }
    Ok(ProviderBoundPhysicalObjectReceipt {
        key: expected.key.clone(),
        revision,
        length: expected.length,
        sha256: expected.sha256.clone(),
    })
}

fn hash_provider_object(hasher: &mut Sha256, object: &ProviderBoundPhysicalObjectReceipt) {
    hash_string(hasher, &object.key);
    hash_optional_string(hasher, object.revision.e_tag.as_deref());
    hash_optional_string(hasher, object.revision.version.as_deref());
    hasher.update(object.length.to_be_bytes());
    hash_string(hasher, &object.sha256);
}

fn hash_optional_string(hasher: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_string(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn hash_string(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn not_implemented(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_owned(),
        implementer: "ProviderBoundObjectStore".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;
    use okv_slate::MvccGcPhysicalObjectReceipt;

    #[test]
    fn provider_bound_v2_fixture_remains_exact() {
        let fixture = include_str!("../fixtures/provider-bound-range-root-v2.json").trim();
        let descriptor: ProviderBoundPersistentRangeBaseDescriptor =
            serde_json::from_str(fixture).unwrap();
        validate_provider_bound_persistent_range_base(&descriptor).unwrap();
        assert_eq!(serde_json::to_string(&descriptor).unwrap(), fixture);
    }

    #[test]
    fn version_1_fixture_remains_eager_only_and_rejects_v2() {
        let v1 = include_str!("../fixtures/persistent-range-base-v1.json").trim();
        let descriptor: crate::PersistentRangeBaseDescriptor = serde_json::from_str(v1).unwrap();
        assert_eq!(descriptor.format_version, 1);
        assert!(descriptor.physical.is_valid());
        assert_eq!(serde_json::to_string(&descriptor).unwrap(), v1);

        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("base.json");
        std::fs::write(
            &path,
            include_bytes!("../fixtures/provider-bound-range-root-v2.json"),
        )
        .unwrap();
        assert!(crate::load_persistent_range_base(&path).is_err());
        assert!(load_provider_bound_persistent_range_base(&path).is_ok());
    }

    #[tokio::test]
    async fn exact_revision_range_read_passes_and_same_bytes_overwrite_refuses() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_path = ObjectPath::from("fixture/manifest");
        let sst_path = ObjectPath::from("fixture/sst");
        let manifest_bytes = Bytes::from_static(b"manifest");
        let sst_bytes = Bytes::from_static(b"0123456789abcdef");
        store
            .put(&manifest_path, manifest_bytes.clone().into())
            .await
            .unwrap();
        store
            .put(&sst_path, sst_bytes.clone().into())
            .await
            .unwrap();
        let physical = physical_receipt(&manifest_bytes, &sst_bytes);
        let receipt = bind_provider_physical_manifest(
            Arc::clone(&store),
            ProviderKind::VersionedTest,
            "test-bucket",
            &physical,
        )
        .await
        .unwrap();
        let bound = ProviderBoundObjectStore::new(
            Arc::clone(&store),
            ProviderKind::VersionedTest,
            "test-bucket",
            &receipt,
        )
        .unwrap();
        assert_eq!(
            bound.get_range(&sst_path, 4..8).await.unwrap(),
            Bytes::from_static(b"4567")
        );
        assert_eq!(
            bound.stats(),
            ProviderBoundReadStats {
                get_requests: 1,
                revision_checks: 1,
                refused_requests: 0,
            }
        );

        store.put(&sst_path, sst_bytes.into()).await.unwrap();
        assert!(bound.get_range(&sst_path, 4..8).await.is_err());
    }

    #[tokio::test]
    async fn binding_refuses_changed_bytes_and_scope_mismatch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_path = ObjectPath::from("fixture/manifest");
        let sst_path = ObjectPath::from("fixture/sst");
        let manifest_bytes = Bytes::from_static(b"manifest");
        let sst_bytes = Bytes::from_static(b"0123456789abcdef");
        store
            .put(&manifest_path, manifest_bytes.clone().into())
            .await
            .unwrap();
        store
            .put(&sst_path, Bytes::from_static(b"changed-contents").into())
            .await
            .unwrap();
        let physical = physical_receipt(&manifest_bytes, &sst_bytes);
        assert!(bind_provider_physical_manifest(
            Arc::clone(&store),
            ProviderKind::VersionedTest,
            "test-bucket",
            &physical,
        )
        .await
        .is_err());

        store.put(&sst_path, sst_bytes.into()).await.unwrap();
        let receipt = bind_provider_physical_manifest(
            Arc::clone(&store),
            ProviderKind::VersionedTest,
            "test-bucket",
            &physical,
        )
        .await
        .unwrap();
        assert!(ProviderBoundObjectStore::new(
            store,
            ProviderKind::VersionedTest,
            "another-bucket",
            &receipt,
        )
        .is_err());
    }

    fn physical_receipt(manifest: &Bytes, sst: &Bytes) -> MvccGcPhysicalManifestReceipt {
        let manifest = MvccGcPhysicalObjectReceipt {
            key: "fixture/manifest".to_owned(),
            length: manifest.len() as u64,
            sha256: format!("{:x}", Sha256::digest(manifest)),
        };
        let sst = MvccGcPhysicalObjectReceipt {
            key: "fixture/sst".to_owned(),
            length: sst.len() as u64,
            sha256: format!("{:x}", Sha256::digest(sst)),
        };
        let mut hasher = Sha256::new();
        hasher.update(b"okv-mvcc-gc-physical-closure-v1");
        hasher.update(7_u64.to_be_bytes());
        for object in [&manifest, &sst] {
            hasher.update((object.key.len() as u64).to_be_bytes());
            hasher.update(object.key.as_bytes());
            hasher.update(object.length.to_be_bytes());
            hasher.update(object.sha256.as_bytes());
        }
        MvccGcPhysicalManifestReceipt {
            manifest_id: 7,
            manifest,
            live_ssts: vec![sst],
            closure_sha256: format!("{:x}", hasher.finalize()),
        }
    }
}
