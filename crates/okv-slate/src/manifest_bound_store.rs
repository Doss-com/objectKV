use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as StoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as StoreResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::ops::Range;
use std::sync::Arc;

/// A read-only object-store view whose `SlateDB` manifest listing stops at one
/// exact authority-selected manifest.
pub(crate) struct ManifestBoundStore {
    inner: Arc<dyn ObjectStore>,
    manifest_prefix: String,
    bound_manifest: String,
}

impl ManifestBoundStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        database_path: &str,
        bound_manifest: &str,
    ) -> Result<Self, String> {
        let (manifest_prefix, file_name) = bound_manifest
            .rsplit_once('/')
            .ok_or_else(|| "bound manifest key has no parent prefix".to_owned())?;
        let expected_prefix = format!("{database_path}/manifest");
        let manifest_id = file_name.strip_suffix(".manifest");
        if manifest_prefix != expected_prefix
            || manifest_id
                .is_none_or(|id| id.len() != 20 || !id.bytes().all(|byte| byte.is_ascii_digit()))
        {
            return Err("bound key is not a SlateDB manifest path".to_owned());
        }
        Ok(Self {
            inner,
            manifest_prefix: format!("{manifest_prefix}/"),
            bound_manifest: bound_manifest.to_owned(),
        })
    }

    fn is_visible(&self, location: &Path) -> bool {
        let location = location.as_ref();
        !location.starts_with(&self.manifest_prefix) || location <= self.bound_manifest.as_str()
    }

    fn hidden_manifest_error(location: &Path) -> StoreError {
        StoreError::NotFound {
            path: location.to_string(),
            source: Box::new(io::Error::new(
                io::ErrorKind::NotFound,
                "manifest is newer than the authority-selected root",
            )),
        }
    }

    fn read_only_error() -> StoreError {
        StoreError::NotSupported {
            source: Box::new(io::Error::other(
                "authority-bound SlateDB view is read-only",
            )),
        }
    }
}

impl Display for ManifestBoundStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ManifestBoundStore({})", self.bound_manifest)
    }
}

impl Debug for ManifestBoundStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManifestBoundStore")
            .field("inner", &self.inner)
            .field("manifest_prefix", &self.manifest_prefix)
            .field("bound_manifest", &self.bound_manifest)
            .finish()
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ManifestBoundStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> StoreResult<PutResult> {
        Err(Self::read_only_error())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        Err(Self::read_only_error())
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        if !self.is_visible(location) {
            return Err(Self::hidden_manifest_error(location));
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        if !self.is_visible(location) {
            return Err(Self::hidden_manifest_error(location));
        }
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        Box::pin(locations.map(|location| location.and(Err(Self::read_only_error()))))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        let manifest_prefix = self.manifest_prefix.clone();
        let bound_manifest = self.bound_manifest.clone();
        Box::pin(self.inner.list(prefix).filter_map(move |result| {
            let manifest_prefix = manifest_prefix.clone();
            let bound_manifest = bound_manifest.clone();
            async move {
                match result {
                    Ok(meta)
                        if meta.location.as_ref().starts_with(&manifest_prefix)
                            && meta.location.as_ref() > bound_manifest.as_str() =>
                    {
                        None
                    }
                    other => Some(other),
                }
            }
        }))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        let manifest_prefix = self.manifest_prefix.clone();
        let bound_manifest = self.bound_manifest.clone();
        Box::pin(
            self.inner
                .list_with_offset(prefix, offset)
                .filter_map(move |result| {
                    let manifest_prefix = manifest_prefix.clone();
                    let bound_manifest = bound_manifest.clone();
                    async move {
                        match result {
                            Ok(meta)
                                if meta.location.as_ref().starts_with(&manifest_prefix)
                                    && meta.location.as_ref() > bound_manifest.as_str() =>
                            {
                                None
                            }
                            other => Some(other),
                        }
                    }
                }),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result
            .objects
            .retain(|meta| self.is_visible(&meta.location));
        Ok(result)
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> StoreResult<()> {
        Err(Self::read_only_error())
    }

    async fn rename_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: RenameOptions,
    ) -> StoreResult<()> {
        Err(Self::read_only_error())
    }
}

#[cfg(test)]
mod tests {
    use super::ManifestBoundStore;
    use futures_util::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::sync::Arc;

    #[tokio::test]
    async fn hides_every_manifest_after_the_authority_root() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for id in 1_u8..=3 {
            inner
                .put(
                    &Path::from(format!("kv-runtime/manifest/{id:020}.manifest")),
                    vec![id].into(),
                )
                .await
                .expect("write manifest fixture");
        }
        let bound = ManifestBoundStore::new(
            Arc::clone(&inner),
            "kv-runtime",
            "kv-runtime/manifest/00000000000000000002.manifest",
        )
        .expect("create bound view");
        assert!(ManifestBoundStore::new(
            Arc::clone(&inner),
            "other-database",
            "kv-runtime/manifest/00000000000000000002.manifest",
        )
        .is_err());
        assert!(ManifestBoundStore::new(
            Arc::clone(&inner),
            "kv-runtime",
            "kv-runtime/manifest/latest.manifest",
        )
        .is_err());
        let listed = bound
            .list(Some(&Path::from("kv-runtime/manifest")))
            .try_collect::<Vec<_>>()
            .await
            .expect("list bound manifests");
        assert_eq!(listed.len(), 2);
        assert!(bound
            .get(&Path::from(
                "kv-runtime/manifest/00000000000000000003.manifest"
            ))
            .await
            .is_err());
    }
}
