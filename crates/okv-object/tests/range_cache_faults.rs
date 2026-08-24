use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use okv_consensus::{
    PublicationAction, PublicationAuthorityContext, PublicationAuthorityFaults,
    PublicationAuthorityPosition, PublicationAuthorityState, PublicationCommandStatus,
    PublicationObjectKind, PublicationObjectReference, PublicationOutcome, SnapshotClosure,
    SnapshotLeaseToken, SnapshotLeaseValidationError,
};
use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_object::{AuthorityBoundRangeView, AuthorityRangeRoot, RangeServingViewError};
use okv_slate::{
    inspect_latest_physical_manifest, AuthorityManifestReference, CountingStore, IoCounters,
    SlateEngine,
};
use slatedb::cached_object_store::CachedObjectStore;
use slatedb::config::Settings;
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use slatedb::db_cache::DbCache;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DATABASE_PATH: &str = "range-cache-corruption";
const CACHE_PART_BYTES: usize = 65_536;

#[tokio::test]
async fn released_or_mismatched_lease_blocks_reopen_before_cache_access() {
    let root = AuthorityRangeRoot {
        cell_id: [0x51; 16],
        tenant_id: [0x71; 16],
        generation: 1,
        manifest: AuthorityManifestReference {
            key: "range/m0/manifest".to_owned(),
            length: 512,
            sha256: "a".repeat(64),
        },
        covered_through: 10,
        minimum_readable_version: 1,
        log_chain_sha256: [0; 32],
    };
    let publication_root = PublicationObjectReference {
        kind: PublicationObjectKind::Manifest,
        key: "range/m0/published-root".to_owned(),
        length: 1_024,
        sha256: "c".repeat(64),
    };
    let (mut authority, lease) = authority_with_lease(&root, &publication_root);
    let counters = Arc::new(IoCounters::default());
    let backend: Arc<dyn ObjectStore> =
        Arc::new(CountingStore::new(InMemory::new(), Arc::clone(&counters)));
    let cache_root = tempfile::tempdir().expect("create resurrection cache root");
    let cache = cached_store(cache_root.path(), backend).await;

    let mut wrong_publication_root = publication_root.clone();
    wrong_publication_root.sha256 = "b".repeat(64);
    let mismatch = AuthorityBoundRangeView::open_historical_with_cache(
        "missing-range",
        Arc::clone(&cache),
        &wrong_publication_root,
        root.clone(),
        10,
        Vec::new(),
        &BTreeMap::new(),
        0x1ea5_e001,
        decoded_cache(),
        &authority,
        &lease,
    )
    .await;
    assert!(matches!(
        mismatch,
        Err(RangeServingViewError::LeaseRootMismatch(_))
    ));
    assert_eq!(counters.total().request_total(), 0);

    let released = authority.apply(
        &PublicationAction::ReleaseLease {
            lease_id: lease.lease_id.clone(),
            expected_lease_epoch: lease.lease_epoch,
        },
        publication_context(4),
        PublicationAuthorityFaults::default(),
    );
    assert_eq!(released.status, PublicationCommandStatus::Accepted);
    let stale = AuthorityBoundRangeView::open_historical_with_cache(
        "missing-range",
        cache,
        &publication_root,
        root,
        10,
        Vec::new(),
        &BTreeMap::new(),
        0x1ea5_e002,
        decoded_cache(),
        &authority,
        &lease,
    )
    .await;
    assert!(matches!(
        stale,
        Err(RangeServingViewError::LeaseAuthority(
            SnapshotLeaseValidationError::LeaseMissing
        ))
    ));
    assert_eq!(counters.total().request_total(), 0);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn corrupted_persistent_cache_never_returns_a_wrong_value() {
    let root = tempfile::Builder::new()
        .prefix("okv-range-cache-fault-")
        .tempdir()
        .expect("create cache-fault root");
    let object_root = root.path().join("objects");
    let cache_root = root.path().join("nvme-cache");
    fs::create_dir_all(&object_root).expect("create object root");
    let local = LocalFileSystem::new_with_prefix(&object_root).expect("open object root");
    let counters = Arc::new(IoCounters::default());
    let backend: Arc<dyn ObjectStore> = Arc::new(CountingStore::new(local, Arc::clone(&counters)));

    let settings = Settings {
        flush_interval: None,
        wal_enabled: false,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    };
    let db = Db::builder(DATABASE_PATH, Arc::clone(&backend))
        .with_settings(settings)
        .with_seed(0xc011_7a11)
        .build()
        .await
        .expect("open cache-fault writer");
    let engine = SlateEngine::new(db);
    let expected = vec![0x5a; 262_144];
    engine
        .apply(CommitBatch {
            version: Version::new(1),
            identity: CommitIdentity::for_test(1),
            mutations: vec![Mutation::Set {
                key: b"k/target".to_vec(),
                value: expected.clone(),
            }],
        })
        .await
        .expect("write cache-fault value");
    engine.flush().await.expect("flush cache-fault value");
    let physical =
        inspect_latest_physical_manifest(Arc::clone(&backend), DATABASE_PATH, 0xc011_7a12)
            .await
            .expect("inspect cache-fault manifest");
    engine.close().await.expect("close cache-fault writer");

    let authority_root = AuthorityRangeRoot {
        cell_id: [0x51; 16],
        tenant_id: [0x71; 16],
        generation: 1,
        manifest: AuthorityManifestReference {
            key: physical.manifest.key,
            length: physical.manifest.length,
            sha256: physical.manifest.sha256,
        },
        covered_through: 1,
        minimum_readable_version: 1,
        log_chain_sha256: [0; 32],
    };
    let prepare_view = AuthorityBoundRangeView::open_with_cache(
        DATABASE_PATH,
        cached_store(&cache_root, Arc::clone(&backend)).await,
        authority_root.clone(),
        1,
        Vec::new(),
        &BTreeMap::new(),
        0xc011_7a13,
        decoded_cache(),
    )
    .await
    .expect("open cache preparation view");
    assert_eq!(
        prepare_view
            .get_at(b"k/target", 1)
            .await
            .expect("populate point cache"),
        Some(expected.clone())
    );
    assert_eq!(
        prepare_view
            .scan_at(b"k/", b"k0", 1, 1)
            .await
            .expect("populate scan cache"),
        vec![(b"k/target".to_vec(), expected.clone())]
    );
    prepare_view.close().await.expect("close preparation view");

    let mut parts = Vec::new();
    collect_cache_parts(&cache_root, &mut parts).expect("enumerate cache parts");
    assert!(
        !parts.is_empty(),
        "cache preparation must persist data parts"
    );
    for part in parts {
        let length = usize::try_from(fs::metadata(&part).expect("stat cache part").len())
            .expect("cache part length fits usize");
        fs::write(&part, vec![0xa5; length]).expect("corrupt cache part");
    }

    let before_reopen = counters.total();
    let reopened = AuthorityBoundRangeView::open_with_cache(
        DATABASE_PATH,
        cached_store(&cache_root, Arc::clone(&backend)).await,
        authority_root,
        1,
        Vec::new(),
        &BTreeMap::new(),
        0xc011_7a14,
        decoded_cache(),
    )
    .await;
    match reopened {
        Err(_) => {}
        Ok(view) => {
            match view.get_at(b"k/target", 1).await {
                Err(_) => {}
                Ok(Some(value)) if value == expected => {
                    let repair_io = counters.total().difference_since(&before_reopen);
                    assert!(
                        repair_io.read_byte_total() > 0,
                        "an exact value after corruption must come from backend repair"
                    );
                    assert!(
                        repair_io
                            .successful_requests
                            .get("get_range")
                            .copied()
                            .unwrap_or(0)
                            > 0,
                        "corruption repair must re-fetch range data"
                    );
                }
                Ok(observed) => panic!("corrupt cache returned non-exact value: {observed:?}"),
            }
            let _ = view.close().await;
        }
    }
}

async fn cached_store(root: &Path, backend: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    CachedObjectStore::builder(root, backend)
        .with_max_cache_size_bytes(Some(16 * 1024 * 1024))
        .with_part_size_bytes(CACHE_PART_BYTES)
        .with_cache_on_flush(false)
        .with_scan_interval(None)
        .with_max_open_file_handles(16)
        .build()
        .await
        .expect("build persistent block cache")
}

fn decoded_cache() -> Arc<dyn DbCache> {
    Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
        max_capacity: 16 * 1024 * 1024,
        time_to_live: None,
        time_to_idle: None,
    }))
}

fn collect_cache_parts(root: &Path, parts: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_cache_parts(&path, parts)?;
        } else if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with("_part"))
        {
            parts.push(path);
        }
    }
    Ok(())
}

fn authority_with_lease(
    root: &AuthorityRangeRoot,
    publication_root: &PublicationObjectReference,
) -> (PublicationAuthorityState, SnapshotLeaseToken) {
    let mut authority = PublicationAuthorityState::default();
    for (action, index) in [
        (
            PublicationAction::ObserveCommittedFrontier {
                committed_frontier: 10,
            },
            1,
        ),
        (
            PublicationAction::SetRetentionWindow {
                expected_policy_epoch: 0,
                retention_window: 100,
            },
            2,
        ),
    ] {
        assert_eq!(
            authority
                .apply(
                    &action,
                    publication_context(index),
                    PublicationAuthorityFaults::default(),
                )
                .status,
            PublicationCommandStatus::Accepted
        );
    }
    let acquired = authority.apply(
        &PublicationAction::AcquireLease {
            lease_id: "range-m0-reader".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            snapshot_version: 10,
            owner: "range-engine-1".to_owned(),
            purpose: "historical-cache-reopen".to_owned(),
            deadline_tick: 20,
            closure: SnapshotClosure {
                manifest: publication_root.clone(),
                object_keys: std::collections::BTreeSet::from([
                    publication_root.key.clone(),
                    root.manifest.key.clone(),
                    "range/m0/data".to_owned(),
                ]),
            },
        },
        publication_context(3),
        PublicationAuthorityFaults::default(),
    );
    let Some(PublicationOutcome::LeaseAcquired { token }) = acquired.outcome else {
        panic!("lease acquisition did not return a token");
    };
    (authority, token)
}

const fn publication_context(index: u64) -> PublicationAuthorityContext {
    PublicationAuthorityContext {
        generation: 1,
        position: PublicationAuthorityPosition { term: 1, index },
    }
}
