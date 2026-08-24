use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
use okv_slate::{AdapterError, MvccHistoryFilterSupplier, MvccRetentionFloor, SlateEngine};
use slatedb::admin::Admin;
use slatedb::config::{CompactionWorkerOptions, CompactorOptions, Settings, SstBlockSize};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{CompactionWorkerBuilder, Db};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

fn settings() -> Settings {
    Settings {
        flush_interval: None,
        wal_enabled: false,
        min_filter_keys: 1,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    }
}

fn batch(version: u64) -> CommitBatch {
    let mut mutations = vec![Mutation::Set {
        key: b"alpha".to_vec(),
        value: format!("alpha-{version}").into_bytes(),
    }];
    if version == 6 {
        mutations.push(Mutation::Clear {
            key: b"deleted".to_vec(),
        });
    } else {
        mutations.push(Mutation::Set {
            key: b"deleted".to_vec(),
            value: format!("deleted-{version}").into_bytes(),
        });
    }
    CommitBatch {
        version: Version::new(version),
        identity: CommitIdentity::for_test(version),
        mutations,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn separate_compactor_collects_history_but_preserves_floor_snapshots() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "okv-mvcc-retention/real-compactor";
    let db = Db::builder(path, Arc::clone(&store))
        .with_settings(settings())
        .with_seed(1103)
        .with_sst_block_size(SstBlockSize::Block64Kib)
        .build()
        .await
        .expect("open writer");
    let engine = SlateEngine::new(db);
    for version in 1..=8 {
        engine.apply(batch(version)).await.expect("apply version");
        engine.flush().await.expect("flush version");
    }
    engine.close().await.expect("close writer");

    let observer = Admin::builder(path, Arc::clone(&store))
        .with_seed(2207)
        .build();
    let initial_l0 = observer
        .read_manifest(None)
        .await
        .expect("read initial manifest")
        .expect("initial manifest")
        .l0()
        .len();
    assert!(initial_l0 >= 8);

    let floor = Arc::new(MvccRetentionFloor::new(Version::new(6)).expect("create floor"));
    let supplier = MvccHistoryFilterSupplier::new(floor);
    let worker = CompactionWorkerBuilder::new(path, Arc::clone(&store))
        .with_seed(3301)
        .with_options(CompactionWorkerOptions {
            max_concurrent_compactions: 1,
            compactions_poll_interval: Duration::from_millis(20),
            heartbeat_interval: Duration::from_millis(40),
            max_subcompactions: 1,
            min_filter_keys: 1,
            ..CompactionWorkerOptions::default()
        })
        .with_sst_block_size(SstBlockSize::Block64Kib)
        .with_compaction_filter_supplier(Arc::new(supplier.clone()))
        .build()
        .await
        .expect("build filtered worker");
    let coordinator = Admin::builder(path, Arc::clone(&store))
        .with_seed(4409)
        .build();
    let coordinator_cancel = CancellationToken::new();
    let coordinator_cancel_task = coordinator_cancel.clone();
    let coordinator_task = tokio::spawn(async move {
        coordinator
            .run_compactor_with_options(
                coordinator_cancel_task,
                CompactorOptions {
                    worker: None,
                    max_concurrent_compactions: 1,
                    poll_interval: Duration::from_millis(20),
                    commit_compacted_interval: Duration::from_millis(20),
                    worker_heartbeat_timeout: Duration::from_secs(2),
                    ..CompactorOptions::default()
                },
            )
            .await
    });
    let worker_cancel = CancellationToken::new();
    let worker_cancel_task = worker_cancel.clone();
    let worker_task = tokio::spawn(async move {
        tokio::select! {
            result = worker.run() => result,
            () = worker_cancel_task.cancelled() => worker.stop().await,
        }
    });

    let started = Instant::now();
    let mut compacted = false;
    while started.elapsed() < Duration::from_secs(10) {
        let manifest = observer
            .read_manifest(None)
            .await
            .expect("poll manifest")
            .expect("polled manifest");
        if manifest.l0().len() < initial_l0 && !manifest.compacted().is_empty() {
            compacted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    coordinator_cancel.cancel();
    worker_cancel.cancel();
    assert!(matches!(coordinator_task.await, Ok(Ok(()))));
    assert!(matches!(worker_task.await, Ok(Ok(()))));
    assert!(compacted, "filtered compaction did not publish");
    assert!(supplier.stats().dropped_older_entries > 0);

    let reopened = SlateEngine::new(
        Db::builder(path, store)
            .with_settings(settings())
            .with_seed(5501)
            .with_sst_block_size(SstBlockSize::Block64Kib)
            .build()
            .await
            .expect("reopen compacted database"),
    );
    assert_eq!(
        reopened
            .get_at_retained(b"alpha", Version::new(6), Version::new(6))
            .await,
        Ok(Some(b"alpha-6".to_vec()))
    );
    assert_eq!(
        reopened
            .get_at_retained(b"alpha", Version::new(8), Version::new(6))
            .await,
        Ok(Some(b"alpha-8".to_vec()))
    );
    assert_eq!(
        reopened
            .get_at_retained(b"deleted", Version::new(6), Version::new(6))
            .await,
        Ok(None)
    );
    assert_eq!(
        reopened
            .scan_at_retained(b"a", b"z", Version::new(6), Version::new(6), 10)
            .await,
        Ok(vec![(b"alpha".to_vec(), b"alpha-6".to_vec())])
    );
    assert_eq!(
        reopened
            .scan_at_retained(b"a", b"z", Version::new(8), Version::new(6), 10)
            .await,
        Ok(vec![
            (b"alpha".to_vec(), b"alpha-8".to_vec()),
            (b"deleted".to_vec(), b"deleted-8".to_vec()),
        ])
    );
    assert_eq!(
        reopened
            .get_at_retained(b"alpha", Version::new(5), Version::new(6))
            .await,
        Err(AdapterError::SnapshotExpired {
            requested: Version::new(5),
            minimum: Version::new(6),
        })
    );
    reopened.close().await.expect("close compacted database");
}
