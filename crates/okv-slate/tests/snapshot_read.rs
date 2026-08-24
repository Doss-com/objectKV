use okv_model::{ApplyOutcome, CommitBatch, CommitIdentity, Mutation, Version};
use okv_slate::{AdapterError, SlateEngine};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::Db;
use std::sync::Arc;

async fn engine(name: &str) -> SlateEngine {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = Db::open(format!("okv-snapshot-read/{name}"), store)
        .await
        .expect("open SlateDB");
    SlateEngine::new(db)
}

fn batch(version: u64, mutations: Vec<Mutation>) -> CommitBatch {
    CommitBatch {
        version: Version::new(version),
        identity: CommitIdentity::for_test(version),
        mutations,
    }
}

#[tokio::test]
async fn reads_points_at_an_exact_external_version() {
    let engine = engine("point-history").await;
    engine
        .apply(batch(
            10,
            vec![
                Mutation::Set {
                    key: b"alpha".to_vec(),
                    value: b"old".to_vec(),
                },
                Mutation::Set {
                    key: b"beta".to_vec(),
                    value: b"present".to_vec(),
                },
            ],
        ))
        .await
        .expect("apply version 10");
    engine
        .apply(batch(
            20,
            vec![
                Mutation::Set {
                    key: b"alpha".to_vec(),
                    value: b"new".to_vec(),
                },
                Mutation::Clear {
                    key: b"beta".to_vec(),
                },
            ],
        ))
        .await
        .expect("apply version 20");

    assert_eq!(
        engine.get_at(b"alpha", Version::new(10)).await,
        Ok(Some(b"old".to_vec()))
    );
    assert_eq!(
        engine.get_at(b"alpha", Version::new(15)).await,
        Ok(Some(b"old".to_vec()))
    );
    assert_eq!(
        engine.get_at(b"alpha", Version::new(20)).await,
        Ok(Some(b"new".to_vec()))
    );
    assert_eq!(engine.get_at(b"beta", Version::new(20)).await, Ok(None));
    assert_eq!(
        engine.get_at(b"alpha", Version::new(21)).await,
        Err(AdapterError::SnapshotUnavailable {
            requested: Version::new(21),
            applied: Version::new(20),
        })
    );
    engine.close().await.expect("close SlateDB");
}

#[tokio::test]
async fn scans_binary_keys_in_logical_order_at_one_version() {
    let engine = engine("ordered-history").await;
    let binary = vec![b'a', 0, b'z'];
    engine
        .apply(batch(
            10,
            vec![
                Mutation::Set {
                    key: b"a".to_vec(),
                    value: b"a-old".to_vec(),
                },
                Mutation::Set {
                    key: binary.clone(),
                    value: b"binary".to_vec(),
                },
                Mutation::Set {
                    key: b"aa".to_vec(),
                    value: b"aa".to_vec(),
                },
                Mutation::Set {
                    key: b"b".to_vec(),
                    value: b"b-old".to_vec(),
                },
            ],
        ))
        .await
        .expect("apply version 10");
    engine
        .apply(batch(
            20,
            vec![
                Mutation::Set {
                    key: b"a".to_vec(),
                    value: b"a-new".to_vec(),
                },
                Mutation::Clear { key: b"b".to_vec() },
                Mutation::Set {
                    key: b"c".to_vec(),
                    value: b"c-new".to_vec(),
                },
            ],
        ))
        .await
        .expect("apply version 20");

    assert_eq!(
        engine.scan_at(b"a", b"d", Version::new(10), 16).await,
        Ok(vec![
            (b"a".to_vec(), b"a-old".to_vec()),
            (binary.clone(), b"binary".to_vec()),
            (b"aa".to_vec(), b"aa".to_vec()),
            (b"b".to_vec(), b"b-old".to_vec()),
        ])
    );
    assert_eq!(
        engine.scan_at(b"a", b"d", Version::new(20), 16).await,
        Ok(vec![
            (b"a".to_vec(), b"a-new".to_vec()),
            (binary, b"binary".to_vec()),
            (b"aa".to_vec(), b"aa".to_vec()),
            (b"c".to_vec(), b"c-new".to_vec()),
        ])
    );

    let replay = batch(
        10,
        vec![
            Mutation::Set {
                key: b"a".to_vec(),
                value: b"a-old".to_vec(),
            },
            Mutation::Set {
                key: vec![b'a', 0, b'z'],
                value: b"binary".to_vec(),
            },
            Mutation::Set {
                key: b"aa".to_vec(),
                value: b"aa".to_vec(),
            },
            Mutation::Set {
                key: b"b".to_vec(),
                value: b"b-old".to_vec(),
            },
        ],
    );
    assert_eq!(engine.apply(replay).await, Ok(ApplyOutcome::AlreadyApplied));
    engine.close().await.expect("close SlateDB");
}

#[tokio::test]
async fn exact_history_survives_close_and_reopen() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = "okv-snapshot-read/reopen-history";
    let db = Db::open(path, Arc::clone(&store))
        .await
        .expect("open first SlateDB");
    let engine = SlateEngine::new(db);
    engine
        .apply(batch(
            10,
            vec![Mutation::Set {
                key: b"key".to_vec(),
                value: b"old".to_vec(),
            }],
        ))
        .await
        .expect("apply old value");
    engine
        .apply(batch(
            20,
            vec![Mutation::Set {
                key: b"key".to_vec(),
                value: b"new".to_vec(),
            }],
        ))
        .await
        .expect("apply new value");
    engine.close().await.expect("close first SlateDB");

    let reopened = SlateEngine::new(
        Db::open(path, store)
            .await
            .expect("reopen SlateDB from object state"),
    );
    assert_eq!(
        reopened.get_at(b"key", Version::new(10)).await,
        Ok(Some(b"old".to_vec()))
    );
    assert_eq!(
        reopened.get_at(b"key", Version::new(20)).await,
        Ok(Some(b"new".to_vec()))
    );
    reopened.close().await.expect("close reopened SlateDB");
}
