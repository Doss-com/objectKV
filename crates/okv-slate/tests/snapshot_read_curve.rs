use okv_slate::{run_snapshot_read_curve_worker, SnapshotReadCurveConfig, SnapshotReadCurveMode};

fn config() -> SnapshotReadCurveConfig {
    SnapshotReadCurveConfig {
        version_depth: 2,
        key_count: 16,
        value_bytes: 32,
        seed: 1103,
        max_rss_bytes: 1_073_741_824,
        timeout_millis: 120_000,
        decoded_cache_bytes: 8 * 1_024 * 1_024,
        nvme_cache_bytes: 16 * 1_024 * 1_024,
        nvme_part_bytes: 64 * 1_024,
        nvme_open_file_handles: 16,
    }
}

#[tokio::test]
async fn correct_worker_reads_every_snapshot_and_reopens() {
    let receipt = run_snapshot_read_curve_worker(&config(), SnapshotReadCurveMode::Correct)
        .await
        .expect("run correct snapshot-read worker");
    assert!(receipt.targets.iter().all(|target| {
        target.point_reads_exact
            && target.ordered_scans_exact
            && target.cold_point_p99_seconds > 0.0
    }));
    assert!(receipt.tombstone_exact);
    assert!(receipt.future_frontier_refused);
    assert!(receipt.binary_key_order_exact);
    assert!(receipt.close_reopen_exact);
    assert!(receipt.safety_bounds_held);
    assert!(receipt.object_files > 0);
    assert!(receipt.total_io.request_total() > 0);
}

#[tokio::test]
async fn every_negative_mode_breaks_its_own_semantic_gate() {
    let mut deep = config();
    deep.version_depth = 16;
    let latest_only = run_snapshot_read_curve_worker(&deep, SnapshotReadCurveMode::LatestOnly)
        .await
        .expect("run latest-only control");
    assert!(latest_only
        .targets
        .iter()
        .any(|target| !target.point_reads_exact || !target.ordered_scans_exact));

    let skipped =
        run_snapshot_read_curve_worker(&config(), SnapshotReadCurveMode::SkipPointTombstone)
            .await
            .expect("run skipped tombstone control");
    assert!(!skipped.tombstone_exact);

    let overstated =
        run_snapshot_read_curve_worker(&config(), SnapshotReadCurveMode::OverstateAppliedFrontier)
            .await
            .expect("run overstated frontier control");
    assert!(overstated.claimed_applied_frontier > overstated.actual_applied_frontier);
    assert!(!overstated.future_frontier_refused);

    let length_prefixed =
        run_snapshot_read_curve_worker(&config(), SnapshotReadCurveMode::LengthPrefixUserKeys)
            .await
            .expect("run length-prefix control");
    assert!(!length_prefixed.binary_key_order_exact);
}
