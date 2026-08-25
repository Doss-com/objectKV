use okv_object::{
    run_range_serving_curve_worker, ProviderCacheEconomicsConfig, ProviderCacheEconomicsMode,
    ProviderCacheTraceDistribution, RangeServingCacheMode, RangeServingCurveConfig,
    RangeServingObjectBackend, RangeServingProviderMode,
};

#[tokio::test]
async fn measures_exact_authority_base_plus_certified_tail() {
    let receipt = Box::pin(run_range_serving_curve_worker(&RangeServingCurveConfig {
        base_key_count: 512,
        value_bytes: 64,
        tail_records: 16,
        point_samples: 8,
        scan_rows: 64,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::Raw,
        decoded_cache_bytes: 67_108_864,
        nvme_cache_bytes: 268_435_456,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 64,
        provider_mode: RangeServingProviderMode::default(),
        object_backend: RangeServingObjectBackend::default(),
        scratch_prefix: None,
        warmup_reads: 0,
        measured_reads: 0,
        economics: None,
        seed: 1103,
    }))
    .await
    .expect("range-serving curve executes");

    assert!(receipt.first_point_exact);
    assert!(receipt.warm_points_exact);
    assert!(receipt.ordered_scan_exact);
    assert_eq!(receipt.authenticated_tail_records, 16);
    assert!(receipt.open_io.request_total() > 0);
    assert!(receipt.total_io.read_byte_total() > 0);
    assert!(receipt.safety_bounds_held);
}

#[tokio::test]
async fn shared_cache_removes_backend_requests_from_repeated_points() {
    let receipt = Box::pin(run_range_serving_curve_worker(&RangeServingCurveConfig {
        base_key_count: 512,
        value_bytes: 64,
        tail_records: 16,
        point_samples: 8,
        scan_rows: 64,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::SharedRamNvme,
        decoded_cache_bytes: 67_108_864,
        nvme_cache_bytes: 268_435_456,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 64,
        provider_mode: RangeServingProviderMode::default(),
        object_backend: RangeServingObjectBackend::default(),
        scratch_prefix: None,
        warmup_reads: 0,
        measured_reads: 0,
        economics: None,
        seed: 2207,
    }))
    .await
    .expect("cached range-serving curve executes");

    assert!(receipt.first_point_exact);
    assert!(receipt.warm_points_exact);
    assert!(receipt.ordered_scan_exact);
    assert!(receipt.open_io.request_total() > 0);
    assert_eq!(receipt.warm_point_io.request_total(), 0);
}

#[tokio::test]
async fn decoded_ram_cold_reopen_reads_from_persistent_nvme_cache() {
    let receipt = Box::pin(run_range_serving_curve_worker(&RangeServingCurveConfig {
        base_key_count: 512,
        value_bytes: 64,
        tail_records: 16,
        point_samples: 8,
        scan_rows: 64,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::NvmeReopen,
        decoded_cache_bytes: 67_108_864,
        nvme_cache_bytes: 268_435_456,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 64,
        provider_mode: RangeServingProviderMode::default(),
        object_backend: RangeServingObjectBackend::default(),
        scratch_prefix: None,
        warmup_reads: 0,
        measured_reads: 0,
        economics: None,
        seed: 3301,
    }))
    .await
    .expect("NVMe-reopen range-serving curve executes");

    assert!(receipt.first_point_exact);
    assert!(receipt.warm_points_exact);
    assert!(receipt.ordered_scan_exact);
    assert!(receipt.cache_prepare_io.request_total() > 0);
    assert!(receipt.open_io.request_total() > 0);
    assert_eq!(receipt.first_point_io.read_byte_total(), 0);
    assert_eq!(
        receipt
            .first_point_io
            .successful_requests
            .get("get_range")
            .copied()
            .unwrap_or(0),
        0
    );
    assert_eq!(receipt.warm_point_io.request_total(), 0);
    assert_eq!(receipt.scan_io.request_total(), 0);
}

#[tokio::test]
async fn provider_bound_curve_checks_every_backend_get_revision() {
    let receipt = Box::pin(run_range_serving_curve_worker(&RangeServingCurveConfig {
        base_key_count: 512,
        value_bytes: 64,
        tail_records: 0,
        point_samples: 16,
        scan_rows: 8,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::SharedRamNvme,
        decoded_cache_bytes: 67_108_864,
        nvme_cache_bytes: 268_435_456,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 64,
        provider_mode: RangeServingProviderMode::Correct,
        object_backend: RangeServingObjectBackend::default(),
        scratch_prefix: None,
        warmup_reads: 8,
        measured_reads: 16,
        economics: None,
        seed: 4409,
    }))
    .await
    .expect("provider-bound range-serving curve executes");

    assert!(receipt.first_point_exact);
    assert!(receipt.warm_points_exact);
    assert!(receipt.ordered_scan_exact);
    assert_eq!(
        receipt.provider_get_requests,
        receipt.provider_revision_checks
    );
    assert!(receipt.provider_get_requests > 0);
    assert_eq!(receipt.provider_refused_requests, 0);
    assert_eq!(receipt.unversioned_fallbacks, 0);
}

#[tokio::test]
async fn provider_controls_refuse_changed_revision_and_detect_unversioned_fallback() {
    let config = |provider_mode| RangeServingCurveConfig {
        base_key_count: 512,
        value_bytes: 64,
        tail_records: 0,
        point_samples: 8,
        scan_rows: 8,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::SharedRamNvme,
        decoded_cache_bytes: 67_108_864,
        nvme_cache_bytes: 268_435_456,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 64,
        provider_mode,
        object_backend: RangeServingObjectBackend::default(),
        scratch_prefix: None,
        warmup_reads: 4,
        measured_reads: 8,
        economics: None,
        seed: 5519,
    };

    assert!(Box::pin(run_range_serving_curve_worker(&config(
        RangeServingProviderMode::ChangedGeneration
    )))
    .await
    .is_err());
    assert!(Box::pin(run_range_serving_curve_worker(&config(
        RangeServingProviderMode::SameBytesNewGeneration
    )))
    .await
    .is_err());
    assert!(Box::pin(run_range_serving_curve_worker(&config(
        RangeServingProviderMode::MissingRevision
    )))
    .await
    .is_err());
    assert!(Box::pin(run_range_serving_curve_worker(&config(
        RangeServingProviderMode::ChangedBytes
    )))
    .await
    .is_err());
    assert!(Box::pin(run_range_serving_curve_worker(&config(
        RangeServingProviderMode::ChangedNamespace
    )))
    .await
    .is_err());

    let fallback = Box::pin(run_range_serving_curve_worker(&config(
        RangeServingProviderMode::SkipRevisionEnforcement,
    )))
    .await
    .expect("unsafe fallback remains executable for the negative control");
    assert_eq!(fallback.provider_revision_checks, 0);
    assert_eq!(fallback.unversioned_fallbacks, 1);
}

#[tokio::test]
async fn gcs_backend_refuses_an_unguarded_scratch_scope_before_network_io() {
    let config = RangeServingCurveConfig {
        base_key_count: 16,
        value_bytes: 64,
        tail_records: 0,
        point_samples: 2,
        scan_rows: 2,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::SharedRamNvme,
        decoded_cache_bytes: 1_048_576,
        nvme_cache_bytes: 1_048_576,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 8,
        provider_mode: RangeServingProviderMode::Correct,
        object_backend: RangeServingObjectBackend::Gcs,
        scratch_prefix: Some("unscoped-eval".to_owned()),
        warmup_reads: 2,
        measured_reads: 2,
        economics: None,
        seed: 6619,
    };

    let error = run_range_serving_curve_worker(&config)
        .await
        .expect_err("unguarded GCS prefix must fail closed");
    assert!(error.contains("guarded scratch prefix"), "{error}");
}

#[tokio::test]
async fn provider_cache_economics_classifies_every_bounded_point_read() {
    let receipt = Box::pin(run_range_serving_curve_worker(&RangeServingCurveConfig {
        base_key_count: 256,
        value_bytes: 1_024,
        tail_records: 0,
        point_samples: 128,
        scan_rows: 1,
        max_rss_bytes: 1_073_741_824,
        cache_mode: RangeServingCacheMode::SharedRamNvme,
        decoded_cache_bytes: 65_536,
        nvme_cache_bytes: 131_072,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 16,
        provider_mode: RangeServingProviderMode::Correct,
        object_backend: RangeServingObjectBackend::Local,
        scratch_prefix: None,
        warmup_reads: 64,
        measured_reads: 128,
        economics: Some(ProviderCacheEconomicsConfig {
            distribution: ProviderCacheTraceDistribution::Uniform,
            zipf_theta_milli: 0,
            hotset_fraction_ppm: 0,
            hot_read_fraction_ppm: 0,
            hotset_shift_every: 0,
            view_reopen_every: 64,
            provider_get_cost_nano_usd: 400,
            mode: ProviderCacheEconomicsMode::Correct,
        }),
        seed: 7727,
    }))
    .await
    .expect("provider cache economics worker executes");

    let economics = receipt
        .economics
        .expect("provider cache economics receipt exists");
    assert_eq!(economics.logical_reads, 128);
    assert_eq!(economics.cache_hits + economics.cache_misses, 128);
    assert_eq!(economics.oracle_checks, 128);
    assert!(economics.oracle_exact);
    assert_eq!(economics.oracle_sha256, economics.observed_sha256);
    assert!(economics.cache_bound_enabled);
    assert!(economics.cache_bound_held);
    assert!(economics.settled_cache_bytes <= economics.cache_capacity_bytes);
    assert_eq!(economics.view_reopens, 1);
    assert_eq!(
        receipt.provider_get_requests,
        receipt.provider_revision_checks
    );
    assert!(receipt.scratch_cleanup_complete);
}
