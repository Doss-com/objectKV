use okv_slate::{
    run_kv_runtime_density_worker, KvRuntimeDensityMode, KvRuntimeDensityTopology,
    KvRuntimeDensityWorkerConfig,
};

fn tracer_config() -> KvRuntimeDensityWorkerConfig {
    KvRuntimeDensityWorkerConfig {
        topology: KvRuntimeDensityTopology::OneDbLogicalRanges,
        target_range_engines: 1,
        seed: 1_103,
        max_rss_bytes: 1_073_741_824,
        timeout_millis: 30_000,
        decoded_cache_bytes: 8_388_608,
        nvme_cache_bytes: 16_777_216,
        nvme_part_bytes: 65_536,
        nvme_open_file_handles: 16,
        keys_per_range: 1,
        value_bytes: 256,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_database_hosts_a_real_range_and_rebuilds_from_empty_caches() {
    let receipt = run_kv_runtime_density_worker(&tracer_config(), KvRuntimeDensityMode::Correct)
        .await
        .expect("run physical density tracer");

    assert_eq!(receipt.topology, "one-db-logical-ranges");
    assert_eq!(receipt.target_range_engines, 1);
    assert_eq!(receipt.completed_range_engines, 1);
    assert_eq!(receipt.database_instances, 1);
    assert_eq!(receipt.decoded_cache_instances, 1);
    assert_eq!(receipt.nvme_cache_instances, 1);
    assert!(receipt.physical_rss_probe_supported);
    assert!(receipt.peak_rss_bytes > 0);
    assert!(receipt.completed_range_reads_exact);
    assert!(receipt.empty_ram_and_nvme_reopen_executed);
    assert!(receipt.object_io.request_total() > 0);
    assert!(receipt.object_files > 0);
    assert!(receipt.nvme_cache_files > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn many_databases_share_one_decoded_and_nvme_cache() {
    let mut config = tracer_config();
    config.topology = KvRuntimeDensityTopology::ManyDbSharedCache;
    config.target_range_engines = 3;

    let receipt = run_kv_runtime_density_worker(&config, KvRuntimeDensityMode::Correct)
        .await
        .expect("run shared-cache density tracer");

    assert_eq!(receipt.topology, "many-db-shared-cache");
    assert_eq!(receipt.completed_range_engines, 3);
    assert_eq!(receipt.database_instances, 3);
    assert_eq!(receipt.decoded_cache_instances, 1);
    assert_eq!(receipt.nvme_cache_instances, 1);
    assert!(receipt.completed_range_reads_exact);
    assert!(receipt.empty_ram_and_nvme_reopen_executed);
}

#[tokio::test(flavor = "current_thread")]
async fn private_cache_topology_reports_one_decoded_cache_per_database() {
    let mut config = tracer_config();
    config.topology = KvRuntimeDensityTopology::ManyDbPrivateCache;
    config.target_range_engines = 3;

    let receipt = run_kv_runtime_density_worker(&config, KvRuntimeDensityMode::Correct)
        .await
        .expect("run private-cache density tracer");

    assert_eq!(receipt.topology, "many-db-private-cache");
    assert_eq!(receipt.completed_range_engines, 3);
    assert_eq!(receipt.database_instances, 3);
    assert_eq!(receipt.decoded_cache_instances, 3);
    assert_eq!(receipt.nvme_cache_instances, 1);
    assert!(receipt.completed_range_reads_exact);
    assert!(receipt.empty_ram_and_nvme_reopen_executed);
}
