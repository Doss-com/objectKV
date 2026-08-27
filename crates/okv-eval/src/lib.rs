//! Configurable evaluation and telemetry primitives for objectKV.

pub mod authority_state_scale;
pub mod cold_read;
pub mod commit_group;
pub mod commit_proxy;
pub mod commit_proxy_object_frontier;
pub mod comparison;
pub mod config;
pub mod frontiered_process_snapshot;
pub mod object_frontier;
pub mod process_snapshot_compaction;
pub mod program;
pub mod provider_incarnation;
pub mod provider_incarnation_provider;
pub mod provider_lifecycle;
pub mod provider_media_loss;
pub mod provider_selection;
#[cfg(feature = "resident-rocksdb")]
pub mod resident;
pub mod result;
pub mod serving_recovery;
pub mod serving_recovery_openraft;
pub mod storage_layout;
pub mod telemetry;
pub mod transaction_batch;
