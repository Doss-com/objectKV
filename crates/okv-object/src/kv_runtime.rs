//! Accounted resource envelope for a disposable KV Runtime.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Stable identity for one logical ordered-range serving assignment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RangeEngineId(pub u64);

/// Non-cache bytes attributed to one Range Engine assignment.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeEngineUsage {
    pub metadata_bytes: u64,
    pub recent_mvcc_bytes: u64,
    pub objectification_debt_bytes: u64,
}

/// Process-wide hard and soft bounds.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KvRuntimeConfig {
    pub ram_limit_bytes: u64,
    pub nvme_limit_bytes: u64,
    pub max_range_engines: usize,
    pub soft_objectification_debt_bytes: u64,
    pub hard_objectification_debt_bytes: u64,
}

/// Ordered pressure actions. Variant order is the controller order.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KvRuntimeAction {
    EvictRamCache,
    EvictNvmeCache,
    RequestObjectification,
    RequestRangeMove,
    RateLimit,
    RefuseCommit,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KvRuntimeAdmission {
    Admit,
    RateLimit,
    Refuse,
}

/// Immutable accounted state after disposable cache has been bounded.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KvRuntimeSnapshot {
    pub range_engine_count: usize,
    pub fixed_range_ram_bytes: u64,
    pub objectification_debt_bytes: u64,
    pub accounted_ram_bytes: u64,
    pub accounted_nvme_bytes: u64,
    pub evicted_ram_cache_bytes: u64,
    pub evicted_nvme_cache_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KvRuntimeDecision {
    pub admission: KvRuntimeAdmission,
    pub actions: Vec<KvRuntimeAction>,
    pub snapshot: KvRuntimeSnapshot,
}

/// Deterministic model of one process-wide serving resource envelope.
#[derive(Clone, Debug)]
pub struct KvRuntime {
    config: KvRuntimeConfig,
    ranges: BTreeMap<RangeEngineId, RangeEngineUsage>,
    ram_cache_demand_bytes: u64,
    nvme_cache_demand_bytes: u64,
}

impl KvRuntime {
    /// Construct a runtime after validating its pressure bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for zero resource or assignment limits, or when the
    /// soft objectification-debt bound exceeds the hard bound.
    pub fn new(config: KvRuntimeConfig) -> Result<Self, String> {
        if config.ram_limit_bytes == 0 {
            return Err("KV Runtime RAM limit must be positive".to_owned());
        }
        if config.nvme_limit_bytes == 0 {
            return Err("KV Runtime NVMe limit must be positive".to_owned());
        }
        if config.max_range_engines == 0 {
            return Err("KV Runtime range limit must be positive".to_owned());
        }
        if config.soft_objectification_debt_bytes > config.hard_objectification_debt_bytes {
            return Err("soft objectification debt cannot exceed hard debt".to_owned());
        }
        Ok(Self {
            config,
            ranges: BTreeMap::new(),
            ram_cache_demand_bytes: 0,
            nvme_cache_demand_bytes: 0,
        })
    }

    /// Assign one logical range without allocating a private cache.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity is already assigned or the
    /// process-wide Range Engine limit has been reached.
    pub fn assign_range_engine(
        &mut self,
        id: RangeEngineId,
        usage: RangeEngineUsage,
    ) -> Result<(), String> {
        if self.ranges.contains_key(&id) {
            return Err(format!("Range Engine {} is already assigned", id.0));
        }
        if self.ranges.len() >= self.config.max_range_engines {
            return Err(format!(
                "KV Runtime range limit {} reached",
                self.config.max_range_engines
            ));
        }
        self.ranges.insert(id, usage);
        Ok(())
    }

    /// Set process-wide cache demand. This is not multiplied by range count.
    pub fn set_cache_demand(&mut self, ram_bytes: u64, nvme_bytes: u64) {
        self.ram_cache_demand_bytes = ram_bytes;
        self.nvme_cache_demand_bytes = nvme_bytes;
    }

    /// Bound disposable cache first and return the current admission decision.
    #[must_use]
    pub fn pressure_decision(&self) -> KvRuntimeDecision {
        let fixed_range_ram_bytes = self.ranges.values().fold(0_u64, |total, usage| {
            total
                .saturating_add(usage.metadata_bytes)
                .saturating_add(usage.recent_mvcc_bytes)
        });
        let objectification_debt_bytes = self.ranges.values().fold(0_u64, |total, usage| {
            total.saturating_add(usage.objectification_debt_bytes)
        });
        let admitted_ram_cache_bytes = self.ram_cache_demand_bytes.min(
            self.config
                .ram_limit_bytes
                .saturating_sub(fixed_range_ram_bytes),
        );
        let admitted_nvme_cache_bytes = self
            .nvme_cache_demand_bytes
            .min(self.config.nvme_limit_bytes);
        let evicted_ram_cache_bytes = self
            .ram_cache_demand_bytes
            .saturating_sub(admitted_ram_cache_bytes);
        let evicted_nvme_cache_bytes = self
            .nvme_cache_demand_bytes
            .saturating_sub(admitted_nvme_cache_bytes);
        let mut actions = Vec::new();
        if evicted_ram_cache_bytes > 0 {
            actions.push(KvRuntimeAction::EvictRamCache);
        }
        if evicted_nvme_cache_bytes > 0 {
            actions.push(KvRuntimeAction::EvictNvmeCache);
        }
        let non_evictable_ram_exceeded = fixed_range_ram_bytes > self.config.ram_limit_bytes;
        let soft_debt_exceeded =
            objectification_debt_bytes > self.config.soft_objectification_debt_bytes;
        let hard_debt_exceeded =
            objectification_debt_bytes > self.config.hard_objectification_debt_bytes;
        if non_evictable_ram_exceeded || soft_debt_exceeded {
            actions.push(KvRuntimeAction::RequestObjectification);
        }
        if non_evictable_ram_exceeded {
            actions.push(KvRuntimeAction::RequestRangeMove);
        }
        if soft_debt_exceeded {
            actions.push(KvRuntimeAction::RateLimit);
        }
        if non_evictable_ram_exceeded || hard_debt_exceeded {
            actions.push(KvRuntimeAction::RefuseCommit);
        }
        let admission = if non_evictable_ram_exceeded || hard_debt_exceeded {
            KvRuntimeAdmission::Refuse
        } else if soft_debt_exceeded {
            KvRuntimeAdmission::RateLimit
        } else {
            KvRuntimeAdmission::Admit
        };

        KvRuntimeDecision {
            admission,
            actions,
            snapshot: KvRuntimeSnapshot {
                range_engine_count: self.ranges.len(),
                fixed_range_ram_bytes,
                objectification_debt_bytes,
                accounted_ram_bytes: fixed_range_ram_bytes.saturating_add(admitted_ram_cache_bytes),
                accounted_nvme_bytes: admitted_nvme_cache_bytes,
                evicted_ram_cache_bytes,
                evicted_nvme_cache_bytes,
            },
        }
    }
}
