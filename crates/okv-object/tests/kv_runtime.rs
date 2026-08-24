use okv_object::{
    KvRuntime, KvRuntimeAction, KvRuntimeAdmission, KvRuntimeConfig, RangeEngineId,
    RangeEngineUsage,
};

fn tiny_config() -> KvRuntimeConfig {
    KvRuntimeConfig {
        ram_limit_bytes: 100,
        nvme_limit_bytes: 1_000,
        max_range_engines: 10,
        soft_objectification_debt_bytes: 100,
        hard_objectification_debt_bytes: 200,
    }
}

#[test]
fn kv_runtime_evicts_disposable_cache_before_refusing_work() {
    let mut runtime = KvRuntime::new(tiny_config()).expect("valid runtime config");
    runtime
        .assign_range_engine(
            RangeEngineId(7),
            RangeEngineUsage {
                metadata_bytes: 20,
                recent_mvcc_bytes: 20,
                objectification_debt_bytes: 0,
            },
        )
        .expect("range assignment fits");
    runtime.set_cache_demand(100, 800);

    let decision = runtime.pressure_decision();

    assert_eq!(decision.admission, KvRuntimeAdmission::Admit);
    assert_eq!(decision.actions, vec![KvRuntimeAction::EvictRamCache]);
    assert_eq!(decision.snapshot.accounted_ram_bytes, 100);
    assert_eq!(decision.snapshot.evicted_ram_cache_bytes, 40);
    assert_eq!(decision.snapshot.accounted_nvme_bytes, 800);
}

#[test]
fn kv_runtime_rate_limits_then_refuses_at_debt_bounds() {
    let mut soft_runtime = KvRuntime::new(tiny_config()).expect("valid runtime config");
    soft_runtime
        .assign_range_engine(
            RangeEngineId(1),
            RangeEngineUsage {
                metadata_bytes: 1,
                recent_mvcc_bytes: 1,
                objectification_debt_bytes: 101,
            },
        )
        .expect("range assignment fits");
    let soft = soft_runtime.pressure_decision();
    assert_eq!(soft.admission, KvRuntimeAdmission::RateLimit);
    assert_eq!(
        soft.actions,
        vec![
            KvRuntimeAction::RequestObjectification,
            KvRuntimeAction::RateLimit,
        ]
    );

    let mut hard_runtime = KvRuntime::new(tiny_config()).expect("valid runtime config");
    hard_runtime
        .assign_range_engine(
            RangeEngineId(1),
            RangeEngineUsage {
                metadata_bytes: 1,
                recent_mvcc_bytes: 1,
                objectification_debt_bytes: 201,
            },
        )
        .expect("range assignment fits");
    let hard = hard_runtime.pressure_decision();
    assert_eq!(hard.admission, KvRuntimeAdmission::Refuse);
    assert_eq!(
        hard.actions,
        vec![
            KvRuntimeAction::RequestObjectification,
            KvRuntimeAction::RateLimit,
            KvRuntimeAction::RefuseCommit,
        ]
    );
}

#[test]
fn kv_runtime_moves_ranges_when_non_evictable_ram_exceeds_limit() {
    let mut runtime = KvRuntime::new(tiny_config()).expect("valid runtime config");
    runtime
        .assign_range_engine(
            RangeEngineId(9),
            RangeEngineUsage {
                metadata_bytes: 60,
                recent_mvcc_bytes: 60,
                objectification_debt_bytes: 0,
            },
        )
        .expect("range assignment fits");

    let decision = runtime.pressure_decision();

    assert_eq!(decision.admission, KvRuntimeAdmission::Refuse);
    assert_eq!(
        decision.actions,
        vec![
            KvRuntimeAction::RequestObjectification,
            KvRuntimeAction::RequestRangeMove,
            KvRuntimeAction::RefuseCommit,
        ]
    );
    assert_eq!(decision.snapshot.accounted_ram_bytes, 120);
}

#[test]
fn kv_runtime_accounts_1_100_1000_ranges_with_one_shared_cache() {
    const MIB: u64 = 1_048_576;
    for range_count in [1_usize, 100, 1_000] {
        let mut runtime = KvRuntime::new(KvRuntimeConfig {
            ram_limit_bytes: 512 * MIB,
            nvme_limit_bytes: 8 * 1_024 * MIB,
            max_range_engines: 1_000,
            soft_objectification_debt_bytes: 64 * MIB,
            hard_objectification_debt_bytes: 128 * MIB,
        })
        .expect("valid runtime config");
        for id in 0..range_count {
            runtime
                .assign_range_engine(
                    RangeEngineId(u64::try_from(id).expect("test range id fits")),
                    RangeEngineUsage {
                        metadata_bytes: 512,
                        recent_mvcc_bytes: 4_096,
                        objectification_debt_bytes: 0,
                    },
                )
                .expect("range assignment fits");
        }
        runtime.set_cache_demand(128 * MIB, 2 * 1_024 * MIB);

        let decision = runtime.pressure_decision();
        let expected_fixed = u64::try_from(range_count).expect("count fits") * 4_608;

        assert_eq!(decision.admission, KvRuntimeAdmission::Admit);
        assert!(decision.actions.is_empty());
        assert_eq!(decision.snapshot.range_engine_count, range_count);
        assert_eq!(decision.snapshot.fixed_range_ram_bytes, expected_fixed);
        assert_eq!(
            decision.snapshot.accounted_ram_bytes,
            expected_fixed + 128 * MIB
        );
        assert_eq!(decision.snapshot.accounted_nvme_bytes, 2 * 1_024 * MIB);
    }
}
