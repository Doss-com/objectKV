# RFC-0042: Native resident concurrent-read curve

- Status: `[VERIFIED]`, GCP R0 admission at 8 and 32 clients
- Authors: DOSS
- Created: 2026-08-27
- Scope: GP3.1.1 concurrency scaling at the admitted single-range read boundary

## Decision to test

Keep GP3.1's recovered topology, owned-value semantics, object closure, txLog
catch-up, and empty replacement. Change only the number of concurrent point-read
clients. Run the native version-bound snapshot and direct RocksDB control at 1,
8, and 32 clients in both process orders.

```text
one recovered range image
          │
          ├── client 0 ──┐
          ├── client 1   │
          ├── ...        ├── one synchronized read window
          └── client N   │
                        ──┘
                          │
                          ├── total reads / wall second
                          └── merged p50 / p95 / p99 / p99.9
```

This tests parallel read scaling. It does not test eviction, a larger-than-cache
working set, multi-range routing, RPC, or replicated commit.

## Frozen load construction

For each seed and subject:

1. Generate the same deterministic 80/20 hot-set operation sequence used by
   GP3.1.
2. Partition the exact total warmup and measurement budgets across clients.
   Remainders go to the lowest client indexes, so every operation executes once.
3. Let every client warm its assigned portion.
4. Hold clients on one barrier, then release one concurrent measurement window.
5. Merge every per-operation latency into one percentile population. Compute
   throughput from the exact total operation count and shared wall duration.
6. Fail if the client count is zero or above 256, any client gets no operation,
   the executed operation count differs from the declared budget, a value is
   incorrect, or an object request occurs.

The operation budget is total across clients, not per client. This prevents the
32-client subject from receiving 32 times more work than the 1-client subject.

## Admission curve

```text
working set:                  4 MiB, 4,096 keys, 1,024-byte values
distribution:                 deterministic 80/20 hot set
clients:                      1, 8, 32
seeds:                        1103, 2207, 3301
repeats:                      5 per seed on GCP R0
warmup reads:                 100,000 total per sample
measured reads:               200,000 total per sample
orders:                       native/control and control/native
native local-byte ceiling:    128 MiB
object operations in window:  0
throughput floor at 8 and 32:  0.80x matched direct RocksDB
p99 ceiling at 8 and 32:       1.20x matched direct RocksDB
```

The 1-client point is a diagnostic regression anchor against GP3.1. GP3.1's
frozen single-client GCP receipt remains the admission anchor, so GP3.1.1 does
not rerun it. The 8-client point shows the first useful parallel curve. The
32-client point exceeds the GCP R0 vCPU count and exposes scheduler and engine
contention. Both 8 and 32 clients are admission subjects.

## Receipt requirements

One admitted GP3.1.1 evidence set contains:

- the 8-client and 32-client native and control workloads in each process
  order, plus the admitted GP3.1 single-client receipt as the regression
  anchor;
- one source revision, binary hash, suite hash, and machine receipt;
- identical object base, txLog suffix, keys, values, seeds, and operation counts;
- exact reported client count and measured-operation count for every sample;
- throughput, p50, p95, p99, and p99.9 for every curve point;
- zero correctness failures and zero measured object operations;
- OTel logs, metrics, and traces for every run ID;
- complete scratch cleanup and infrastructure teardown.

## Tradeoff

This optimizes for attribution. A failure means the native snapshot boundary or
its RocksDB use scales differently from the direct control under concurrent
readers.

It gives up immediate evidence about cache pressure. Both subjects still use a
working set that fits the current point-lookup cache. The next gate must expose
one explicit block-cache budget and use a reusable object fixture larger than
that budget. Combining those changes here would obscure whether contention or
eviction caused a regression.

## Stop rule

If native throughput falls below 0.80x control or native p99 exceeds 1.20x
control at 8 or 32 clients in both process orders, stop concurrency optimization
at this boundary and profile the exact losing path before changing architecture.
Do not advance native replicated commit based on a single-client result alone.

## Evidence

- GP3.1 admitted baseline:
  `docs/artifacts/eval-receipts/single-range-native-matched-gcp-r0-2026-08-27/`
- GP3.1.1 admitted concurrency curve:
  `docs/artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/`
- `[VERIFIED]` On clean source `e478806`, the 8-client AB/BA throughput ratios
  were 0.8798x and 0.8734x; p99 ratios were 1.1842x and 1.1220x. The 32-client
  AB/BA throughput ratios were 0.8803x and 0.8906x; p99 ratios were 1.1072x and
  1.1478x. Every explicit comparison constraint passed in both orders. The
  eight workload results contain 120 total samples, 24,000,000 measured reads,
  zero wrong values, zero measured object operations, and correlated OTel
  logs, metrics, and traces. All leased resources were destroyed.
- Suite: `evals/suites/single-range-native-concurrency-admission.toml`
- Dirty local 32-client diagnostic: source `def98f5+dirty`, suite hash
  `f78d1d9db73947d6cd0e98e45875488406fd6bc71e0eecf5e15dadb94d92d019`.
  AB native run `dfabff16-4194-4719-b2b4-9d354cde0403` and control run
  `61ee981e-8a65-43cb-a6ca-1d76308ddc8a` produced 0.9835x throughput and
  0.8321x p99 ratios. BA control run `b136cee1-f09b-42a5-a6af-6ae58f3b791b`
  and native run `e76b9f09-c5c4-4863-b852-5eafc5fad2d0` produced 1.0324x
  throughput and 0.8717x p99 ratios. All runtime gates passed. These are dirty
  one-sample diagnostics, not admission receipts.
