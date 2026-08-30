# Architecture evidence map

Status: `[EVALUATING]` evidence index. The
[master performance matrix](../BOOTSTRAP-PLAN.md#master-performance-matrix) is
canonical when this summary and the matrix differ.

## Proof ladder

```text
┌─[ EVIDENCE STRENGTH ]──────────────────────────────────────────────┐
│ 1  deterministic model                              [VERIFIED]    │
│ 2  synchronized local files                         [VERIFIED]    │
│ 3  local protocol service, including pinned MinIO   [VERIFIED]    │
│ 4  independent OS processes on one host             [VERIFIED]    │
│ 5  independent machines and media                   [PROPOSED]    │
│ 6  zones plus GCS                              [EVALUATING]       │
│ 7  sustained failure, performance, and cost curves  [PROPOSED]    │
└───────────────────────────────────────────────────────────────────┘
```

Higher rungs do not inherit from lower ones. A verified local process failure
does not prove host-loss recovery. A verified GCS fixture reconstruction does
not prove the GCS authority capability profile.

## Matrix by layer

| Layer | Matrix rows | Current proof | Missing admission evidence |
| --- | --- | --- | --- |
| `okv-fabric` | 9 | `okv-log`, Tetris, and Chess have bounded local semantics; unified fabric `[PROPOSED]` | Specialist log, Redis, search, and filesystem contracts and curves |
| Public kernel | 0, 1, 7 | `[VERIFIED]` single-range resident read boundary, 64 MiB preflight, and first four complete 1 GiB T27 strata; full curve `[EVALUATING]` | Remaining 23 T27 strata and two buffered sentinels, then multi-range transactions and scaling |
| Transaction plane | 4, 5, 7 | `[EVALUATING]` local OpenRaft, conflict, batching, and recovery mechanisms | Independent media, same-durability control, host loss, bounded recovery, multi-range serializability |
| RangeEngine | 0, 1, 2, 3, 8 | `[VERIFIED]` RocksDB single-range point-read boundary and three 1 GiB cache and skew strata at near-RocksDB parity | Complete 1 GiB cache curve, GCS cold misses, raw NVMe cache, RAM profile, handoff |
| Objectification | 5, 6 | `[VERIFIED]` scoped publication recovery mechanisms; integrated service `[EVALUATING]` | Sustained `C - O`, compaction, brownout, safe reclamation, branch-size sweep |
| Manifested object state | 2, 3, 6, 11 | `[VERIFIED]` immutable closure identity and exact GCS reuse; layouts `[EVALUATING]` | Cold geometry, clean split-run GCS, branch independence, scaled HTAP tail |
| Object provider | 2, 3, 5, 6, 12 | `[VERIFIED]` memory and MinIO authority profiles, filesystem segment profile, scoped GCS fixture use | GCS authority conformance, provider economics, sustained faults |
| PostgreSQL | 10 | Architecture `[PROPOSED]` | Storage adapter, compatibility suite, crash recovery, latency curve |
| DataFusion HTAP | 11 | `[VERIFIED]` exact bounded overlay semantics; performance row `[EVALUATING]` | Tail-ratio curve, complete-query memory, GCS, OLTP interference |

## Passing measured boundaries

```text
┌─[ ROW 0 · RESIDENT NVME POINT READS ]──────────────────────────────┐
│ throughput     0.873x–0.920x of matched direct RocksDB            │
│ p99            0.913x–1.184x of matched direct RocksDB            │
│ workload       24,000,000 reads, 1/8/32-client boundary           │
│ object I/O     zero measured-window object operations             │
│ result         [VERIFIED]                                         │
└───────────────────────────────────────────────────────────────────┘
```

Admission was at least `0.80x` throughput and at most `1.20x` p99 with exact
values and bounded local bytes.

```text
┌─[ ROW 1 · CORRECTED 64 MIB CALIBRATION ]───────────────────────────┐
│ throughput     0.943x and 0.973x of control                       │
│ p99            1.044x and 0.995x of control                       │
│ CPU/read       1.059x and 1.030x of control                       │
│ workload       60,000,000 measured reads                          │
│ result         calibration [VERIFIED], complete row [EVALUATING]  │
└───────────────────────────────────────────────────────────────────┘
```

This calibration passed its non-inferiority gates. It did not expose physical
NVMe reads because the operating-system cache absorbed them.

```text
┌─[ ROW 1 · DIRECT-READ ATTRIBUTION PREFLIGHT ]──────────────────────┐
│ native          2,960.75 physical bytes per logical read          │
│ control         2,966.00 physical bytes per logical read          │
│ ratio           0.9982x                                           │
│ result          mechanism [VERIFIED], performance not admitted    │
└───────────────────────────────────────────────────────────────────┘
```

This verifies the evaluator can expose device work under matched options. One
sample is not a performance curve.

```text
┌─[ ROW 1 · FIRST COMPLETE 1 GIB STRATUM ]───────────────────────────┐
│ profile        50% cache · Zipf 0.8 · seed 1103 · 8 readers       │
│ throughput     0.994982x AB · 0.997260x BA                        │
│ p99            0.999051x AB · 1.000304x BA                        │
│ CPU/read       1.017306x AB · 1.011997x BA                        │
│ physical/read  0.997738x AB · 0.997837x BA                        │
│ result         one stratum [VERIFIED], complete row [EVALUATING]  │
└───────────────────────────────────────────────────────────────────┘
```

This result contains five fresh-process pairs in each order and 20 million
measured reads. All correctness, pressure, runtime, receipt, and OTel gates
passed. It does not admit the other cache, skew, or seed strata.

```text
┌─[ GCS OBJECT-FRONTIER BOUNDARY ]───────────────────────────────────┐
│ fixture         one immutable closure reused across fresh subjects│
│ equality        fixture, tail, trace, and logical-image digests   │
│ correctness     zero aggregate and sampled failures               │
│ hot object I/O  zero measured requests                            │
│ result          cross-invocation boundary [VERIFIED]              │
└───────────────────────────────────────────────────────────────────┘
```

This is construction and recovery evidence, not a new throughput point.

## Useful measurements that are not admissions

| Matrix row | Measurement | Why it remains `[EVALUATING]` |
| ---: | --- | --- |
| 3 | DataFusion range source reached 2.544M source rows/s and reduced projection requests from 1,761 to 54 | Dirty local diagnostic; no exact live-tail, complete-memory, OTel, or GCS curve |
| 4 | One-host commit composition reached 1,075.343 resolved outcomes/s and 104.274 ms maximum p99, 28.776x one-entry control | Both quorums and object files shared one host |
| 5 | Exact object-base plus txLog-suffix recovery works | Sustained debt, physical bounds, brownout, and host-loss curves are open |
| 6 | Local branch, replay, and empty replacement worker are exact | Parent-size independence and GCS request curve are open |
| 11 | Streaming base-plus-tail operator is exact on bounded fixtures | Tail-size, query-memory, GCS, and OLTP-interference curves are open |

## Current first-unverified gate

```text
┌─[ T27 ]────────────────────────────────────────────────────────────┐
│ fixture       1 GiB logical, content addressed, one locator       │
│ cache         50% · 20% · 5% coverage                             │
│ skew          Zipf 0.8 · 1.4 · 2.0                               │
│ clients       8                                                    │
│ subjects      native RangeEngine · matched direct RocksDB         │
│ order         fresh-process ABBA                                  │
│ primary gate  throughput ≥ 0.80x control                          │
│ hard gates    p99 ≤ 1.20x · CPU/read ≤ 1.25x · I/O ≤ 1.25x       │
│ progress      3 of 27 direct-NVMe strata · 0 of 2 sentinels       │
│ state         [EVALUATING]                                        │
└───────────────────────────────────────────────────────────────────┘
```

The immutable locator, separate writer and read-only consumers, standalone
direct control, descriptor generation, and base-seed boundary are `[VERIFIED]`.
The fresh-process ABBA controller and its 64 MiB GCS plus direct-NVMe preflight
are `[VERIFIED]`. Native throughput was 0.8652x and 0.9739x direct RocksDB;
p99, CPU/read, physical bytes/read, and read amplification passed in both
orders. The controller flushed and shut down logs, metrics, and traces before
sealing their six outcomes into the admission receipt; collector inspection
found the run ID in every required signal. Failed comparison or exporter
completion persists a sealed failure receipt before exit. The five isolated
plan, position-inventory, and missing-locator poisons are `[VERIFIED]` against
that exact evidence. The immutable 1 GiB fixture and 540-position plan are also
`[VERIFIED]`: 266 objects, 1,101,701,925 physical bytes, 27 strata, and exact
native/direct treatment parity. The first four complete strata are
`[VERIFIED]`. Across three Zipf 0.8 seeds and one Zipf 1.4 seed, native
throughput spans 0.974144x to 1.012558x control and p99 spans 0.875320x to
1.003334x. The remaining 23 strata and two buffered sentinels remain open.

```text
immutable plan + independent oracle + machine envelope
  -> host-global lease
     -> fresh position wrapper
        -> measured replacement worker
           -> raw report + worker PID/boot/start identity
        -> sealed position receipt
     -> AB and BA median gates + cache-pressure gate
     -> one bound OTLP logs, metrics, and traces run
  -> sealed run receipt
```

The wrapper manages one position. The nested replacement worker owns the
measured RocksDB instance and read window. Receipts name both processes and
bind the latter to the worker identity reported inside the raw result.

## Layer unlocks

```text
┌─[ PERFORMANCE DEPENDENCIES ]───────────────────────────────────────┐
│ row 0 resident read                                               │
│   ↓                                                               │
│ row 1 cache pressure                                              │
│   ↓                                                               │
│ rows 2–3 cold read and object layout                              │
│   ↓                                                               │
│ rows 4–6 commit, bounded recovery, and branching                  │
│   ↓                                                               │
│ rows 7–8 multi-range cell and RAM profile                         │
│   ↓                                                               │
│ rows 9–11 fabric workloads, PostgreSQL, and HTAP                  │
│   ↓                                                               │
│ row 12 complete-stack economics                                   │
└───────────────────────────────────────────────────────────────────┘
```

The active program advances the first unverified dependency. Upper-layer code
may be built as a semantic probe, but it cannot substitute for a missing lower
layer receipt.

## Evidence sources

- [Master performance matrix](../BOOTSTRAP-PLAN.md#master-performance-matrix)
- [Project tracking](../PROJECT-TRACKING.md)
- [Real-infrastructure contract](../REAL-INFRA-EVALS.md)
- [Proof-status contract](../STATUS-TAXONOMY.md)
- [T27 fresh-process 64 MiB preflight](../artifacts/eval-receipts/t27-fresh-process-preflight-gcp-r0-2026-08-29/README.md)
- [T27 preflight poison replay](../artifacts/eval-receipts/t27-preflight-poisons-r0-2026-08-29/README.md)
- [T27 1 GiB fixture and frozen plan](../artifacts/eval-receipts/t27-1gib-fixture-plan-gcp-r0-2026-08-29/README.md)
- [T27 first complete 1 GiB stratum](../artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s1103-gcp-r0-2026-08-29/README.md)
- [T27 second complete 1 GiB stratum](../artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s2207-gcp-r0-2026-08-29/README.md)
- [T27 third complete 1 GiB stratum](../artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s3301-gcp-r0-2026-08-30/README.md)
- [T27 fourth complete 1 GiB stratum](../artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s1103-gcp-r0-2026-08-30/README.md)
- [T27 GCS placement-boundary receipt](../artifacts/eval-receipts/t27-gcs-placement-boundary-gcp-r0-2026-08-28/README.md)
- [Native matched single-range receipt](../artifacts/eval-receipts/single-range-native-matched-gcp-r0-2026-08-27/README.md)
- [Native concurrent-read receipt](../artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/README.md)
- [Corrected cache calibration](../artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/README.md)
- [Direct-read attribution preflight](../artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/README.md)

The current implementation slice adds the verified 1 GiB fixture, frozen live
plan, authenticated resumable stratum runner, and first four passing strata.
Master-matrix row 1 remains `[EVALUATING]` until all remaining strata and
buffered sentinels execute and pass.
