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
| Public kernel | 0, 1, 7 | `[VERIFIED]` single-range resident read boundary and provider-v2 physical footprint; tail curve `[EVALUATING]` with p99 1.742x control in the bounded replay | Deferred hit/miss attribution and complete T27 curve, then multi-range transactions |
| Transaction plane | 4, 5, 7 | `[EVALUATING]` local OpenRaft, conflict, batching, and recovery mechanisms | Independent media, same-durability control, host loss, bounded recovery, multi-range serializability |
| RangeEngine | 0, 1, 2, 3, 8 | `[VERIFIED]` RocksDB point-read boundary and provider-v2 sparse history at 1.000037x control local bytes; bounded tail diagnostic `[EVALUATING]` | GCS cold point and layout curves now; deferred T27 tail attribution; raw NVMe cache, RAM profile, and handoff later |
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
┌─[ RFC-0046 -> T28.0 / T28.1 ]─────────────────────────────────────┐
│ fixture       existing 1 GiB generation-pinned GCS closure       │
│ version       exact object frontier T = O                        │
│ states        empty reader · metadata warm, data cold            │
│ subjects      indexed objectKV block · precomputed raw GCS range │
│ order         three seeds · 15 fresh-process paired blocks       │
│ primary gate  every block p99 ≤ 1.25x raw-range control          │
│ object gate   one data range GET · no LIST · no full hydration   │
│ authority     attested read-only objectViewer principal          │
│ result        pooled p99 1.048x; 2/15 blocks rejected at >1.25x │
│ cause         failed end-to-end ratios track GCS provider ratios │
│ next          exact provider/local residual attribution          │
│ state         [EVALUATING]                                        │
└───────────────────────────────────────────────────────────────────┘
```

The immutable locator, separate writer and read-only consumers, descriptor
generation, base-seed boundary, and one reusable 1 GiB object closure are
`[VERIFIED]`. `[CODE-COMPLETE]` T28 now has the lazy reader, sealed block plan,
read-only authority binding, no-retry GCS adapter, per-attempt trace,
independent value oracle, RAM-retained authenticated indexes, and
fresh-process concurrent candidate/control positions. `[EVALUATING]` The
three-seed curve completed 15 blocks and 30,720 reads per subject. Candidate
p99 was 61.752 ms versus 58.920 ms raw control, or 1.048x when pooled. The
frozen every-block gate rejected 2 of 15 blocks at 1.298x and 1.378x. Their
provider-only ratios were 1.299x and 1.379x, so the observed misses follow GCS
variance rather than measured candidate-local work. The rejection is retained;
`[CODE-COMPLETE]` Exact per-operation attribution records end-to-end, provider,
and local-residual latency. One fresh diagnostic measured candidate/raw
local-residual p99 at 428.507/407.242 microseconds while its end-to-end gap
tracked its provider gap within 16 microseconds. An admitted curve still
requires a precommitted variance-aware addendum and OTel binding.

Evidence:
`docs/artifacts/eval-receipts/rfc0046-t28-point-curve-gcp-r0-2026-08-30/README.md`.

```text
generation-pinned locator + immutable operation plan
  → fresh read-only process
    → lazy descriptor and manifest open
      → selected index range
        → one checksummed data-block range
          → exact value at T = O
```

Row 1 remains `[EVALUATING]` as deferred performance debt. Provider v2 fixed
local footprint at 1.000037x control, but its bounded replay measured p99 at
1.742x control. Its complete cache-pressure sweep resumes after hit/miss
attribution; it does not block T28 under D68.

## Layer unlocks

```text
┌─[ PERFORMANCE DEPENDENCIES ]───────────────────────────────────────┐
│ row 0 resident read                                               │
│   ↓                                                               │
│ row 1 cache pressure [deferred debt]                              │
│                                                                   │
│ row 2 cold read → row 3 object layout [active path]               │
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
- [T27 fifth complete 1 GiB stratum](../artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s2207-gcp-r0-2026-08-30/README.md)
- [T27 sixth 1 GiB stratum, retained rejection](../artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s3301-failed-gcp-r0-2026-08-30/README.md)
- [RFC-0047 provider-v2 footprint and tail diagnostic](../artifacts/eval-receipts/rfc0047-resident-v2-preflight-tail-diagnostic-gcp-r0-2026-08-30/README.md)
- [RFC-0047 sparse resident history](../../rfcs/0047-sparse-resident-history.md)
- [T27 GCS placement-boundary receipt](../artifacts/eval-receipts/t27-gcs-placement-boundary-gcp-r0-2026-08-28/README.md)
- [Native matched single-range receipt](../artifacts/eval-receipts/single-range-native-matched-gcp-r0-2026-08-27/README.md)
- [Native concurrent-read receipt](../artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/README.md)
- [Corrected cache calibration](../artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/README.md)
- [Direct-read attribution preflight](../artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/README.md)

The current implementation slice adds a `[VERIFIED]` provider-v2 footprint
correction and an `[EVALUATING]` bounded tail diagnostic. Master-matrix row 1
remains deferred and unverified because p99 is 1.742x control. The active
frontier is row 2, generation-pinned cold indexed reads from GCS.
