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
│ throughput     0.873x to 0.920x of matched direct RocksDB         │
│ p99            0.913x to 1.184x of matched direct RocksDB         │
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
| 4 | The first open-loop matched-media diagnostic stayed stable through 40k offered records/s at 5.434 ms record p99, saturated near 45k to 46k ack/s, and reached approximately 1.18x dedicated `pd-ssd` saturation throughput. All 39 node checks across 13 named runs were exact | One repeat, no CPU or OTel attribution, frozen 1 ms and 100k targets missed, no failure injection or transaction resolver |
| 5 | Exact object-base plus txLog-suffix recovery works | Sustained debt, physical bounds, brownout, and host-loss curves are open |
| 6 | Local branch, replay, and empty replacement worker are exact | Parent-size independence and GCS request curve are open |
| 11 | Streaming base-plus-tail operator is exact on bounded fixtures | Tail-size, query-memory, GCS, and OLTP-interference curves are open |

## Latest lower-layer result

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
│ result        corrected pooled p99 1.094x; 2/15 blocks rejected  │
│ local         15/15 pass; max ratio 1.078x; max delta 33.932 us  │
│ cause         failed end-to-end ratios track GCS provider ratios │
│ next          matched row-versus-column object geometry          │
│ state         [EVALUATING]                                        │
└───────────────────────────────────────────────────────────────────┘
```

The immutable locator, separate writer and read-only consumers, descriptor
generation, base-seed boundary, and one reusable 1 GiB object closure are
`[VERIFIED]`. `[CODE-COMPLETE]` T28 now has the lazy reader, sealed block plan,
read-only authority binding, no-retry GCS adapter, per-attempt trace,
independent value oracle, RAM-retained authenticated indexes, and
fresh-process concurrent candidate/control positions. `[EVALUATING]` The
corrected three-seed execution completed 15 blocks and 30,720 reads per
subject. Candidate/raw pooled p99 was 62.304/56.964 ms, or 1.094x. The original
gate rejected two blocks at 1.595x and 1.383x; their provider ratios were
1.598x and 1.386x. `[VERIFIED]` The precommitted local addendum passed all 15
blocks with candidate/raw pooled local-residual p99 at 446.575/439.678
microseconds, a maximum 1.078x block ratio, and a maximum 33.932-microsecond
increment. All 61,440 reads were exact with one range GET and zero retries.
The independent collector contains the run in logs, metrics, and traces. The
original end-to-end gate remains rejected.

Evidence:
`docs/artifacts/eval-receipts/rfc0046-t28-corrected-point-curve-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` RFC-0048 now has one real GCS root over matched C0 indexed-row
and C5 columnar-main children. All nine child objects and the root are bound to
numeric generations. A fresh objectViewer-only process reopened both closures
and returned the same point value. C5 stored 1.009x C0 total bytes and retained
1.170x C0 metadata; its projection object is 0.116x C0 total bytes. These are
publication and media-shape results, not admitted point or scan performance.
The runtime's objectCreator grant was removed and a new create-only attempt was
denied without leaving an object. Point, scan, full recovery, compaction, and
branch gates remain `[EVALUATING]`.

Evidence:
`docs/artifacts/eval-receipts/rfc0048-t28-layout-publication-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` The fresh-process resource preflight retained a C5 v1 rejection.
C5/C0 point p99 was 2.540x against the frozen 2.50x guard, despite moving
0.351x the bytes and measuring only 1.060x per-call provider p99. The two
sequential C5 GETs are the remaining point-path exposure. The projected scan
returned the exact 15,742 rows at 67,227 rows/s versus C0 at 2,128 rows/s,
31.595x, with 6 versus 203 GETs and 0.117x response bytes. No admission
positions ran. Row 3 remains `[EVALUATING]` while a new compatible primary
index exposes both point ranges for concurrent gather.

Evidence:
`docs/artifacts/eval-receipts/rfc0048-t28-layout-preflight-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` RFC-0049 now has one immutable GCS C5v2 closure that reuses the
exact RFC-0048 C0 child. Its viewer-only preflight measured C5v2/C0 point
p50/p95/p99/p99.9 at 1.037x/0.793x/0.869x/0.541x, moved 0.267x point bytes,
and observed all 256 projection/payload pairs overlapping. Its exact
projection-only DataFusion scan measured 59,758 versus 1,886 rows/s, 31.692x,
with 7 versus 203 GETs, 0.130x bytes, and zero payload reads. The runtime
objectCreator role was revoked before measurement and a fresh create probe was
denied. C5v2 admission remains `[EVALUATING]` pending the frozen 15-block curve
and OTel confirmation.

Evidence:
`docs/artifacts/eval-receipts/rfc0049-t28-aligned-preflight-gcp-r0-2026-08-30/README.md`.

`[CODE-COMPLETE]` The RFC-0049 admission controller now executes 60 point and
30 scan positions in fresh processes, validates every provider attempt against
persisted object descriptors, derives OTel counts from the raw JSONL exports,
and replays the complete persisted evidence graph before finalization. It binds
the candidate parent and commit, executable, `Cargo.lock`, machine, read-only
IAM, both locators, object generations, oracle, media, children, and telemetry.
The GCP runner passed 155 of 155 evaluator tests and strict changed-surface
Clippy; Fable returned `SHIP`. This establishes evaluator integrity, not the
performance claim. Admitted r1 then stopped at position 2 of 90 because its
validator treated the first of two valid C0 data descriptors as the only valid
object. The sealed failure is retained. Exact-key selection across same-role
descriptors passes the original 1,024-read C0 shape and the full 158-test
remote library suite. Fable returned `SHIP`. Admission r2 remains
`[EVALUATING]`.

Evidence:
`docs/artifacts/eval-receipts/rfc0049-t28-aligned-r1-failed-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` The C5v2 complete-child recovery reads the exact root,
manifest, index, projection, and payload without LIST or writes. Its first GCS
run reconstructed 25,014 retained records and 15,742 live rows, verified 792
proofs per data object, and matched the independent canonical history digest
in 792.221 ms. The sealed cloud control changed byte zero of the exact
projection object, bound both object digests, repeated the five exact GETs, and
failed at the generation-pinned child digest. Independent OTel confirmation
remains `[EVALUATING]`.

Evidence:
`docs/artifacts/eval-receipts/c5v2-closure-recovery-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` The real GCS C5v2 media evaluator closed the frozen compaction
and branch gates. It created one 4,344-byte branch root that reused 26,820,839
bytes of exact parent children with zero child-object PUTs. Six C5v2 runs wrote
27,304,907 provider-accounted bytes versus 26,253,246 bytes for the matched C0
control, a 1.040058x ratio against the 1.10x ceiling. The run used 24
create-only object PUTs, zero LIST, and reconstructed the exact final history
of 25,014 records and 15,742 live rows. This verifies media geometry and
branch reuse, not latency or independent OTel.

Evidence:
`docs/artifacts/eval-receipts/c5v2-media-gates-gcs-r0-2026-08-30/README.md`.

`[VERIFIED]` RFC-0050 R2 checks the integrated 3-node model through 2,484,568
generated states and the 2-transaction concurrency scope through 4,496,463
generated states. Six exact fault controls produced their named invariant
counterexamples, and Fable returned `SHIP`. `[VERIFIED]` Three current-model
GCP staged-prefix traces each replayed 36 events and three stable-quorum
assertions with zero anomalies; the 15-event early-ack trace was rejected. The
stale-epoch and divergent-segment poisons remain process-oracle checks outside
the current trace vocabulary. Complete-cell mechanical refinement remains
`[EVALUATING]`.

Evidence:
`formal/evidence/gcp-r2-2026-08-30.json`.

Trace evidence:
`docs/artifacts/eval-receipts/cell-trace-refinement-r2-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` The RFC-0045 L2a preflight validates a bounded consecutive record
batch before physical mutation, writes its frames under one journal sync,
advances memory only after sync, and preserves no-growth exact retries. Three
corrected independent-machine runs acknowledged 196,608/196,608 records with
exact final state on all nine node checks and zero anomalies. Across 768 quorum
batches, p50/p95/p99/p99.9 was 4.357/4.535/4.716/5.343 ms; median throughput
was 49,028 records/s. The first run's 47.336 ms p99 exposed server-side
Nagle/delayed-ACK coupling and was retained as a rejected result. Row 4 remains
`[EVALUATING]` until the open-loop matched-control curve, batching dwell,
failure, transaction, and independent OTel gates pass.

Evidence:
`docs/artifacts/eval-receipts/staged-txlog-l2a-gcp-r1-2026-08-30/README.md`.

`[EVALUATING]` The first RFC-0045 L2 open-loop diagnostic ran 64 Poisson
producers and 256 streams through one bounded active-writer queue. The local
NVMe candidate remained unsaturated at 40k offered records/s, where record p99
was 5.434 ms and the maximum queue was 153 records. At 60k, it reached 45,451
acknowledged records/s while queue dwell contributed 534.928 ms of 539.902 ms
record p99. Dedicated `pd-ssd` saturated near 38.4k records/s; local NVMe
improved saturated throughput by approximately 1.18x and p99 by 0.404x to
0.848x across the sweep. Increasing the batch cap to 512 and 1,024 did not move
throughput above 48.3k records/s and approximately doubled quorum service time
with each doubling. The next falsifier is therefore batch journal framing plus
binary wire framing, with node-side stage timing, not a larger batch.

Evidence:
`docs/artifacts/eval-receipts/staged-txlog-l2-open-loop-gcp-r1-2026-08-30/README.md`.

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
│ row 2 cold read [deferred tail] → row 3 object layout [active]    │
│   ↓                                                               │
│ rows 4 to 6 commit, bounded recovery, and branching               │
│   ↓                                                               │
│ rows 7 to 8 multi-range cell and RAM profile                      │
│   ↓                                                               │
│ rows 9 to 11 fabric workloads, PostgreSQL, and HTAP               │
│   ↓                                                               │
│ row 12 complete-stack economics                                   │
└───────────────────────────────────────────────────────────────────┘
```

The active program advances row 3, the first objectKV-controlled layout
uncertainty after row 2 verified bounded local point overhead. Upper-layer code
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
- [T28 corrected GCS point curve](../artifacts/eval-receipts/rfc0046-t28-corrected-point-curve-gcp-r0-2026-08-30/README.md)
- [RFC-0048 typed GCS publication](../artifacts/eval-receipts/rfc0048-t28-layout-publication-gcp-r0-2026-08-30/README.md)
- [RFC-0048 typed GCS preflight](../artifacts/eval-receipts/rfc0048-t28-layout-preflight-gcp-r0-2026-08-30/README.md)
- [Native matched single-range receipt](../artifacts/eval-receipts/single-range-native-matched-gcp-r0-2026-08-27/README.md)
- [Native concurrent-read receipt](../artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/README.md)
- [Corrected cache calibration](../artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/README.md)
- [Direct-read attribution preflight](../artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/README.md)

The current implementation slice adds a `[VERIFIED]` typed C0/C5 GCS
publication and a retained preflight rejection. Master-matrix row 1 remains
deferred and unverified because p99 is 1.742x control. The active frontier is
row 3, the C5 point-index revision that removes sequential range-fetch latency
without changing the verified columnar scan path.
