# objectKV bootstrap and implementation plan

## Program goal

Build and verify objectKV as the open-source, open-format kernel for
object-native data platforms, then admit its first production-credible cell
one bounded mechanism at a time.

The target is a cell-scoped, strict-serializable ordered KV composed from
reusable `okv-log` and `okv-wal` primitives, a quorum-durable txLog hot path,
bounded disposable RAM or SSD serving state, and immutable S3-compatible
objects as permanent, branchable history. Prove that one version history can
support low-latency point and range OLTP, replay and branching, log-oriented
systems, PostgreSQL storage semantics, Redis-like access, virtual filesystems,
and DataFusion base-plus-tail OLAP without ETL. `okv-fabric` is the single API
boundary through which these surfaces consume the kernel.

Advance each layer from `[CODE-COMPLETE]` to `[VERIFIED]` only through frozen
correctness, crash-recovery, bounded-state, latency, throughput, and economics
gates. Run those gates with OTel telemetry and immutable receipts on real
infrastructure against same-durability RocksDB, TiKV, FoundationDB, or other
appropriate controls.

The evaluator enforces the distinction in
[`docs/EVAL-WORKLOAD-CONTRACT.md`](EVAL-WORKLOAD-CONTRACT.md). Contract tests
prove semantics, smoke profiles prove wiring, and workload profiles prove one
bounded performance or economics claim. Only the third class can enter a
paired admission comparison.

Evaluations select, reject, or reshape mechanisms. They do not decide whether
the objectKV program continues. A failed curve changes the implementation or
provider profile while preserving the program goal and the evidence that led
to the change.

If native transaction authority cannot establish material object-native
leverage over an incumbent, pivot transaction authority to TiKV or FoundationDB
while retaining `okv-log`, `okv-wal`, object publication, branching, and the
version-aligned PostgreSQL plus DataFusion history. Material leverage means a
measured advantage in at least one load-bearing property such as independent
storage and compute scaling, branch and recovery cost, open-format access, or
total economics without an unacceptable correctness or latency regression.

The current program milestone is reached when one production-credible cell has
contributor-ready specifications, runnable examples, code, operational bounds,
and immutable eval receipts. Until those receipts exist, objectKV remains a
research program rather than an admitted database product.

The immediate critical path is RFC-0046 row 3, matched row-versus-column object
layout on GCS. Row 2's corrected cold-point execution leaves its original
every-block end-to-end gate `[EVALUATING]`, but its precommitted local-overhead
addendum is `[VERIFIED]` across all 15 blocks. RFC-0047 V2.1 is `[VERIFIED]`:
provider v2 removed full-base
history duplication and measured 1.000037x direct RocksDB local bytes on the 1
GiB image. Its bounded V2.2 diagnostic leaves tail latency `[EVALUATING]`.
P50, p95, and p99.9 were 1.026x, 1.131x, and 1.032x control, while p99 was
1.742x. The program intentionally defers the complete 27-stratum replay and
cache-hit versus cache-miss attribution so that cold-read and object-layout
leverage can be evaluated next. The original 1.20x p99 target remains unchanged.

### Current golden-path frontier

`[VERIFIED]` The R0 mechanism reconstructs one public range from regional GCS
plus a retained txLog suffix into bounded local NVMe, survives worker loss, and
performs zero object operations during the measured hot-read window.

`[VERIFIED]` The topology-matched GP3.1 rerun admitted the single-range native
read boundary. Native retained 90.89 and 91.97 percent of owned-value direct
RocksDB throughput in opposite process orders. P99 was 0.913x control in both
orders. Exact replay, bounded native state, empty-worker reconstruction, zero
measured object operations, and all three OTel signals passed.

`[VERIFIED]` GP3.1.1 admits the resident read boundary at 8 and 32 concurrent
clients. Native retained 87.34 through 89.06 percent of matched direct RocksDB
throughput and kept p99 between 1.107x and 1.184x control in both orders.

`[VERIFIED]` The first 64 MiB cache-pressure calibration executed on clean GCP
R0 and produced a negative result. Native retained 56.59 through 59.68 percent
of direct RocksDB throughput, p99 was 1.331x through 1.557x control, and CPU
time was 1.668x through 1.746x control. All semantic gates and OTel correlation
passed, and measured physical reads were zero.

`[VERIFIED]` The corrected 64 MiB rerun retained 94.32 and 97.35 percent of
control throughput while passing p99 and CPU/read bounds. A later matched
direct-read smoke produced 2,960.75 and 2,966.00 Linux physical bytes per read
for native and control, a 0.9982x ratio, while both paths passed 22 of 22 hard
gates. The direct-read result verifies the measurement treatment only.

`[EVALUATING]` T27 now persists one fixture across all subjects before the 1
GiB admission. `[VERIFIED]` Its RFC-0044 phase-0 falsifier established `O=2`
on 20 independent fresh authorities, 60 real OpenRaft processes total, with
one empty retained record, zero mutations, zero live keys, and exact retry
after a lost response. The changed-identity bypass poison was detected.
`[VERIFIED]` Phase 1 now fixes the 4 MiB descriptor, complete immutable
closure, exact seven-record tail, and distinct native/control semantic image
identities. Five clean-source candidate and poison receipts passed.
`[VERIFIED]` Phase 2 built native and direct-control RocksDB images in
independent empty processes from that closure and tail. Their physical IDs
differ, their complete logical digest is equal, both use nonzero local bytes,
and the regenerated-control poison failed closed. `[VERIFIED]` Phase 4
persisted a 64 MiB closure to regional GCS, reopened its exact descriptor
three times across fresh ABBA subjects, held transaction-authority scratch to
0.001623x, and detected the reuse-bypass poison. `[VERIFIED]` The phase-5
boundary then prepared a generation-pinned fixture in one invocation and
consumed it from separate native and direct processes under object-viewer
credentials. Commit `1cfad27` separated fixture seed `4244` from trace seed
`1103`; both subjects returned one equal trace, tail, and logical-image digest
with zero correctness failures and zero measured object requests. The 1 GiB
curve remains `[EVALUATING]`. `[VERIFIED]` The first immutable fresh-process
preflight then reused one canonical 64 MiB fixture across a native/direct ABBA
plan under object-viewer credentials and direct NVMe reads. Native retained
0.8652x and 0.9739x control throughput, p99 was 1.0048x and 0.9882x, CPU/read
was 1.0718x and 0.9797x, physical bytes/read were 1.0647x and 1.0638x, and
read amplification was 1.0000x in both orders. Every gate passed. The sealed
run flushed and shut down all three OTel signals; the collector independently
contained the run ID in logs, metrics, and traces. All nine leased resources
were destroyed after evidence capture.
`[VERIFIED]` The exact plan and one real direct-position receipt then passed
five isolated negative controls at source `9ca447d`. AABB, missing-position,
and option-mismatch artifacts retained recomputed digests but failed the
frozen plan decoder. One hidden native provider failed the direct-position
inventory gate. A missing locator exited before producing a plan, while the
20-object, 68,857,626-byte fixture manifest retained the same SHA-256 before
and after. Eight structured artifacts are immutable in versioned GCS. No new
performance point was produced.
`[VERIFIED]` Source `9cf5014` then published the immutable 1 GiB fixture under
temporary writer authority, revoked that authority, exact-opened its pinned
descriptor under object-viewer credentials, and froze the complete
540-position plan. The fixture occupies 266 objects and 1,101,701,925 physical
bytes. The plan binds 27 strata across three cache levels, three Zipf skews,
three trace seeds, and five fresh-process ABBA blocks. The viewer binding and
all nine leased resources were removed after immutable evidence capture. This
is setup evidence, not a new performance point.
`[VERIFIED]` Source `95dedb0` then retained its exact executable and source in
versioned GCS, bound a live private runner and NVMe filesystem into plan
`40d4559a`, and executed the first five complete 1 GiB strata. The first three
use 50 percent cache, Zipf 0.8, and eight readers. Seed 1103 produced AB and BA
throughput ratios of 0.994982x and 0.997260x and p99 ratios of 0.999051x and
1.000304x. Seed 2207 produced throughput ratios of 1.012558x and 0.998886x and
p99 ratios of 0.989567x and 0.998296x. Seed 3301 produced throughput ratios of
1.008552x and 0.981275x and p99 ratios of 0.987784x and 1.003334x. The fourth
and fifth strata retain 50 percent cache and eight readers while raising Zipf
skew to 1.4. Seed 1103 produced throughput ratios of 0.974144x and 0.976563x
and p99 ratios of 0.875320x and 0.901665x. Seed 2207 produced throughput ratios
of 0.965184x and 0.992665x and p99 ratios of 1.075079x and 1.188676x. CPU/read,
physical bytes/read, read amplification, all 100 fresh-process positions,
cache pressure, runtime identities, and logs, metrics, and traces passed.
`[VERIFIED]` The sixth stratum, `c50-z14-s3301`, then rejected the frozen p99
gate in both orders at 1.307614x and 1.339897x control. Throughput, CPU/read,
physical bytes/read, read amplification, correctness, pressure, runtime, and
all 20 fresh-process telemetry gates passed. Native local state was
2,215,101,820 bytes versus 1,099,175,660 bytes for control, a 2.015239x ratio
caused by activation copying the full object base into both current head and
history. The queue stopped before a seventh stratum. T27 remains
`[EVALUATING]`; provider-v1 now has five passing strata, one retained
rejection, 21 unexecuted strata, and zero buffered sentinels. RFC-0047 owns the
provider-v2 correction and exact failed-stratum replay.
Three-node replicated commit, RAM, multi-range, PostgreSQL, and HTAP remain
blocked on the complete T27 gate.
FoundationDB remains the semantic oracle and fallback profile. The immutable
concurrency evidence is under
`docs/artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/`.
The negative cache-pressure evidence is under
`docs/artifacts/eval-receipts/native-resident-cache-pressure-gcp-r0-2026-08-28/`.

### Master performance matrix

This is the canonical program scoreboard and the order for lighting up workload
metrics. Every substantive implementation turn starts from the first unverified
row on the critical path and ends by updating its observed result, gap, next
experiment, and evidence. A turn that produces no new measurement records that
fact rather than changing status. Each row owns a separate comparison lane and
receipt. Later rows may reuse artifacts from earlier rows, but they cannot
substitute an upper-layer result for a missing kernel result.

A layer-oriented view of the same evidence lives in
[`docs/architecture/EVIDENCE.md`](architecture/EVIDENCE.md); this table remains
the status authority.

| # | Workload curve | Status | Current measured position | Admission target | Next experiment |
|---:|---|---|---|---|---|
| 0 | Resident NVMe point reads, 1, 8, and 32 clients | `[VERIFIED]` | Native retains 0.873x to 0.920x direct RocksDB throughput; p99 is 0.913x to 1.184x; 24 million concurrency reads issue zero object operations | At least 0.80x throughput, at most 1.20x p99, exact values, bounded bytes | Keep as regression control for row 1 |
| 1 | Cache coverage, skew, and eviction | `[EVALUATING]` | `[VERIFIED]` Provider v2 fixes resident footprint: 1.000037x control at 1 GiB and 1.000703x at the 64 MiB preflight. A bounded 10-position diagnostic measured native/control p50 1.026x, p95 1.131x, p99 1.742x, and p99.9 1.032x. It is not a complete stratum or admission receipt. | At least 0.80x throughput, at most 1.20x p99 and 1.25x CPU/read across the coverage and skew sweep; exact values, bounded cache, named physical-read behavior | Deferred: attribute individual samples to cache hit or miss, then resume the frozen provider-v2 sweep without weakening the p99 gate |
| 2 | Cold indexed point reads and cache refill on GCS | `[EVALUATING]` | The corrected single execution completed 15 paired blocks and 30,720 reads per subject. Exact reads used one range GET with zero retries or anomalies. Pooled p99 was 1.094x raw, but 2 blocks rejected the original 1.25x gate at 1.595x and 1.383x. Provider ratios were 1.598x and 1.386x. `[VERIFIED]` The frozen local addendum passed 15/15 blocks; pooled local-residual p99 was 446.575/439.678 us and the maximum block increment was 33.932 us | One bounded metadata path plus one to three named data requests; bytes and decode independent of database size; no LIST authority; every paired block p99 at most 1.25x raw control | Defer repeated provider-tail sampling without weakening the original gate. Return for cache refill and a provider-tail strategy after row 3 fixes the object layout |
| 3 | Object-layout point and projected-scan geometry | `[EVALUATING]` | `[VERIFIED]` C5v2 passed the real GCS viewer-only preflight at 0.869x C0 point p99 and 31.692x scan throughput. Admitted r2 completed all 60 point and 30 scan positions, then failed final replay on mismatched evaluator-only scan names. Diagnostic C5v2/C0 point p50/p95/p99/p99.9 was 1.064x/1.082x/1.179x/1.319x; all 15 p99 blocks were below 2.00x. Scan throughput was 21.523x to 33.031x, 28.426x median. Media was 1.043x. `[VERIFIED]` Exact GCS complete-child recovery reproduced 25,014 records and 15,742 live rows through five full GETs, 13,700,110 bytes, 1,584 verified frame proofs, zero LIST or writes, and 792.221 ms elapsed. Its cloud poison failed at the exact child digest. `[VERIFIED]` The real GCS media gates wrote one 4,344-byte branch root with zero child copies and compacted six C5v2 runs at 1.040058x C0 bytes through 24 create-only PUTs, zero LIST, and exact final-history recovery. Independent OTel confirmation remains open. No sealed curve verdict exists | Preserve row-class point cost, materially improve projected scans, bound resident index and compaction amplification, recover one authenticated closure | Confirm the recovery and media run in independent OTel exports, then close row 3 without rerunning the completed performance curve solely for receipt repair |
| 4 | Native three-node replicated commit | `[EVALUATING]` | One-host G4.10b reaches 1,075.343 resolved outcomes/s and 104.274 ms maximum p99, 28.776x its one-entry control; independent-media latency is unmeasured. RFC-0045 L0 verifies deterministic protocol semantics. L1 verifies three real TCP log-node processes, synchronized local journals, restart and torn-tail repair, epoch fencing, and deterministic segment bytes across three seeds and three poisons. `[VERIFIED]` RFC-0050 TLC R2 covers two finite scopes and six named poisons. `[VERIFIED]` Three current-model GCP staged-prefix traces each replayed 36 events and three stable-quorum assertions with zero anomalies; the 15-event early-ack trace was rejected. Stale-epoch mutation and divergent segment bytes remain process-oracle checks outside the trace vocabulary. `[CODE-COMPLETE]` The L2 preflight batches consecutive records into one validated journal sync and uses persistent client-to-node connections. It is not yet a performance result. | One-range p99 within 1.25x matched-durability control, exact retries and conflicts, zero normal-path object operations, quorum acknowledgement on independent media | Run the bounded three-machine batch preflight; if it has headroom, implement the frozen open-loop queue and compare RFC-0045 L2 against its matched remote-block control. Then extend the trace through commit and delivery |
| 5 | Objectification, brownout, host loss, and local-media bounds | `[EVALUATING]` | Exact object-base plus txLog-suffix recovery and local failover exist on one host; sustained debt and physical bounds are unmeasured | Stable `C - O` lag, bounded txLog, at most 8x local state, exact host-loss recovery, declared brownout backpressure | Sustained write plus publication run with object-store fault schedule |
| 6 | Metadata branch and lazy empty-worker reopen | `[EVALUATING]` | Local branch/replay and empty replacement worker are exact; G4.4 p99 is 120.183 ms; parent-size independence on GCS is unmeasured | Branch time and initial bytes independent of parent size; first exact read avoids full hydration | Dataset-size sweep with branch, empty worker, and GCS request accounting |
| 7 | Multi-range cell throughput and transactions | `[PROPOSED]` | No admitted multi-range throughput or cross-range transaction receipt | Throughput rises with added range groups until a named resource saturates; strict-serializable cross-range outcome | Cell v0 first, then 1, 2, 4, and 8 range groups |
| 8 | RAM serving profile and SSD/RAM handoff | `[PROPOSED]` | Disposable RAM replay works in the playground; no matched end-to-end performance receipt | At least 20 percent gain on one predeclared metric, bounded memory, exact bidirectional handoff | Matched RAM versus admitted SSD profile after row 7; Garnet becomes a control only under the same operation subset, durability depth, network, and load |
| 9 | `okv-fabric` log, Redis, search, and virtual filesystem | `[PROPOSED]` | `okv-log`, Tetris, and Chess verify bounded local semantics; no specialist workload admission | One semantic oracle and one latency, throughput, retention, or branch curve per surface | Freeze separate log, Redis, search, and filesystem workload contracts; use Garnet and Valkey only for the declared Redis-like subset |
| 10 | PostgreSQL page-storage OLTP | `[PROPOSED]` | Architecture and page boundary are specified; no PostgreSQL storage prototype receipt | First prototype within 2x local PostgreSQL, resident steady state within 1.25x, exact crash recovery, bounded amplification | Page read/write adapter after cell contract stabilizes |
| 11 | DataFusion base-plus-tail HTAP | `[EVALUATING]` | Exact local four-row overlay uses 5,518 bytes with zero spill; source scan reaches 2.544M rows/s; scaled tail and interference curves are unmeasured | Exact snapshot; tail at most 1 percent adds at most 20 percent; materialize before 10 percent; explicit OLTP interference budget | Scale exact base-plus-tail at 0, 0.1, 1, and 10 percent tail |
| 12 | Complete-stack economics and operations | `[PROPOSED]` | Component measurements exist; no matched complete-stack cost or failure envelope | Publish every loss and at least one material branch, restore, footprint, elasticity, HTAP, or cost advantage | Run only after the chosen profiles in rows 1 through 11 are admitted |

Current architecture synthesis: the 2026-08-30
[VLDB working paper](../papers/objectkv-vldb/objectkv-vldb.pdf) presents the
storage-to-fabric construction, cell services, commit/read/HTAP flows, C5v2
layout, formal boundary, infrastructure ladder, and this matrix in one stable
artifact. It changes no row status by itself.

The matrix is updated under this closeout rule:

```text
implementation or experiment
  -> immutable receipt and OTel correlation
  -> observed value versus named control
  -> status and gap update in this matrix
  -> one next experiment, owned by the first unverified critical-path row
```

The active row is 3. Rows 1 and 2 remain `[EVALUATING]` under explicit
deferrals, not passing admissions. Row 3 owns the object-layout decision and
must close recovery rejection, compaction amplification, and branch-reference
reuse before the program advances to replicated commit. Row 1's control is
direct owned-value RocksDB under the
same recovered topology. `[VERIFIED]` The first bounded correction removed the
forced post-advance flush that produced a second SST probe on latest reads.
The focused R0 regression returned exactly one cache lookup per read, all eight
RangeEngine package tests passed, and the 60-million-read rerun cleared all
eight throughput, p99, CPU/read, and zero physical-read comparison constraints.
Row 1 remains `[EVALUATING]` because the operating-system page cache masked
physical reads and the declared coverage and skew sweep has not executed. The
negative and corrected calibration evidence is under
`docs/artifacts/eval-receipts/native-resident-cache-pressure-gcp-r0-2026-08-28/`
and
`docs/artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/`.
The matched direct-read mechanism is also `[VERIFIED]`: one native and one
control smoke sample exposed 2.96 KiB of physical reads per logical read with
nearly identical physical cost. Its evidence is under
`docs/artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/`.
The RFC-0044 phase-0 falsifier is `[VERIFIED]` on a clean release build and a
disposable GCP host. Twenty fresh authority clusters independently assigned
`O=2`; the suite and deliberate second-identity bypass passed 24 formal gates
across two receipts. Evidence is under
`docs/artifacts/eval-receipts/object-fixture-anchor-gcp-r0-2026-08-28/`.
RFC-0044 phase 1 is also `[VERIFIED]` on clean source `fc8189e`. The 4 MiB
contract reconstructed 4,096 records from 11 immutable objects at `O=2`, kept
all base values out of txLog, bound an exact seven-record suffix, produced
distinct semantic native/control image IDs with one equal complete logical
digest, and detected four deliberate poisons. Evidence is under
`docs/artifacts/eval-receipts/object-fixture-contract-gcp-r0-2026-08-28/`.
RFC-0044 phase 2 is `[VERIFIED]` on clean source `1ae2ede`. Independent empty
native and direct-control processes verified the same fixture and tail, built
distinct nonempty RocksDB images, and returned one equal complete logical
digest. The candidate pair and regenerated-control poison returned `keep`.
Evidence is under
`docs/artifacts/eval-receipts/object-fixture-resident-process-gcp-r0-2026-08-28/`.
RFC-0044 phase 4 is `[VERIFIED]` on clean source `6f812dd`. The regional GCS
preflight persisted one 64 MiB fixture as 20 objects totaling 68,857,626 bytes.
Four fresh ABBA subjects shared one fixture and tail identity, three reopened
the exact descriptor, all retained one empty anchor with zero base values in
txLog, and all reproduced one complete logical image with zero measured-window
object requests. The candidate and reuse-bypass poison returned `keep`; both
run IDs occur in OTel traces, metrics, and logs. Evidence is under
`docs/artifacts/eval-receipts/object-fixture-gcs-preflight-gcp-r0-2026-08-28/`.
Provider v1 produced five passing 1 GiB admission points and one retained
rejection. Row 1 remains `[EVALUATING]`. Provider v1 has five passing strata,
one rejected stratum, 21 unexecuted strata, and zero of two buffered sentinels.
The passing strata are under
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s1103-gcp-r0-2026-08-29/`
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s2207-gcp-r0-2026-08-29/`,
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s3301-gcp-r0-2026-08-30/`,
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s1103-gcp-r0-2026-08-30/`,
and
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s2207-gcp-r0-2026-08-30/`.
The retained rejection is under
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s3301-failed-gcp-r0-2026-08-30/`.
Provider v2 then passed its physical-footprint preflight and reduced the 1 GiB
local-byte ratio to 1.000037x control. Its intentionally bounded five-native,
five-control diagnostic measured p50 1.026x, p95 1.131x, p99 1.742x, and
p99.9 1.032x control. The p99 issue remains open; the full replay stopped by
program decision so row 2 can advance. Evidence and the recurring four-panel
performance figure are under
`docs/artifacts/eval-receipts/rfc0047-resident-v2-preflight-tail-diagnostic-gcp-r0-2026-08-30/`.
Their immutable GCS evidence binds plan `40d4559a`, workload `7019d0e1`, exact
runtime executable `aac675c7`, machine instance `141366064138072137`, 120
position receipts, and all three OTel signals. The phase-5 cross-invocation
correctness receipt remains under
`docs/artifacts/eval-receipts/t27-gcs-placement-boundary-gcp-r0-2026-08-28/`.
Row 2 now has one real single-pair GCS mechanism diagnostic under
`docs/artifacts/eval-receipts/rfc0046-t28-point-preflight-gcp-r0-2026-08-30/`.
It proves exact shared-range execution and the 64 KiB byte ceiling, but does
not update the recurring figure because paired-process, IAM, poison, and OTel
admission gates remain open.
Rows 2 through 7 contain useful mechanism evidence, but none may advance past
`[EVALUATING]` while its own admission receipt is missing.

The critical path is rows 2 through 7, with row 1 retained as deferred
performance debt. Row 8 is optional and cannot delay the
SSD-backed cell. Rows 9 through 11 begin only after the cell contract is stable
enough that adapter work cannot conceal kernel defects. Row 12 selects profiles
and documents tradeoffs; it is not a stop decision for the project.

The first executable sequence is:

```text
RFC-0046 generation-pinned GCS cold point
  -> matched object-layout curves
  -> independent-media replicated commit
  -> objectification debt, host loss, and media bounds
  -> branch and lazy reopen
  -> multi-range cell
  -> RAM profile
  -> okv-fabric surfaces
  -> PostgreSQL OLTP
  -> exact DataFusion HTAP
  -> comparative economics

deferred side loop
  -> cache hit/miss attribution
  -> T27 provider-v2 cache-pressure curve
```

### Recurring performance figure

The program maintains one four-panel view covering latency shape, throughput
scaling, resident footprint, and tier evidence. Every admitted experiment must
update its relevant panel. Missing RAM, GCS, TiKV, MultiRaft, or FoundationDB
performance controls remain visibly unmeasured rather than receiving estimated
points. The current figure is in
[`docs/artifacts/eval-receipts/rfc0047-resident-v2-preflight-tail-diagnostic-gcp-r0-2026-08-30/README.md`](artifacts/eval-receipts/rfc0047-resident-v2-preflight-tail-diagnostic-gcp-r0-2026-08-30/README.md).

### What the goal optimizes for

- One small, composable ordered transaction substrate for distributed
  applications.
- Fast resident reads and writes with permanent, open, branchable object state.
- Independent storage and compute scaling without putting object latency in the
  normal commit path.
- One exact version history usable by OLTP serving models, PostgreSQL, and
  DataFusion rather than an ETL-copied analytical truth.
- Claims backed by reproducible receipts, including negative controls and
  matched-durability alternatives.

### What the goal gives up

- Cross-cell transactions and a global synchronous version space.
- Object-only low-latency commits as the default durability profile.
- Immediate work on MultiRaft, PostgreSQL completeness, metaclusters, or HTAP
  optimization before the resident and cold-object performance curves pass.
- A commitment to owning transaction authority if TiKV or FoundationDB is the
  stronger measured foundation.

## Original intent

Launch objectKV as a contributor-ready open-source project, then use an eval-led
research program to progress from a versioned object engine to an object-native
transaction kernel, full PostgreSQL compute, and ZebraDB HTAP over one logical
history. Distributed Redis semantics and inverted search are earlier serving
models that pressure-test the kernel without replacing the PostgreSQL north star.

## Workstreams, Level 1

- W1. Physical object engine: prove object-store read, write, cache, compaction,
  and reopen economics.
- W2. Version and correctness model: define visibility, replay, deletion,
  snapshots, and storage-engine contracts.
- W3. Durability boundary: add a low-latency replicated WAL and object durable
  watermark without acknowledging impossible states.
- W4. Disposable serving: recover correct reads from metadata, recent WAL, and
  object storage with empty local state.
- W5. Logical distribution: route, split, move, fence, and compact logical
  ranges without copying their durable bytes.
- W6. Transaction semantics: global snapshots, OCC conflict ranges, retries,
  and strict serializability.
- W7. Eval and research system: freeze oracles, budgets, suites, result records,
  and keep/discard rules.
- W8. PostgreSQL and HTAP path: preserve PostgreSQL behavior, then connect one
  commit/version history to OLTP and DataFusion columnar execution.
- W9. OSS operations: establish naming, license, RFCs, governance, contributor
  tasks, release contracts, and public claims.

## Depth findings

### W1. Physical object engine

- Mechanism: Apache `object_store` for providers, a SlateDB adapter or extracted
  segment layer for the first implementation, RAM/NVMe cache, and immutable
  segment publication.
- First files: `crates/okv-segment`, `crates/okv-object`,
  `evals/suites/phase0.toml`.
- Finding: the pinned SlateDB revision publicly accepts externally assigned
  sequence numbers and custom WAL traits. Explicit public read-at-version and
  standalone segment-building seams remain upstream questions.
- Failure mode: the engine looks fast when warm but cold point reads or
  compaction generate uneconomic object requests and bytes.
- Rough scope: medium, 1 to 2 weeks for a comparable baseline, 2 to 4 weeks for
  a versioned adapter.

### W2. Version and correctness model

- Mechanism: a single-threaded executable specification, generated histories,
  differential checks, and an immutable-segment interface.
- First files: `crates/okv-model`, RFC-0002, RFC-0003, RFC-0004.
- `[VERIFIED]` The logical model fixes generation-aware versions, gaps, canonical
  exact replay, range tombstones, scans, read-your-writes, and an inclusive
  oldest-readable boundary. Large-value references remain open.
- Failure mode: implementation and oracle share the same bug or the version
  contract changes silently under benchmarks.
- Rough scope: small for point operations, large once range deletes, snapshots,
  and concurrent histories enter.

### W3. Durability boundary

- Mechanism: one three-node Raft log per cell, quorum fsync before acknowledge,
  asynchronous materialization, and conservative global watermarking.
- First files: future `crates/okv-wal-api`, `crates/okv-wal-raft`, RFC-0005,
  RFC-0007, RFC-0009.
- Entry gate: exact seeded simulation of generation recovery, virtual time, and
  injected log, network, clock, and object-store failures exists before WAL code.
- Open question: which persisted state surrounds `raft-rs`, and how are commit
  versions fenced across generation recovery?
- Failure mode: an acknowledged commit exists in neither reconstructable object
  state nor the retained WAL.
- Tigris-derived constraint: upload immutable bytes before publishing their
  authoritative pointer; commit data, index intent, and asynchronous task intent
  together; recover ambiguous uploads by identity; derive deletion safety from
  retained roots rather than incremental counters alone.
- Rough scope: large, 3 to 6 weeks after the versioned engine is credible.

### W4. Disposable serving

- Mechanism: serving workers subscribe to committed mutations, publish segments,
  maintain applied versions, and rebuild caches lazily.
- First files: future `crates/okv-storage-worker`, objectification evals, and
  empty-cache recovery scenarios.
- Open question: how much metadata must be eagerly loaded for bounded time to
  first correct read?
- Failure mode: logical readiness waits on copying or scanning the full dataset.
- Tigris-derived constraint: cache entries are version-addressed, bytes or
  blocks land before visible metadata, and every delayed populate is fenced by
  a newer invalidation or generation. Cache hits are part of the correctness
  history, not a performance-only layer.
- Rough scope: large, 3 to 6 weeks after the WAL path.

### W5. Logical distribution

- Mechanism: bounded complete cells, tenant database transaction domains,
  ordered key ranges, generation-fenced assignments, metadata-only split/move,
  shared historical segments, and background physical realignment. A future
  metacluster maps tenants to cells without cross-cell transactions.
- First files: future `crates/okv-router`, `crates/okv-control`, RFC-0006.
- Open question: how are segment references shared safely across child ranges
  while watermarking and GC remain correct?
- Failure mode: a stale owner publishes state or range movement triggers bulk
  durable-byte transfer.
- Rough scope: extra large, multi-month with fault testing.

### W6. Transaction semantics

- Mechanism: read versions, explicit read/write conflict ranges, partitionable
  resolver state, one ordered commit stream first, then measured scaling inside
  one cell. Transactions may span arbitrary in-cell ranges inside one tenant
  database.
- First files: future `crates/okv-txn`, `crates/okv-resolver`, RFC-0008.
- Open question: ordered versus hashed resolver domains, transaction lifetime,
  commit-unknown handling, and log partitioning threshold.
- Failure mode: a generated concurrent history observes a result the reference
  serializable model cannot produce.
- Rough scope: extra large, begins after ranges and global snapshots.

### W7. Eval and research system

- Mechanism: frozen correctness gates, lane-specific fixed-budget benchmarks,
  public and held-out seeds, machine profiles, noise measurement, and an
  append-only experiment ledger.
- First files: `program.md`, `docs/EVALS.md`, `evals/`, `experiments/`.
- Open question: which stable machine/backend profiles produce results worth
  comparing across contributors?
- Failure mode: an optimizer changes the test, buys speed with correctness, or
  overfits one visible workload.
- Rough scope: small for the chassis, ongoing for each system phase.

### W8. PostgreSQL and HTAP path

- Mechanism: use narrow Redis and inverted-index adapters as early pressure
  tests; first prototype PostgreSQL page/storage bridging over objectKV; later
  decide whether to deepen that fork or move logical relations/indexes directly
  onto objectKV. Materialize version-aligned Parquet for DataFusion before
  evaluating Vortex.
- First files: `docs/POSTGRES-PATH.md`, RFC-0010, a future isolated bridge crate
  or PostgreSQL fork repository.
- Open question: which PostgreSQL durability and MVCC responsibilities remain in
  PostgreSQL versus move into objectKV?
- Failure mode: two independent transaction/MVCC systems duplicate work and
  cannot agree on snapshot visibility.
- Rough scope: one to two weeks per thin bridge prototype, multi-quarter for full
  compatibility and HTAP.

### W9. OSS operations

- Mechanism: Apache-2.0, vendor-led RFC governance, issue templates, claim-safe
  README, public benchmark receipts, and one adopter-independent API.
- First files: `README.md`, `CONTRIBUTING.md`, `rfcs/`, `.github/`.
- Open question: whether and when public crates need a separate namespace from
  the `okv` shorthand.
- Failure mode: launch claims distribution or performance before evidence, or
  contributors implement conflicting invariants.
- Rough scope: small bootstrap, then continuous maintenance.

## Merged workstreams

- M1. Prove the object engine, covers W1 + early W2 + early W7. Produce a
  versioned engine and Gate 1 report.
- M2. Prove durable objectification, covers W3 + W4 + fault portions of W7.
  Produce low-latency commits and empty-cache recovery.
- M3. Prove the distributed transaction kernel, covers W5 + W6. Produce
  metadata-only range movement, global snapshots, and serializable transactions.
- M4. Prove PostgreSQL and HTAP consumption, covers W8. Produce real PostgreSQL
  regression evidence and version-aligned analytical snapshots.
- M5. Operate the OSS project, covers W9 + research governance from W7. Keep the
  public contract, RFC queue, and contribution lanes coherent through all phases.

## Sequence

### S0. Bootstrap the project

Status: `[EVALUATING]`.

Deliverables:

- public naming and OSS-boundary decisions;
- compiling Rust workspace;
- executable reference-model smoke eval;
- eval result contract and autonomous research program;
- RFC queue and contributor-ready tasks.

Exit: all local checks pass and one human can select a task without reconstructing
the architecture memo.

### S1. Establish the Phase 0 baseline

Status: `[EVALUATING]`. Object-store conformance and the repaired SlateDB
filesystem scale curve now execute; MinIO physical storage, GCS, compaction,
one bounded layout pass, and target-workload ceilings remain open.

Memory, filesystem, and pinned MinIO now have executable capability-profiled
conformance evidence. Filesystem is intentionally segment-only because the
shared Apache `object_store` API does not expose conditional update for its
local backend. A bounded real-GCS cache-admission canary now executes in
`doss-objectkv-dev`; Phase 0 SlateDB and object-authority conformance on GCS,
clean-source repetition, and required OTel remain open.

RFC-0021 and `phase0-slate-filesystem-v1` run 8 MiB per seed through pinned
SlateDB at revision `e0161973`. The original 129.9 ms reopen result included
closing the old instance and is superseded. Candidate `361a0fd` separates every
phase and gives each raw artifact a unique `run_id`. The repaired 1, 8, and
64 MiB runs kept exact logical results at 4.85, 6.19, and 424.13 ms for open
through first correct read. The 64 MiB open read 210,773,938 bytes, crossing
RFC-0022's stop threshold for the untuned incumbent. The repaired warm-instance
poison `402f095c` discarded on the fresh-cache gate.

Implement the same fixed dataset/workloads against:

1. SlateDB with filesystem storage;
2. SlateDB with local S3/MinIO;
3. SlateDB with one real cloud object-store profile;
4. the in-memory oracle for correctness only.

Gate 1 passes only if hot/cold latency, object request amplification, rewritten
bytes, compaction cost, and empty-cache reopen are measured and acceptable for a
named target workload. A blended average cannot pass the gate. One bounded
SlateDB layout and compaction pass may continue; another dataset-sized reopen
stops SlateDB as the incumbent without stopping objectKV.

### S2. Build the versioned object engine

Status: `[EVALUATING]` logical contract; storage-engine implementation remains
`[PROPOSED]`.

Introduce externally assigned versions, point reads, range reads, exact replay,
immutable segment publication, checksums, and manifest inspection behind a small
storage-engine contract. Differentially test every generated history against
`okv-model`.

The logical contract and generated differential gate now exist. The pinned
SlateDB adapter owns a full 16-byte latest-version metadata record, rejects
unsupported generations and range clears explicitly, and proves concurrent
lower versions cannot replace a higher one. Explicit historical reads and
atomic range clears remain adaptation seams.

Exit: Gate 1 re-runs against the objectKV adapter with no correctness failures.

### S3. Add the fast durability tier

Status: `[EVALUATING]` the simulator, local stable storage, three-node OpenRaft
replication, real-process retry recovery, and generation handoff gates exist;
the production WAL remains `[PROPOSED]`.

The first exact replay probe now preserves synced control authority across
crash/restart and rejects a stale generation after partition/repair. Extend that
harness to the coordinator, durable log, object store, and recovery state
machine. Then add one ordered replicated WAL, acknowledge after quorum
durability, consume it into immutable objects, and advance the conservative
object durable watermark. Ratekeep and eventually refuse commits at declared
`C - O` and retained-WAL bounds.

The persistence and consensus slices now frame the frozen commit envelope,
synchronize independent per-node journals, replicate through three OpenRaft
processes, recover a lost reply after real leader death, and fence a G1 to G2
generation handoff through signed recovery evidence. Remaining work includes
snapshot persistence and retained-outcome expiry, disk-full behavior, replica
repair, independent-disk loss, production timing and placement, object-root
reconciliation, and sustained throughput and latency curves.

Gate 2: every failing seed replays exactly; low-millisecond local-region commit
is demonstrated; brownout, kill/restart, disk-full, and lost-ack scenarios
preserve every acknowledged commit within the published RPO contract.

### S4. Make serving disposable

Status: `[EVALUATING]` for the complete publication and disposable-serving
lifecycle. Single-range materialization recovery and resident point reads
through 32 clients are `[VERIFIED]` on R0. Cache pressure and the real-GCS lazy
reopen curve remain open.

Separate read/materialization workers from permanent bytes. Start with an empty
cache and demonstrate bounded logical readiness independent of dataset size.

Four publisher gates now start replacement processes with empty scratch. The
first recovers after quorum-durable `Prepare` but before object effects. The
second recovers after the first immutable PUT takes effect and its response is
lost, using replicated intent plus exact named identity to finish the closure.
The third recovers after the immutable manifest PUT takes effect and its
response is lost, then replays and verifies every named child before root
visibility. The fourth recovers the exact retained `Publish` outcome after its
reply, publisher, and accepting authority leader are lost, then proves exact
retry causes no second authority or object effect. Multipart residue, repeated
unknowns, abandoned work, sweeper recovery, independent-media empty-disk read
serving, and bounded readiness remain open. The public range now has bounded
local and GCS recovery diagnostics, but not a clean cold-read admission receipt.

Gate 3: complete worker loss does not require durable dataset copy.

### S5. Add ranges, snapshots, and OCC

Status: `[PROPOSED]`.

Add logical ranges, assignment generations, metadata-only split/move, global
cell read versions, tenant transaction domains, conflict ranges, and resolver
checks in that order. The first cell centralizes throughput while keeping role
protocols explicit; resolver, proxy, and log partitioning follow measured
ceilings. Cross-cell transactions are out of scope.

Gates 4 and 5: range movement copies approximately zero durable database bytes;
generated histories remain strictly serializable at useful measured throughput.

### S6. Put full PostgreSQL compute on objectKV

Status: `[PROPOSED]`.

Begin the page/storage bridge spike once S2 establishes stable versioned storage.
Do not claim the PostgreSQL path until S3 provides a credible durability boundary.
Use PostgreSQL's own regression suite as the compatibility oracle.

Gate 6A: a real PostgreSQL server boots, creates a database, survives restart,
and passes a declared regression subset with objectKV as its durable backing.

### S7. Build the ZebraDB HTAP path

Status: `[EVALUATING]` for the exact model, streaming overlay, and direct
columnar range source; the complete PostgreSQL-derived path remains
`[PROPOSED]`.

Map records and indexes to atomic objectKV transactions. Materialize Parquet from
the authoritative commit history with explicit per-partition coverage versions.
Query one exact version `T` through a DataFusion base-plus-tail overlay. The
durable analytical tail may outlive the recovery WAL. Predicate pushdown must
retain keys needed to invalidate stale base rows. Long queries use snapshot
leases; writes based on their results use transactional projections or later
dependency validation.

Gate 6B: a representative ZebraDB workload is simpler or materially better than
the current dual-system path enough to justify owning the substrate.

## Recalibration check

The core intent still holds, but the north star is sharper than the source memo:
full PostgreSQL compute is now an explicit consumer program. It does not change
the first gate. If a mechanism misses its gate, stop advancing that mechanism,
record the result, and redesign or select another provider profile rather than
compensating with distribution or PostgreSQL work. The objectKV program
continues.
