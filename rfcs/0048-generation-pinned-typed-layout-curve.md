# RFC-0048: Generation-pinned typed object-layout curve

- Status: `[PROPOSED]`, pre-implementation review SHIP on 2026-08-30
- Authors: DOSS
- Created: 2026-08-30
- Scope: RFC-0046 T28.3, RFC-0037 C0 and C5, `okv-object`, `okv-htap`, and `okv-eval`

## Decision to review

Evaluate the current indexed row object, C0, against the columnar-main plus
disposable RangeEngine overlay, C5, on one prepublished, generation-pinned,
typed GCS fixture. Publication and workload execution are separate authority
phases. Fresh read-only processes execute independent point and projected-scan
lanes without creating, rewriting, enumerating, or fully hydrating fixture
objects.

```text
writer identity
  -> canonical typed MVCC history
  -> C0 indexed-row child closure
  -> C5 columnar-main child closure
  -> one typed-layout root authenticating both children
  -> generation-pinned placement locator
  -> writer authority revoked

read-only identity
  -> exact root and selected child
  -> metadata and index warmup only
  -> point lane or projected-scan lane
  -> exact oracle plus request, byte, latency, and memory receipt
```

C0 and C5 are separate physical representations of one canonical history.
Their shared root authenticates the history digest, schema identity, covered
version, child manifests, object identities, and GCS generations. The root is
an evaluation envelope, not a stable public objectKV manifest format.

## Why C5 is the candidate

C5 directly tests the proposed columnar source-of-truth direction:

```text
bounded row delta
  -> immutable columnar main
       projection stripes
       opaque payload pages
       primary key -> stable row position index
  -> disposable RAM or NVMe RangeEngine cache
```

The existing release-local diagnostic measured C5 at 0.839x C0 point p99,
0.353x point bytes, 4.718x projected-scan throughput, 1.010x storage
amplification, and 1.170x resident index bytes. It also used 1.982x point
requests. Those results are `[EVALUATING]` because they came from a dirty local
tree and filesystem backend. GCS request latency can reverse the point result,
which is the reason for this experiment.

C4, the split row-sidecar plus columnar projection, remains a fallback. Its
local point geometry is safer, but it does not answer whether one columnar main
can be the permanent typed object representation. It receives a separate GCS
curve only if C5 fails or the product explicitly chooses duplicated typed
projection state.

## Non-decisions

This RFC does not:

- make C5 the format for opaque KV ranges;
- make Parquet, Vortex, or any file label the kernel contract;
- combine point and scan metrics into one score;
- put object storage in the foreground commit path;
- verify the RangeEngine cache-refill policy;
- verify exact base-plus-live-tail DataFusion execution;
- change the C0, C5, MVCC, point, scan, or value encodings.

Opaque KV ranges retain C0 unless a later format passes their own curve. C5 is
eligible only for typed namespaces whose schema identity is part of the
authenticated closure.

## Frozen semantic input

The plan is `evals/plans/t28-layout-geometry-v1.toml`. It freezes one fixture
seed independently from the three workload trace seeds:

```text
keys                         16,384
canonical live row bytes        512
opaque payload bytes             480
base version                       1
delta cycles                        4
updates per cycle                12.5 percent
deletes per cycle                 1.0 percent
target run object bytes      8,388,608
row block bytes                  65,536
columnar block rows                 128
fixture seed                         5699
trace seeds                5701, 5702, 5703
```

The canonical generator emits ordered MVCC entries, point outcomes at named
read versions, the final projected rows, aggregates, tombstones, and one
history digest. The producer materializes both children from those exact
entries. A child that reports a different logical digest is invalid before any
performance comparison.

Fixture seed 5699 alone determines keys, values, updates, deletes, and the
complete immutable history. Trace seeds 5701, 5702, and 5703 determine only
point-operation order and paired subject order. They never regenerate or
mutate the fixture.

Before storage producer code exists, a standalone reference generator that
imports no objectKV crate must seal
`evals/oracles/t28-layout-geometry-v1-oracle.json`. The plan binds that
artifact's exact SHA-256. The artifact contains the fixture history digest,
record count, live-row count, ordered projection digest, aggregate result,
and each trace's ordered operation-plus-expected-outcome digest. It also seals
the exact abstract workload-plan digest. Candidate code may consume expected
digests but may not produce them.

Frozen oracle identities:

```text
reference generator SHA-256
  c1b06252161baf973c459757905db63198c1fc046e62d0272f6d3df693b84e4c
oracle artifact SHA-256
  b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86
workload-plan SHA-256
  fa337ae95089b7c9e5771575568480769267468c271778e6781e18b99de337e1
plan-file SHA-256
  ec6fa45d7f9db2adc7d980cda97da5dbae30996b196e06d712543c032f8b5d48
canonical history SHA-256
  d4be64434f6b69990a2787876f514c6036727b41dcf1c5e120f91b6ce968ecd4
```

The independent generator reports 25,014 MVCC records and 15,742 live rows.
The existing Rust logical generator independently reproduced the same history
digest and live-row count on the GCP runner. This cross-check verifies the
oracle input without making candidate output the oracle.

The pre-implementation review is recorded at
`docs/research/reviews/fable-rfc0048-preimplementation-review-2026-08-30.md`.

## Eval-only typed fixture envelope

`TypedLayoutFixtureV1` is a versioned evaluation envelope:

```text
schema_version
fixture_id
canonical_history_sha256
schema_id + schema_sha256
covered_through_version
bucket + project + region
children
  c0_indexed_row
    format identity
    manifest object + generation + bytes + sha256
    every child object + generation + bytes + sha256
    complete child-closure digest
  c5_columnar_main
    format identity
    manifest object + generation + bytes + sha256
    every child object + generation + bytes + sha256
    complete child-closure digest
root_sha256
```

Each child descriptor is a closed ordered list of every data, index,
projection, payload, and nested-manifest object. Each entry binds the exact GCS
object name, generation, byte length, SHA-256, and semantic role. The decoder
rejects unknown schema versions, duplicate subjects, duplicate object names,
missing children, objects not reachable from the child manifest, mutable or
absent generations, cross-bucket children, unequal covered versions, unequal
history or schema identities, malformed digests, and an unrecognized
capability set. A canonical JSON compatibility fixture and one independently
corrupted fixture are required before the producer exists.

The root does not grant transitive trust. Producer preflight and empty-worker
recovery must walk each selected child's named objects and verify every length,
generation, and checksum. Every measured point or scan read must pass the
descriptor's expected generation to GCS and verify the returned generation,
range length, and checksummed content. A read without an expected generation
is invalid even when the returned bytes happen to be correct.

## Authority phases

### Phase A: publication

One writer identity with object-creator and object-viewer roles may create and
verify immutable subject media and the typed-layout root. It records every PUT,
named verification read, generation, byte count, and response. Publication
ends with a generation-pinned placement locator and complete-closure receipt.

### Phase B: freeze

The writer loses storage-write authority. A dedicated object-viewer identity
opens the exact root and derives one sealed execution plan. The execution plan
binds the workload-plan digest, plan-file digest, oracle and generator digests,
published root and child descriptors, every object generation, IAM and machine
identities, cache state, transport retry and attempt policy, point and scan
orders, concurrency, timer boundaries, budgets, and thresholds. Its
`execution_plan_sha256` does not exist before publication because the object
generations do not exist before publication. The controller, every measured
position, and every receipt bind both the frozen `workload_plan_sha256` and the
postpublication `execution_plan_sha256`. The execution plan does not contain
candidate-produced outcomes.

### Phase C: execution

Every measured position is a fresh process. Runtime verifies the metadata
service identity, IAM receipt, token lifetime, bucket, root generation, plan
digest, executable, lockfile, machine, boot, and process start. A create-only
write probe must be denied and leave no object. Measured positions may issue no
PUT, DELETE, or LIST operation. Point and scan positions construct the same
no-retry GCS transport. SDK attempt accounting wraps every provider call and
must prove exactly one attempt for every successful or failed range request.

## Point-preservation lane

The point lane compares complete C5 point lookup with complete C0 point lookup.
It does not compare either subject with a precomputed raw GCS range.

Both subjects receive metadata-warm, data-cold state. The child manifest and
primary index are resident within the declared 8 MiB metadata budget. No data
block, projection stripe, or payload page is retained between operations.
Every position performs a transport-only canary warmup, then 1,024 measured
points across eight concurrent tasks.

Three seeds each execute five paired ABBA or BAAB blocks. Seed 5702 inverts the
starting subject. Each block pools 2,048 latencies per subject and computes
nearest-rank p99.

Primary metric:

```text
every block C5 end-to-end point p99 / C0 point p99 <= 2.00
```

Hard gates:

- exact outcome at every key and target version;
- zero retries and zero correctness anomalies;
- at most two C5 data GETs and one C0 data GET per point;
- C5 maximum point bytes at most 0.50x C0;
- no complete-object GET;
- provider and local-residual latency reported separately;
- metadata budget at most 8 MiB and fetch buffer at most 256 KiB;
- all six OTel exporter completion checks and independent collector evidence.

The request limit reflects C5's projection-stripe plus payload-page gather.
The two requests are sequential because the current primary index locates the
projection stripe, and that stripe locates the payload page. The 2.00x gate
preserves the already frozen C5 mechanism threshold. It does not claim cold
parity. A lower byte count cannot excuse a point-latency failure, and a passing
cold result cannot replace the zero-object resident requirement. If the second
round trip dominates, the next candidate adds payload-page location to the
resident primary index and fetches both ranges concurrently under a new format
and compatibility fixture.

The production hot path remains the admitted resident RangeEngine profile from
row 0. This lane measures the permanent layout's miss and rebuild path. T28.2
owns cache fill, eviction, and repeated-hit verification rather than hiding
those states inside this cold comparison.

## Projected-scan lane

Point and scan processes share no state. Every scan position is a fresh
read-only process with metadata warm and data cold. It executes one complete
projection of key, tenant, category, and quantity through the final version.
C5 may coalesce adjacent projection stripes into at most 256 KiB fetches. C0
uses its indexed row layout and must decode the same logical projection.

Three trace seeds each execute five paired AB or BA blocks. Seed 5702 inverts
the starting subject. There are 15 paired ratios, one per block. Scan fetch
concurrency is frozen at one for both source operators. Each receipt records
configured concurrency, observed peak in-flight provider calls, and rejects
any value other than one.

Primary metric:

```text
nearest-rank median of the 15 within-block
(C5 projected rows per second / C0 rows per second) ratios >= 2.00
```

The scan SQL is identical for both subjects and returns the complete ordered
`key, tenant, category, quantity` projection plus count and quantity-sum window
aggregates. End-to-end DataFusion time begins before SQL parse and physical
planning and ends only after the final `RecordBatch` is drained and the ordered
projection and aggregate digests are finalized. Fixture open, descriptor
verification, and declared metadata warmup occur before the timer. Ratio of
subject medians is reported diagnostically but is not the admission statistic.

Hard gates:

- exact ordered projected rows and aggregate digest;
- zero C5 opaque-payload requests and bytes;
- C5 scan response bytes at most 0.50x C0;
- C5 scan GETs at most 64 per complete projection and reported against C0;
- peak fetch buffer at most 256 KiB;
- peak emitted Arrow batch at most 128 rows;
- no LIST, PUT, DELETE, unbounded complete-object hydration, or hidden local
  data;
- no provider retry and exactly one SDK attempt per request;
- configured and observed peak scan fetch concurrency exactly one;
- all six OTel exporter completion checks and independent collector evidence.

The scan lane keeps throughput as its only primary metric. Request count,
bytes, memory, and correctness are hard eligibility gates, not components of a
blended score.

Both subjects execute the same DataFusion query above separate source
operators. C5 uses `RangeStripeTableProvider`; C0 requires a bounded
`RangeRowTableProvider` that emits the same schema and source metrics. A direct
loop for C0 is not a valid DataFusion control. Both operators fetch and emit in
one deterministic stream for this first curve. Parallel scan scheduling is a
separate matched experiment.

## Shared media and recovery gates

The publication receipt must report C0 and C5 separately:

- data, index, manifest, and total bytes;
- stored/live amplification;
- compaction written bytes and write amplification;
- resident metadata bytes;
- build duration and rows per second;
- object count and generation count;
- branch incremental and shared bytes.

C5 must remain at or below 1.10x C0 storage amplification, 1.10x C0 compaction
write amplification, and 2.00x C0 resident metadata. One empty reader must
reconstruct each complete child closure from the root and reproduce the
canonical history digest. Branch creation must reference unchanged children
without copying their objects.

## Scheduling and isolation

The existing GCS layout runner is not admission eligible. It publishes each
subject inside the measured invocation and issues point reads serially. The new
controller separates producer, planner, and position commands. It bounds
in-flight GCS operations to eight for the point lane and one range fetch for
the scan lane. Candidate and control use identical concurrency limits.

Before the full curve, one uncounted seed-5701 preflight executes 256 points
per subject and one scan per subject. It must pass every correctness,
authority, request-shape, byte, and memory gate. It stops before the admission
positions if C5 cold-point p99 exceeds 2.50x C0 or projected-scan throughput is
below 1.25x. The preflight is a resource guard, never an admission sample.

No process-local fixture state crosses a position. No subject may read the
other subject's child closure. The host controller takes an exclusive lock and
refuses an existing output directory. The plan executes once. A later run
requires a new plan ID and one named code, configuration, or infrastructure
change.

## Required negative controls

The production decoders and controller must reject:

1. root with one missing child object;
2. swapped C0 and C5 manifest identity;
3. child generation mismatch;
4. unequal schema or canonical history digest;
5. subject-local publication during a measured position;
6. reused data cache across subjects or positions;
7. C5 point lookup that fetches a complete object;
8. C5 projected scan that fetches opaque payload;
9. corrupted projection stripe or payload page;
10. AABB order, missing position, overlapping process, or option drift;
11. runtime writer authority;
12. hidden local fixture or LIST-based discovery.

A poison is useful only when the unchanged production boundary rejects it.

## Receipt boundary

Every aggregate receipt binds:

```text
source + executable + Cargo.lock
plan + fixture + root + both child closures
writer receipt + IAM revocation receipt + runtime object-viewer identity
machine + boot + every process start
point and scan orders + seeds + cache and concurrency budgets
per-operation end-to-end + provider + local latency
requests + bytes + ranges + generations + errors
CPU + peak RSS + resident metadata + fetch and batch memory
correctness + recovery + branch + complete-media accounting
OTel run IDs + exporter completion + collector confirmation
```

Point and scan produce separate verdicts. C5 is not admitted unless both pass
and the shared media/recovery gates pass. Failure preserves C0 as the generic
object base and selects either C4 or a redesigned C5 mechanism.

## Tradeoff

Optimizes for: answering whether a columnar permanent base can preserve
transactional point behavior while materially improving analytical access,
using only objectKV-controlled mechanisms and one real cloud closure.

Gives up: a faster C4 confirmation, one blended layout score, and reuse of the
self-publishing serial runner as performance evidence.
