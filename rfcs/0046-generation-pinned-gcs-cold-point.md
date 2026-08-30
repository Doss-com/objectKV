# RFC-0046: Generation-pinned GCS cold-point and layout curve

- Status: `[EVALUATING]`; design `[VERIFIED]`, implementation `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-30
- Scope: T28, `okv-object`, `okv-eval`, and the RangeEngine object-refill path

Pre-implementation review: `[VERIFIED]` Fable `SHIP` after two adversarial
passes. D68 activates implementation after the bounded T27 provider-v2
diagnostic while leaving T27 tail performance `[EVALUATING]`.

## Decision

Implement T28.0 and T28.1 as a cold point-read gate at the object frontier `O`.
A fresh read-only process consumes one existing, generation-pinned RFC-0044
fixture and a frozen operation plan. It must not generate, publish, rewrite,
enumerate, or fully hydrate fixture objects. It exact-opens only the descriptor,
manifest, selected indexes, and selected data ranges required by the plan.

Bounded refill, dataset-size independence, and the projected DataFusion lane
remain `[PROPOSED]`. They require separate cache, hierarchical-metadata, and
authenticated typed-closure contracts before implementation. T28 remains
`[EVALUATING]` until every required lane receives its own clean GCS receipt.

## Context and invariant

T27 measures the resident RocksDB path down to local NVMe. It intentionally
performs no object refill. The current `cold_read` runner proves the indexed
`OKVB` and `OKVI` mechanism on a temporary local filesystem, but it creates its
fixture in the same invocation and rejects non-filesystem backends. That path
cannot support a GCS latency, request, or empty-worker claim.

RFC-0044 already supplies the required durable boundary:

```text
writer invocation
  -> immutable content-addressed row objects
  -> immutable indexes and manifest
  -> generation-pinned placement locator
  -> writer authority revoked

fresh reader invocation under an attested objectViewer principal
  -> exact locator and descriptor generation
  -> named manifest and selected indexes
  -> bounded named data ranges
  -> denied write-capability probe before measurement
  -> no PUT, DELETE, or LIST in the measured window
```

The invariant is:

> A point read from empty process-local state returns the exact object-base
> value at `T = O` using one selected checksummed data block. Descriptor,
> manifest, and index work is measured separately and is not claimed to be
> database-size independent in format v1.

## Proposed contract

### Placement input

The reader accepts:

```text
FixturePlacementLocatorV1
expected locator-envelope SHA-256
expected fixture ID
expected descriptor generation
read version, exactly equal to locator base version O
immutable operation-plan SHA-256
reader-principal and IAM-receipt SHA-256
reader profile, retry policy, and local-cache budgets
```

It rejects a missing expected digest, a mutable or unpinned descriptor, a
provider or bucket mismatch, any `T != O`, and any object identity or checksum
mismatch. T28.1 does not apply `(O, C]`; tail composition remains a later gate.

The existing 1 GiB T27 fixture is the first input. Later size points may reuse
the same generator and object format, but each receives its own locator and
source, suite, binary, and infrastructure identities.

### Read-only execution identity

The plan binds a dedicated GCP service-account email and unique ID, bucket,
project, region, IAM-policy receipt digest, credential source, token expiry
floor, and exactly `roles/storage.objectViewer`. The infrastructure receipt
must show no inherited or direct storage writer role for that principal. At
runtime the process binds the metadata-server principal and token lifetime to
the plan, then attempts one create-only PUT under a unique probe name. The probe
must return permission denied and leave no object before measurement begins.

The measured window contains no PUT, DELETE, or LIST call. A zero write count
without the principal and IAM evidence is not sufficient.

### Point-read stages

The measured pipeline is:

```text
placement locator
  -> descriptor GET at exact generation
  -> manifest GET by content identity
  -> locate row object for key
  -> index GET on metadata-cache miss
  -> locate one checksummed block
  -> one data range GET
  -> verify returned object length, byte range, and block checksum
  -> apply MVCC visibility at read version
```

The reader uses a new lazy opener that returns the authenticated descriptor and
manifest without reading any index or data object. Calling
`open_existing_fixture_at_revision` is forbidden in the measured reader because
that helper verifies the complete closure by downloading every index and data
object.

T28.1 implements two states:

1. `empty_reader`: no process-local metadata or data cache. Measure time and
   bytes to the first exact read.
2. `metadata_warm_data_cold`: locator, manifest, and required index metadata
   are cached, while selected data blocks are not. This is the admitting GCS
   point-latency lane.
Warmup is state-specific. Every position first performs transport-only warmup
against a plan-pinned immutable canary outside the fixture namespace. It may
reuse DNS, TLS, token, and connection state, but no fixture descriptor,
manifest, index, data byte, or decoded value. `empty_reader` then executes with
its process-local metadata and data caches disabled. Each operation constructs
a new lazy metadata reader so no decoded fixture state crosses operations.
`metadata_warm_data_cold` additionally warms the descriptor, manifest, and
required indexes before the measured data-latency window. All warmup requests,
bytes, and time remain separately counted. An evaluator may not prefetch
complete data objects or the complete database. `bounded_refill` remains
`[PROPOSED]` under T28.2.

For the admitted 1 GiB fixture, `empty_reader` permits at most 256 KiB of total
response payload, including the 662-byte descriptor, 72,985-byte manifest, one
selected index, and one at-most-64-KiB data block. The metadata cache is limited
to 4 MiB, the data cache is disabled, and the maximum fetch buffer is 64 KiB.

### Matched controls

The point lane compares:

- candidate: manifest plus `OKVI` lookup followed by `OKVB` block range GET;
- raw-range control: the same GCS backend reads the exact precomputed block
  ranges without kernel lookup work;
- full-object poison: one point causes a complete data-object GET;
- enumeration poison: discovery depends on LIST instead of the locator.

One planner invocation reads the authenticated manifest and indexes, derives
the exact block key, byte range, checksum, and expected value digest for every
operation, and seals those fields before candidate execution. The expected
value comes from the independently versioned RFC-0044 fixture generator, not a
candidate read. The candidate decodes the selected `OKVI` index and must derive
the same planned range. The raw control consumes the planned range directly.

Candidate and raw-range control use the same process topology, credentials,
machine, object identities, key trace, concurrency, retry policy, and
state-specific warmup. Three seeds each execute five fresh-process paired
blocks. The sealed plan alternates candidate-control-control-candidate and
control-candidate-candidate-control, with the starting order rotated by seed.
Every position performs 128 unmeasured warmup operations followed by 1,024
measured reads at eight concurrent tasks. For `empty_reader`, those 128
operations address only the disjoint transport canary. For
`metadata_warm_data_cold`, the warmup additionally opens the named fixture
metadata and selected indexes without fetching a fixture data block. No
process-local state crosses a position boundary. The raw control is a hardware
and provider floor, not an application alternative.

The latency gate has one frozen aggregation. Within each paired block, pool the
2,048 measured candidate latencies from its two candidate positions and
compute nearest-rank p99. Pool and compute the raw-range p99 the same way. The
block ratio is candidate p99 divided by raw-range p99. All 15 block ratios must
be at most 1.25. Report the minimum, median, p95, and maximum block ratio, plus
per-seed and per-order diagnostics. A pooled run-wide percentile is diagnostic
only and cannot admit the gate.

### Dataset and trace matrix

The first clean run reuses the existing 1 GiB fixture and freezes uniform traces
for seeds 1103, 2207, and 3301. It proves only the 1 GiB constant data-block
path. Metadata bytes, decode CPU, and peak RAM are reported without a
dataset-size claim. The later 64 MiB, 1 GiB, and 10 GiB matrix remains T28.4.

### Output receipt

Every result binds:

```text
source, executable, lockfile, suite, and plan digests
machine, boot, process, principal, IAM receipt, bucket, and region
fixture, locator envelope, descriptor generation, and manifest identities
operation-plan, trace, order, cache state, retry policy, and cache budgets
logical operations plus provider SDK attempts, ranges, status, generations,
bytes,
latency distribution, CPU, and peak RAM
OTel run ID plus six exporter completion results
collector-side logs, metrics, and traces run-ID receipt
```

The receipt separates descriptor, manifest, index, and data requests. A single
combined GET counter is insufficient because it can hide full hydration or
metadata work proportional to database size.

### Provider-attempt accounting

T28.1 configures `object_store` `RetryConfig.max_retries = 0` and binds the
complete retry configuration into the execution plan. A T28-specific observed
adapter records one event before and after every `object_store` provider call:

```text
operation ID and subject
object key and requested range
expected descriptor generation, when present
attempt ordinal and start time
success or error class
returned generation, object length, and returned range
response payload bytes and elapsed time
```

With library retries disabled, one logical `object_store` request maps to one
recorded provider SDK attempt. This adapter does not observe HTTP redirects,
transport retries below `object_store`, TCP retransmissions, or provider-side
billing events. Those require lower-level transport or provider telemetry and
remain deferred to the economics lane. T28.1 makes no physical-request or cost
claim.

## Failure model

The gate covers:

- missing, malformed, stale, or cross-bucket placement locators;
- descriptor generation mismatch;
- manifest, index, data block, and returned-range corruption;
- permission denial under the read-only principal;
- an unexpected application or `object_store` retry or additional provider SDK
  attempt;
- process death after metadata read and during data refill;
- reader restart between fresh positions;
- accidental complete-object GET, LIST authority, or hidden local fixture;
- cross-fixture index or data substitution;
- incomplete split closure.

The first run does not claim regional availability during GCS outage. Object
brownout and bounded recovery-tail behavior remain T30.

## Eval plan

### T28.0. Frozen decoder and negative controls

Add a point-only two-phase GCS suite, immutable operation plan, lazy opener,
read-only identity receipt, provider-attempt trace, and schemas. The unchanged
oracle must reject the full-object, LIST, stale-generation,
cross-fixture-index, hidden-local-fixture, unexpected-retry, writer-authority,
unrestricted-read-version, overlapping-process, and reused-state poisons.

Primary metric: `correctness.anomalies`, total must be zero.

### T28.1. One GiB read-only point mechanism

Reuse the admitted RFC-0044 fixture at `T = O` under one attested dedicated
`roles/storage.objectViewer` principal. Run fresh `empty_reader` and
`metadata_warm_data_cold` paired-block processes on the R0 runner.

Hard gates:

- exact values and zero correctness anomalies;
- a denied preflight write with no resulting object, then zero PUT, DELETE, and
  LIST operations in the measured window;
- runtime principal, IAM receipt, bucket, region, token lifetime, source,
  executable, lockfile, plan, machine, boot, and process identities match;
- one descriptor and one manifest identity, both checksum verified;
- at most one selected-index GET on an index-cache miss;
- exactly one logical data range GET, one recorded provider SDK attempt, and
  zero complete-data GETs per successful cold point;
- response data bytes at most the declared maximum block bytes per point;
- empty-reader response payload at most 256 KiB, metadata cache at most 4 MiB,
  no data cache, fetch buffer at most 64 KiB, and no complete fixture hydration;
- every one of the 15 paired-block candidate p99 ratios at most 1.25x the
  matched raw-range control under the frozen nearest-rank aggregation;
- all six exporter completion results succeed and an independent collector
  receipt finds every run ID in logs, metrics, and traces.

The 1.25x latency ratio is an initial R0 gate. It may be changed only before the
run or after publishing a failed receipt, never after observing an unsealed
candidate result.

### T28.2. Bounded refill curve `[PROPOSED]`

Sweep admitted RAM and NVMe block-cache budgets using the same traces. Report
hit ratio, GETs per operation, bytes per operation, p50, p95, p99, p99.9,
evictions, and local bytes. No cache state crosses a candidate/control or seed
boundary. Implementation requires a separately frozen cache-state and
eviction contract.

### T28.3. Projected DataFusion scan `[PROPOSED]`

Run the C0 row layout and C5 range-stripe source against the same typed closure.
Require exact rows and aggregates, zero opaque-payload requests, maximum fetch
buffer at most the declared coalescing bound, and at least 2x source rows per
second or 50 percent fewer GCS requests than C0. This is a separate lane and
receipt from point lookup. Implementation is blocked until C0 and C5 consume
one authenticated typed GCS closure instead of publishing subject-local media
inside the measured invocation.

### T28.4. Size and recovery closure `[PROPOSED]`

Repeat the admitted point mechanism at 64 MiB, 1 GiB, and 10 GiB. Point data
requests and maximum bytes must remain constant. First-read metadata is reported
separately and must remain within its declared bound. Rebuild an empty reader
from the generation-pinned split closure and reproduce its logical digest.
Because format-v1 `OKVM` embeds every segment reference, this gate must report
manifest bytes, metadata GETs, decode CPU, and peak RAM versus dataset size. A
failed metadata-growth ceiling selects a hierarchical manifest before a general
database-size-independent claim.

## Alternatives

### Reuse the current local-filesystem runner and change its backend label

Optimizes for: minimal implementation work.

Gives up: a valid cloud request, latency, credentials, generation, and
empty-process boundary. Rejected.

### Publish the fixture inside every measured process

Optimizes for: self-contained invocations.

Gives up: read-only isolation, stable object identity, comparable cold state,
and independent setup cost. Rejected.

### Warm every index and hydrate every data object before measurement

Optimizes for: lower measured read latency.

Gives up: the disposable RangeEngine and database-size-independent cold-read
claim. Complete data hydration is rejected. Complete index warmup is allowed
only as an explicitly named control with all bytes and time counted.

### Combine point and projected-scan results into one score

Optimizes for: one simple layout ranking.

Gives up: visibility into opposite access economics. Rejected.

## Compatibility and migration

T28 reads existing `FixturePlacementLocatorV1`, `OKVM`, `OKVI`, and `OKVB`
bytes without changing them. New evaluation receipts receive their own schema
version. A future hierarchical manifest or stable transactional-segment format
must run beside format v1 until both readers reproduce the same logical
history.

The local-filesystem runner remains a mechanism test. Its existing results are
not upgraded to cloud evidence.

## Unresolved questions

- Whether the point champion keeps a complete range manifest resident or adds
  a hierarchical manifest after the 10 GiB metadata curve.
- Whether selected `OKVI` indexes live only in RAM or use the same bounded NVMe
  metadata cache as data blocks.
- Whether GCS request concurrency should be per range, per worker, or governed
  by one cell-wide refill budget.
- Whether C5 can consume the existing RFC-0044 fixture through a typed sidecar
  or requires one separately authenticated typed projection closure.
