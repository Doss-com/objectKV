# RFC 0069: Assigned-range placement and root-bound readiness

- Status: frozen before implementation
- Authors: objectKV contributors
- Created: 2026-08-25
- Supersedes: none

## Decision

Measure whether one logical range assignment can become a complete,
root-bound local serving image without copying unrelated database bytes. A KV
Runtime may route hot-SLO reads to a Range Engine only after that Range Engine
publishes a `placed-ready` receipt for the exact assignment epoch, authority
root, target version, and verified local image.

After readiness, every exhaustive assigned-range point and scan must complete
from the authenticated MVCC overlay plus the placed local image. Any provider
request after readiness withdraws the hot-read claim. Provider fallback remains
valid only through a separately declared cold-read path.

The first lane minimizes placement amplification. It does not select RocksDB,
SlateDB, or a new segment format as the permanent local representation.

## Question

RFC 0066 measured a persistent-NVMe point at about `0.295 ms` and a GCS data
miss at about `48.6 ms`. RFCs 0067 and 0068 then proved that a generic local
cache holding 25 percent of cell data cannot produce the required 97.5-percent
hit ratio under the frozen Zipfian or moving-hotset workloads, even with ideal
admission.

Explicit assignment changes the locality model. It does not prove that the
current physical layout can place one logical range efficiently. RFC 0057
selected one SlateDB database with many logical range prefixes, which keeps
database, task, cache, and object cardinality bounded. Its SSTs and cache parts
may cross logical range boundaries.

The next falsifier is therefore:

> Can one assigned logical range become a complete disposable local image at
> no more than `1.50x` placed bytes, no more than `2.00x` hydration bytes, and
> zero provider requests after readiness?

If the current shared physical layout cannot meet that bound, objectKV needs a
derived range-local image, prefix-aware physical packing, or a different
serving layout. A logical assignment alone is not a performance mechanism.

## Terms

- **assignment**: one cell, tenant, half-open key interval, assignment epoch,
  authority root, and target version authorized for one Range Engine;
- **logical assigned bytes**: exact key and value bytes visible in the assigned
  interval at the target version, excluding unrelated ranges;
- **placed bytes**: every durable local byte reserved by that assignment after
  staging cleanup, including data, indexes, checksums, metadata, and receipt;
- **placement amplification**: placed bytes divided by logical assigned bytes;
- **hydration provider amplification**: provider bytes read while building and
  verifying the image divided by logical assigned bytes;
- **placed-ready**: an atomic local publication proving that one exact local
  image passed the frozen completeness and identity checks;
- **provider request after readiness**: any backing object-store operation
  issued while opening or reading the ready image. The first contract does not
  exempt metadata misses.

## Serving lifecycle

```text
RangeMap assignment(root R, interval K, epoch E, target T)
  -> authenticate R and its exact provider closure
  -> hydrate K into an isolated staging directory
  -> verify every visible key, value, tombstone, frontier, and local part
  -> fsync the image and receipt
  -> atomically publish placed-ready(R, K, E, T, image digest)
  -> admit hot-SLO routing

hot read at T
  -> authenticated recent MVCC overlay
  -> decoded RAM
  -> placed local image
  -> zero provider requests
```

The ready receipt is a capability guard, not storage authority. The replicated
publication authority still selects the immutable root. Loss of local bytes is
safe. It removes readiness and forces a verified rebuild or a typed cold-path
decision.

## `PlacedRangeReceipt`

The first experimental receipt binds:

```text
format_version
cell_id
tenant_id
range_begin
range_end
assignment_epoch
authority_generation
authority_manifest_identity
provider_closure_digest
target_version
final_log_chain_sha256
local_image_format
local_image_digest
logical_row_count
logical_assigned_bytes
placed_bytes
placement_amplification
hydration_provider_requests
hydration_provider_bytes
hydration_duration
oracle_digest
published_at_unix_millis
```

The receipt becomes routable only after an atomic staging-to-ready rename. A
ready directory with a missing, corrupt, mismatched, or partially written
receipt is not ready. A receipt for root `R1`, assignment epoch `E1`, or target
`T1` cannot authorize `R2`, `E2`, or `T2`.

## Exactness oracle

For each seed, the controller independently generates the full MVCC history
and expected visible rows for every logical interval. The subject receives
only the selected assignment and normal authority inputs. Before readiness it
must compare its exhaustive local scan to the oracle digest and row count.

After readiness a fresh decoded-RAM view must:

1. open from retained NVMe without a provider request;
2. read every assigned key in deterministic shuffled order;
3. scan the complete assigned interval;
4. refuse points and scan bounds outside the interval;
5. repeat after unrelated-range pressure greater than the assigned logical
   bytes;
6. return the same exact digest and visible row count;
7. issue zero provider requests and read zero provider bytes.

The root-advance workload installs 64 certified txLog mutations above the
placed immutable base. The old receipt must stop routing when the authority
input advances. The subject may authenticate a bounded overlay or rebuild the
image, but it must publish a new receipt before serving the new target.

## Frozen workloads

The local fixture contains 4,096 deterministic 8 KiB high-entropy values,
about 32 MiB of logical data, in one SlateDB database. The ordered keyspace is
divided evenly into 1, 4, or 16 logical ranges. One range is assigned.
Compression cannot turn unrelated physical bytes into an apparent placement
win.

The correct workloads are:

1. one assigned range out of one, no overlay;
2. one assigned range out of four, 64 certified overlay mutations;
3. one assigned range out of sixteen, 64 certified overlay mutations and
   unrelated-range cache pressure;
4. retained-NVMe process reopen for the one-of-four assignment;
5. root advance for the one-of-four assignment.

Each performance point uses five fixed seeds. The process fixture uses an
explicit temporary build directory and removes its staging and ready
directories after the compact result identity is recorded.

## Metrics and gates

The primary metric is `provider_bound.placement_amplification`, minimized.
The practical improvement threshold is 25 percent because a smaller movement
does not justify a second local representation.

Every correct workload must satisfy:

```text
correctness anomalies                         = 0
placement amplification                       <= 1.50
hydration provider byte amplification         <= 2.00
post-ready provider requests                  = 0
post-ready provider bytes                     = 0
post-ready point p99                          <= 1 ms
outside-range reads admitted                  = 0
staging and scratch bytes after cleanup       = 0
```

Hydration duration, request count, bytes, and throughput are mandatory curve
outputs but are not local-machine hard gates. A later GCS execution of the
same semantic contract will set a remote rebuild-throughput target only after
the local image mechanism exists. The first local result must not project a
cloud hydration duration.

The placement threshold allows 50 percent for indexes, checksums, alignment,
metadata, and the receipt. It is not a claim that `1.50x` is the final target.
Results are also projected at one and two ready serving copies so that compute
failover cost is visible. This RFC does not require two simultaneously ready
copies.

## Unsafe controls

Four controls must produce schema-valid `discard` receipts:

1. publish readiness before the exhaustive image verification completes;
2. reuse an `R1/E1/T1` receipt after authority or assignment advances;
3. corrupt one placed local part after readiness;
4. accept provider fallback during the post-ready exhaustive read.

The corruption control may repair by withdrawing readiness and rebuilding. It
may not return a value or preserve the old hot-ready state after provider work.

## Candidate sequence

The first implementation measures the incumbent shape: one database with many
logical prefixes, `CachedObjectStore`, and direct exhaustive range hydration.
It may fail because cache parts are evictable, physical blocks cross logical
boundaries, or a fresh view still requires backing metadata.

If that baseline discards, the first orthogonal candidate materializes a
separate range-local serving image from the authenticated logical scan. That
image is derived, disposable, and never publication authority. The suite does
not prescribe its engine or file format.

Do not tune cache size or omit unrelated-range pressure to turn the incumbent
green. Do not run GCS until one local subject passes correctness, readiness,
and cleanup gates.

## Candidate surface

An experiment may change only:

- a new `crates/okv-object/src/placed_range.rs` module and its minimum export;
- focused tests for placement, readiness, corruption, and root transition;
- the minimum `crates/okv-eval` operation-dispatch plumbing needed to execute
  the frozen suite.

The RFC, suite, metric registry, result schema, authority oracle, exact seeds,
dataset, thresholds, controls, and budgets are frozen during an implementation
experiment. A contract defect starts a separate contract commit.

## Bounds and tradeoffs

This contract optimizes for predictable local reads and explicit byte
economics. It gives up the idea that any worker can serve any hot key directly
from object storage. Capacity planning must place the active ranges, and a
newly recruited worker has a measurable hydration interval.

A successful one-copy result still does not prove production availability.
Two ready copies roughly double placed bytes. One ready copy requires a cold
or unavailable interval after worker loss. The later orchestration decision
must choose that tradeoff per workload rather than hiding it inside the storage
engine.

## Compatibility

This contract adds no public storage format or client API. The receipt is an
experimental local artifact. Any future stable receipt or range-image format
requires its own compatibility fixtures and accepted RFC.

## Unresolved questions

1. Does direct SlateDB block placement stay near one logical copy as the
   assigned fraction falls from 100 to 6.25 percent?
2. Can a retained local image reopen with zero provider metadata requests?
3. Should a root advance extend a certified overlay, rebuild a range-local
   image, or switch between both by tail size?
4. What hydration concurrency reaches useful GCS throughput without request or
   memory amplification?
5. When are two ready copies economical compared with one, two, or three local
   RocksDB replicas?
