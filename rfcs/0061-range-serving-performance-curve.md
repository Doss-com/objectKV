# RFC 0061: Authority-Bound Range Serving Performance Curve

Status: `[ACTIVE-WORK]`

## Decision

Measure the composed Range Engine read path as two independent axes before making a cloud performance claim:

1. immutable-base cost as logical key count grows;
2. certified txLog overlay cost as the unobjectified tail grows.

Every curve point uses the real authority-bound SlateDB reader, real Ed25519 txLog certificates, exact point and ordered range reads, object I/O accounting, and a fresh worker process. The first profile uses a local filesystem whose OS cache is warm after base construction. It is not a GCS or S3 latency claim.

## System under measurement

```text
replicated publication authority
        |
        | exact outer published root + frontier K
        v
verified closure and inner immutable-base manifest
        |
        v
immutable SlateDB object base <= K
        |
        | authority-bound reader
        v
Range Engine worker
        ^
        | signed commits (K, T]
certified txLog suffix
        |
        v
ordered in-memory MVCC overlay
        |
        +--> exact point read at T
        +--> exact ordered scan at T
```

The controller is outside the timing loop. One child process builds a deterministic fixture, closes the writer, opens the selected immutable root, authenticates the tail, executes reads, records RSS and I/O, writes one receipt, and exits.

## What is measured

```text
view_open
  = manifest identity read
  + authority-bound SlateDB reader open
  + base frontier check
  + every txLog envelope decode
  + every quorum certificate verification
  + in-memory overlay construction

first_point
  = first exact read after view_open

warm_point_p99
  = p99 of subsequent exact reads on the same view

ordered_scan_throughput
  = exact base-plus-tail rows emitted / scan duration
```

Object requests and transferred bytes are recorded separately for open, first point, warm points, and scan. Peak resident memory is reported by the disposable child process.

## Frozen dimensions

The initial suite contains these slices:

| Slice | Base keys | Tail records | Question |
| --- | ---: | ---: | --- |
| base-small | 1,024 | 0 | fixed reader-open overhead |
| base-medium | 16,384 | 0 | base scaling |
| base-large | 65,536 | 0 | base scaling at about 16 MiB of values |
| tail-short | 16,384 | 64 | normal objectification lag |
| tail-long | 16,384 | 1,024 | overlay and certificate slope |
| combined | 65,536 | 64 | larger base with a live suffix |

Values are 256 bytes. Each point executes 64 sampled point reads and an ordered scan of up to 1,024 rows across three deterministic seeds. The initial three-seed run is diagnostic. A performance admission claim requires a frozen 21-process sample run on the declared machine profile.

## Required shape

The useful design does not require object storage to serve every foreground read. It requires the following shape:

1. Base-only view open must not scan all logical rows. Base size may affect manifest and index metadata, but the slope must remain sublinear in logical base bytes.
2. Tail authentication and overlay memory may grow linearly with retained tail records. Superlinear growth is a stop signal.
3. A tail hit should be served from the overlay without an object read.
4. A base hit may reach RAM, local NVMe, or object storage depending on cache state. The local curve reports request amplification but does not model cloud RTT.
5. Ordered scans stream one authority-bound base cursor and one ordered resident-tail iterator through a primary-key merge. Object reads must scale with emitted base rows, not with unrelated tail cardinality.

## Provisional targets and stop rules

These are engineering gates, not achieved results.

| Curve | `[PROPOSED]` useful target | Stop or redesign signal |
| --- | --- | --- |
| view open vs base size | less than 2x growth from 1K to 64K keys on local profile | proportional to all logical rows or bytes |
| tail auth/index | linear, less than 100 microseconds per record on local release build | superlinear slope or unbounded allocation |
| warm point p99 | less than 250 microseconds locally | grows materially with total base keys |
| first local point | less than 5 milliseconds with OS-warm object files | requires broad base scan |
| 1,024-row scan | more than 100K rows/s locally | degrades with unrelated tail keys |
| point request amplification | zero object requests for tail hits, bounded requests for base hits | requests grow with base size |
| worker RSS | fixed reader/cache overhead plus O(tail bytes) | proportional to complete base bytes |

The exact thresholds will be recalibrated after the first release-build run. The shape is the primary gate.

## Cache taxonomy

`[EXISTS]` The first suite profile is `process-cold-os-warm-local-filesystem`:

```text
fresh Range Engine process state
fresh SlateDB decoded state
same local object files created by fixture setup
operating-system page cache may contain those files
```

`[PROPOSED]` The next profiles are:

```text
RAM warm + NVMe warm + GCS
RAM cold + NVMe warm + GCS
RAM cold + NVMe cold + GCS
```

Only the last profile measures an object-store cold miss. Cache labels are part of the result identity and cannot be silently compared.

## Correctness dependency

This curve does not replace the process-composed correctness gate. It depends on `cell-range-serving-handoff-v1`, which already proves exact root resolution, signed quorum reconstruction, sparse commit-chain verification, authority failover, lease-protected old-root deletion, and fresh-worker reconstruction after reclamation.

The performance worker still checks:

* exact first point against an independent oracle;
* exact warm points against the oracle;
* exact ordered scan against the oracle;
* exact certified tail count;
* object I/O observation;
* RSS bound;
* deterministic semantic replay.

## Tradeoff

This design optimizes for an honest early answer about data-plane shape. It gives up a claim about network object-store latency and full production cache behavior. Moving immediately to GCS would mix reader design, cache configuration, network variance, credentials, and object-store behavior into one curve, making a bad slope harder to diagnose.

## Next gate

`[ACTIVE-WORK]` After the local curve is stable:

1. run release builds with 21 fresh processes per point;
2. fit base-size and tail-length slopes with confidence intervals;
3. `[EXISTS]` isolate a fresh decoded-RAM cache over the same bounded NVMe cache;
4. `[EXISTS]` reject stale-authority resurrection in the process handoff;
5. `[EXISTS]` fail closed on authority unavailability and reject stale fallback;
6. `[EXISTS]` promote overwrite and torn-write corruption to process receipts;
7. `[EXISTS]` prove bounded eviction across many Range Engines;
8. replay the same matrix in `objectKV-dev` on GCS;
9. add a remote uncached point-miss curve and failure injection.

## First clean release result

`[EXISTS]` Candidate `1ee9de4` ran all six points on the declared arm64 local
profile. Every run kept with zero correctness anomalies, exact semantic replay,
accounted object I/O, and bounded RSS.

| Point | Median view open | Median tail auth | Median first point | Median warm p99 | Median scan |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1K base, 0 tail | 0.60 ms | 0 ms | 125 us | 107 us | 210K rows/s |
| 16K base, 0 tail | 0.73 ms | 0 ms | 125 us | 103 us | 196K rows/s |
| 64K base, 0 tail | 0.72 ms | 0 ms | 132 us | 255 us | 182K rows/s |
| 16K base, 64 tail | 4.68 ms | 4.07 ms | 150 us | 104 us | 173K rows/s |
| 16K base, 1,024 tail | 62.82 ms | 62.06 ms | 258 us | 159 us | 91K rows/s |
| 64K base, 64 tail | 4.65 ms | 3.91 ms | 130 us | 94 us | 180K rows/s |

The base-open and tail-authentication shapes clear their provisional targets.
The scan target narrowly fails at the longest tail, and its request count grows
from 80 to 159. More importantly, every base point read causes one object
`get_range`. Local latency is not evidence that this is acceptable remotely.
The current reader must not be taken to GCS as a performance candidate until
it owns an explicit RAM/NVMe cache path.

## First shared-cache result

`[EXISTS]` Candidate `7071e33` adds a caller-owned decoded cache and bounded
local block cache below the manifest-bound visibility filter. It replays each
sample point once to populate the shared cache, then measures the same points
again. Zero backend requests on that repeated pass is a hard gate.

| Point | Raw | Shared RAM/NVMe | Tradeoff |
| --- | ---: | ---: | --- |
| 16K, view open | 0.51 ms | 2.15 ms | cache construction adds 1.64 ms |
| 16K, first point | 129 us | 353 us | first miss is 224 us slower |
| 16K, repeated point backend GETs | 64 | 0 | all repeated points avoid object storage |
| 16K, scan backend GETs | 80 | 1 | request amplification collapses |
| 16K, scan throughput | 196K rows/s | 248K rows/s | 1.26x local improvement |
| 16K + 64 tail, scan backend GETs | 85 | 1 | tail stays exact |
| 16K + 64 tail, scan throughput | 178K rows/s | 233K rows/s | 1.31x local improvement |

The result proves the combined path, not an NVMe-only hit. Candidate `79afb08`
adds the decoded-RAM-cold reopen point below. Candidate `63c9531` adds the first
corruption control. Resurrection and eviction remain separate controls.

## First streaming-merge result

`[EXISTS]` Candidate `20899e7`, suite hash `268beac9`, profile hash
`91f891a1`, and release executable SHA-256 `1ff9c635` replace the bounded
`limit + affected_tail_keys` map with an authority-bound base cursor and an
ordered merge against the already authenticated in-memory tail. The result
stops after the requested logical row count and never materializes an enlarged
base prefix.

| Point | Run | Median view open | Median first point | Median scan GETs | Median scan |
| --- | --- | ---: | ---: | ---: | ---: |
| 16K raw, 0 tail | `527a947c` | 0.54 ms | 111 us | 80 | 209K rows/s |
| 16K raw, 1,024 tail | `58a99734` | 61.59 ms | 208 us | 80 | 186K rows/s |
| 16K shared cache, 0 tail | `7241c792` | 2.29 ms | 341 us | 1 | 238K rows/s |
| 16K shared cache, 64 tail | `2d02b03a` | 6.13 ms | 347 us | 1 | 236K rows/s |

All four clean release workloads kept across three seeds with exact ordered
results. The long-tail raw scan previously issued 159 backend range GETs and
ran at 91K rows/s. It now issues the same 80 GETs as the no-tail raw scan and
runs at 186K rows/s. The suite hard-fails raw scans above 96 backend requests
and shared-cache scans above four.

This admits the merge shape and removes unrelated tail cardinality from base
request amplification. It does not remove the roughly 61 microseconds of
certificate verification and tail indexing per record during view open. The
tail iterator is resident in a `BTreeMap`, so worker memory still grows with
the unobjectified tail. The run is local, OS-warm, and not noise-qualified.

## First persistent-NVMe reopen result

`[EXISTS]` Candidate `79afb08`, suite hash `c31143ca`, profile hash
`91f891a1`, and release executable SHA-256 `03e1616a` populate the bounded
local block cache, close the authority-bound view, discard its decoded cache,
rebuild the cache object from the same local directory, and open a new view
with a fresh decoded cache.

| Point | Run | Open | First point | Warm p99 | Scan | Backend data after open |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 16K, 0 tail | `497b2745` | 0.53 ms | 101 us | 64 us | 262K rows/s | 0 bytes, 0 range GETs |
| 16K, 64 tail | `ccc85321` | 4.42 ms | 92 us | 79 us | 237K rows/s | 0 bytes, 0 range GETs |

Both three-seed clean release workloads keep exact point and ordered scan
results. Cache preparation issues a median 65 backend range GETs. After reopen,
the measured scan issues zero backend requests. The first point issues no
successful backend range GET and transfers zero backend bytes.

This is not an offline reopen. Opening the new view still performs two
successful manifest GETs, one list, two failed metadata GETs, and transfers 788
backend bytes at the median. The first point also performs one failed metadata
GET even though its data block comes from NVMe. The current boundary therefore
supports persistent local data acceleration while retaining an object-store
dependency for worker bootstrap and manifest verification. A separate
authority-bound metadata-cache design would be required to remove that
dependency.

## First persistent-cache corruption control

`[EXISTS]` Candidate `63c9531` prepares a real SlateDB base through the bounded
cache, closes the view, overwrites every persisted data part without changing
its length, and reopens with fresh decoded RAM. The gate permits only two safe
outcomes: reject the corrupted cache during open or read, or return the exact
value after observed backend range re-fetch. Any non-exact value fails.

The current path returns the exact value after backend repair. This proves that
corrupted local data does not silently become database state in the focused
fixture. The process-isolated overwrite and torn-write gate below supersedes
this focused evidence. Multi-range eviction remains required before the cache
hierarchy is admitted as a long-running service boundary.

## Process-isolated overwrite and torn-write gate

`[EXISTS]` Candidate `505c997`, suite hash `07a33107`, profile hash
`bb0068ee`, and release executable SHA-256 `a74e48ff` make cache-byte integrity
a dedicated process contract. Each seed executes two independent subjects:

```text
prepare worker -> build immutable base -> point + scan populate cache -> exit
  -> overwrite every persistent part at the same length, or truncate each part
  -> fsync mutated parts and directory
  -> fresh reopen worker -> rebuild persistent cache + decoded RAM
  -> read through the authority-bound SlateDB view
```

Correct run `83a36734` kept all 24 checks across three seeds and 12 worker
starts. It overwrote 15 parts and truncated 15 parts. Every reopen returned the
exact 256 KiB value after backend repair, with 36 successful backend range GETs
and 1,778,994 backend bytes in aggregate. No subject refused and no wrong value
was returned.

Four controls prove the gate is live. Skip-overwrite run `9dc32afc` and
skip-torn run `8863fe8e` each discarded with two anomalies per seed because
the required physical fault was absent. Accepted-wrong-overwrite run
`555c59e1` and accepted-wrong-torn run `dfebb7cd` each discarded with one
anomaly per seed. The accepted-wrong controls exercise the receipt oracle, not
an observed SlateDB wrong read.

Decision: persistent cache byte damage may change latency or refuse the read,
but it may not change database contents. Backend repair is admitted because
object storage and the authority-selected immutable closure remain canonical.
This optimizes for disposable NVMe and automatic recovery. It gives up serving
availability when both local bytes and the backing object are unavailable.
Bounded eviction and contention across many Range Engines are measured in the
next gate.

## First bounded multi-range eviction gate

`[EXISTS]` Candidate `5f7bf82`, suite hash `f240110e`, profile hash
`e7f6fc24`, and release executable SHA-256 `9b8556c7` force eight logical
range assignments through one shared persistent cache. The deterministic base
contains 64 incompressible 32 KiB values, eight per range. Its roughly 2 MiB
working set is larger than the 192 KiB cache cap. A fresh decoded cache then
rereads every range in reverse order.

Correct run `9375c874` kept all hard gates across seeds 1103, 2207, and 3301.
The cache settled at a maximum 131,292 bytes, below its declared cap. Reverse
rereads remained exact and issued 130 backend range GETs totaling 8,414,900
bytes, so the result observes real eviction and refill.

Three controls prove the boundary. Disable-bound run `77e7adea` retained
2,105,380 bytes, exceeded the cap, and made zero reread range GETs. Skip-reread
run `e92471bc` failed the exercise and refill gates. Accepted-wrong run
`3ad2d888` exercised eviction and refill but failed exactness.

Decision: one KV Runtime owns one physical cache budget across logical Range
Engines. Capacity pressure evicts disposable bytes and later refills from the
authority-selected immutable base. It does not create a private cache or
durable replica per range. This optimizes for bounded local media and dense
range assignment. It gives up guaranteed cache residency for any individual
range. Fairness, concurrent tenants, and remote refill latency are not admitted
by this sequential local gate.

`[ACTIVE-WORK]` Candidate `f496e8d` adds a GCS backend to this same worker and
a validated `gcs-dev` suite profile. Every process writes under a unique
scratch prefix, and a GCS result cannot pass unless the prefix is deleted. Clean
local regression `2e1ce017` kept under suite hash `2fb134c2`. The remote profile
has not run. Interactive gcloud credentials are expired, and the available
application-default identity lacks access to the candidate project. Project,
bucket, latency, request, byte, and cleanup behavior remain unverified.

## First historical-cache authority control

`[EXISTS]` Candidate `7eae670` adds a typed read-side validator to publication
state and two historical Range Engine open methods. Before reading cache or
object storage, the opener requires:

1. the exact lease token is still present in the supplied current authority state;
2. its deadline remains beyond the authority clock;
3. its snapshot version equals the requested target;
4. its manifest identity equals the outer published Range Engine root;
5. its closure includes that outer root and the inner immutable-base manifest.

The focused control proves that token drift, root drift, expiry, and release
are refused. Released-lease and wrong-root opens issue zero storage
requests, so persistent cache contents cannot override the supplied authority
decision.

The opener does not equate publication-authority generation with the generation
that produced the immutable base. Those histories can legitimately differ
after recovery.

## First process-composed stale-authority control

`[EXISTS]` Candidate `e06a159`, suite hash `f1bfd782`, profile hash `40046fbd`,
and release executable SHA-256 `0c81ed42` move the authority rule into
`cell-range-serving-handoff-v1`.

Each seed now executes:

```text
publish M0 and acquire its snapshot lease
  -> worker M0 opens exact base + certified tail and warms persistent cache
  -> compact and publish an independent M1 closure
  -> worker M1 opens exact base + shorter certified tail
  -> release M0 lease
  -> fourth worker reads live authority and refuses M0 before storage
  -> reclaim M0 outer root, inner manifest, and data object
  -> post-GC M1 worker remains exact
```

Correct run `2b1bdc6a` kept 60 checks across three seeds. Its three old-root
reopen attempts opened zero views, all 9 delete permits retired, all 9 M0-only
objects were absent, and every post-GC M1 worker matched the transaction
oracle. Negative run `93773b96` injected the pre-release authority snapshot
into the fourth worker. It reopened M0 in all three seeds and discarded with
one bounded anomaly per seed.

Decision: a persistent cache is a data acceleration tier, not a source of
read authority. Every historical worker bootstrap requires a current
replicated-authority decision. The control does not claim offline cache reopen;
the current cache still consults backing-store metadata. Bounded eviction is
admitted by the dedicated multi-range gate above.

## First authority-unavailable control

`[EXISTS]` Candidate `52ca95e`, suite hash `2beb3824`, profile hash `aa483bfe`,
and release executable SHA-256 `58a82868` add a fifth worker with a bounded
live-authority deadline. Correct run `805cc0cf` records three
`live_unavailable` receipts, validates no historical lease, and opens zero
views. Negative run `1c769733` falls back to the pre-release snapshot after the
failed live read, reopens M0 in all three seeds, and discards.

This admits the fail-closed policy, not the production timeout value. The 50 ms
deadline is local eval configuration. A later operating curve must select the
retry horizon and expose the corresponding client error contract.

## Immutable publication generations

`[EXISTS]` Candidate `e0f1b12` makes the process-local publication boundary
explicit. `AuthorityBoundRangeView` is immutable after it has opened the
authority-selected base and authenticated the complete txLog suffix. The KV
Runtime exposes it through `RangeServingState`:

```text
authenticate replacement outside the publication lock
  -> compare {authority root, target version, final txLog chain}
  -> atomically replace current Arc
  -> old Arc remains valid until its readers finish
```

The complete compare matters when the base does not change. A view at `T=5`
and a view at `T=8` can share the same SlateDB manifest at `K=2`. Comparing the
manifest alone permits a stale `T=5` publisher to replace `T=8`.
`RangeServingViewToken` closes that ABA case.

The focused regression coordinates 16 readers across a same-base `T=5` to
`T=8` replacement. All retained readers return the exact `T=5` rows, all later
readers return the exact `T=8` rows, and a stale same-manifest replacement is
refused. It admits the in-process correctness primitive only. Sustained
publication frequency, retained old-generation memory, process failure,
mixed-read latency, and OTel evidence remain open.

`[EXISTS]` Candidate `e3866b2` promotes the rule into a frozen child-process
lane. Correct run `0aa7c992` kept 18 successive same-base publications across
three seeds. Eight readers per seed retained each prior view before the swap,
then verified both generations. The result contains 144 exact old-view reads,
144 exact new-view reads, zero mixed results, and exact semantic replay.

The local compare-and-swap section measured 250 ns median and 625 ns p99. This
is not the tail-publication latency. Base open, certificate authentication, and
overlay construction happen before the timer and remain governed by the
earlier view-open curve. Accepted rollback, omitted overlap, accepted mixed
result, and omitted stale-probe controls all discard. Sustained load,
slow-reader retention, memory, process failure, remote object storage, and OTel
export remain open.
