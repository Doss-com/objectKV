# objectKV system shape and constraint map

Status: `[ACTIVE-WORK]` architecture thesis for expert review. The executable
surfaces are enumerated below; the complete distributed cell remains proposed.

## Target shape

```text
global fabric / metacluster
  -> tenant directory and routing epochs
       -> bounded cell
            -> tenant database transaction domains
                 -> ordered keyspace and logical ranges
                      -> disposable KV Runtime processes
                           -> Range Engine assignments
                                -> immutable transactional segments
                                     -> S3-compatible object API

inside each cell:
  read-version + commit proxies + resolvers + transaction logs
  versions + generations + range map + watermarks + GC roots

authoritative commit history
  -> transactional KV segments
  -> durable table-change tail
  -> Parquet / Vortex columnar bases
       -> DataFusion base-plus-tail overlay at exact version T
```

The FoundationDB inspiration belongs in the transaction and distribution model:
ordered keys, explicit read versions, optimistic conflict ranges, logical key
ranges, stateless transaction clients, and deterministic fault testing. It does
not imply FoundationDB API compatibility or that its local-disk storage-server
design transfers unchanged to object storage.

The cell boundary limits fleet size, recovery, and operations. It does not make
one key, range, segment, or tenant the unit of the storage architecture. The
normal tenant transaction may span arbitrary ranges inside its cell.

## Current module and caller map

The repository currently proves contracts below the complete-cell line. The
proposed role names are protocol boundaries, not claims that their services
exist.

| Module or caller | State | Owns now | Intended downstream caller |
|---|---|---|---|
| `okv-model` | `[EXISTS]` | cell-scoped versions, canonical batches, point/range MVCC, exact reads, retention, differential oracle, and ZebraDB base-plus-tail exactness oracle | transaction, storage, and analytical adapters use it as semantic reference, never as production authority |
| `okv-slate` | `[EXISTS]` spike | externally versioned point mutations and durable logical-version metadata over pinned SlateDB | cell substrate storage adapter after range-clear and historical-read seams exist |
| `okv-object` | `[EXISTS]` | named-object, conditional-publication, fault, request, byte, provider-conformance, publication-root and commit-visibility fixtures, plus the public accounted KV Runtime resource envelope | segment builders, manifest authority, materializers, txLog workers, KV Runtime adapters, and evals |
| `okv-postgres` | `[ACTIVE-WORK]` | ordered physical page and relation-extent keys, authenticated 8 KiB page values, separate objectKV-version and PostgreSQL-LSN frontiers, literal PostgreSQL 18.6 read/write/block-count/stable-sync callbacks, WAL-before-page admission, atomic page-plus-extent Cell commit, an external transaction-authority harness, stable roots from object base plus certified txLog tail, checkpoint-captured immutable certified delta objectification, root-pinned signed txLog pop, base-plus-delta-plus-tail page-service restart, replicated stable-root selection, the first five-seed release economics baseline, a clean 16 KiB through 512 MiB relation-size crossover, and a process-isolated local worker-readiness curve split from complete closure audit | compact binary delta encoding, provider-bound lazy-read integrity, GCS cache-state curves, delta compaction and garbage collection, publication I/O outside the state lock, authority restart, database-wide roots, lifecycle callbacks, concurrent generation publication, production AIO, remote empty-cache recovery, and regression compatibility |
| `okv-sim` | `[EXISTS]` contract models | exact seeded generation fencing, canonical replay, and the Cell v0 commit envelope, quorum, recovery, retry, resolver, tag, and generation contract | generation authority, WAL, resolvers, materializers, and recovery protocol |
| `okv-wal` | `[EXISTS]` local persistence primitives | local quorum frames plus a per-node vote, commit, append, truncate, and purge journal with compatibility fixtures | networked replicated log and deterministic disk fault seam |
| `okv-consensus` | `[EXISTS]` bootstrap contract | pinned OpenRaft adapter, per-node durable storage, real-process and Turmoil TCP transports, quorum replication, durable retry outcomes, an external three-node generation authority, generation-fenced commits, quiesced voter-set takeover, Ed25519 data-quorum recovery certificates, a replicated staged-to-visible transaction state, memory-only partitioned resolver batches with whole-generation replacement, and bounded global batch ordering across three commit proxies | root reconciliation, replica repair, proxy-failure gap recovery, online resolver-map movement, and bounded recovery-time and throughput curves |
| `okv-eval` | `[EXISTS]` | suite configuration, semantic runners, hard gates, receipts, and OTel signals | CI, contributor experiments, and the autonomous research loop |
| cell substrate | `[ACTIVE-WORK]` | versioned storage, object publication contracts, local quorum frames, and an admitted per-node consensus store | consensus replication and bounded recovery root |
| `GenerationAuthority` plus `DurableLog` | `[ACTIVE-WORK]` | replicated per-cell generation state, active/fencing/recovering phases, pinned transaction-system membership, a replicated data-log fence barrier, exact-position quorum certificates, one ordered replicated log, and quiesced membership handoff | read-version and commit proxies, recovery, ratekeeper |
| `ReadVersionService` plus `CommitProxy` plus `Resolver` | `[ACTIVE-WORK]` | centralized Cell v0 transactions, actual-read and range witnesses, two independent read-version proxy processes that enforce the session causal floor, three ordered memory-only resolver processes that recover through one transaction-system generation replacement, and three bounded commit proxies ordered by replicated predecessor tickets | proxy-failure gap recovery, online resolver-map movement, lag policy, metadata propagation, direct serving-worker reads, then Redis, search, PostgreSQL, and ZebraDB record adapters |
| `RangeMap` plus `KvRuntime` plus `DataDistributor` | `[ACTIVE-WORK]` | one public resource-envelope model accounts Range Engines, shared RAM/NVMe cache, objectification debt, movement pressure, and refusal; one SlateDB database per KV Runtime; exact-version point and range reads; authority-rooted M0/M1 bases plus signed txLog tails through disposable workers; atomic immutable view generations fenced by full base, target, and txLog-chain identity; a bounded process-level TCP router with tenant/range/epoch fencing, its first independent-process latency/failure gate, and a bounded client refresh that restarts multi-range fan-out at unchanged `T`; lease-pinned old-root reclamation; first base-size, tail-length, shared-cache, streaming-merge, decoded-RAM-cold NVMe-reopen curves; process-composed stale and unavailable authority refusal; process-isolated overwrite, torn-cache repair, and bounded multi-range eviction controls | replicated RangeMap publication, replacement routing, sustained multi-tenant reads, GCS cache-state curves, placement, movement, PostgreSQL transaction client, remote rebuild, and materializers |
| ZebraDB record and change layer | `[FUTURE]` | schema history, transactional projections, complete analytical change capture | Parquet/Vortex materializers and DataFusion snapshot provider |
| `SnapshotOverlayExec` | `[EXISTS]` bounded candidate | exact base plus tail at one target `T` with ordered streaming output and bounded operator buffering | `[FUTURE]` leased manifests, storage-level tail intervals, then DataFusion joins, aggregates, and certified analytical decisions |
| metacluster directory | `[FUTURE]` | tenant-to-cell placement and routing epochs | client bootstrap and tenant movement, not ordinary cell commit or recovery |

The present call path is `suite -> okv-eval -> model/object/sim runner -> OTel
and receipt`. The first complete-cell read path will be `tenant session -> read
version -> range map -> KV Runtime -> Range Engine`. Its write path will be
`tenant session -> commit proxy -> version authority -> all required resolvers
-> all required txLog sets -> KV Runtime tail application -> objectification`.

## PostgreSQL physical page-service slice

`[EXISTS]` The maintained PostgreSQL fork remains the logical database. Its WAL,
tuple MVCC, transaction status, and SQL visibility stay authoritative. objectKV
is the subordinate physical page and relation-extent store for one selected
main fork.

```text
PostgreSQL backend/checkpointer
  -> WAL flush through dirty page LSN
  -> smgr_nblocks selects current physical page-store generation
  -> smgr_writev sends native 8 KiB page and WAL frontier
       -> WAL-before-page gate
       -> page + unchanged relation extent in one Cell transaction
       -> exact committed Cell envelope
            -> signed txLog set 10, 3 local nodes, quorum 2
            -> signed txLog set 20, 3 local nodes, quorum 2
       -> authenticate base + complete certified suffix
       -> publish fresh in-process Range Engine view
  <- advancing objectKV physical version
  -> register SYNC_HANDLER_OKV relation tag
  -> PostgreSQL checkpointer processes deduplicated tag
       -> capture stable target B and object base frontier O <= B
       -> schedule complete relation objectification outside state mutex
       -> require PostgreSQL WAL through maximum page LSN at B
       -> persist content-addressed base O plus certified tail (O,B] manifest
       -> replicated authority prepare + publish + linearizable read-back
       -> authorize txLog pop only through O
  <- checkpoint sync may complete
```

```text
local durable root
  postgres-root.json       local relation and base identity
  range-base.json          exact manifest plus live-object closure
  objects/                 immutable SlateDB base objects
  txlog-10/node-{0,1,2}/   signed retained Cell envelopes
  txlog-20/node-{0,1,2}/   signed retained Cell envelopes

fresh page-service process
  -> ignore source heap when the durable descriptor exists
  -> verify every object in the exact physical closure
  -> require one unique quorum history in both txLog sets
  -> rebuild and authenticate every required-log certificate
  -> verify Cell identity, generation, ordering, and hash chain
  -> replay exact envelopes into the bounded Cell baseline
  -> read authority-selected stable root and verify it against recovered bytes
  -> open Range Engine at recovered target
  -> serve reads and accept the next page write
```

The first local proof reached base `O=5`, recovered `T=10`, committed after
recovery through `T=11`, and recovered again at `T=12`. A missing required-log
quorum and a missing live SST both refuse startup. This closes process-memory
loss only.

`[EXISTS]` The next proof published recoverable version 13 through a
three-process authority at term 3, index 4. PostgreSQL's checkpointer spent 160
ms in the stable-sync handler and did not complete before exact root read-back.
The page service then restarted from a nonexistent source path, reconciled the
same authority root, and served the still-running PostgreSQL process. Removing
the authority let hot txLog state reach version 14, but PostgreSQL refused the
checkpoint and stable version 13 did not move.

`[EXISTS]` Stable target `B` may now remain ahead of immutable object frontier
`O` while the complete certified suffix `(O, B]` stays retained. Candidate
`171b14c` recovered `B=10` from base `O=9` plus one record without a source
heap. Objectification completed independently of a 6.044-second authority
timeout, but still rewrites the full relation per captured checkpoint.

`[ACTIVE-WORK]` The authority harness must persist and recover, objectification
must become incremental, publication I/O must leave the state lock, the
transaction system must recover by generation rather than deterministic
baseline replay, and the same control must pass against empty remote caches.

## KV Runtime mechanics

```text
                         one KV Runtime process
                                  |
             +--------------------+--------------------+
             |                    |                    |
      process RAM cache     process NVMe cache   pressure controller
             |                    |                    |
             +----------+---------+                    |
                        |                              |
             +----------+----------+                   |
             |                     |                   |
       Range Engine A        Range Engine B          ... N
       metadata + live       metadata + live
       MVCC overlay          MVCC overlay
             |                     |
             +----------+----------+
                        |
          immutable object base + retained txLog tail
```

`[EXISTS]` Each Range Engine read generation is immutable. The worker fully
authenticates a replacement base plus txLog view before it acquires the short
process-local publication lock. Readers clone an `Arc` to the selected view and
release the lock before object, NVMe, RAM, or overlay I/O. Existing readers keep
their prior generation; later readers load the replacement.

```text
txLog commits (K,T] -> authenticate + build replacement off-path
                                         |
                                         v
current token == {full base root, T_old, final chain_old} ?
                  | yes                         | no
                  v                             v
          atomic Arc replacement         stale publisher refused
             /             \
 old reader -> old view     new reader -> new view
```

The manifest key alone is not a safe compare token. Several tail frontiers can
share one immutable base. `[EXISTS]` `RangeServingViewToken` therefore binds
the full authority root, target version, and final authenticated txLog-chain
digest. `[EXISTS]` The focused 16-reader regression and frozen three-process,
18-publication gate close the coordinated same-manifest ABA case. `[ACTIVE-WORK]`
A sustained mixed-load, slow-reader memory, failure, and OTel curve is still
required before calling concurrent service performance admitted.

`[EXISTS]` RFC-0056 now makes the resource semantics executable. The process
cache is shared. Fixed metadata and recent MVCC bytes grow per Range Engine.
The controller evicts disposable cache, requests objectification, requests
range movement, rate-limits, then refuses additional commits at a hard bound.

`[EXISTS]` RFC-0057 selects the physical relationship:

```text
one KV Runtime child process
  -> one shared filesystem cache
  -> either one SlateDB with logical range prefixes
     or N SlateDB databases with one shared decoded cache
     or N SlateDB databases with N private decoded caches
  -> explicit flush
  -> close all handles and decoded caches
  -> empty RAM + empty NVMe reopen
  -> one exact read per completed Range Engine
```

The accepted one-database layout held one database, one decoded cache, 9 live
tasks, and 9 object files through 1,000 logical assignments. Both
database-per-range layouts reached 8,001 tasks and 9,000 object files. The
receipt also records RSS, threads, descriptors, NVMe files, requests, bytes,
phase durations, and point-read latency. This decides embedded-engine
cardinality only. It does not yet implement routed concurrent service,
prefix-aware range movement, mixed load, or the production KV Runtime capacity
envelope.

`[ACTIVE-WORK]` The read path inside that one database is now:

Interactive system view: [exact-version read path](diagrams/04-exact-version-read.html).

Retention and compaction view:
[snapshot floor and history collection](diagrams/05-snapshot-retention.html).

```text
cell read version T
  -> route tenant key interval to one Range Engine assignment
  -> require KV Runtime applied frontier >= T
  -> seek escaped user key + complemented T
  -> first version <= T wins
  -> point tombstone returns absent

ordered scan [begin, end) at T
  -> scan one shared physical key interval
  -> group versions by decoded user key
  -> first version <= T wins once per key
  -> stop after visible-row limit
```

Range Engine IDs remain routing metadata. They are not embedded in durable user
keys, so split and movement do not change key identity. This gives movement a
logical interval-copy problem, then a tail catch-up problem. It does not give
SlateDB one independently movable manifest per range.

The first exact-read layout deliberately retained every tested version. That
was a measurement subject, not the final retention design. RFC-0059 now makes
the local collection boundary executable:

```text
active snapshot leases
  -> minimum readable version per tenant or cell
  -> safe version-GC floor
  -> compact versions older than the floor
  -> retain one floor-visible value or tombstone per key
  -> preserve newer versions and every pinned snapshot
```

`[EXISTS]` Candidate `3c9f008` validates the per-entry and physical rewrite
mechanism locally. Depth-256 history at retained windows 1, 16, and 64
converged to `1.225x`, `1.111x`, and `1.107x` retained logical bytes. Cold
floor-scan bytes scaled with the retained window, while point latency stayed
nearly flat. Five unsafe variants discarded.

The first diagnostic also hit SlateDB's eight-overlapping-SST per-key L0
backpressure bound because measurement compaction was disabled during ingest.
Production must compact continuously or rate-limit before that bound.

`[ACTIVE-WORK]` The remaining gap is not the per-entry, pure authority, or
basic replicated process rule. Candidate `5f62082` now replays lost acquire,
renew, and publish responses through leader replacement. The open gap is the
rest of the unsafe process matrix, a real disposable collection worker,
root-walk deletion, and remote object storage. Until those paths are
crash-tested, the KV Runtime remains a local bounded-history prototype rather
than a production long-running serving system.

`[EXISTS]` RFC-0060 places that state in the publication authority. A lease is
admitted and its exact manifest closure is pinned in one pure transition.
Prepared collection jobs freeze `F_job`, input root, authority generation, and
range epoch. Publication advances the manifest root and `G` once or not at all.
A deterministic logical clock owns expiry. These actions are carried by the
OpenRaft state machine and survive a checksummed snapshot restart. The correct
three-process gate is `[EXISTS]`; the remaining negative subjects and physical
worker composition are `[ACTIVE-WORK]`.

`[EXISTS]` RFC-0061 measures the composed serving view in a fresh release-build
worker process:

```text
current authority -> outer published Range Engine root
  -> closure-verified inner immutable-base manifest
  -> immutable SlateDB reader open
  -> verify every certified txLog record
  -> build ordered MVCC overlay
  -> first point, warm points, bounded ordered scan
  -> object requests, bytes, RSS, exactness receipt
```

The local `process-cold-os-warm` curve kept all six points. Base-only view open
was 0.60 ms at 1K keys, 0.73 ms at 16K, and 0.72 ms at 64K. The base path did
not scan logical rows. Tail authentication and indexing was 4.07 ms for 64
records and 62.06 ms for 1,024, about 61 microseconds per record.

The baseline rejects the idea that the raw reader is already production-fast.
Every measured base point read made one `get_range` call through the object
store. Local p99 remained near 0.10 to 0.26 ms only because the files and OS
cache were local. A cloud miss would pay remote latency. The original bounded
scan also increased its base limit by every tail key in the requested range.
At a 1,024-record tail, 1,024-row throughput fell from about 196K to 91K rows/s
and range GETs rose from 80 to 159.

The next Range Engine data path is therefore:

```text
point read
  -> recent MVCC overlay
  -> shared decoded RAM cache
  -> shared bounded NVMe block cache
  -> immutable object GET on miss

ordered scan
  -> ordered base iterator ----+
                               +-> streaming primary-key merge -> row limit
  -> ordered tail iterator ----+
```

This optimizes for fast steady-state reads and disposable compute. It gives up
the simplicity of a raw authority-bound object-store handle. Cache entries must
remain content or version addressed, cache loss must be safe, and the KV
Runtime pressure controller must own the shared RAM and NVMe budgets.

`[EXISTS]` Candidate `7071e33` implements the first combined cache path without
moving authority into the cache. `ManifestBoundStore` still filters visibility
to the exact authority-selected manifest. Beneath it, one caller-owned decoded
cache and one bounded local block cache can be shared by Range Engine views.

At 16K keys, repeating 64 exact points changed backend `get_range` count from
64 to zero. The 1,024-row scan changed from 80 backend requests and 196K rows/s
to one request and 248K rows/s. With a 64-record certified tail, the scan moved
from 85 requests and 178K rows/s to one request and 233K rows/s. Base plus tail
exactness did not change.

`[EXISTS]` Candidate `20899e7` now implements the ordered merge shown above.
The base side is an authority-bound MVCC cursor. The tail side walks the
already authenticated in-memory `BTreeMap`. The merge advances by primary key,
suppresses overwritten or deleted base rows, emits tail inserts, and stops at
the requested logical row count.

On the clean 16K-key release points, both the zero-tail and 1,024-record-tail
raw scans made 80 backend range GETs. The long-tail scan ran at 186K rows/s,
versus 91K rows/s and 159 GETs before the change. Shared-cache scans with zero
and 64 tail records each made one backend GET and ran at 238K and 236K rows/s.
This removes unrelated tail cardinality from base request amplification. It
does not remove the linear certificate-verification and overlay-build cost at
view open, or the resident-memory cost of the unobjectified tail.

The cache adds a visible miss cost. Base-only view open moved from 0.51 to 2.15
ms and the first point from 129 to 353 microseconds in the combined path.

`[EXISTS]` Candidate `79afb08` isolates persistent local data reuse. A first
view populates the block cache and closes. The worker discards decoded RAM,
reconstructs `CachedObjectStore` from the same directory, and opens a new view
with a fresh decoded cache. Zero-tail and 64-tail first points transfer zero
backend bytes and make zero successful backend range GETs. Their scans make
zero backend requests and run at 262K and 237K rows/s.

The worker is still not independent of object storage during bootstrap. The
new view reads 788 bytes of manifest metadata through two successful GETs and
one list, alongside two failed metadata GETs. Its first point performs one
additional failed metadata GET. The current hierarchy is therefore:

```text
worker bootstrap
  -> authority root
  -> backing-store manifest verification
  -> reconstruct persistent local block-cache index

foreground data after bootstrap
  -> recent MVCC overlay
  -> fresh decoded RAM
  -> persistent bounded NVMe
  -> object storage only on local data miss
```

`[EXISTS]` Candidate `63c9531` corrupts every persisted data part before a
fresh decoded-RAM reopen. The focused gate rejects any wrong value. The current
path detects the corrupt data and re-fetches exact range bytes from the backing
store. A safe future implementation may instead fail closed.

Removing the bootstrap dependency would require a separately verified
authority-bound metadata cache, not merely more block-cache capacity.

`[EXISTS]` Candidate `7eae670` narrows stale-root resurrection at the opener.
A historical open now requires the exact active `SnapshotLeaseToken` to match a
current `PublicationAuthorityState`, the outer published Range Engine root,
the target snapshot version, and a closure containing both the outer root and
inner immutable-base manifest before the cache or object store is touched.
Release, expiry, token drift, and root drift refuse with typed errors. The
focused released-lease and wrong-root controls observe zero storage requests.

The check intentionally does not compare publication-authority generation with
the generation that produced the immutable base. Those can differ after
recovery while the historical snapshot remains valid. The caller must still
obtain a fresh replicated authority snapshot.

`[EXISTS]` Candidate `e06a159` proves that freshness boundary through the
process-composed handoff. The sequence is:

```text
publish M0 -> acquire lease -> worker M0 warms persistent cache
  -> compact and publish independent M1 -> release M0 lease
  -> fourth worker reads live authority -> refuse M0 before storage
  -> reclaim outer M0 root + inner M0 manifest + M0 data object
  -> fresh M1 worker remains exact at T
```

The stale-authority negative substitutes the pre-release authority snapshot in
the fourth worker. It validates the old token and reopens M0 in all three
seeds, which makes the suite discard. This proves that persistent cache bytes
and an old lease token are not authorization. It does not make the cache an
offline database image. The current SlateDB cache still uses object-store
metadata during bootstrap.

The clean correct run `2b1bdc6a` kept 60 checks, 12 worker processes, 9 delete
permits, 9 reclaimed objects, and zero old-root reopens. Negative run
`93773b96` discarded with three old-root reopens.

`[EXISTS]` Candidate `52ca95e` adds a fifth worker whose live authority read is
bounded by an explicit deadline. Correct run `805cc0cf` kept 63 checks and
recorded three unavailable-authority refusals with zero storage opens. Unsafe
fallback run `1c769733` reused the pre-release snapshot, opened M0 in all three
seeds, and discarded. A cache miss and an authority outage therefore have
different semantics: a cache miss may reach object storage, but an authority
outage may not authorize any historical root. Torn cache writes, bounded
eviction across many Range Engines, and remote GCS behavior were the next
controls.

`[EXISTS]` Candidate `505c997` isolates physical cache-byte faults from the
authority handoff. Per seed, a prepare worker creates a real immutable base,
reads through the bounded persistent cache, and exits. The controller then
mutates every `_part` file, preserving length with overwrite in one subject and
truncating to half length in the other. Fresh reopen workers reconstruct the
cache index and decoded RAM before reading the authority-bound value.

```text
prepare process -> exact base read -> populate bounded NVMe -> exit
  -> controller overwrites or truncates every cache part -> fsync
  -> fresh reopen process -> authority-bound view
       -> checksum/cache validation
       -> exact backend range repair or typed refusal
       -> never a wrong database value
```

Clean run `83a36734` kept 24 checks across 12 workers and three seeds. It
overwrote 15 parts, truncated 15 parts, repaired all values exactly, and used
36 backend range GETs totaling 1,778,994 bytes. The skip-overwrite and
skip-torn controls discarded because the physical fault was absent. Two
explicit unsafe receipt controls discarded when a non-exact result was marked
accepted. This proves the oracle detects both unexercised faults and unsafe
outcomes. It does not prove eviction fairness or exact reads while many Range
Engines contend for one bounded cache. That is `[ACTIVE-WORK]`.

`[EXISTS]` Candidate `5f7bf82` closes that focused contention gap. One worker
builds a 2 MiB immutable base containing eight disjoint logical ranges. All
range scans share one decoded-cache policy, one persistent-cache directory,
and one authority-selected object base. The 192 KiB NVMe cap is below the
working set. After the first pass, the worker closes the view, creates fresh
decoded RAM, and rereads all ranges in reverse order.

```text
8 logical Range Engines -> 1 authority root -> 1 decoded-cache policy
                                          -> 1 bounded NVMe directory
                                                | eviction
                                                v
                                      immutable object range refill
```

Clean run `9375c874` kept exact reads across three seeds. The persistent cache
settled at a maximum 131,292 bytes, while reverse rereads issued 130 backend
range GETs and transferred 8,414,900 bytes. The unbounded control retained
2,105,380 bytes and issued zero reread range GETs, so the accepted path is not
mistaking a fully resident cache for eviction. Skip-reread and accepted-wrong
controls also discarded. This proves bounded capacity and exact refill for one
sequential local fixture. It does not prove fairness under concurrent tenants,
remote miss latency, or GCS request economics.

## Decisions to review

### D1. Serving models are consumers, not kernel modes

Redis, inverted search, PostgreSQL, and DataFusion each own a semantic adapter
and compatibility suite. They do not add protocol-specific operations to the
kernel.

Optimizes for: one testable ordered transaction contract.

Gives up: shortcuts such as kernel-native Redis expiry, PostgreSQL pages, or
posting-list mutation until evidence justifies a general primitive.

### D2. Transactional and analytical formats share history, not one trait

Every transactional segment declares a format ID, format version, logical key
and version bounds, checksums, and required reader capabilities. It must encode
kernel-owned visibility, tombstones, range deletes, merge operands, transaction
atomicity, and ordering. Analytical Parquet or Vortex artifacts are separate,
schema-aware materializations labeled with their covered-through version.

Optimizes for: workload-specific layouts without multiple authoritative
histories or one interface that hides incompatible responsibilities.

Gives up: treating Parquet and Vortex as OLTP segment plugins. Their pruning,
projection, and scan economics belong above the byte-opaque kernel.

### D3. Object storage is data publication, not commit coordination

The commit path requires an external ordered, fenced, quorum-durable log. Object
storage receives immutable segments and conditionally published metadata after
commit. `LIST` is never authoritative.

Optimizes for: explicit acknowledgement and recovery semantics.

Gives up: a storage-only deployment for low-latency strictly serializable
writes.

### D4. PostgreSQL is the compatibility-critical consumer

The first literal PostgreSQL path uses upstream PostgreSQL compute and its own
compatibility oracle. Redis and search can exercise the kernel earlier, but they
cannot substitute for PostgreSQL durability, MVCC, recovery, and extension
requirements.

Optimizes for: keeping the north star honest.

Gives up: describing a PostgreSQL-wire frontend as full PostgreSQL.

## Semantic constraints that must be explicit

| Surface | Initial proposed constraint | Why it is load-bearing |
|---|---|---|
| Ordering | one monotonically ordered commit-version domain before partitioning | snapshots and replay need one unambiguous history |
| Fleet topology | independent bounded cells; no cross-cell transaction | one global recovery and transaction system has unbounded blast radius |
| Transaction domain | one tenant database, spanning arbitrary in-cell ranges | physical shards must not leak into application atomicity |
| Isolation | strict serializability with explicit read/write conflict ranges | PostgreSQL and general transactions cannot inherit vague snapshot semantics |
| Durability | acknowledgement means quorum WAL durability; object durability has a separate watermark | object publication is too slow for the target commit path |
| Lag control | retained WAL has a hard bound; `C_cell - O_cell` drives throttle then refusal | an object-store brownout cannot create unbounded recovery debt |
| Commit unknown | clients receive an explicit indeterminate result and must use idempotency identity | lost replies cannot become duplicate commits |
| Reads | callers choose latest or explicit version; unavailable history fails visibly | silent fallback produces impossible snapshots |
| Range ownership | generation-fenced leases; stale owners cannot publish or acknowledge | object visibility alone cannot prevent split brain |
| Publication | immutable objects plus conditional manifest transition; no overwrite-in-place | retry and recovery need stable content identity |
| Object API | GET, range GET, PUT, conditional create/update, DELETE, and checksums; `LIST` only for audit | S3-compatible providers differ around errors, throttling, and conditional behavior |
| Recovery | reconstruct from object-durable version plus retained WAL suffix | every acknowledged commit needs one complete recovery path |
| Bootstrap | one external or explicitly bootstrapped consensus authority owns epochs, range maps, and watermarks | control metadata cannot depend circularly on the transaction system it starts |
| GC | mark checkpoint, clone, backup, analytical-lease, and tenant-move roots from one durable epoch; revalidate before named deletion | premature reclamation is unrecoverable data loss |
| Transactions | bounded bytes, conflict ranges, duration, and retained versions | unbounded transactions pin log, metadata, and history |
| Tenancy | transaction domain plus quotas and encryption/identity boundary | shared transaction roles, compaction, and cache can violate isolation operationally |
| Analytics | exact base plus durable table tail at one `T`; snapshot lease, not open OLTP transaction | a lagging base must change cost rather than silently weaken freshness |
| Compatibility | each serving model publishes supported and unsupported semantics | protocol acceptance is not semantic compatibility |

## Expected bottlenecks

| Bottleneck | First symptom | Design response | Required evidence |
|---|---|---|---|
| Object request latency | cold point p99 dominates | index/cache tiers, range coalescing, row format | p50/p95/p99 plus GETs and bytes per operation |
| Object request cost | cheap compute, expensive API bill | larger immutable runs, caching, prefetch | cost curve by dataset, hit rate, and workload |
| Write/compaction amplification | bandwidth and background debt grow faster than ingest | compaction policy and format-specific champions | logical versus object bytes over long overwrite runs |
| Metadata authority | manifest contention or unbounded open cost | sharded fenced metadata after a single-domain baseline | publication contention and empty-cache reopen curves |
| WAL retention | objectification stalls fill the log | backpressure, watermark alarms, repairable publishers | faulted lag, retained bytes, recovery RPO/RTO |
| Generation recovery | overlapping role failures reuse versions or expose stale authority | exact seeded simulation before WAL, epoch encoding, publication fencing | exact failing-seed replay and zero acknowledged loss |
| Hot ordered ranges | one range owner or resolver saturates | split ranges, partition conflict domains only after proof | hotspot throughput and serializability histories |
| Range movement | durable-byte copy causes long rebalancing | reference existing objects, generation cutover | bytes copied and unavailable time during split/move |
| Cache churn | restart or tenant competition collapses latency | bounded metadata boot, admission, tiered cache | time to first correct read and steady-state recovery |
| PostgreSQL double work | two WAL/MVCC systems amplify latency and recovery complexity | establish one authority and map LSN to commit versions | crash matrix and WAL/page byte amplification |
| HTAP overlay | columnar data lags, misses invalidations, or double-counts deltas | explicit covered-through version plus durable table tail | result equality, tail size, and overlay cost at fixed write rate |
| Cell ceiling | proxy, resolver, log, or recovery role saturates | partition roles behind stable protocols, then add cells | throughput and recovery curves by cell size and tenant count |
| Format abstraction leak | point reads regress on analytical layout or scans regress on row layout | capability negotiation and workload-specific materialization | same logical history across format conformance suite |

## Serving-model risks

### Distributed Redis

Start with a declared subset: binary-safe strings, atomic single-key commands,
multi-key transactions only within explicit kernel limits, and version-backed
expiry semantics. Cluster redirection, Lua, streams, pub/sub, blocking commands,
eviction, and wall-clock expiry each require separate decisions. A RESP endpoint
that parses commands is not yet Redis-compatible.

### Distributed inverted search

Posting updates should be immutable segments plus versioned delete state, not
large in-place lists. Query snapshots must bind term dictionaries, postings,
deletes, and document values to one visible version. Merge debt, skewed terms,
top-k fanout, and read-after-write freshness are the first expected ceilings.

### Distributed PostgreSQL

The authority split among PostgreSQL WAL, page state, PostgreSQL MVCC, and okv
transactions must be resolved before implementation. The page bridge is the
fastest compatibility probe, not necessarily the final mapping. Vacuum, full
page writes, checkpoints, replication, PITR, extensions, temporary relations,
and catalog bootstrap all need explicit supported-state tables.

### DataFusion and ZebraDB HTAP

DataFusion reads immutable columnar bases labeled with a covered-through okv
version `W_p` and overlays the durable table-change tail `(W_p, T]` for one exact
query version `T`. A lagging base increases overlay work while the durable
analytical tail remains complete through `T`. If that coverage or the complete
leased object closure is unavailable, the query waits within policy or returns
`snapshot_unavailable`; it never rebases silently. The tail must retain base-row
invalidation keys even when the new row fails a pushed predicate. Parquet is the
first control format. Vortex is an experiment after logical equivalence,
pruning, recovery, and mixed-version fixtures pass.

Queries use snapshot leases that pin immutable bases and tails. Invariant-critical
aggregates remain transactional projections. Other analytical decisions must
validate dependency tokens in a short transaction before writing.

## Contributor-first proof order

1. Freeze the semantic questions as RFCs and invite adversarial review.
2. Publish the exact SlateDB seam inventory and adapter baseline.
3. Make object-store conformance, deterministic histories, and economics suites
   easy to run without a distributed cluster.
4. Build exact deterministic simulation before any replicated WAL component.
5. Add narrow Redis-string and immutable-posting consumers as kernel pressure
   tests, without compatibility claims.
6. Prove fenced WAL durability, recovery, and metadata-only range movement.
7. Begin the PostgreSQL bridge against a stable version and durability contract.
8. Materialize version-aligned Parquet and compare Vortex only after correctness.

This order optimizes for expert contributions that can falsify an invariant or
measure a bottleneck in days. It gives up an early all-in-one demo whose failure
would be hard to localize.
