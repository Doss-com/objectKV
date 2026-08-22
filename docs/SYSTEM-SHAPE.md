# objectKV system shape and constraint map

Status: `[PROPOSED]` architecture thesis for expert review. Only the in-memory
model, pinned SlateDB adapter spike, eval runner, and OTel path currently exist.

## Target shape

```text
global fabric / metacluster
  -> tenant directory and routing epochs
       -> bounded cell
            -> tenant database transaction domains
                 -> ordered keyspace and logical ranges
                      -> disposable serving/materialization workers
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
| `okv-model` | `[EXISTS]` | cell-scoped versions, canonical batches, point/range MVCC, exact reads, retention, differential oracle | transaction and storage adapters use it as semantic reference, never as production authority |
| `okv-slate` | `[EXISTS]` spike | externally versioned point mutations and durable logical-version metadata over pinned SlateDB | cell substrate storage adapter after range-clear and historical-read seams exist |
| `okv-object` | `[EXISTS]` | named-object, conditional-publication, fault, request, byte, and provider-conformance boundary | segment builders, manifest authority, materializers, and evals |
| `okv-sim` | `[EXISTS]` probe | exact seeded generation fencing and canonical replay | generation authority, WAL, resolvers, materializers, and recovery protocol |
| `okv-eval` | `[EXISTS]` | suite configuration, semantic runners, hard gates, receipts, and OTel signals | CI, contributor experiments, and the autonomous research loop |
| cell substrate | `[ACTIVE-WORK]` | versioned storage and object publication contracts | first replicated WAL and bounded recovery root |
| `GenerationAuthority` plus `DurableLog` | `[PROPOSED]` | per-cell generation, membership, ordered quorum durability, and recovery root | read-version and commit proxies, recovery, ratekeeper |
| `ReadVersionService` plus `CommitProxy` plus `Resolver` | `[PROPOSED]` | strict-serializable tenant transactions across ranges | Redis, search, PostgreSQL, and ZebraDB record adapters |
| `RangeMap` plus `ServingWorker` plus `DataDistributor` | `[PROPOSED]` | direct exact reads, range placement, split, movement, and empty-cache service | transaction clients and materializers |
| ZebraDB record and change layer | `[FUTURE]` | schema history, transactional projections, complete analytical change capture | Parquet/Vortex materializers and DataFusion snapshot provider |
| `SnapshotOverlayExec` | `[FUTURE]` | exact base plus tail at one leased `T` | DataFusion joins, aggregates, and certified analytical decisions |
| metacluster directory | `[FUTURE]` | tenant-to-cell placement and routing epochs | client bootstrap and tenant movement, not ordinary cell commit or recovery |

The present call path is `suite -> okv-eval -> model/object/sim runner -> OTel
and receipt`. The first complete-cell path will be `tenant session -> read
version or range map -> serving worker`, and `tenant session -> commit proxy ->
version authority -> all required resolvers -> all required log sets -> storage
materializers`.

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
| GC | delete only below global read, clone, backup, range, and object-durable watermarks | premature reclamation is unrecoverable data loss |
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
