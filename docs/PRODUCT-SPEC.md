# objectKV product specification

objectKV should become an open, object-native transactional ordered KV kernel:
Raft and a selected SSD or RAM serving profile keep the regional hot path fast,
immutable object state makes storage portable and independently scalable, and
PostgreSQL, search, Redis, and DataFusion remain consumers above one versioned
history. The product is worth building only if it keeps hot-path performance
near a TiKV/RocksDB control while making recovery, branching, cold capacity,
and analytical reuse materially better.

## Abstract

`[VERIFIED]` objectKV has executable contracts for versioned MVCC semantics,
quorum-durable commit envelopes, OpenRaft failover, immutable object
publication, recovery of lost responses, and an exact DataFusion base-plus-tail
operator. It does not yet have a complete transactional cell, production range
serving, PostgreSQL integration, or measured performance curves for the target
system.

`[PROPOSED]` The target product is a bounded regional database cell with an
ordered transactional keyspace. Each range selects either bounded RocksDB on
disposable NVMe or a bounded in-memory image behind one `ServingImage`
contract. Serving profile and durability profile are independent. A replicated
durable txLog is the default commit path. Immutable objects become the
permanent row base, and schema-aware Parquet or Vortex artifacts become derived
analytical bases.

`[PROPOSED]` The first target deployment uses Raft. The more consequential
decision is whether the cell keeps one ordered FDB-like transaction log or
moves to many range-local Raft groups plus a cross-range transaction protocol.
This specification recommends the latter as the target, while retaining the
current single-log Cell v0 as the correctness control.

## Context / Motivation

The ecosystem has capable local storage engines, distributed transactional KV
systems, object-native analytical systems, and PostgreSQL storage
disaggregation. It does not have one broadly adopted open kernel that combines:

- a fast ordered transactional KV contract;
- object storage as the permanent, open-format recovery substrate;
- disposable and independently scalable serving compute;
- exact row and columnar projections from one commit history;
- metadata-cheap branches and snapshots;
- a credible path to a PostgreSQL-compatible database above the kernel.

TiDB X demonstrates that the physical architecture is possible: object storage
is the source of truth, local row and column caches accelerate serving, writes
enter shard logs before asynchronous upload, and remote compaction produces
immutable state. It also means the architecture alone is not objectKV's
differentiation. The differentiator must be an open, self-hostable,
engine-neutral ordered KV kernel with public object formats and first-class
PostgreSQL and DataFusion paths. See the
[TiDB X architecture](https://docs.pingcap.com/tidbcloud/tidb-x-architecture/).

## Problem Framing

The product must solve five problems together.

1. A hot point read or commit cannot normally wait for object storage.
2. Local NVMe cannot be the only permanent copy or grow with all database bytes
   on every serving node.
3. Multi-range transactions must remain atomic and expose a declared isolation
   contract.
4. Row-serving and columnar-serving layouts must represent one exact logical
   version without turning an external ETL pipeline into a second truth.
5. PostgreSQL compatibility must not leak PostgreSQL-specific semantics into
   the ordered KV kernel.

The initial non-goals are equally important.

- `[PROPOSED]` No synchronous object PUT is required for the normal regional
  commit path.
- `[PROPOSED]` No cross-cell transaction is provided.
- `[PROPOSED]` No claim is made that a sparse object cache has local-RocksDB
  p99 latency.
- `[PROPOSED]` No SQL parser, planner, trigger runtime, or PostgreSQL catalog
  behavior belongs in the kernel.
- `[PROPOSED]` No claim of full PostgreSQL compatibility is made from wire
  protocol acceptance or a small regression subset.

## Prior Work

### FoundationDB

FoundationDB supplies the most useful semantic model: ordered keys, explicit
read versions, optimistic conflict ranges, transaction proxies, partitioned
resolvers, replicated transaction logs, and bounded transactions. Its
transaction system is the reference for strict-serializable multi-range
semantics. objectKV changes the permanent storage and serving ownership model,
not the need for an explicit transaction protocol.

### TiKV and TiDB

TiKV is the strongest implementation control for a range-partitioned design.
Each region is a Raft group, RocksDB stores the local state, and Percolator-style
two-phase commit provides distributed transactions. TiKV documents snapshot
isolation, not PostgreSQL Serializable Snapshot Isolation, so it is a storage
and transaction reference rather than proof of full PostgreSQL semantics. See
the [TiKV storage architecture](https://tikv.org/docs/dev/reference/architecture/storage/)
and [Percolator transaction model](https://tikv.org/deep-dive/distributed-transaction/percolator/).

### TiDB X

TiDB X is the closest product-architecture comparison. It combines per-shard
Raft logs, local row and column caches, asynchronous log and SST upload, remote
compaction, and object storage as source of truth. objectKV must benchmark
against this shape and avoid presenting it as novel.

### pgRust

pgRust is a serious PostgreSQL compute reference, but not an object-native row
store. Its stated goal is PostgreSQL 18.3 compatibility down to the same on-disk
format, and its source preserves PostgreSQL heap, index, WAL, buffer-manager,
and storage-manager boundaries. Its current project also reports a vectorized
push executor, direct-code JIT, thread-based concurrency, query scheduling,
pipelined fsync, and a columnar format. See the
[pgRust repository](https://github.com/malisper/pgrust),
[performance goal](https://github.com/malisper/pgrust/blob/main/GOAL.md), and
[benchmark kit](https://github.com/malisper/pgrust/tree/main/benchmarks).

The lessons to adopt are its differential PostgreSQL oracle, named unsupported
frontier, exact-hardware benchmark receipts, per-hot-unit attribution, batched
execution, operator fusion, and explicit durability settings. Its AGPL-3.0 code
must not be copied into the proposed Apache-2.0 objectKV kernel. Integration as
a separately licensed consumer or clean-room implementation of general ideas
remains possible.

### Current objectKV evidence

`[VERIFIED]` The repository already proves several narrow seams:

- deterministic generation-aware MVCC and conflict contracts;
- quorum-synchronized local commit envelopes and exact replay;
- a three-node OpenRaft failover and stale-suffix replacement contract;
- immutable object publication with replicated intent and lost-response
  recovery;
- exact DataFusion overlay semantics across independently lagging base
  watermarks;
- configurable eval suites, receipts, hard gates, and OpenTelemetry signals.

These are contract proofs, not a complete database or performance result.

## Definitions

| Term | Meaning |
|---|---|
| Cell | One bounded regional transaction, durability, storage, recovery, and control cluster. Cells have independent version spaces and no cross-cell transaction. |
| Tenant database | The normal transaction domain. A bounded transaction may span its keys and ranges inside one cell. |
| Range | A contiguous ordered-key interval used for routing, placement, splitting, and serving. |
| Range group | `[PROPOSED]` The Raft replication group responsible for durable ordered mutations to one range. |
| txLog | A replicated transaction log. Use `txLog`, not `tLog`, in prose, CLI output, metrics, and code names. |
| Object row base | An immutable, indexed, row-oriented representation that reconstructs a range through a declared version. |
| Recovery suffix | Quorum-durable commit records newer than the object-durable version that are required to reconstruct acknowledged state. |
| Row overlay | Queryable recent MVCC entries newer than a selected object row base. It remains in DRAM in either serving profile and must retain tombstones. |
| Serving image | The common disposable, bounded, range-local contract implemented by `ssd_resident` or `ram_resident`. It is never permanent authority. |
| Serving profile | The hot-state implementation selected per range: RocksDB on bounded disposable NVMe (`ssd_resident`) or a bounded in-memory index (`ram_resident`). |
| Resident range | A range or admitted working set with complete current coverage in its selected serving image and eligibility for that profile's hot latency service class. |
| Elastic range | A range served from a row overlay plus bounded object-block cache, with explicit cold-miss latency. |
| Analytical tail | Durable table-level changes after a columnar base watermark, retained independently of the recovery suffix when required. |
| Objectification | Publishing immutable row objects and advancing a fenced manifest so the covered recovery suffix can be reclaimed. |

## What We Are Building

### Product boundary

```text
PostgreSQL / Redis / Search / custom applications
                      |
          ordered transactional KV API
                      |
              cell transaction layer
          start version, validation, commit
                      |
        +-------------+-------------+
        |                           |
  range group A                 range group B
  Raft + RocksDB                Raft + RocksDB
  serving image                 serving image
        |                           |
        +-------------+-------------+
                      |
         asynchronous objectification
                      |
       immutable open object row bases
                      |
        +-------------+-------------+
        |                           |
  row serving/recovery      table change projection
                                    |
                         Parquet / Vortex bases
                                    |
                         DataFusion base + tail
```

`[PROPOSED]` objectKV owns ordered binary keys, point and range reads, exact
snapshot versions, bounded transactions, conflict declarations, atomic
mutations, durable commit identity, range routing, recovery, and public object
state. Consumers own schemas, SQL behavior, Redis behavior, search structures,
and analytical planning.

The kernel is value-native, not page-native or row-native. The first
PostgreSQL adapter is page-native so PostgreSQL can retain its existing heap,
index, MVCC, catalog, constraint, trigger, and recovery semantics while only
the durable storage boundary changes. A future row-native adapter would be a
different PostgreSQL compute architecture, not a required kernel rewrite.

### Consensus decision

`[PROPOSED]` Use Raft for the first regional range groups and control quorums.
Raft and stable-leader Multi-Paxos have the same basic steady-state shape: the
leader sends an entry to replicas and waits for a quorum. Raft's advantage here
is a complete, explicit log-replication, election, and membership protocol plus
an existing OpenRaft prototype in this repository. The original Raft paper
describes it as equivalent in result and efficiency to Multi-Paxos. See
[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf).

Paxos remains a valid alternative, not an inferior algorithm. Flexible Paxos
can trade larger election quorums for smaller steady-state replication quorums,
and EPaxos can avoid a fixed leader for non-conflicting commands. Those are
useful if geo-distributed leader latency or quorum geometry becomes the primary
problem. They also enlarge the correctness and operational surface before
objectKV has proven its storage thesis. See
[Flexible Paxos](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.OPODIS.2016.25)
and [EPaxos](https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf).

The consensus library is not the storage format. objectKV must continue to own
the txLog entry encoding, request identity, generation fencing, snapshot
contract, and recovery fixtures behind a `ReplicatedLog` boundary.

### Commit pipeline

The recommended target separates range replication from transaction atomicity.

```text
client transaction
  -> acquire start/read version S
  -> read keys and declare read/write conflict ranges
  -> route mutations to affected range groups
  -> each range validates and quorum-persists prewrite/intent
  -> cell transaction coordinator chooses commit version C
  -> durable transaction decision makes every participant resolvable
  -> range groups expose committed versions through C
  -> object publishers asynchronously advance row-base watermarks
```

For one-range transactions, one range-group leader can validate and commit
without distributed two-phase commit. For multi-range transactions, the cell
needs a crash-resolvable transaction record, participant validation, idempotent
retry, and a rule for choosing `C`. A TiKV-style primary-key transaction record
is one implementation option. A FoundationDB-style global resolver and ordered
commit service is the correctness control.

The unresolved issue is serializability, not atomicity alone. Snapshot isolation
plus write-write conflict detection permits write skew. Full PostgreSQL
Serializable requires predicate or range dependency tracking and anomaly
detection. PostgreSQL documents that its Serializable mode adds predicate-lock
dependency monitoring above snapshot isolation. See the
[PostgreSQL transaction isolation documentation](https://www.postgresql.org/docs/current/transaction-iso.html).

### Point reads

A point read at exact snapshot `T` follows this path:

```text
logical key + T
  -> tenant and range router
  -> transaction-local write overlay
  -> serving worker
       1. recent DRAM MVCC overlay
       2. selected ServingImage at newest visible version <= T
            ssd_resident -> RocksDB block cache and bounded NVMe image
            ram_resident -> admitted DRAM record, block, or complete range
       3. manifest-selected object row base
            -> bloom and sparse block index
            -> object range GET on miss
       4. apply newer row-overlay value or tombstone
  -> exact value, not-found, version-too-old, or unavailable
```

A serving-image miss is not automatically logical absence. The worker must know
the selected row-base watermark and whether a newer overlay tombstone exists.
The object format therefore needs key bounds, a sparse block index, bloom
filters, per-block checksums, version coverage, and unambiguous tombstone
semantics. Coverage metadata can make a miss in a complete resident image
authoritative; the object fallback is required only when the local image does
not completely cover the requested snapshot.

`[PROPOSED]` The service exposes two honest latency classes:

- Resident: the requested records or assigned range have complete current
  coverage in the selected serving profile. Point reads issue no object request
  after admission.
- Elastic: only the overlay and admitted object blocks are local. Cold misses
  pay object request, transfer, decode, and cache-fill latency.

The SSD profile declares independent DRAM and NVMe bounds. The RAM profile
counts its index, values, overlays, allocator overhead, and hydration debt
against one hard memory bound and prohibits swap. The data distributor admits
records, blocks, or complete resident ranges until worker high-watermarks,
moves or splits hot ranges, and demotes colder ranges to the elastic class.
Object storage retains the complete authoritative base, so demotion removes
disposable bytes rather than data.

### PostgreSQL implementation shapes

There are two credible PostgreSQL paths, and they produce different physical
systems.

| Path | Point-read key | PostgreSQL feature ownership | Benefit | Cost |
|---|---|---|---|---|
| Page or storage-manager bridge | database, relation, fork, block number | PostgreSQL or pgRust keeps heap, indexes, WAL, MVCC, catalogs, constraints, triggers, and views | Fastest compatibility test; pgRust's format-preserving architecture fits this seam | Double-WAL and double-MVCC risk; DataFusion cannot consume logical rows without decoding pages and visibility |
| Row-native PostgreSQL compute | table and encoded primary key, plus explicit index keys | PostgreSQL-compatible compute owns SQL semantics while objectKV owns logical row and index transactions | Cleanest HTAP path and natural ordered-KV sharding | Rebuilds a large PostgreSQL semantic surface; extension and physical compatibility are not inherited |

`[EVALUATING]` No PostgreSQL path exists. The accepted research order remains
an upstream PostgreSQL page bridge first. `[PROPOSED]` pgRust should become a
second compute candidate and compatibility reference, not a source-code donor.
Its same-disk-format goal makes it closer to the page bridge than to a
row-native objectKV adapter.

If a full row-native PostgreSQL engine is built, the feature map is:

| PostgreSQL feature | Proposed objectKV representation | Hard edge |
|---|---|---|
| Heap row | `/tenant/db/table/<rel>/<encoded-pk>` to versioned tuple | PostgreSQL tables need an internal row identity when no primary key exists |
| B-tree index | `/tenant/db/index/<index>/<encoded-key>/<row-id>` | Collation, operator classes, NULL ordering, included columns, and MVCC visibility must match PostgreSQL |
| Primary and unique constraints | Unique index entries written atomically with the row | Concurrent inserts, deferred checks, and `NULLS NOT DISTINCT` require exact conflict rules |
| Foreign keys | Referenced-index read plus conflict or lock protecting concurrent delete/update | Deferred constraints and cascading actions can span many ranges |
| CHECK and NOT NULL | Evaluated in the SQL executor before commit | Functions used by constraints must preserve PostgreSQL volatility and NULL semantics |
| Exclusion constraints | Range or predicate conflicts over an index-defined operator relation | Point-key OCC is insufficient |
| Triggers | BEFORE, AFTER, and INSTEAD OF execution in the PostgreSQL compute transaction | Ordering, recursion, statement transition tables, and external side effects; side effects need an outbox |
| Views | Versioned catalog query trees expanded by the rewriter and planner | Permissions and dependency invalidation are part of semantics |
| Materialized views | Transactional projection or asynchronous versioned projection with watermark | Refresh and concurrent visibility rules must be declared |
| Sharding | Tenant-prefixed ordered keys split into ranges | Global indexes and cross-range transactions require coordination |
| Sequences | Dedicated atomic keys or leased blocks with PostgreSQL-compatible non-transactional behavior | A single sequence key can become a hotspot |
| Serializable isolation | Snapshot reads plus predicate/range dependency tracking and retry | TiKV-style snapshot isolation alone is not PostgreSQL Serializable |
| DDL and catalogs | Transactionally versioned catalog rows and schema versions | Online changes, cache invalidation, dependencies, and bootstrap |
| CDC and logical replication | Ordered commit/change stream with schema version | PostgreSQL physical replication compatibility is a separate page-level concern |

PostgreSQL itself implements primary and unique constraints with indexes,
supports multiple index access methods, executes row-level and statement-level
triggers, and rewrites ordinary views from cataloged query trees. Those are SQL
compute responsibilities, not new kernel primitives. See the official
[constraint](https://www.postgresql.org/docs/current/ddl-constraints.html),
[index](https://www.postgresql.org/docs/current/indexes.html),
[trigger](https://www.postgresql.org/docs/current/trigger-definition.html), and
[view](https://www.postgresql.org/docs/current/rules-views.html) documentation.

### OLTP and OLAP from one history

objectKV should not force one file format to serve contradictory access paths.

```text
commit history
   |
   +-> row object base at O_r + row overlay (O_r, T] -> OLTP
   |
   +-> columnar base at O_c + analytical tail (O_c, T] -> DataFusion
```

Every query receives one target version `T`. The OLTP range reader returns the
newest visible value at `T`. The DataFusion source merges each partition's
columnar base with its analytical tail through the same `T`. Different physical
watermarks change work and latency, not the requested logical snapshot.

The analytical tail is not the recovery suffix. It is schema-aware, queryable,
and may need longer retention. It must retain invalidation keys even when an
updated row does not satisfy a pushed predicate.

### Control and worker map

| Component | State | Responsibility |
|---|---|---|
| Cell bootstrap and generation authority | `[VERIFIED]` narrow prototype | Membership generation, fencing, and recovery roots |
| Replicated log / Raft group | `[VERIFIED]` single-group prototype; `[PROPOSED]` MultiRaft | Quorum-durable ordered commands and membership |
| Transaction coordinator | `[PROPOSED]` | Start versions, multi-range validation, commit decision, idempotent outcomes |
| Range router and placement driver | `[PROPOSED]` | Key-to-range map, leases, splits, movement, and replica placement |
| Serving worker | `[PROPOSED]` | DRAM overlay, selected SSD or RAM `ServingImage`, indexed object fallback, profile handoff, and exact reads |
| Object publisher and compactor | `[VERIFIED]` publication contracts; `[PROPOSED]` formats and policy | Build immutable row objects and advance fenced manifests |
| Change projector | `[PROPOSED]` | Produce complete schema-aware analytical tail |
| Columnar materializer | `[PROPOSED]` | Publish Parquet first, then evaluate Vortex |
| DataFusion provider | `[VERIFIED]` narrow streaming overlay | Exact base-plus-tail scans at one version |
| Metacluster | `[FUTURE]` | Tenant-to-cell placement and movement, never ordinary transaction commit |

### Performance curves and falsifiers

Absolute targets require a frozen machine, topology, durability profile, and
dataset. Until those are frozen, relative controls are more honest.

| Curve | X axis | Primary Y axis | `[PROPOSED]` target or falsifier |
|---|---|---|---|
| Hot SSD point read | resident hit ratio and concurrent clients | p50/p95/p99 latency, CPU/op | At 99.9% resident hits, wrapper p99 and throughput remain within 20% of direct NVMe RocksDB under the same durability and RPC path |
| Hot RAM point read | resident hit ratio and concurrent clients | p50/p95/p99 latency, CPU/op | Productize only when one named end-to-end p99, throughput, or CPU metric improves by at least 20% over admitted SSD without breaking memory, recovery, or cost gates |
| Cold point read | object-block miss ratio and block size | p99, GETs/op, bytes/op, cost/op | Miss penalty is explicit and bounded; a 1% miss rate must not collapse the resident service class |
| Commit | transaction bytes, keys, ranges, and contention | commits/s, p99, retries | One-range commits within 25% of a same-durability MultiRaft control; multi-range overhead attributed separately |
| Cross-range transaction | participant count and conflict rate | p99, abort rate, recovery time | No acknowledged partial commit; latency growth remains explainable by participant quorums and validation |
| Objectification | ingest rate and object-store fault duration | lag versions, retained txLog bytes, object amplification | Backpressure prevents unbounded recovery suffix; recovery remains exact through every tested fault |
| Worker rebuild | resident bytes and range count | time to first correct read, full hydration time, downloaded bytes | First correct read does not require full-cell download; hydration scales with assigned ranges |
| Local footprint | total object bytes and working-set size | RAM/NVMe bytes per worker | Local bytes track admitted resident ranges and cache budget, not total database size |
| HTAP overlay | tail/base row and byte ratio | query p99, peak memory, scan bytes | At tail <= 1% of base, exact overlay adds <= 20% over base-only control; materialization policy acts before tail reaches 10% |
| Branching | database bytes and branch count | branch-create latency and incremental bytes | Branch creation is metadata-scale and does not copy the base closure |

The product thesis is invalidated if any of these remain true after one focused
optimization cycle:

- hot p99 requires local durable replicas of the complete database rather than
  bounded serving assignments;
- the object row format cannot support one range GET plus bounded decode for a
  cold point lookup;
- multi-range commit is materially slower or less robust than TiKV without a
  stronger semantic benefit;
- objectification stalls can exhaust the txLog before safe backpressure;
- exact DataFusion overlay cost stays dominated by tail processing under the
  target materialization policy;
- an independent reader cannot reconstruct a snapshot from the public manifest
  and object formats.

### Eval and telemetry contract

The atomic requirements, failure matrix, capacity dimensions, and admission
gates live in the [detailed product and system specification](PRODUCT-SPEC-SHEET.md).
The workstream decomposition and execution order live in the
[BIDEC evaluation program](BIDEC-EVAL-PROGRAM.md).

`[CODE-COMPLETE]` The
[`GoldenPathScenario`](../evals/scenarios/objectkv-golden-path-v1.toml) freezes
one logical history, twelve architecture surfaces, fifteen checkpoints, and
their artifact dependencies. The
[`EvalProgram`](../evals/programs/objectkv-golden-path-v1.toml) binds each
checkpoint to requirements, a suite, workload, lane, control, poison subjects,
and a falsifier. This is a validated plan, not end-to-end evidence. A golden
checkpoint becomes Verified only when its receipts share the scenario identity
and required artifact digests.

Every performance result is one immutable receipt containing:

- objectKV, workload, format, and dependency revisions;
- hardware, kernel, filesystem, object backend, region, and topology;
- durability and isolation profile;
- dataset scale, key/value distribution, resident budget, and warm/cold state;
- exact seed, operation mix, transaction shape, and fault schedule;
- p50/p95/p99/p99.9, throughput, CPU, memory, NVMe, network, object requests,
  bytes, cost, retries, compaction, txLog lag, and rebuild time;
- correctness gates and negative-control results.

OpenTelemetry spans connect `transaction -> range participant -> Raft append ->
quorum fsync -> apply -> objectification -> manifest advance`. Metrics use
bounded-cardinality range classes and workload identities. Individual user keys
or values are never telemetry labels.

Suites are composable configuration, not hard-coded benchmark binaries. A suite
selects workloads, parameter sweeps, hard gates, reference controls, and output
sinks. Adding a metric must not require changing transaction or storage code.

## Convictions

1. Object storage is the permanent capacity and recovery substrate; the normal
   regional commit path remains a quorum-durable txLog on fast local media.
2. SSD and RAM are alternative disposable serving profiles. Predictable OLTP
   latency requires an explicit resident-range service class rather than
   pretending all object misses are cache hits.
3. PostgreSQL and DataFusion share objectKV versions and change history, not one
   physical format or one execution engine.

## Open Questions

1. How do range-local Raft groups provide strict-serializable multi-range
   transactions without recreating one global commit bottleneck?
2. How should resident-range admission, hydration, demotion, and replica
   placement bound p99 while keeping local bytes independent of total database
   size?
3. How should the public manifest and row-object index represent historical
   snapshots, tombstones, branches, and GC roots with bounded open cost?
4. How should the PostgreSQL program choose between an upstream page bridge, a
   pgRust page bridge, and a future row-native engine while preserving an exact
   compatibility oracle?
5. How should durability profiles expose the trade among regional quorum
   latency, objectification lag, cost, RPO, and recovery time without weakening
   acknowledgement semantics silently?

## Milestones

Pending: human-generated. PRD owner must define milestones, owners, and target
dates.

The required validation order is still clear: freeze the transaction model,
prove one resident range, prove exact object recovery, prove a cross-range
transaction under faults, measure the curves above, then start the PostgreSQL
bridge and broader serving models.

## Decisions Log

| ID | Status | Decision | Reason and tradeoff |
|---|---|---|---|
| D1 | audited | Keep consumer semantics above the ordered KV kernel | Preserves one testable kernel; gives up consumer-specific shortcuts until generalized |
| D2 | unaudited | Target per-range Raft groups plus a cell transaction coordinator; retain single-log Cell v0 as control | Optimizes for TiKV-like horizontal write and recovery scaling; takes on distributed commit, lock resolution, and serializability work |
| D3 | audited | Select SSD or RAM per range behind one serving contract, and select durability separately per tenant generation | Preserves workload-specific hot-state choices without weakening acknowledgement semantics; adds profile placement and generation-fenced handoff |
| D4 | unaudited | Expose resident and elastic range service classes | Makes the latency and local-footprint trade explicit; gives up one simple undifferentiated read SLA |
| D5 | unaudited | Keep the upstream PostgreSQL page bridge first and add pgRust as a second compute/reference lane | Preserves a literal compatibility control; delays the cleaner row-native HTAP mapping |
| D6 | audited | Keep transactional row bases and analytical columnar bases separate but version-aligned | Lets each format fit its access path; requires complete change capture and exact overlay |
