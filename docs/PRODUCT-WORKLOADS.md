# objectKV workload portfolio

Status: `[EVALUATING]`

## The call

objectKV should be sold to data-system builders as a versioned state kernel,
not as one database that claims to run every workload through one physical
layout.

The shared product is one ordered transaction history, one object frontier,
and one recovery model. OLTP, OLAP, Redis-like serving, and virtual-filesystem
metadata each get a read model optimized for their access pattern.

```text
                         tenant transaction history
                                   |
                         ordered commits + okv-log
                                   |
             +---------------------+---------------------+
             |                                           |
       hot range image                         immutable object frontier
        RAM or SSD                         segments + manifests + snapshots
             |                                           |
      +------+------+                         +----------+----------+
      |             |                         |                     |
  OLTP reads   Redis-like state         OLAP materialization   file content
  row/page KV  TTL + atomic ops          columnar base + tail   chunk objects
```

This is the important product boundary: shared writes and history, specialized
reads. "No ETL" means one commit, schema, snapshot, and catalog history. It
does not require every workload to read the same bytes.

## Product promise

`[PROPOSED]` objectKV lets a system author keep mutable state close to compute,
publish durable immutable state to S3-compatible storage, recover disposable
serving workers, and derive new read layouts without creating a second source
of truth.

The first buyer is an OSS database or stateful-compute author. A direct Redis
replacement, SQL database, and POSIX filesystem are adapters or products above
the kernel, not the initial kernel itself.

## Shared kernel contract

Every workload may depend on these capabilities:

1. Ordered binary keys and deterministic ordered range reads.
2. Short, bounded, strict-serializable transactions within one cell and tenant
   transaction domain.
3. A commit version and ordered mutation position for every committed change.
4. A retained `okv-log` stream that can reconstruct every version after an
   immutable object frontier.
5. Immutable named objects, manifests, snapshots, and garbage-collection roots.
6. Disposable serving workers with RAM or SSD hot-state profiles.
7. Snapshot and branch identities that can share immutable objects.
8. OpenTelemetry signals and eval receipts at every public boundary.

The kernel should not absorb SQL planning, Redis protocols, columnar query
optimization, file byte transport, or cross-cell transactions.

## Proposed package taxonomy

| Package or surface | Owns | Does not own |
|---|---|---|
| `okv` | Transactions, ordered keys, point and range reads, versions, snapshots | SQL, file semantics, query planning |
| `okv-log` | Ordered append and replay over the commit history | Independent durability or a second consensus path |
| `okv-wal` | A WAL-oriented contract implemented with `okv-log` | Database page or row semantics |
| `okv-table` | Versioned row changes, columnar snapshots, DataFusion overlay | Transaction admission |
| `okv-fs` | Path, inode, directory, branch, and object-reference metadata | Large file byte transport |
| `okv-redis` | A deliberately small Redis-like command adapter | Full Redis compatibility |
| `okv-pg` | Postgres storage and transactional integration | A second storage truth outside `okv` |

Only `okv` and `okv-log` are kernel-level product commitments. The other names
are proposed adapters until their workload lane earns a stable contract.

## Workload portfolio

| Surface | Customer job | Optimized read model | Kernel advantage | Explicit boundary |
|---|---|---|---|---|
| OLTP | Build a transactional database or metadata service | Ordered row/page index in RAM or SSD | Strict transactions plus object-native recovery and branching | SQL and database semantics live above `okv` |
| OLAP | Query current and historical state without a second ingestion system | Columnar object base plus exact committed tail | One snapshot history, independent compute, open object formats | Columnar files are derived, not the commit authority |
| Redis-like | Serve sessions, actors, caches, counters, and coordination state | RAM-resident keys, TTL index, atomic operations, `okv-log` streams | Fast disposable compute with selectable durability | Initial scope is a useful primitive subset, not full Redis compatibility |
| Virtual filesystem | Store catalogs, namespaces, branches, and file metadata | Transactional path and inode index; contents are direct chunk objects | Atomic metadata plus cheap object sharing and snapshots | File payloads bypass the KV value path; full POSIX is out of scope |

## OLTP shape

### Product job

Give a database engine the primitives needed for point reads, range scans,
secondary indexes, uniqueness checks, constraints, and short transactions,
without assigning permanent byte ownership to serving workers.

### Functional path

```text
SQL or application transaction
        |
        +-> point/range reads -> range image in RAM or SSD -> object fallback
        |
        +-> commit -> conflict resolution -> replicated txLog -> commit receipt
                                                    |
                                             async objectification
```

The OLTP fast path is not a columnar scan. The serving worker maintains a
key-addressable image and recent MVCC overlay. Object blocks are the cold and
recovery path. A database adapter chooses row-native keys, page-native keys, or
both.

### Product claims and limits

- `[CODE-COMPLETE]` One public `SingleRange` can open an object base, catch up
  through retained txLog entries, commit, and serve bounded point reads.
- `[EVALUATING]` The current receipt proves the recovery equation locally. It
  does not establish competitive hot-read or commit latency.
- `[PROPOSED]` A cell provides transactions across arbitrary resident ranges in
  one tenant domain.
- `[FUTURE]` SQL, indexes, foreign keys, triggers, and views belong to the
  relational layer. The kernel only supplies atomic ordered state.

### Eval lane

Measure hot and cold point reads, ordered scans, commit latency, conflict rate,
write amplification, object request amplification, catch-up lag, recovery time,
and cost per committed operation. Compare the same admitted durability and
failure envelope, not an in-memory unsafe candidate against a durable system.

Primary comparators are an embedded ordered engine for the single-node floor
and TiKV or FoundationDB for the distributed transaction envelope.

## OLAP shape

### Product job

Run a query at exact version `T` using independently scalable query compute,
even when a columnar base is only materialized through watermark `W`.

```text
logical table at T = columnar base at W + row changes in (W, T]
```

The columnar materializer consumes `okv-log`, writes Parquet or Vortex objects,
and publishes a versioned snapshot manifest. A DataFusion source reads the base
and overlays the latest tail change per primary key before returning rows.

### Product claims and limits

- `[PROPOSED]` Query freshness is bounded by retained-tail availability, not by
  the columnar materialization watermark.
- `[PROPOSED]` Historical snapshots and branches share immutable objects and
  query compute can attach without moving the whole database.
- `[FUTURE]` Predicate pushdown must retain tail keys needed to invalidate base
  rows, even when the replacement row does not satisfy the predicate.
- `[FUTURE]` Broad analytical results cannot safely control a later transaction
  without a maintained aggregate or dependency validation certificate.

### Eval lane

Sweep base-tail ratios from fully materialized to tail-heavy. Measure scan
throughput, time to first batch, overlay CPU and memory, object bytes read,
freshness lag, materialization cost, and OLTP interference. The critical curve
is query cost as `T - W` grows. A flat correctness line and a bounded,
explainable performance slope are required.

Compare direct DataFusion reads of a frozen columnar base, base plus object
deltas, and base plus the live okv tail. This separates query-engine cost from
freshness cost.

## Redis-like shape

### Product job

Serve ephemeral and semi-durable application state with a small operation
surface: get, set, delete, compare-and-set, counters, TTL, atomic batches, and
append/read streams through `okv-log`.

### Durability profiles

| Profile | Acknowledgement condition | Optimizes for | Gives up |
|---|---|---|---|
| memory-volatile | mutation applied to one RAM image | minimum latency | process loss loses acknowledged state |
| memory-replicated | mutation admitted by replicated in-memory txLog | low latency with process failover | correlated cell restart can lose unobjectified state |
| SSD-durable | mutation persisted by replicated SSD txLog | durable low-latency state | SSD fleet and replication cost |
| object-durable | object store has acknowledged the commit group | minimal durable infrastructure | object latency on acknowledgement path |

These are different products and must never share one unlabeled latency claim.

### Product claims and limits

- `[PROPOSED]` Redis-like state is the clearest demonstration of the RAM serving
  profile and explicit durability tradeoffs.
- `[PROPOSED]` `okv-log` can back streams, replay, watches, and state-machine
  recovery from the same commit history.
- `[FUTURE]` Protocol compatibility and complex structures such as sorted sets,
  pub/sub, scripting, and cluster emulation are separate adapter decisions.

### Eval lane

Measure point-operation latency and throughput by durability profile, TTL expiry
cost, counter contention, stream append/read latency, resident bytes per key,
recovery after process and cell loss, and acknowledged-data loss under injected
failure. Compare against Valkey or Redis only for the exact implemented command
and durability subset.

## Virtual-filesystem shape

### Product job

Provide an atomic, branchable namespace for databases, ML artifacts, build
outputs, checkpoints, and user files while keeping large payloads in the object
store.

```text
okv transaction                         object store
----------------                        ------------
path -> inode                           immutable chunks
inode -> metadata                +----> content object
directory membership             |
snapshot and branch roots         +----> multipart or ranged reads
content object references
```

Rename, link-count changes, and directory membership changes are small metadata
transactions. File contents are content-addressed or immutable named objects.
The KV does not copy large file payloads through the transaction system.

### Product claims and limits

- `[PROPOSED]` Atomic namespace changes, snapshots, branches, and cheap clones
  fit the kernel directly.
- `[PROPOSED]` Large-file bandwidth scales with object storage rather than the
  transaction path.
- `[FUTURE]` Initial semantics should be a database-oriented virtual filesystem,
  not full POSIX locking, mmap coherence, or arbitrary cross-cell rename.

### Eval lane

Measure stat/open metadata reads, directory listing, create and rename
transactions, branch creation, clone bytes written, small-file packing,
large-file ranged throughput, empty-worker recovery, and garbage-collection
reachability. Sweep directories, path depth, object size, and snapshot count.

## Cross-workload golden path

The eval program should admit product surfaces in this order. These W lanes are
workload lanes inside the existing G0 through G7 program, not a replacement for
the program gates.

| Gate | Surface | Admission question |
|---|---|---|
| W0 | Single range | Can public `okv` reconstruct and serve exact committed state? |
| W1 | OLTP point and transaction | Is the hot path competitive for its stated durability profile? |
| W2 | Redis-like state | Do TTL, counters, batches, and streams remain fast and exact under failure? |
| W3 | Virtual-filesystem metadata | Are namespace mutations atomic while file bytes bypass the tx path? |
| W4 | Columnar snapshot | Can DataFusion query one published columnar snapshot efficiently? |
| W5 | Exact HTAP overlay | Does base plus tail return exact `T`, and what is the tail-lag curve? |
| W6 | Mixed interference | Can OLTP SLOs hold while objectification and OLAP queries run? |
| W7 | Multi-range cell | Do routing, movement, and cross-range transactions preserve the same contract? |

Every gate requires:

1. a public application path rather than a private evaluator implementation;
2. a named workload, dataset, seed, backend, and durability profile;
3. zero correctness anomalies and an injected-failure receipt;
4. OpenTelemetry traces, metrics, and structured logs;
5. one relevant comparator under the same admitted semantics;
6. an absolute curve plus a relative price/performance delta;
7. a clean-source receipt before the status becomes `[VERIFIED]`.

## Positioning we can defend

`[PROPOSED]` The concise product message is:

> objectKV is an open, versioned state kernel for building databases and
> stateful systems with hot mutable compute and durable, branchable object
> storage.

The differentiators to prove are:

1. Compute is disposable without making object storage the per-operation hot
   path.
2. Immutable state is directly addressable, open-format, and reusable by
   multiple compute engines.
3. OLTP and OLAP share commit and snapshot history without sharing one
   unsuitable read layout.
4. RAM, SSD, and object acknowledgement are explicit product profiles rather
   than hidden implementation details.
5. Recovery, branching, and new projections are ordinary uses of the same log
   and manifests.

## What we should not claim yet

- `[EVALUATING]` Competitive OLTP latency or throughput.
- `[FUTURE]` A complete Redis, TiKV, FoundationDB, Postgres, or POSIX replacement.
- `[FUTURE]` Exact fresh OLAP until the base-plus-tail operator is implemented.
- `[FUTURE]` Cross-cell serializable transactions.
- `[FUTURE]` Object-only durability with RAM-class acknowledged-write latency.
- `[FUTURE]` One physical layout that is optimal for every workload.

## Recommended implementation wedge

Keep one kernel and build two thin reference adapters before widening the cell:

1. An OLTP and Redis-like adapter over `SingleRange` that exercises point
   operations, atomic batches, counters, TTL, and `okv-log` replay.
2. A virtual-filesystem metadata adapter that stores paths and inode metadata in
   `okv` while writing contents as direct immutable objects.

These two adapters test opposite value sizes and reuse the same point-read and
transaction core. The first columnar milestone should then consume their shared
commit history into a DataFusion-readable snapshot. That proves the product
thesis with one continuous lineage instead of four disconnected demos.

## Decisions to calibrate

- D1: The initial wedge is a state kernel for system builders, not an end-user
  database. This optimizes for a small coherent contract and gives up immediate
  SQL-level completeness.
- D2: Versioned logical state is authoritative and reconstructable from object
  base plus txLog tail; row, page, column, and path layouts are serving
  projections. This optimizes for composability and gives up the simplicity of
  one physical representation.
- D3: Durability profile is part of every benchmark identity. This makes claims
  comparable and gives up one universal latency number.
- D4: Large file payloads bypass transactional values. This preserves metadata
  atomicity and object-store bandwidth, but file-content mutation is publish and
  swap rather than in-place sector writes.
- D5: The next architecture expansion is earned by W1 and W3 evidence. A larger
  distributed cell before those results would add coordination without proving
  the product advantage.
