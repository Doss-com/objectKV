# Columnar LSM source-of-truth architecture fork

Status: `[EVALUATING]` research direction, 2026-08-26.

## Call

Do not redefine objectKV as "Parquet is the database." Evaluate a manifested,
multi-layout LSM in which immutable row-delta runs and random-access columnar
runs form one authoritative object closure.

The logical source of truth is:

```text
ManifestedObjectState(O) + txLog(O, C]
```

It is not one physical encoding. A manifest can reference different immutable
run formats while preserving one ordered MVCC entry algebra and one commit
history.

The strongest candidate is:

```text
quorum txLog
    -> RAM or SSD mutable tail
    -> row-oriented L0 delta runs
    -> random-access columnar L1..Ln runs
    -> authenticated manifest frontier O
```

The kernel must continue to provide exact primary-key `get` and ordered `scan`.
Consumers may add semantic secondary indexes, but they must not have to invent
the primary access path or reconstruct transaction visibility themselves.

The current architectural call is narrower than a columnar-first rewrite:

```text
logical truth
    active manifest at O + quorum txLog suffix (O, C]
                         |
                         v
physical runs selected by capability
    L0 mutations       -> row delta
    opaque KV L1..Ln   -> indexed row by default
    typed L1..Ln       -> random-access columnar only if admitted
    DataFusion         -> typed projection + exact live tail
```

This keeps the useful part of the proposal, one branchable manifested history
consumed by KV and scan readers, without forcing opaque bytes through a
columnar representation that cannot understand them.

## Why evaluate this now

G4.11a.1 shows that aligned `R`, `Q(client)`, and `O` frontiers bound snapshot
growth, but the current six-process physical representation still reaches
19.692719x maximum amplification against the frozen 8x gate. Snapshot growth
is 1.091759x against the 1.25x ceiling. The lifetime curve is controlled; the
constant factor and state ownership are not.

This creates a useful decision point. A new compact checkpoint codec could fix
only the immediate amplification. A storage-layout fork can test whether the
same object state can serve both primary-key retrieval and analytical scans,
which would remove a larger architectural duplication if it works.

## Current architecture map

```text
Consumer semantics
    PostgreSQL adapter / Redis / search
                    |
                    v
Ordered transaction contract
    okv-transaction + okv-consensus
                    |
                    v
Quorum durability
    okv-log -> okv-wal -> OpenRaft txLog
                    |
                    +-------------------+
                    |                   |
                    v                   v
Transactional object base       Analytical projection
    OKVM + OKVI + OKVB              Parquet/Vortex
    ordered opaque values           typed table rows
                    |                   |
                    +---------+---------+
                              v
                         object store
```

`okv-object` owns the row-object pilot and authenticated publication.
`okv-htap` independently owns Parquet plus Arrow-tail overlay through
DataFusion. `okv-eval` is currently the only composition point between the
transaction, object, and analytical paths.

The duplication is intentional in RFC-0003 because the transactional segment
understands opaque ordered MVCC entries while the analytical artifact
understands schemas and columns. The proposed experiment challenges the
physical duplication, not those distinct logical responsibilities.

## The important distinction

Three claims are easy to conflate:

1. The permanent bytes are columnar.
2. The logical database truth is a columnar file.
3. Analytical queries and KV reads consume one manifested object closure.

Claim 1 is possible for typed data. Claim 2 is too narrow because the exact
database state also includes manifests, versions, tombstones, pending
publication state, and the durable txLog suffix. Claim 3 is the useful target.

For opaque KV values, a columnar file can expose only generic columns such as:

```text
user_key | commit_version | operation | value_bytes
```

That may improve compression and selected metadata scans, but DataFusion
cannot push predicates into the opaque value. Real OLAP benefits require a
schema-aware serving model above the generic kernel.

## Candidate architecture

### Permanent state

```text
Cell manifest at O
    |
    +-- range map and format capabilities
    +-- L0 delta runs
    |     key, version, operation, opaque or typed payload
    |
    +-- L1..Ln compacted runs
    |     primary-key ordered
    |     random-access columnar pages
    |     key fences, filters, row identifiers, zone maps
    |
    +-- range tombstone runs
    +-- schema and codec references for typed namespaces
    +-- checksums, provenance, and GC roots
```

The manifest closure through `O` plus the quorum-durable suffix `(O, C]`
reconstructs every acknowledged commit. Compaction changes the physical run
set but not the logical snapshot.

### Write pipeline

```text
bounded transaction
    -> commit proxy batch
    -> conflict resolution
    -> quorum txLog sync
    -> COMMITTED at C
    -> RAM or SSD serving tail
    -> immutable L0 delta pack
    -> background compaction
    -> columnar L1..Ln run plus primary index
    -> authenticated manifest activation at O
    -> advance retention frontiers and reclaim covered txLog
```

L0 is allowed to optimize for ingest and exact replay. Higher levels optimize
for scan, compression, and bounded point-read fanout. This is one LSM with
different level formats, not two databases connected by ETL.

### Point read

```text
get(key, T)
    -> mutable-tail index
    -> newest L0 filters and key indexes
    -> compacted-run key fences and filters
    -> primary index maps key to stable row id or row position
    -> gather required column pages or one row capsule
    -> merge versions and tombstones through T
```

The cold path must bound metadata requests, data requests, bytes fetched, and
the number of overlapping runs. A format that requires one request per column
for a full-row point read is not automatically admissible. A row capsule,
covering primary index, or co-located mini-block may be necessary.

### Analytical scan

```text
DataFusion snapshot T
    -> scan compacted columnar runs through O or partition watermark W
    -> prune columns, zones, and runs
    -> merge row-delta runs and live tail in (O, T] or (W, T]
    -> suppress invalidated base rows
    -> emit one version-aligned Arrow stream
```

The existing `StreamingSnapshotOverlayExec` remains relevant. Its base changes
from a separately materialized Parquet snapshot to the admitted compacted-run
view of the same manifest closure.

### Branching and history

```text
manifest M100
    +-- branch main -> M140 + txLog(140, C]
    +-- branch test -> M100 + branch suffix
```

Immutable runs are shared by reference. A branch initially creates metadata
and a new log suffix, then copy-on-write compaction produces branch-specific
runs only when needed.

## Options

| Option | Optimizes for | Gives up | Call |
| --- | --- | --- | --- |
| Row transactional base plus derived columnar base | Lowest-risk point reads and an opaque KV contract | Duplicate durable layouts, separate watermarks, more compaction | Keep as control |
| Parquet-only source of truth | Ecosystem interoperability and scans | Predictable point reads, update efficiency, generic values | Reject as primary candidate |
| Random-access columnar-only LSM | One durable base and typed scan efficiency | Simpler ingest, opaque value analytics, proven OLTP behavior | Evaluate, not assume |
| Multi-layout manifested LSM | One logical object state with level-specific economics | More reader and compaction complexity | Recommended candidate |
| Log plus arbitrary consumer projections | Small kernel and maximum composability | Bounded recovery and a guaranteed point-read serving base | Reject for objectKV's KV claim |

## External evidence

Apache Paimon proves that primary-key lake tables can use an LSM over columnar
objects. Its own documentation says reads merge overlapping sorted runs and
recommends roughly 200 MB to 1 GB buckets. Paimon's PFile proposal is also a
warning: its lookup path dynamically converted columnar files to a KV form and
failed to scale to thousands of GB, motivating a dedicated KV file format.

RisingWave made the opposite production choice. It uses row-based Hummock on
object storage for low-latency point lookups and frequent updates, and a
columnar Iceberg engine for analytical scans.

Lance and Vortex make the columnar-primary candidate more credible than it was
with Parquet alone. Lance specifies immutable manifests, fragments, deletion
files, row addressing, and independent indexes. Its columnar pages are built
for predictable random access. Vortex provides extensible columnar layouts and
claims materially better random access than Parquet, but its default file
layout is still explicitly analytical, with 8,000-row zones and 2 MB chunks.
These are candidate mechanisms, not evidence that they satisfy objectKV's
transactional point-read curve.

Sources:

- Apache Paimon primary-key LSM overview:
  <https://paimon.apache.org/docs/1.1/primary-key-table/overview/>
- Apache Paimon PFile lookup motivation:
  <https://cwiki.apache.org/confluence/spaces/PAIMON/pages/311628201/PIP-25%2BIntroduce%2Ba%2Bkey-value%2Bfile%2Bformat%2Bfor%2Bpaimon%2Bprimary%2Bkey%2Btable>
- RisingWave storage overview:
  <https://docs.risingwave.com/store/overview>
- Lance table and file specifications:
  <https://lance.org/format/table/> and <https://lance.org/format/file/>
- Vortex file layout:
  <https://docs.vortex.dev/concepts/file-format>

## Proposed module boundary

Do not add these modules until the layout gate is frozen.

```text
okv-transaction
    logical mutations, versions, conflicts

okv-consensus
    commit authority, txLog, R/Q/O frontiers

okv-object
    backend semantics, manifests, publication, GC

okv-run [PROPOSED]
    immutable run algebra
    row-delta reader/writer
    random-access columnar reader/writer
    primary filters and indexes
    compaction equivalence

okv-htap
    typed schema binding
    DataFusion scan over manifested runs plus tail
```

`okv-run` would expose logical capabilities, not a misleading universal file
trait. A point reader, ordered scanner, typed column scanner, and compactor may
be separate capabilities over one format descriptor.

## Frozen evaluation to write next

Use identical logical histories, object sizes, cache states, and object-store
fault semantics for four candidates:

1. current indexed `OKVB` row-object control;
2. sorted Parquet plus an external primary-key index;
3. random-access columnar run using Lance or Vortex primitives;
4. hybrid columnar run with a covering key index or row capsule.

Required curves:

| Lane | Primary metric | Hard gates |
| --- | --- | --- |
| Cold point | object requests per exact get | exact MVCC result, bounded bytes, corruption detection |
| Warm point | p99 latency | exact result, bounded index RAM, no full-object read |
| Ordered scan | useful rows per second | exact ordering, tombstones, projection and predicate correctness |
| Update ingest | committed mutations per second | same quorum durability and acknowledgement boundary |
| Compaction | physical write amplification | snapshot equivalence, crash-safe manifest publication |
| Storage | bytes per live logical byte | complete media accounting, including indexes and manifests |
| HTAP | base-plus-tail query duration | exact snapshot, invalidation correctness, bounded operator memory |
| Branching | new physical bytes per branch | exact version identity and shared immutable roots |

The first run is diagnostic and establishes distributions. Freeze admission
thresholds before tuning the second run. Local filesystem results select viable
mechanisms only. GCS cold and warm curves are required before an architecture
decision.

## First executable preflight

`[CODE-COMPLETE]` C0, C1, C3, C4, and request-coalesced Parquet now run through
`okv_eval::storage_layout`. One deterministic generator emits 1,024 keys, four
delta cycles, 512-byte values, requested historical reads, and one exact final
projection. The three candidates consume the same history digest. The
Parquet reader uses primary-key fences, selected row groups, projected columns,
and independently checksummed 64 KiB object ranges.

Seed 5701, 256 point reads, debug build, local filesystem:

```text
                         point        bytes/       point       scan       stored/
                         requests     point        p99         rows/s     live

indexed row object       1            64,066       2.473 ms    19,149     1.582x
indexed Parquet          10        1,047,296      44.519 ms    35,868     1.523x
hybrid row capsule       4           697,344      30.047 ms    32,621     3.046x
```

The results are `[EVALUATING]`, not metric receipts. They are too small, local,
and build-profile-sensitive to admit a layout. They do falsify one naive shape:
selected Parquet row groups plus projection do not by themselves produce a
competitive generic point path. The format reads separate column chunks, and
checksum-block expansion makes the physical cost visible instead of hiding it
inside an in-process buffer.

The next useful candidates are now constrained:

1. Vortex random access in an isolated Rust 1.95 helper.
2. A coalescing Parquet reader that proves it can merge overlapping checksum
   blocks without converting or caching the complete file.
3. A typed compacted base with a small sidecar point structure, counted as
   complete durable media.
4. The indexed row-object control at the full frozen profile and on GCS.

If none keeps cold point requests and bytes near C0, columnar stays a typed
analytical layout above the opaque kernel. That is still one history and one
manifest protocol, but not one universal data encoding.

## Release-local mechanism admission

`[EVALUATING]` The fully counted C4 subject splits one manifested typed run into
an indexed MVCC row sidecar and a narrow columnar analytical projection. The
opaque 480-byte payload is stored only in the sidecar. Key, version, operation,
tenant, category, and quantity are present in the projection. This differs from
C3, which embedded a complete duplicate row capsule beside the full column set.

The frozen local admission uses 16,384 keys, 4,096 point reads, three seeds,
three repeats, alternating subject order, a Rust release build, and the same
canonical history per paired sample. C4 passed every configured gate:

```text
point request ratio vs row control             1.000x   max 1.00x
point response-byte ratio vs row control       1.000x   max 1.05x
median point p99 ratio vs row control          1.033x   max 1.25x
median projected-scan throughput ratio         9.124x   min 3.00x
stored/live amplification ratio                1.030x   max 1.10x
compaction write-amplification ratio           1.035x   max 1.10x
resident-index ratio                           1.137x   max 2.00x
```

The candidate median scan rate was 2.867 million rows per second. All exactness,
manifest-closure, checksum, no-LIST, bounded-point-read, and branch-sharing
checks passed. Run `f5dbba62-0f47-46af-8bb7-d1f7efa6a353` remains
inconclusive because the source was dirty and the backend was a local
filesystem.

This changes the architectural question. The relevant comparison is no longer
row truth versus columnar truth. It is whether one manifested logical truth can
carry two non-overlapping access representations cheaply enough that typed
namespaces get both point and scan behavior. Local evidence says yes. Clean GCS
cold and warm curves, split-closure restart, DataFusion overlay execution, and
object economics remain required.

`[CODE-COMPLETE]` The paired runner now accepts an external backend and scopes
every sample to a unique object prefix. The frozen GCS suite repeats the same
candidate, control, histories, ordering, and ratios against real remote range
GETs. `[EVALUATING]` It has not run because objectKV-dev credentials and the
target GCS bucket have not been verified. No cloud latency or economics claim
is inferred from the local result.

## Decision boundary

Admit the multi-layout LSM only if it does all of the following:

1. preserves the generic ordered KV and strict-serializable transaction API;
2. keeps cold point-read object requests and bytes close to the row control;
3. materially improves projected analytical scan economics;
4. does not move foreground commit acknowledgement to object storage;
5. produces one crash-safe manifested object closure usable by both readers;
6. bounds index RAM, overlapping runs, compaction amplification, and small-file
   creation;
7. recovers exactly from `ManifestedObjectState(O) + txLog(O, C]`.

If it fails point reads, retain row objects as the transactional base and use
columnar artifacts as derived layouts. If it passes only for typed namespaces,
admit it in the PostgreSQL or table layer while keeping opaque KV ranges on the
row format. A mixed answer is acceptable because the manifest, transaction,
publication, and recovery contracts remain shared.
