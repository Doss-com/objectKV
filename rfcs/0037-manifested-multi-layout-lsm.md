# RFC-0037: Manifested multi-layout LSM

- Status: `[EVALUATING]`, evaluation contract frozen
- Authors: DOSS
- Created: 2026-08-26
- Scope: permanent object state, point reads, analytical scans, compaction, and branching

## Decision to test

Evaluate a single manifested object LSM whose immutable levels may use different
physical layouts while preserving one ordered MVCC entry algebra.

```text
quorum txLog
    -> RAM or SSD mutable tail
    -> row-oriented L0 delta runs
    -> compacted L1..Ln runs
         row, random-access columnar, or hybrid
    -> authenticated manifest frontier O
```

The database source of truth at commit version `C` remains:

```text
ManifestedObjectState(O) + txLog(O, C]
```

It is not one Parquet, Vortex, Lance, or proprietary file. The active manifest
identifies a complete immutable object closure and its logical coverage. The
txLog suffix contains every acknowledged mutation newer than that closure.

The kernel continues to own exact `get(key, T)` and ordered
`scan(begin, end, T)`. A consumer may add secondary indexes and typed serving
semantics, but it does not reconstruct MVCC visibility or invent the primary
point-read path.

## Why this gate precedes G4.11b

G4.11a.1 aligned resolver frontier `R`, a bounded retry frontier `Q(client)`,
and authenticated object frontier `O` across four complete process cycles. It
passed the frozen lifetime-growth gate at 1.091759x but failed the complete
physical-media gate at 19.692719x against 8x.

The frontier protocol bounds history. The current replicated state ownership
and encoding still cost too much. Running that representation on three cloud
machines would verify host separation without admitting an economical local
state shape.

The storage-layout fork tests a larger opportunity than a compact checkpoint
codec alone: whether the permanent object base can serve both transactional
lookup and analytical scan paths without maintaining a second full columnar
base.

## Non-decision

This RFC does not assert that all compacted levels should be columnar. It does
not make Parquet the database. It does not add schema meaning to opaque KV
values. It freezes an experiment that can retain a row base, admit a typed
columnar base, or admit different layouts for different namespaces and levels.

## Logical run contract

Every run consumes and reproduces the RFC-0002 logical stream:

```text
user key ascending
    -> commit version descending
    -> stable entry-kind order
```

The complete contract includes:

- point values and point tombstones;
- half-open range tombstones;
- commit version and transaction batch order;
- schema or codec identity when a typed payload is present;
- source txLog interval and authority generation;
- exact checksum and stored-byte length;
- deterministic visibility at every retained read version.

A physical candidate that cannot preserve one entry kind returns
`unsupported_format`. It cannot silently weaken the logical stream.

## Manifest closure

`RunManifestV1` is the proposed logical descriptor. It is not yet a stable wire
format.

```text
RunManifestV1
    generation
    covered_through_version O
    parent_manifest_digest
    range_map_epoch
    schema_references[]
    runs[]
        run_id
        level
        format_id + format_version
        min_key + max_key
        min_version + max_version
        entry_count + tombstone_count
        data_objects[]
        primary_index_objects[]
        optional typed_scan_objects[]
        capabilities[]
        source_txLog_interval
        stored_bytes + sha256
    manifest_sha256
```

Run capabilities are explicit:

- `point_get`;
- `ordered_scan`;
- `typed_projection`;
- `predicate_pruning`;
- `stable_row_id`;
- `range_tombstones`;
- `mixed_version_read`.

No reader infers a capability from a format name.

## Capability boundaries

Do not hide different economics behind one broad storage trait. The proposed
internal boundaries are:

```rust
trait PointRunReader;
trait OrderedRunReader;
trait TypedProjectionReader;
trait RunBuilder;
trait RunCompactor;
```

One implementation may satisfy several capabilities. The transaction and
recovery layers depend only on the logical point and ordered-read contracts.
DataFusion may select `TypedProjectionReader` when a run declares a compatible
schema.

## Write and compaction pipeline

```text
bounded transaction
    -> commit proxy batch
    -> conflict resolution
    -> quorum txLog synchronization
    -> COMMITTED at C
    -> mutable serving tail
    -> packed immutable L0 delta run
    -> merge and compaction
    -> new immutable compacted runs
    -> conditional manifest preparation
    -> closure validation
    -> authenticated activation at O
    -> retention-frontier advancement
```

Foreground commit acknowledgement performs no object operation. L0 packing
prevents one object per transaction. A publisher closes a delta pack on byte,
entry, or time bounds. Compaction rewrites immutable inputs into larger runs and
publishes one replacement manifest before old inputs become collectible.

The initial evaluation uses 8 MiB target run objects and 64 KiB row blocks or
1,024-row columnar blocks. These are frozen experiment parameters, not public
format constants.

## Point-read algorithm

For `get(key, T)`:

1. read the mutable RAM or SSD tail;
2. test L0 filters and primary indexes newest first;
3. test at most one non-overlapping run per compacted level;
4. map the key to a row position, stable row ID, or row capsule;
5. fetch only the required block or column pages;
6. merge point and range tombstones through `T`;
7. return the newest visible value or absence.

The kernel owns run fanout, index memory, and visibility. Consumers may not be
required to scan a file or dynamically convert the full compacted base into a
second KV store.

## Analytical-read algorithm

For a typed snapshot at `T`:

1. acquire the active manifest and schema references;
2. scan typed compacted runs through `O` or a declared partition watermark;
3. prune columns, blocks, and zones;
4. merge L0 runs and live changes through `T`;
5. preserve invalidation keys even when predicates filter after-images;
6. emit ordered Arrow batches through the existing DataFusion overlay.

Opaque values can expose only generic key, version, operation, and byte
columns. A PostgreSQL row-native or typed-table adapter may expose real columns.
A page-native PostgreSQL value remains an opaque byte payload unless the adapter
publishes a separate typed projection.

## Candidate layouts

### C0: indexed row-object control

Use the existing checksummed `OKVB` data block and `OKVI` sparse index. It is the
point-read control and has no typed projection capability.

### C1: indexed Parquet control

Sort typed rows by primary key, write bounded row groups, and publish a separate
primary fence index. Use asynchronous range reads for footer, metadata, and
selected row-group column chunks. This is the ecosystem-interoperability
control, not the expected point-read champion.

### C2: random-access columnar candidate

Use one pinned random-access columnar implementation. Vortex is the first
candidate, but it currently crosses the workspace compatibility boundary.
Vortex 0.43.0 supports Rust 1.88 but depends on Arrow 55.2. Vortex 0.85.0 uses
Arrow 58.3, matching objectKV, but requires Rust 1.95. It therefore cannot
become an in-process workspace dependency while objectKV claims a Rust 1.88
floor. The diagnostic may use an isolated Rust 1.95 helper with exact binary
and format identities. A toolchain-floor change is a separate decision. A
later Lance candidate is allowed only as a separately frozen subject.

### C3: hybrid columnar run

Use a primary fence index plus independently fetchable columnar blocks. The
primary index may carry a compact row capsule for full-row point reads while
analytical scans read typed columns. The capsule bytes count in all storage and
compaction accounting.

### C4: split analytical projection plus row sidecar

Store the exact MVCC value once in the existing indexed row-object sidecar.
Store only the typed columns required by the analytical contract in the
columnar object. The active manifest authenticates both objects and the nested
row manifest as one closure.

```text
manifested typed run
  -> row sidecar
       key, version, tombstone, complete value
       exact point and historical-read path
  -> columnar projection
       key, version, tombstone, typed analytical columns
       projected scan and DataFusion path
```

This is not a full row copy embedded in Parquet. Opaque payload bytes appear
once. Key, version, operation, and selected typed fields are duplicated because
they serve different access paths. Every duplicated byte, index, nested
manifest, and compaction rewrite counts toward admission.

## Local preflight, not an admission result

`[CODE-COMPLETE]` The eval runner now executes C0, C1, C3, C4, and a
request-coalesced Parquet control from one typed
MVCC history, one point oracle, and one ordered-scan oracle. Parquet reads use
an external key-to-row-group fence index. Every requested byte range expands to
64 KiB checksum blocks, verifies those blocks, and returns only the requested
slice. This prevents random access from bypassing the immutable-media integrity
contract.

One 1,024-key, 256-point, Rust debug-build preflight at seed 5701 produced:

| Subject | Point requests | Bytes per point | Point p99 | Projected rows/s | Stored/live | Compaction write amp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Indexed row object | 1 | 64,066 | 2.473 ms | 19,149 | 1.582x | 2.040x |
| Indexed Parquet | 10 | 1,047,296 | 44.519 ms | 35,868 | 1.523x | 1.987x |
| Hybrid Parquet plus row capsule | 4 | 697,344 | 30.047 ms | 32,621 | 3.046x | 3.948x |

All three returned the same canonical history digest, exact point results, and
exact final projection. These measurements are `[EVALUATING]`: they use a
small local-filesystem fixture, a debug build, one seed, warmed manifest and
format metadata, and no object-service latency. They select the next
mechanisms to test; they do not establish a performance curve or storage-layout
winner.

The early signal is useful. Generic Parquet projection improved the small scan
rate by 1.87x while its full-row point path multiplied requests by 10 and bytes
by 16.35x against the row control. The hybrid reduced request count but almost
doubled durable amplification. A columnar compacted base therefore needs a
different random-access mechanism, a sidecar row path, or typed-namespace-only
admission. Parquet alone is not the objectKV point-read format.

## Frozen local admission

`[EVALUATING]` The first full local admission compared C4 directly with C0
using a Rust release build, 16,384 keys, 4,096 point reads per sample, three
seeds, and three repeats. Candidate and control order alternated for every seed
and repeat. The source tree was dirty, so this is a mechanism admission and not
a reproducible metric receipt.

The thresholds are encoded in
`evals/suites/storage-layout-admission.toml`:

| Gate | Required | Observed |
| --- | ---: | ---: |
| point object-request ratio vs C0 | at most 1.00x | 1.000x |
| point response-byte ratio vs C0 | at most 1.05x | 1.000x |
| median point p99 ratio vs C0 | at most 1.25x | 1.033x |
| median projected-scan throughput vs C0 | at least 3.00x | 9.124x |
| stored/live amplification ratio vs C0 | at most 1.10x | 1.030x |
| compaction write-amplification ratio vs C0 | at most 1.10x | 1.035x |
| resident-index ratio vs C0 | at most 2.00x | 1.137x |

All nine candidate samples and all nine controls returned exact point and scan
results, the same per-seed canonical histories, complete manifested media, no
LIST dependency, no complete-object point read, and shared immutable branch
roots. Median C4 projected-scan throughput was 2,867,012 rows per second. Its
largest measured point p99 was 242.875 microseconds and its largest point
response was 64,835 bytes per operation.

Two non-alternating exploratory runs produced millisecond-scale point outliers.
Backend-duration instrumentation and the subsequent alternating 18-subject run
did not reproduce a stable layout-specific penalty. Repeats and alternating
order are therefore part of the frozen contract rather than an optional
benchmark refinement.

The local result admits C4 to clean-source GCS evaluation. It does not admit C4
as the universal objectKV format. Opaque KV ranges retain C0. Typed namespaces
may use C4 only if cold and warm GCS curves, split-closure recovery, and exact
DataFusion base-plus-tail execution also pass.

## Deterministic diagnostic history

The first diagnostic uses three fixed seeds and identical logical rows:

```text
keys:                         16,384
canonical live row bytes:        512
typed columns: key, tenant, category, quantity, updated_version
opaque payload bytes:             480
base version:                       1
delta cycles:                        4
updates per cycle:               12.5 percent of live keys
deletes per cycle:                1.0 percent of live keys
point reads per seed:             4,096
scan projection: tenant, category, quantity
target run object bytes:      8,388,608
row block bytes:                 65,536
columnar block rows:              1,024
seeds:                    5701, 5702, 5703
```

The generator emits one canonical logical digest. Every candidate must produce
the same point results, ordered snapshot rows, projected scan rows, delete
effects, and post-compaction digest.

## Evaluation sequence

### G4.11a.2d: diagnostic

Measure all candidates without an admission claim. Record:

- cold and warm point p50, p95, p99, requests, and response bytes;
- full and selective projected scan throughput and bytes;
- manifest, index, data, and total stored bytes;
- resident index bytes;
- build duration and ingest throughput;
- compaction write amplification and resulting run count;
- branch creation bytes and shared-prefix bytes;
- DataFusion base-plus-tail duration and peak memory.

The diagnostic freezes the dataset, implementation revisions, machine, cache
states, and complete-media accounting. It does not select thresholds after
seeing a tuned candidate.

### G4.11a.2: admission

After the diagnostic, freeze one primary metric for each independent lane and
rerun unchanged implementations. A candidate is eligible only if every
correctness and recovery gate passes. Tuning after thresholds are frozen creates
a new candidate revision and receipt.

### G4.11a.3: GCS confirmation

Run only the eligible row control and candidate on GCS. Record actual object
requests, bytes, latency, stored media, and estimated request cost. Local file
latency cannot admit an object-read architecture.

`[CODE-COMPLETE]` `storage-layout-gcs-admission-v1` scopes every subject, seed,
and repeat below one immutable run prefix and alternates candidate order. The
suite is frozen. `[EVALUATING]` The objectKV-dev project and bucket now execute
bounded GCS canaries, but this full suite has not produced a receipt. Its serial
cloud request path exceeded the useful canary budget and requires bounded
parallel scheduling plus OTel before the complete curve runs.

## Frozen diagnostic hard gates

Every subject must satisfy:

1. exact point results at every requested read version;
2. exact ordered scan results and tombstone effects;
3. exact post-compaction logical digest;
4. one active manifest identifies the complete object closure;
5. no LIST dependency on point or scan reads;
6. every fetched range is checksum-covered;
7. complete media includes data, index, manifest, delete, and row-capsule bytes;
8. point-read work never scans the complete object;
9. branch creation never copies an unchanged immutable run;
10. all candidates complete within the frozen 240-second local budget.

The Parquet full-file point-read poison must fail gate 8. A hybrid accounting
poison that omits row-capsule bytes must fail gate 7. A columnar invalidation
poison that applies the predicate before delete suppression must fail gate 2.

## Decision boundary

Admit the manifested multi-layout LSM only if at least one candidate:

- preserves the generic ordered-KV contract;
- keeps point requests, transferred bytes, and p99 close to the indexed row
  control;
- materially improves projected scan throughput or bytes;
- reduces total durable duplication relative to separate row and analytical
  bases;
- bounds primary-index RAM and overlapping-run fanout;
- keeps compaction amplification within its frozen lane threshold;
- reconstructs exactly from `ManifestedObjectState(O) + txLog(O, C]`.

If no columnar candidate clears point and storage gates, keep the row-object
transactional base and derived analytical artifacts. If a columnar candidate
passes only for typed namespaces, admit it above the opaque KV layer. If the
row control itself cannot clear storage and GCS economics, the objectKV native
authority remains subject to the TiKV or FoundationDB pivot rule.

## C5: columnar main with a disposable RangeEngine overlay

The split sidecar is not the only way to preserve point behavior. C5 follows
the HANA unified-table and hybrid-column-store pattern more directly:

```text
bounded row L1 delta
    -> append-friendly column L2 delta
    -> immutable columnar main fragments

RangeEngine access mode per primitive
    -> resident vector, dictionary, index, or bitmap
    -> paged object ranges through RAM or NVMe cache
```

The durable main stores typed columns and the opaque value column once. A
resident primary index maps keys to stable row IDs. Point reads gather the
required slots; scans stream projected vectors. The same primitive can be
page-loaded or fully resident without conversion. The row L1 delta is bounded
and reconstructable from the retained txLog, so it is not a durable duplicate.

C5 must measure isolated cold points, working-set fill, repeated warm points,
projected scans, empty-overlay restart, full closure reconstruction, storage,
compaction, resident metadata, and branch sharing. Warm-cache success cannot
compensate for an unbounded cold path. Projected scans must not fetch the opaque
payload column.

The owning research record is
`docs/research/hana-datafusion-columnar-range-engine-2026-08-26.md`.

`[CODE-COMPLETE]` C5 now has compact binary range metadata, independently
checksummed projection stripes and payload pages, a bounded disposable cache,
projection-only scans, and exact empty-cache reconstruction. `[EVALUATING]`
Dirty-tree local run `49d6cd06` passed the frozen gates at 1.982x point
requests, 0.353x point bytes, 0.839x point p99, 4.718x scan throughput, 1.010x
storage amplification, 1.010x compaction amplification, and 1.170x resident
index size relative to the indexed-row control. Warm replay and projected-scan
payload requests were both zero. This admits C5 to a real Arrow/DataFusion
source and remote GCS comparison, not to `[VERIFIED]` status.

`[CODE-COMPLETE]` The first direct DataFusion source now uses
`RangeStripeTableProvider` and `RangeStripeExec` over the same `OKCP` ranges.
Point and scan access intentionally use different request geometry:

```text
point path -> one approximately 7.8 KiB stripe
scan path  -> one at most 256 KiB coalesced range
           -> verify every nested stripe
           -> emit one at most 128-row Arrow batch at a time
```

`[EVALUATING]` Dirty release-local run `a7d4f3bf` returned exact SQL aggregates,
zero payload requests, 54 projection GETs, a 257,506-byte maximum fetch buffer,
a 1,646-byte maximum Arrow batch, and 2,543,552 median source rows per second.
The same-suite one-stripe control `d788c75f` issued 1,761 GETs and reached
1,246,835 rows per second. Coalescing therefore used 0.0307x requests and
reached 2.040x throughput without increasing transferred projection bytes. A
payload-prefetch poison `b4fe7c11` added 1,761 requests, tripled read bytes, and
reduced throughput to 820,215 rows per second.

This admits dual-granularity range fetching to exact base-plus-tail and GCS
evaluation. It does not prove complete-query memory, live-tail invalidation,
parallel range scheduling, or cloud latency.

## CloudJump III tiering constraint

CloudJump III is a production InnoDB page-tiering system, not a columnar
engine. Its hierarchy is DRAM, volatile direct-attached SSD, durable
network-attached ESSD, and versioned 2 MiB objects in OSS. WAL protects dirty
pages until they reach the durable storage tier. The durable OSS Buffer then
combines 16 KiB page updates before asynchronous object publication.

This evidence strengthens three parts of C5:

1. Point, scan, and publication granularity must remain independent.
2. RAM and local SSD caches remain disposable and outside durability claims.
3. Cache admission, fast-tier ratio, skew, and background publication debt
   require explicit curves.

It also adds one falsifier. objectKV currently proposes a quorum `txLog` plus
disposable materialization state where CloudJump III uses WAL plus durable page
images on ESSD. C5 is not admitted until retained-log size, replay work,
foreground p99, and recovery remain bounded through object-store slowdowns
without that durable page-image buffer. If they do not, add a durable
range-image buffer or retain a mature hot durable kernel.

The first executable ablation keeps the default undecided. Dirty release-local
runs at 20 percent cache and Zipf alpha 1.4 show ghost two-chance at a 74.46
percent post-scan hit ratio versus 71.34 percent for full admission, with 16.2
percent fewer post-scan requests. A one-seed real-GCS canary shows 42.19 versus
32.03 percent and 20.5 percent fewer post-scan GCS requests. These results
admit cache policy as a material design axis. They do not admit the policy or
the C5 architecture without the complete ratio, skew, phase-shift, durability,
and recovery curves.

The corresponding evaluation sweeps fast-tier ratios of 5, 10, 20, 30, 40,
and 50 percent; Zipf alpha from 0.8 through 2.0; full, first-reference-discard,
and ghost two-chance admission; publication units from 256 KiB through 8 MiB;
and object-store pause, slowdown, and unknown-response failures. The
all-fast-tier subject is a same-durability control. The detailed review is
`docs/research/cloudjump-iii-objectkv-review-2026-08-26.md`.

## External evidence

- Apache Paimon uses an LSM for primary-key lake tables, but its PFile proposal
  documents the scaling cost of converting columnar files for KV lookup:
  <https://paimon.apache.org/docs/1.1/primary-key-table/overview/> and
  <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=311628201>.
- RisingWave uses row-based Hummock for low-latency point lookup and frequent
  updates, with columnar storage for analytical workloads:
  <https://docs.risingwave.com/store/overview>.
- Lance specifies immutable manifests, row addressing, deletion files, and
  independent indexes: <https://lance.org/format/table/>.
- Vortex provides an extensible random-access columnar format:
  <https://docs.vortex.dev/concepts/file-format>.
- CloudJump III places page admission and write routing inside InnoDB, uses a
  volatile SSD BPE plus a durable ESSD object-write buffer, and publishes
  versioned 2 MiB OSS objects asynchronously:
  <https://dl.acm.org/doi/10.1145/3788853.3803084>.

## Not claimed

- a stable public run or manifest wire format;
- a production Vortex, Lance, or Parquet dependency;
- columnar point-read superiority;
- PostgreSQL row-native semantics;
- GCS performance or economics;
- admission of G4.11b or the native transaction authority.
