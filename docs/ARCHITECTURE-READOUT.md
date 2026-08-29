# objectKV architecture readout

Status: `[EVALUATING]`

Updated: 2026-08-27

Canonical layered review set:
[`docs/architecture/README.md`](architecture/README.md). It owns the compact
fabric-to-storage map, layer contracts, RangeEngine profiles, and evidence
index. This file retains the detailed narrative and historical measurements.

Canonical living visual:
[`docs/artifacts/objectkv-architecture/objectkv-architecture.html`](artifacts/objectkv-architecture/objectkv-architecture.html).
This Markdown file retains the detailed prose record. The HTML artifact owns
the current implementation map and infrastructure-evidence boundary.

## The answer

objectKV now has one continuous application-facing slice, but it is not yet a
complete Cell v0. The implemented slice proves that one public range can select
an authoritative object base, recover the exact retained transaction suffix,
commit through replicated authority, survive serving-process replacement, and
perform a bounded point read from local object storage or real GCS.

The architecture is converging on a versioned state kernel with this invariant:

```text
State at C = immutable object state through O + ordered txLog suffix (O, C]
```

RAM and SSD are replaceable serving projections. Object storage holds immutable
capacity state. The quorum txLog is the low-latency durability boundary. OLTP,
OLAP, Redis-like, and virtual-filesystem consumers share logical versions and
history, but use read layouts optimized for their workload.

## Bottom-up construction

```text
application adapters
  PostgreSQL | Redis-like | search | virtual filesystem | DataFusion
                                |
public objectKV client and range API
  read version | transaction | point/range read | snapshot | okv-log
                                |
serving compute
  native RAM or SSD engine | index cache | selected object-block fetch
                                |
transaction and recovery authority
  generation | conflict resolution | commit ordering | replicated txLog
                                |
object publication
  immutable row/column runs | indexes | manifests | snapshots | GC roots
                                |
provider-neutral object boundary
  named PUT | exact GET | ranged GET | conditional metadata | no LIST authority
                                |
S3-compatible object storage | GCS | local protocol fixtures
```

Only the middle single-range path is continuous today. Several lower-layer
contracts are stronger in isolation than the integrated public path.

## 1. Provider-neutral object boundary

`[VERIFIED]` `okv-object` owns named-object semantics and provider
instrumentation. Its `Backend` contract exposes named put, full or ranged get,
delete, and list. Object identities carry revision tokens, length, and digest.
The GCS adapter uses standard Google credentials and bucket configuration.

`[CODE-COMPLETE]` A prefixed backend scopes every experiment below one
traversal-free object namespace. This allows identical logical object names in
independent runs without turning LIST into authority.

Current immutable transactional base:

```text
manifest
  -> row segment reference
       -> sparse checksummed index object
       -> checksummed data object
            -> independently checksummed blocks
```

A point read opens the named manifest, chooses one segment by key bounds, reads
its sparse index, and requests only the selected byte range from the data
object. The caller does not list the bucket or restore every object.

What is not integrated yet:

- `[FUTURE]` automatic objectification from a running cell;
- `[FUTURE]` safe object garbage collection under snapshots, branches, and
  retained reads;
- `[EVALUATING]` the permanent transactional format choice between opaque row
  runs and typed multi-layout runs;
- `[FUTURE]` repair from a corrupt or unavailable immutable object.

## 2. Ordered history and stable media

`[CODE-COMPLETE]` `okv-log` is the storage-independent ordered-record algebra.
It owns append, suffix replacement, bounded reads, purge identity, and replay
validation. It is not independently durable.

`[VERIFIED]` `okv-wal` applies that algebra to checksummed stable journals.
`okv-consensus` places the journals behind OpenRaft and exposes replicated
authority processes.

The current authority split is:

```text
GenerationAuthority
  -> active generation
  -> transaction-system identity
  -> logical txLog root
  -> fencing credential

TransactionAuthority
  -> ordered commit version
  -> batch order within one version
  -> conflict outcome
  -> retained transaction stream

PublicationAuthority
  -> prepared immutable closure
  -> visible manifest root
  -> exact publication identity
```

`[CODE-COMPLETE]` Batch entries use the versionstamp
`(commit_version, batch_order)`. This matters because a recovery page may end
inside several transactions committed at one scalar version. A scalar-only
cursor would skip committed mutations.

The current integrated smoke starts three publication-authority and three
transaction-authority OS processes. They use separate local journals but share
one physical machine. This proves process replacement and logical quorum
behavior, not independent failure domains.

What is not integrated yet:

- `[FUTURE]` independent stable media for the public range path;
- `[FUTURE]` bounded txLog pop driven by a live object frontier;
- `[FUTURE]` sustained-overload backpressure tied to `C - O` debt;
- `[FUTURE]` repair, voter replacement, and multi-generation recovery as one
  long-running cell service.

## 3. Publication and object frontier

The object frontier `O` is the highest commit version whose complete state is
recoverable from an authenticated immutable closure. Versions after `O` remain
in the retained txLog.

```text
commits through C
      |
      +-> retained txLog (O, C]
      |
materializer chooses frontier O
      -> build immutable runs and indexes
      -> write exact named objects
      -> verify complete closure
      -> publish manifest root at O
      -> allow txLog pop below safe retention floor
```

`[CODE-COMPLETE]` The repository has the individual publication, ambiguous PUT,
lost response, frontier, snapshot, and compaction contracts needed for this
pipeline.

`[FUTURE]` The public `SingleRange` experiment still uses the eval controller as
the materializer and publisher. There is no continuously scheduled storage
materializer that advances `O` while serving live traffic.

## 4. Disposable serving compute

The current public serving object is `okv::SingleRange`.

```text
SingleRange
  generation credential
  published row manifest at O
  retained-stream cursor through C
  RAM MVCC tail overlay
  sparse-index cache
  optional fully hydrated object cache
  bounded request and resident-byte counters
```

Open performs a generation, publication, generation sandwich. It rejects an
inactive or changing generation, verifies the published manifest, starts its
cursor after `O`, and catches up to one frozen transaction high watermark.

Point read order:

```text
read key at T
  -> reject T outside [O, recovered C]
  -> consult RAM tail for latest point or range-clear action at T
  -> locate the immutable range segment
  -> if a complete serving image is active, read RocksDB or RAM locally
  -> otherwise use the cached or fetched sparse index
       -> issue at most one selected data range GET
  -> return Value | Tombstone | Absent
```

Commit order:

```text
request identity + read/write conflicts + mutations
  -> generation-fenced transaction-authority call
  -> quorum commit outcome
  -> catch local range through committed version
  -> return commit receipt with local read coverage
```

The serving process is disposable because authoritative state is not defined by
its RAM or local scratch. The eval kills the first process, starts a replacement
with empty scratch, and requires byte-identical logical replay.

The current bounds are explicit:

- one range;
- exact point reads only at the public boundary;
- RAM tail grows until object frontier advancement and safe pop exist;
- sparse indexes remain cached without eviction;
- full hydration is explicit and optional;
- no serving lease or external routing epoch;
- the RocksDB image activates the complete assigned range, with no partial
  admission or incremental base refresh yet.

`RangeEngine` remains a useful name for the eventual runtime that owns a set of
assigned range images, caches, catch-up, reads, and materialization work.
`SingleRange` is currently a library object inside the eval worker, not that
long-running service.

## 5. Complete Cell v0, proposed shape

The cell is the transaction boundary. Cells do not synchronize one global
version space and do not execute cross-cell transactions.

```text
client session
  |-> ReadVersionService
  |-> RangeMap -> RangeEngine reads
  |-> CommitProxy -> Resolver partitions -> txLog sets
                                      |
                                      +-> RangeEngine catch-up
                                      +-> StorageMaterializer -> object frontier

cell control
  GenerationAuthority | membership | assignments | recovery | rate limits
```

`[FUTURE]` A tenant database may transact across arbitrary ranges inside one
cell. A metacluster only maps tenants to cells and moves them with snapshot plus
tail migration. It is not part of transaction execution.

The missing complete-cell services are:

| Service | Current status | Next proof |
|---|---|---|
| Read-version service | `[PROPOSED]` | stable snapshot selection under concurrent commit |
| Commit proxy | `[CODE-COMPLETE]` isolated batching contract | integrate with public client and range reads |
| Resolver | `[CODE-COMPLETE]` model and authority contract | partition only after single-range admission |
| txLog set | `[CODE-COMPLETE]` local-process OpenRaft | independent media and loss-of-voter run |
| RangeEngine | `[CODE-COMPLETE]` one `SingleRange` with object, RAM-tail, and RocksDB-image paths | optimized named-device SSD curve, then RAM provider |
| Storage materializer | `[CODE-COMPLETE]` isolated frontier contracts | advance `O` continuously under writes |
| Range map and distributor | `[PROPOSED]` | split and movement without copying immutable base |
| Ratekeeper | `[FUTURE]` | bound tail, publication, memory, and compaction debt |

## 6. OLTP, OLAP, Redis-like, and filesystem surfaces

The product surfaces share versioned logical state, not one physical read
layout.

| Surface | Fast read layout | Shared kernel value | Current status |
|---|---|---|---|
| OLTP | key-addressable RAM or SSD range image | serializable commit, snapshots, object recovery | `[EVALUATING]` one-range point path |
| Redis-like | RAM keys, TTL index, counters, okv-log streams | explicit latency and durability profiles | `[PROPOSED]` adapter |
| Virtual filesystem | path and inode metadata in okv; payload chunks in objects | atomic namespace, branches, cheap clones | `[PROPOSED]` adapter |
| OLAP | columnar object base plus exact row-change tail | one version and snapshot history | `[CODE-COMPLETE]` isolated DataFusion source; live public-kernel integration is `[FUTURE]` |

For exact HTAP at query version `T`:

```text
logical partition at T = columnar base at W + latest row changes in (W, T]
```

The columnar watermark changes query cost, not freshness. The tail must include
keys that invalidate older base rows. DataFusion can scan and join the resulting
Arrow batches after the overlay restores exact logical state.

See `docs/PRODUCT-WORKLOADS.md` for product boundaries and W0 through W7
workload lanes.

## Current performance evidence

These receipts measure different isolated mechanisms and must not be blended
into one system claim.

| Mechanism | Observation | Evidence status |
|---|---|---|
| Public SingleRange, local objects | 108.627 ms recovery to first correct read in the adjacent control; 12 of 12 gates passed | `[EVALUATING]`, one dirty local sample |
| Public SingleRange, GCS objects | 756.950 ms recovery to first correct read; 6,177 object response bytes; 12 of 12 gates passed | `[EVALUATING]`, one dirty Mac-to-GCS sample |
| Public SingleRange, RocksDB serving image | 824,252 reads/s and 1.583 microseconds p99 over 100,000 reads; 0 post-activation object operations | `[EVALUATING]`, dirty debug build, 256 keys, named local Apple NVMe without isolated-load control |
| Public SingleRange versus direct RocksDB, GCP R0 | 516,973 versus 702,142 reads/s; 2.482 versus 1.749 microseconds median p99; 15 samples each; 0 measured object operations | `[VERIFIED]` mechanism and comparison validity; GP3.1 is `[EVALUATING]` after a 26.37% throughput and 41.91% p99 miss |
| Optimized public SingleRange versus direct RocksDB, GCP R0 AB and BA | 575,498 versus 713,304 and 573,999 versus 717,362 reads/s; p99 ratios 1.353x and 1.300x; 15 samples per subject and order | `[VERIFIED]` optimization and comparison validity; throughput entered the envelope, executable p99 failed twice, GP3.1 remains `[EVALUATING]` |
| Native resident engine versus bare owned-value direct RocksDB, GCP R0 AB and BA | 589,717 versus 701,119 and 587,199 versus 710,184 reads/s; p99 ratios 1.210x and 1.272x; 15 samples per subject and order | `[VERIFIED]` historical unmatched-topology result; its permanent pivot conclusion is superseded by D56 and the matched control below |
| Native resident engine versus topology-matched owned-value direct RocksDB, GCP R0 AB and BA | 656,662 versus 722,443 and 660,783 versus 718,492 reads/s; p99 ratios 0.913x and 0.913x; 15 samples per subject and order | `[VERIFIED]` GP3.1 single-range mechanism admission; all throughput, p99, correctness, object-operation, identity, and OTel constraints passed twice |
| Native resident engine versus topology-matched direct RocksDB, GCP R0 at 8 clients | Throughput ratios 0.8798x and 0.8734x; p99 ratios 1.1842x and 1.1220x; 15 samples per subject and order | `[VERIFIED]` GP3.1.1 8-client admission; all constraints passed twice |
| Native resident engine versus topology-matched direct RocksDB, GCP R0 at 32 clients | Throughput ratios 0.8803x and 0.8906x; p99 ratios 1.1072x and 1.1478x; 15 samples per subject and order | `[VERIFIED]` GP3.1.1 32-client admission; all constraints passed twice |
| Native resident direct-read preflight versus matched direct RocksDB, GCP R0 | 122,702 versus 117,440 reads/s; p99 ratio 0.9651x; CPU/read ratio 1.0337x; physical bytes/read ratio 0.9982x | `[VERIFIED]` direct-read mechanism and Linux physical attribution only; one smoke sample per subject, not a performance admission |
| Commit-proxy batch32 | 1,157.369 median tx/s and 76.101 ms maximum p99, 6.356x one-entry control | `[EVALUATING]`, dirty single-host isolated contract |
| DataFusion columnar range source | 2.544M rows/s, 54 projection requests versus 1,761 one-stripe requests | `[EVALUATING]`, dirty release-local isolated contract |
| Prior R0 GCS infrastructure smoke | 6.925 s wall time, 24 gates, all three OTel signals | `[VERIFIED]` infrastructure plumbing, not performance |

The new GCS receipt establishes one useful fact: the public kernel can recover
without downloading the 42,305-byte immutable closure. Its replacement fetched
6,177 object bytes along the named manifest, index, and selected-block path.

The second matched R0 execution tested one variable: complete-image reads no
longer locate and clone a row-object reference first. Candidate throughput
improved by 11.18 percent and entered the 20 percent envelope in both process
orders. P99 improved by less than 1 percent and failed its executable gate in
both orders. The remaining miss is a software-composition result, not an SSD
cache-pressure result. The 756.950 ms GCS timer separately includes process
start, authority reads, txLog catch-up, and three remote object operations from
an uncontrolled client machine.

The third execution corrected the direct control to return owned values and
built the native resident boundary, but it still compared a full six-process
native recovery path with a bare direct RocksDB process. The fourth execution
put both read subjects inside the same recovered topology. Native passed the
throughput and p99 envelope in both orders. This admits one read boundary; it
does not admit replicated commit, range distribution, or a complete cell.

## Performance curves that decide the project

| Curve | Desired shape | Falsifier |
|---|---|---|
| Hot point read | RAM near in-memory control; SSD near direct RocksDB after network cost | wrapper, MVCC, or routing overhead dominates p99 |
| Commit | group commit approaches matched three-voter control while object PUT stays asynchronous | object work enters the acknowledgement path or native authority remains structurally slower |
| Cold point read | fixed manifest/index/block work independent of database bytes | first read grows with total database size or requires full restore |
| Recovery | grows with metadata plus retained suffix, not full durable bytes | worker cannot serve until complete hydration |
| Tail debt | bounded by materialization and explicit backpressure | RAM, log bytes, or recovery time grows without a hard ceiling |
| HTAP freshness | exact results at T; cost rises predictably with `T - W` | missing invalidations, unbounded overlay memory, or OLTP interference |
| Branch creation | metadata-scale latency and bytes for shared history | branch requires physical copy proportional to database size |
| Economics | object capacity savings exceed added compute, request, and engineering cost | no material lifecycle, recovery, HTAP, or retained-byte advantage |

## Latest GCS evaluation

`[EVALUATING]` Run `6723ce8a-ea92-48d6-8020-60f12575fcb8` used real regional
GCS, required OTel, three publication processes, three transaction processes,
one killed worker, and one empty replacement. It passed all 12 hard gates in
7.254 seconds.

```text
GCS scratch closure: 14 objects, 84,610 bytes across run and replay
replacement object reads: manifest 1, index 1, range 1, full 0, LIST 0
replacement object bytes: 6,177
retained txLog: 7 reads, 4,403 response bytes, 5 in-batch cursor resumes
OTel: 2 logs, 1 trace span, 8 metric points
```

The adjacent local suite passed after the backend seam was added. Omitting OTLP
from the GCS profile fails before workload execution and creates no receipt.

## Program call

Continue the native objectKV lane. The topology-matched single-range read
boundary is admitted, while FoundationDB remains the semantic oracle and
fallback profile. Do not infer a distributed-system admission from a resident
read result.

Next sequence:

1. Persist one content-addressed resident fixture across native, control, A/B,
   and B/A, then run the 1 GiB cache-pressure curve with the verified matched
   direct-read treatment, CPU time, physical bytes, amplification, and object
   attribution.
2. Split the resident-kernel eval binary from the full DataFusion build and
   record the Rust toolchain rather than `unknown` in real-infrastructure
   receipts.
3. Run the native three-node replicated commit path against a same-durability
   control, including leader loss and recovery.
4. Keep FoundationDB current as the strict-serializability and lifecycle
   control while the native transaction plane is admitted one gate at a time.
5. Retain explicit cache sizing, CPU time, read amplification, and reusable
   content-addressed fixtures as required identities for later 10 GiB and 100
   GiB capacity points.
6. Admit RAM, multi-range, PostgreSQL, and HTAP only after the native commit
   path is stable.
