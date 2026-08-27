# CloudJump III implications for objectKV

Status: `[EVALUATING]` primary-source architecture review. This document does
not convert CloudJump III's production claims into objectKV evidence.

Primary source: Zongzhi Chen et al., "CloudJump III: Optimizing Cloud Databases
for Tiered Storage," SIGMOD Companion 2026, pages 266-280,
DOI [10.1145/3788853.3803084](https://dl.acm.org/doi/10.1145/3788853.3803084).
The full CC BY paper was read from the
[first author's mirror](https://desert0616.github.io/pdf/CloudjumpIII.pdf),
SHA-256 `b6cded5c7c40056cdebfb05d116876570d2fd8bdeca968eedc9b0c501dc6e7b3`.

## Clarity

Question: Does CloudJump III validate the objectKV columnar RangeEngine
direction?

Punchline: It validates engine-integrated tiering and a durable foreground
buffer before asynchronous object publication, but it does not validate a
columnar source of truth, an objectKV transaction authority, or object-only
OLTP.

Counter: CloudJump III's results may depend on InnoDB page semantics, a mature
WAL and recovery implementation, durable network block storage, moderate cache
ratios, and workload skew that a generic ordered KV cannot assume.

Next: retain the C5 columnar experiment, then add CloudJump-style cache-ratio,
skew, admission, write-combination, tail-latency, and recovery curves before
admitting its serving architecture.

## What the paper actually builds

CloudJump III is an InnoDB page-tiering system for an Alibaba Cloud
MySQL-compatible service. It is not a columnar database. The paper scopes
itself to transactional OLTP storage without optimizer changes or query hints.

```text
Cache tier, volatile
    InnoDB buffer pool, DRAM
        -> BPE, direct-attached SSD, clean-page cache

Storage tier, durable
    OSS Buffer, network-attached ESSD
        -> OSS, versioned 2 MiB objects
```

The page and object geometry is explicit:

```text
InnoDB page       16 KiB
OSS object         2 MiB
pages per object     128
```

The direct-attached SSD BPE is disposable. The network-attached ESSD OSS Buffer
is durable. A dirty page is protected by WAL until it reaches the durable
storage tier. The corresponding WAL can then be trimmed. The OSS Buffer
coalesces page updates and later publishes a complete 2 MiB versioned object.
Remote GET may fetch one 16 KiB page by range or prefetch the complete 2 MiB
object.

This distinction is load-bearing. CloudJump III does not acknowledge a durable
transaction on DRAM or local SSD and hope that asynchronous object publication
finishes. It retains an independent durable path.

## Observed mechanisms

### Placement is inside the engine

Placement decisions happen at buffer-pool eviction and flush, where the engine
can see page age, page type, table identity, temporary-table status, residency,
and per-table quotas. Placement remains orthogonal to MVCC and WAL semantics,
but participates in recovery and snapshot protocols.

Three mechanisms are especially relevant to objectKV:

1. The BPE uses delayed, reuse-aware admission. First-reference pages enter a
   bounded ghost list without page data. Reuse makes them eligible for SSD.
2. The durable OSS Buffer combines dirty 16 KiB pages within one 2 MiB object,
   applies watermarks and rate limits, then performs an asynchronous object PUT.
3. Temporary data stays out of durable object writeback, while DDL and BLOB
   traffic can bypass shared queues so it does not stall transactional work.

### Durability and object publication are separate

Local durable writes use metadata-before-data ordering to record intent. Remote
publication sends data before marking metadata clean. Recovery either replays
redo for missing data or repeats a remote flush when data exists but metadata
is stale. Object versions and per-table metadata bind snapshots and recovery.

CloudJump III therefore has two distinct mechanisms:

```text
transaction durability
    WAL plus durable ESSD buffer

object permanence and backup
    versioned OSS objects plus snapshot metadata
```

This closely matches objectKV's separation between quorum-durable `txLog`
state and immutable object publication, although CloudJump III's durable ESSD
buffer stores page images while objectKV currently proposes replayable
mutations plus disposable materialization state.

### The production curve has conditions

The experiments use 50 GiB and 5 TiB datasets, 16 and 128 clients, stabilized
warm caches, 5 to 50 percent fast-tier coverage, and Zipf skew from 0.8 to 2.0.
The reported knee is usually at 20 to 30 percent fast-tier coverage when skew
is moderate. Low-skew workloads with a 10 percent or smaller cache remain well
below the all-ESSD control.

For the Voter workload, the tiered system reports about 1.5x the all-ESSD p99
at 16 and 64 threads and 79 to 80 percent of all-ESSD throughput at 64 threads.
For the Game workload, throughput stays within plus or minus 5 percent of the
all-ESSD control. These are strong production results, but not evidence that
remote object misses are cheap. They show that a sufficiently effective fast
tier can make misses uncommon and shift publication work off the foreground
path.

The cache-policy ablation is unusually useful. Ghost two-chance reaches 36,422
TPS, 12 percent above full admission, with a 72.7 percent hit ratio and about
91 percent fewer BPE IOPS. This directly supports measuring admission policy,
not merely cache capacity.

Snapshot creation is 0.64 seconds versus 0.52 seconds for all ESSD and 53.73
seconds for OSS only. Recovery is 52.26 seconds versus 1.15 seconds for all
ESSD and 57.64 seconds for OSS only. The design preserves object-level recovery
economics; it does not preserve all-SSD recovery latency.

## Mapping to objectKV

| CloudJump III | objectKV analogue | Current status | Difference |
| --- | --- | --- | --- |
| InnoDB DRAM buffer pool | RAM RangeEngine profile | `[PROPOSED]` | objectKV is not tied to one SQL engine |
| Volatile local SSD BPE | SSD RangeEngine profile | `[EVALUATING]` | ghost admission is code complete; SSD profile and full curve remain open |
| WAL | replicated `txLog` | `[EVALUATING]` | objectKV also uses it to rebuild disposable serving state |
| Durable ESSD OSS Buffer | bounded objectification buffer plus retained `txLog` | `[PROPOSED]` | objectKV has not proven it can omit durable page-image staging |
| 2 MiB OSS block | immutable run or object publication unit | `[EVALUATING]` | C5 separately uses approximately 7.8 KiB point stripes and 256 KiB scan GETs |
| Versioned OSS object | immutable manifested object | `[EVALUATING]` | objectKV adds content identity, root publication, branching, and GC |
| Per-table metadata | manifested range and layout metadata | `[EVALUATING]` | objectKV needs tenant and range ownership plus exact closure verification |
| InnoDB row pages | C0 row blocks or C5 columnar stripes | `[EVALUATING]` | CloudJump III does not evaluate columnar storage |
| MySQL execution | PostgreSQL and DataFusion consumers | `[PROPOSED]` | CloudJump III has no exact HTAP base-plus-tail path |

## Decisions

### D1. Keep cache state disposable

Retain the RAM and local-NVMe RangeEngine profiles as performance state, never
durability state. A RAM-only serving profile is safe only when the replicated
`txLog` and immutable object root still satisfy the recovery equation.

Optimization: cheap replacement, fast handoff, independent compute scaling.

Tradeoff: every cache miss and cold restart must be paid by bounded object and
tail reads.

### D2. Do not silently equate `txLog` with the OSS Buffer

CloudJump III persists page images to durable ESSD before trimming WAL.
objectKV proposes retaining mutations in the quorum `txLog` until immutable
object publication advances. These are not automatically equivalent.

The objectKV design is admitted only if it proves:

```text
bounded retained txLog under sustained ingest and object-store slowdown
bounded replay work for one assigned range
bounded memory or SSD materialization state
exact restart without a durable page-image cache
```

If those gates fail, add a durable range-image buffer or use TiKV,
FoundationDB, or PostgreSQL as the hot durable kernel.

### D3. Separate publication, point-read, and scan geometry

CloudJump III's 2 MiB object is a publication and prefetch unit, not its point
read unit. C5 now expresses the same principle with smaller independently
verified structures:

```text
publication object   -> many checksummed stripes and payload pages
point read           -> one approximately 7.8 KiB projection stripe
scan read            -> bounded coalesced range, currently at most 256 KiB
```

The remote publication unit should be swept from 256 KiB through 8 MiB. It
must not force the same granularity on cold point reads or Arrow batches.

### D4. Add placement classes above the ordered-KV contract

CloudJump III benefits from engine-visible semantics that a generic byte-key
kernel does not have. objectKV should not put SQL types into the transaction
kernel, but serving-model adapters may attach bounded range-level placement
classes:

```text
default
index_or_metadata
temporary
large_sequential
analytical_projection
```

These classes may control cache admission, prefetch, publication queues, and
quotas. They must not change transaction correctness, version visibility, or
recovery requirements.

### D5. Admit cache policy through a curve, not one warm-cache number

The C5 cache now has full-admit, never-admit, and bounded ghost two-chance
subjects. Dirty local and one-seed GCS receipts show a material policy effect.
The next serving evaluation must expand the same-history comparison across
cache ratio, skew, phase shifts, SSD, and concurrent remote scheduling.

## New evaluation gates

### T1. Fast-tier ratio and skew

```text
fast-tier ratios:  5, 10, 20, 30, 40, 50 percent
Zipf alpha:        0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0
operation mixes:   read-only, read-write, write-only
```

Record throughput, p50, p95, p99, hit rate, object requests and bytes, local
cache IOPS, publication debt, and cost per million operations. Include an
all-fast-tier same-durability control.

### T2. Cache admission ablation

Compare full admission, first-reference discard, and bounded ghost two-chance.
Add a long projected scan between hot point phases. The admitted policy must
avoid scan pollution without starving a newly hot range.

### T3. Publication-unit sweep

Sweep 256 KiB, 512 KiB, 1 MiB, 2 MiB, 4 MiB, and 8 MiB immutable publication
units. Measure object PUT rate, read-modify-write bytes, dirty-page merge ratio,
write amplification, object count, cold point bytes, and projected scan rate.

### T4. Durable-buffer falsifier

Run sustained commit load while object publication is paused, slowed, and made
intermittently unknown. The candidate must bound retained `txLog`, memory, SSD,
replay duration, and foreground p99. A control that acknowledges from volatile
cache or trims `txLog` before authenticated object publication must fail.

### T5. Crash, recovery, and empty-worker replacement

Kill RAM and local-NVMe RangeEngines at every transition, then reconstruct from
the manifested object root plus the retained `txLog`. Compare against an
optional durable range-image buffer. Record first-read latency, range-ready
time, full recovery time, bytes, and exact version digest.

### T6. Engine-aware routing ablation

Mix point OLTP, long scans, temporary spill, and large sequential values.
Compare a neutral kernel with placement classes. The placement-aware subject
must preserve identical transactions while reducing cache pollution and
foreground queue interference.

### T7. Exact HTAP overlay

CloudJump III does not address this claim. Run the C5 DataFusion base through
one target version with 0, 0.1, 1, 5, and 10 percent live-tail coverage. Record
exact invalidation, tail bytes, complete-query memory, time to first batch,
throughput, and materialization crossover.

## Program check-in

CloudJump III strengthens the overall program, but narrows the honest claim.
The credible near-term architecture is:

```text
strict transactions
    -> quorum-durable txLog
    -> disposable RAM or SSD RangeEngine
    -> asynchronous immutable object publication
    -> point-sized and scan-sized reads over one manifested layout
    -> exact DataFusion base-plus-tail execution
```

It does not support an object-store WAL or an object-only low-latency OLTP
path. It makes the durable-buffer and cache-admission gates more important. It
also confirms that the useful product boundary is not merely an object format.
The placement, write-combination, recovery, and observability control loop is
part of the storage engine.

The original objectKV direction remains viable as an experiment. The next
decision is whether retained quorum log plus disposable materialization can
replace CloudJump III's durable ESSD page buffer without losing bounded
recovery or economics. That question is not yet answered.

## Fable check-in

Fable's adversarial review agrees with the architectural mapping and tightens
the evidence boundary:

1. CloudJump III strengthens C5 tiering, but invalidates a repeated warm trace
   as cache-admission evidence.
2. The existing C5 source pushes projections only. It does not yet push
   filters or limits, expose parallel partitions, or prove exact base plus live
   tail.
3. The next remote curve needs a dataset materially larger than cache, cache
   ratios from 5 to 50 percent, Zipf alpha from 0.8 to 2.0, phase changes, and
   scan pollution.
4. The durability falsifier must compare retained `txLog` alone with a durable
   block or range-image staging control during delayed publication.
5. Multi-tenant isolation, failover warmup, publication and GC crash points,
   and total economics remain open.

The review therefore preserves C5 as an experiment, not as the default objectKV
storage architecture.

## First executable admission receipts

`[CODE-COMPLETE]` `columnar-cache-admission-v1` now compares full admission, a
never-admit negative control, and bounded ghost two-chance around an explicit
scan-pollution phase. The negative control is deliberately named
`never_admit_control`; first-reference discard plus a ghost is the
`ghost_two_chance` policy.

`[EVALUATING]` Dirty release-local runs over three seeds and three repeats at a
20 percent cache ratio and Zipf alpha 1.4 produced:

| Policy | Post-scan hit ratio | Post-scan object requests | Wall time |
| --- | ---: | ---: | ---: |
| Full admission | 71.34% | 8,829 | 2.734 s |
| Ghost two-chance | 74.46% | 7,398 | 2.498 s |

Ghost admission improved the hit ratio by 3.13 percentage points and reduced
post-scan requests by 16.2 percent. Every exact point, capacity, pollution,
policy-state, repeatability, schema, and budget gate passed. The receipts remain
inconclusive because the tree is dirty and OTel is disabled.
The four machine-readable receipts are retained under
`docs/artifacts/eval-receipts/cloudjump-cache-admission-2026-08-26/`.

`[EVALUATING]` The same mechanism then ran through the real GCS adapter in the
`doss-objectkv-dev` project and `us-central1` bucket. The bounded canary uses
one seed, 512 keys, 128 Zipf point operations, one scan-pollution phase, and a
20 percent cache ratio:

| Policy | Post-scan hit ratio | Post-scan GCS requests | Wall time | Run |
| --- | ---: | ---: | ---: | --- |
| Full admission | 32.03% | 161 | 75.06 s | `8574f64c` |
| Ghost two-chance | 42.19% | 128 | 67.14 s | `a1c6be8a` |

Ghost admission improved the hit ratio by 10.16 percentage points, reduced
post-scan requests by 20.5 percent, and reduced wall time by 10.6 percent. It
issued more requests during the pollution phase, 68 versus 37, because first
references remain remote. This is the expected tradeoff: pay during scans to
preserve the reusable hot set afterward.

An earlier 16,384-key, nine-repeat remote profile was stopped after five
minutes because it had reached only seed 5701, repeat 1. Its isolated objects
remain under `objectkv/evals/storage-layout/b853a433-...`. This is an eval-design
finding, not a storage result. Serial remote point loops are suitable for a
canary, not for the full cloud curve.

These receipts do not admit the architecture. They prove that the GCS adapter
and namespaced immutable layout execute, and that admission policy has a
measurable remote effect. Multiple seeds, a clean revision, OTel, larger
dataset-to-cache ratios, concurrent requests, phase shifts, and an all-fast-tier
same-durability control remain required.
