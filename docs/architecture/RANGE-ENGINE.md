# RangeEngine serving profiles

Status: `[EVALUATING]`. The RocksDB resident path has scoped verified metrics.
The three-switch RangeEngine composer described here is `[PROPOSED]`.

## The contract

RangeEngine is the disposable compute runtime for one or more assigned ordered
key ranges. It verifies assignment and version coverage, applies the recent
transaction tail, serves reads, manages local storage budgets, and rebuilds
from object state plus txLog after loss.

```text
┌─[ RANGEENGINE INPUT ]──────────────────────────────────────────────┐
│ generation G · assigned range R · object frontier O · applied A   │
│ read version T · txLog suffix · immutable manifest identity       │
└──────────────────────────────┬────────────────────────────────────┘
                               ↓
┌─[ RANGEENGINE ]────────────────────────────────────────────────────┐
│ bounded MVCC working overlay                                     │
│                                                                  │
│ base-serving switches, target contract requires at least one ON  │
│ [ RAM IMAGE ]   [ NVMe BLOCK CACHE ]   [ ROCKSDB IMAGE ]         │
│                                                                  │
│ coverage · admission · eviction · handoff · metrics              │
└──────────────────────────────┬────────────────────────────────────┘
                               ↓ miss or rebuild
┌─[ PERMANENT STATE ]────────────────────────────────────────────────┐
│ immutable object closure through O + durable txLog suffix (O, C] │
└───────────────────────────────────────────────────────────────────┘
```

Every process uses some working RAM. In this document, turning off `ram_image`
means no admitted base-value image in RAM. It does not mean the process uses
zero memory for metadata, the recent MVCC overlay, buffers, or request state.

## One state, several fabric views

`[PROPOSED]` The switches select physical serving mechanisms inside one
versioned RangeEngine state. They do not create independent consistency domains
for byte KV, objects, ranges, logs, and columnar access.

```text
one okv-fabric session and transaction envelope
  -> byte KV | structured object | ordered range | log | columnar
  -> read | upsert | atomic_modify | delete | ordered_scan
  -> one MVCC overlay and coverage map
  -> RAM image | native NVMe cache | RocksDB image
```

Current Garnet v2 provides useful evidence for this shape: string, object,
unified, and vector sessions share one Tsavorite store and hybrid log. objectKV
retains ordered keys, range conflicts, strict cell transactions, and immutable
object closures, so Garnet is a design input rather than a dependency. See
[`garnet-storage-and-distribution-study-2026-08-30.md`](../research/garnet-storage-and-distribution-study-2026-08-30.md).

The likely native provider is one hybrid-log engine whose resident portion is
RAM and whose colder pages may use direct NVMe. RocksDB remains a separate
resident-engine provider. If RAM and RocksDB are both enabled, RAM is a bounded
cache or overlay over the same logical state, not a second authoritative
database. The eval must decide whether a three-provider composer creates enough
benefit to justify duplicate bytes and write fanout.

## Three switches, two kinds of thing

| Switch | Kind | Function | Cost and behavior | Current evidence |
| --- | --- | --- | --- | --- |
| `ram_image` | Resident engine | Keep admitted records, blocks, or a complete range in DRAM | Lowest lookup latency, highest cost per resident byte, volatile local state, no swap | `[PROPOSED]` |
| `nvme_block_cache` | Physical cache | Cache immutable object blocks or packed runs directly on local NVMe | Larger and cheaper than DRAM, avoids rebuilding another LSM, cold misses still reach objects | `[PROPOSED]` |
| `rocksdb_image` | Ordered resident engine | Materialize current range state into RocksDB | Mature point/range lookup and MVCC encoding; consumes CPU, write amplification, RAM block cache, and a filesystem | `[VERIFIED]` for bounded single-range reads; cache curve `[EVALUATING]` |

NVMe is a storage medium. RocksDB is an engine that usually stores SST files on
NVMe or SSD. The switches remain independent because `nvme_block_cache` means a
direct cache of immutable object bytes, while `rocksdb_image` means a separately
materialized ordered database image. RocksDB can use an NVMe filesystem while
the direct object-block cache is disabled.

## Target validation rule

```text
┌─[ ROUTABLE PROFILE ]───────────────────────────────────────────────┐
│ enabled(ram_image, nvme_block_cache, rocksdb_image) ≥ 1           │
│ every enabled mechanism has a hard byte budget                    │
│ every read reports the tier that resolved it                      │
│ no local mechanism changes commit durability                      │
└───────────────────────────────────────────────────────────────────┘
```

The current `SingleRangeConfig` does not implement this composer. It accepts at
most one resident provider and can fall through directly to an indexed object
read. The target production RangeEngine treats object-direct as a recovery and
elastic-miss path, not as a zero-local-tier resident profile.

### Proposed configuration shape

This is a review sketch, not a current CLI or compatibility promise:

```toml
[range_engine]
profile = "ram_rocks"

[range_engine.ram_image]
enabled = true
max_bytes = 17179869184

[range_engine.nvme_block_cache]
enabled = false
path = "/var/lib/okv/blocks"
max_bytes = 0

[range_engine.rocksdb_image]
enabled = true
path = "/var/lib/okv/rocks"
max_bytes = 274877906944
block_cache_bytes = 4294967296
```

Validation rejects a zero-provider profile, missing or overlapping paths,
unbounded enabled tiers, swap for `ram_image`, and a RocksDB configuration that
hides its block cache outside the declared RAM budget.

## Proposed profiles

| Profile | RAM image | NVMe block cache | RocksDB image | Optimizes for | Gives up | Status |
| --- | ---: | ---: | ---: | --- | --- | --- |
| `ram_resident` | on | off | off | Minimum resident read latency and no local data files | Highest RAM cost; complete local loss on process death; rebuild or replica needed | `[PROPOSED]` |
| `nvme_elastic` | off | on | off | Large cheap cache over immutable object layout with no second LSM | Cold-miss latency; index design and historical overlay complexity | `[PROPOSED]` |
| `rocks_resident` | off | off | on | Predictable ordered reads, mature snapshots, dense local capacity | Local compaction, CPU, write amplification, and image build cost | `[VERIFIED]` only for the scoped row-0 read boundary; complete profile `[EVALUATING]` |
| `ram_rocks` | on | off | on | RAM hot set with RocksDB capacity fallback and faster same-volume restart | Highest combined local cost and duplicate cache policy | `[PROPOSED]` |
| `ram_nvme_rocks` | on | on | on | Independent experiments on every local mechanism | Likely duplicate bytes and unclear ownership; not a default | **Review**, `[PROPOSED]` |

These are serving profiles, not durability profiles.

## Durability combinations

```text
┌─[ SERVING × DURABILITY ]───────────────────────────────────────────┐
│ ram_resident  × regional quorum txLog → durable, volatile serving │
│ rocks_resident× regional quorum txLog → durable, restart-friendly │
│ ram_rocks     × regional quorum txLog → fast, costly, durable     │
│ any profile   × volatile buffer   → BUFFERED, never COMMITTED     │
└───────────────────────────────────────────────────────────────────┘
```

“Durable” in this table means the acknowledged database state is recoverable
from the selected quorum or journal and the immutable object frontier. It does
not mean the local RangeEngine cache is authoritative.

RAM plus SSD can improve availability and recovery speed, but spill is not
redundancy by itself. Redundancy requires another failure domain or the durable
txLog and object closure. A single host holding both tiers can still lose both.

## Read pipeline

```text
┌─[ READ KEY K AT T ]────────────────────────────────────────────────┐
│ 1  verify generation, assignment, and T ∈ [O, A]                 │
│ 2  check transaction-local writes and recent MVCC overlay         │
│ 3  probe enabled tiers in the profile's declared order            │
│ 4  require coverage before interpreting a miss as Absent          │
│ 5  on elastic miss, range-read one verified immutable block       │
│ 6  optionally admit the block or value under the local budget     │
└───────────────────────────────────────────────────────────────────┘
```

The tier order is policy, not a semantic promise. A likely `ram_rocks` order is
RAM then RocksDB then object fallback. A likely `nvme_elastic` order is recent
overlay then sparse index then NVMe cached block, with object range GET on
cache miss.

## Write and catch-up pipeline

```text
┌─[ AFTER COMMIT ]───────────────────────────────────────────────────┐
│ txLog batch at C                                                  │
│   ↓                                                               │
│ validate generation and consecutive versionstamp                 │
│   ↓                                                               │
│ apply recent overlay                                              │
│   ├─ RAM image: update versioned in-memory index                  │
│   ├─ NVMe cache: invalidate or shadow stale immutable blocks      │
│   └─ RocksDB: atomic head + history + frontier write batch        │
│   ↓                                                               │
│ expose new read handle only after complete visibility            │
└───────────────────────────────────────────────────────────────────┘
```

The exact write fanout for a multi-tier profile remains under review. A profile
must not synchronously update redundant disposable tiers unless the measured
read or recovery benefit justifies the write cost.

## Pressure and spill

```text
┌─[ BUDGET RESPONSE ]────────────────────────────────────────────────┐
│ below soft limit → admit and prefetch                             │
│ soft limit crossed → evict reconstructable bytes, stop prefetch  │
│ hard local limit → reject new residency or demote range           │
│ hard txLog debt limit → rate-limit, then refuse commits           │
└───────────────────────────────────────────────────────────────────┘
```

- RAM eviction can fall through to RocksDB, NVMe cached blocks, or objects.
- RocksDB and NVMe-cache eviction deletes only reconstructable local bytes.
- Object-store outage can preserve fully covered local reads but can block
  cold reads and rebuilds.
- txLog pressure is not solved by evicting RangeEngine state. It is solved by
  objectification progress or write backpressure.
- `ram_image` prohibits swap. OOM is a failed gate, not an eviction policy.

## Current and target boundary

| Capability | Current | Target |
| --- | --- | --- |
| Resident provider selection | `[CODE-COMPLETE]` at most one `ServingImage` or `ResidentRangeEngine` | `[PROPOSED]` validated composition of three switches |
| RocksDB activation and exact snapshot reads | `[VERIFIED]` scoped single-range mechanism and performance | Preserve as one profile |
| Explicit RocksDB block-cache budget and direct reads | `[VERIFIED]` mechanism; full curve `[EVALUATING]` | Profile-owned budgets and tier attribution |
| Complete RAM image | `[PROPOSED]` | Bounded versioned image with no swap or local data files |
| Direct NVMe immutable-block cache | `[PROPOSED]` | Cache named blocks without creating a second authority |
| Multi-tier spill and promotion | `[PROPOSED]` | Deterministic policy with per-tier metrics and no semantic change |
| Live profile handoff | `[PROPOSED]` | Hydrate, catch up, prove coverage, flip generation, fence old owner |
| Multi-range process | `[PROPOSED]` | Independent budgets and fair admission across assigned ranges |

## What is ready to lock

1. RangeEngine is disposable serving compute, not durability authority.
2. Every tier is explicitly bounded and observable.
3. At least one local base-serving mechanism is enabled for a routable
   production profile.
4. A tier miss is not logical absence without exact coverage evidence.
5. Serving and durability profiles are independent axes.
6. Rebuild always starts from an authenticated object closure plus durable
   txLog suffix.

## Design review queue

1. Does `nvme_block_cache` remain a first-class provider after T28 compares it
   with RocksDB, or is it only a RocksDB/table-cache implementation detail?
2. Does `ram_image` store decoded values, immutable blocks, complete ranges, or
   different forms by workload?
3. Which tier owns old-version reads when the recent overlay is reclaimed?
4. Does `ram_rocks` synchronously update both providers or treat RAM as a
   read-through cache over one RocksDB truth image?
5. How are per-tenant and per-range budgets enforced without allowing one hot
   tenant to evict the cell?
6. Which profile transitions preserve service while avoiding double the normal
   memory or local-storage budget?
7. Does an object-direct deployment remain a supported elastic class even
   though it is not a resident RangeEngine profile?

T27 answers RocksDB cache pressure. T28 is the first experiment capable of
deciding whether raw NVMe object-block caching deserves its own provider.
