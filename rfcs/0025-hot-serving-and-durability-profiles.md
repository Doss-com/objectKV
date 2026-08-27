# RFC-0025: SSD and RAM serving profiles with independent durability

- Status: proposed
- Authors: DOSS
- Created: 2026-08-25
- Scope: serving and durability profile selection

## Decision

`[PROPOSED]` objectKV exposes two hot-state profiles behind one `ServingImage`
contract:

- `ssd_resident`: a bounded RocksDB image on local NVMe, with a DRAM overlay and
  block cache;
- `ram_resident`: a bounded DRAM image with no worker-local data files or swap.

Both profiles use the same object row base, recent mutation semantics, coverage
metadata, range assignment, and indexed object fallback. The selected serving
profile is a range placement decision. It may change without changing key,
transaction, durability, or object-format semantics.

This does not make the transaction durability boundary volatile. The result of
a write is `COMMITTED` only after the selected durability provider has made the
commit envelope, consensus hard state, and required outcome recoverable across
that profile's declared failures. The default provider remains a regional
durable txLog. A memory quorum with asynchronous objectification may return a
distinct `BUFFERED` result, but never `COMMITTED`. RAM serving therefore does
not require weak durability, but the `ram_turbo` preset deliberately combines
RAM serving with volatile acknowledgement for workloads that choose that
tradeoff.

## Product axes and presets

The public configuration has two orthogonal axes:

```text
ServingProfile   = ssd_resident | ram_resident
DurabilityProfile = regional_quorum | external_journal | object_ack
                    | volatile_buffered
```

Initial presets are conveniences, not separate storage engines:

| Preset | Serving profile | Durability profile | Result |
|---|---|---|---|
| `ssd_standard` | `ssd_resident` | `regional_quorum` | Durable, capacity-efficient hot state and fast restart |
| `ram_durable` | `ram_resident` | `regional_quorum` | Durable commit with the fastest serving path |
| `ram_turbo` | `ram_resident` | `volatile_buffered` | Fastest visibility with an explicit `C - O` loss window |
| `ram_object` | `ram_resident` | `object_ack` | No local durable media, with object latency on commit |

Serving profile may vary by range. Durability profile is fixed for a tenant
transaction domain during one generation so a cross-range transaction cannot
mix acknowledgement meanings. Every receipt includes both selected profiles.

## Separate the three latency questions

```text
read latency
  -> recent DRAM overlay, then selected SSD or RAM ServingImage

write visibility
  -> applied to the common recent DRAM overlay after ordering

write durability
  -> regional durable txLog, synchronous object log, or another declared
     DurableLog implementation
```

Switching between SSD and RAM changes resident lookup, local maintenance,
capacity, and rebuild behavior. It does not remove the third path from a
durable database.

## Bottom-up topology

```text
immutable blob base
  row objects, manifests, snapshots, analytical objects
  authoritative through O
          ^
          | asynchronous packing, publication, and compaction
          |
materializer
  bounded DRAM buffers, no unique worker-local state
          ^
          |
durable tail
  DurableLog implementation retains (O, C]
  default: three regional stable-media voters, commit after two
          ^
          |
transaction system
  DRAM read-version, proxy, resolver, routing, and in-flight state
          ^
          |
serving worker
  DRAM MVCC overlay
  ServingImage
    ssd_resident -> RocksDB on bounded disposable NVMe
    ram_resident -> admitted DRAM blocks or complete ranges
  indexed object fallback
```

The load-bearing invariant is unchanged:

```text
Database(C) = ObjectState(O) + DurableLog(O, C]
```

## Serving images

### SSD resident

`ssd_resident` combines the recent DRAM overlay with a complete or partially
admitted RocksDB range image on bounded local NVMe. The NVMe image is
reconstructable and never the only permanent database copy. It optimizes for:

- more hot bytes per dollar than DRAM;
- predictable restart without full rehydration;
- mature ordered reads, compaction, snapshots, and concurrent access.

It gives up the lowest possible local lookup latency and spends local I/O and
CPU maintaining a disposable LSM image.

### RAM resident

`ram_resident` has two independently bounded portions:

1. A recent versioned overlay containing committed mutations, tombstones, and
   range-clear effects not yet folded into the selected row base.
2. Admitted immutable-base blocks, decoded records, or a complete range image.

The first implementation should use a simple ordered in-memory index with
explicit version chains and coverage metadata. The experiment must not select a
complex concurrent tree before a single-process control shows that RocksDB
lookup cost is material in the complete request path.

```text
key + snapshot T
  -> transaction-local writes
  -> recent DRAM MVCC overlay
  -> complete admitted DRAM record or block
  -> warm manifest and sparse index
  -> one verified object range GET
  -> admit or evict under the DRAM budget
```

A local miss is logical absence only when coverage metadata proves the selected
range and version complete. Otherwise the worker must perform the indexed
object lookup or return an explicit availability error.

## Profile transitions

Serving-profile changes use the ordinary range assignment generation:

```text
SSD -> RAM
  hydrate RAM image through H
  replay committed tail (H, C]
  prove coverage and generation
  atomically flip serving assignment

RAM -> SSD
  build RocksDB image through H
  replay committed tail (H, C]
  prove coverage and generation
  atomically flip serving assignment
```

The old assignment may continue serving during hydration but cannot serve or
publish after the generation flip. Profile changes never move permanent bytes
and never change the transaction domain.

Changing durability profile is heavier. The tenant transaction domain drains
in-flight commits, establishes one durable frontier, activates a new generation,
and records the new profile in its root. It is not a per-range cache operation.

## Durability profiles

| Profile | Successful result | Required medium | Normal object dependency | Intended use |
|---|---|---|---|---|
| `regional_quorum` | `COMMITTED` | Two of three stable-media txLog voters | Asynchronous | Default durable OLTP |
| `external_journal` | `COMMITTED` | Declared durable journal service | Asynchronous | RAM-only objectKV compute nodes |
| `object_ack` | `COMMITTED` | Immutable object log plus fenced durable decision | Synchronous | No-local-media durability experiments and latency-tolerant ingest |
| `volatile_buffered` | `BUFFERED` | Live DRAM quorum only | Asynchronous | Explicit ephemeral or cache-like workloads |

No profile silently degrades into another. `volatile_buffered` has an RPO equal
to the unobjectified interval and loses that interval if the live quorum is
destroyed or restarted. It is excluded from PostgreSQL durable commit claims.

Consensus term, vote, membership, committed entries, transaction outcomes,
generation barriers, publication roots, safe-pop positions, snapshot pins, and
GC reservations must be durable before they authorize a durable response or
destructive action. Replicating these facts only in ordinary DRAM is agreement
while the copies remain alive, not crash durability.

## Resource pressure and object outage

Each serving profile declares hard budgets for the recent overlay, admitted
base, decoded-block cache, indexes, in-flight hydration, and retained txLog.
`ssd_resident` separately bounds DRAM and NVMe. `ram_resident` counts all
resident and allocator overhead against one memory limit.

```text
below soft watermark
  -> admit and prefetch normally

soft watermark crossed
  -> evict reconstructable blocks, stop prefetch, or move the range

hard serving watermark crossed
  -> reject new residency, preserve exact reads through object fallback

hard durable-tail watermark crossed
  -> rate-limit, then refuse writes before unique acknowledged state is lost
```

No swap is permitted in `ram_resident`. A process OOM is a failed gate, not an
eviction mechanism. `ssd_resident` must not hide unbounded DRAM block-cache
growth behind an NVMe capacity result.

During an object-store outage, fully covered DRAM reads may continue. Cold
reads, worker rebuilds, analytical scans, and branch opens may become
unavailable. The durable tail grows only to its hard bound, after which commits
are refused.

## Failure and recovery

| Event | Required behavior |
|---|---|
| RAM-serving process loss | Reroute to a complete replica or rebuild from objects plus durable tail |
| SSD-serving process loss | Reroute, reopen a surviving disposable image, or rebuild from objects plus durable tail |
| Serving-profile transition | Serve from the old generation until the new image proves coverage, then fence the old assignment |
| One durable-log voter loss | Continue only while the declared quorum remains durable |
| Whole DRAM serving fleet restart | Open authoritative manifests, replay the durable tail, and reject reads until coverage is honest |
| Volatile quorum destruction | Lose at most the explicitly reported `BUFFERED` interval; never lose a reported `COMMITTED` interval |
| Object outage | Continue covered reads, bound tail growth, then refuse new commits |
| Missing or corrupt object | Reject the closure and fail closed |

The first correct read must not require complete database hydration. A resident
service-level objective may require complete admission of one range before the
assignment becomes ready. SSD and RAM profiles publish separate recovery-time
and first-read objectives.

The empty-worker object-base path is:

```text
authoritative manifest key from cell state
  -> exact named manifest GET and checksum validation
  -> in-memory key-bound lookup
  -> exact named selected-index GET and manifest validation
  -> one verified data-block range GET
  -> first exact value or tombstone
  -> optional background hydration under the range budget
```

The first-read path never uses LIST, fetches indexes for unrelated objects, or
hydrates complete data objects. The full-range hydration control performs those
reads before decoding the same key and must remain separately visible in
request, byte, and latency receipts.

## PostgreSQL and HTAP consequences

The page-native PostgreSQL bridge remains unchanged. PostgreSQL shared buffers
are the first DRAM cache. On a miss, objectKV may return a page from either
serving profile or an indexed packed-page object. PostgreSQL WAL, LSNs, tuple
MVCC, timelines, and fuzzy-checkpoint recovery remain the PostgreSQL authority.
`ram_turbo` cannot satisfy durable PostgreSQL `COMMIT`; it is analogous to an
explicitly weaker acknowledgement mode.

DataFusion execution remains naturally memory and object oriented. Exact HTAP
queries acquire one leased object closure at `T`, read columnar bases and the
durable analytical tail, then merge in DRAM. Query-process memory is disposable;
the lease, manifests, schema and partition epochs, and tail coverage are not.

## First eval matrix

`[PROPOSED]` `hot-profile-point-v1` runs both serving profiles over the same
object base, overlay semantics, dataset, access trace, and coverage rules.

- `ssd-object-point-v1` compares the SSD profile with direct NVMe RocksDB.
- `ram-object-point-v1` compares the RAM profile with RAM-backed RocksDB and
  the admitted SSD candidate.
- `hot-profile-transition-v1` proves exact SSD-to-RAM and RAM-to-SSD handoff
  while reads and committed tail replay continue.

- Parent dataset: 65,536 incompressible 1,024-byte values. The first RAM gate
  selects one complete 16,384-key range and compares it with SSD over the same
  range and trace.
- Row layout: 4 MiB immutable objects with checksummed 64 KiB blocks.
- Admission: one deterministic 16 MiB logical range, approximately 25 percent
  of the parent keyspace. The RAM profile gets a 24 MiB total memory cap; its
  matched SSD control gets a 48 MiB local-byte cap. Neither path may fetch
  objects after complete admission.
- Access: 100,000 warmup reads and 200,000 measured reads, four repeats and
  three fixed seeds in separate-process ABBA order.
- Primary metric: hot point-read operations per second.
- Shared hard gates: exact values, zero measured object GETs after admission,
  no budget overrun, no stale-generation serve, and complete telemetry.
- RAM gates: zero local data-file writes, zero swap, and no OOM.
- SSD gates: local bytes stay below the declared NVMe cap and restart does not
  depend on downloading the complete object base.
- Cold-path gate: after the manifest and sparse index are warm, one successful
  cold lookup performs at most one verified data range GET and does not hydrate
  the full base.
- Poisons: stale generation, incomplete coverage treated as absence, ignored
  memory watermark, whole-object point read, and full-base hydration before
  first correct read.

The distributed follow-up repeats the complete request path at 1, 4, 16, and 64
clients. It measures RPC, routing, overlay, CPU, RSS, local I/O, object requests,
tail latency, and estimated cost so an in-process memory win is not mistaken
for a product win.

## Go or stop

Keep both profiles behind the common contract only while they remain exact,
bounded, and clear the indexed cold-read gate.

Productize `ssd_resident` if its wrapper stays within 20 percent of direct
RocksDB on p99 and throughput under the matched profile.

Productize `ram_resident` as a distinct premium profile only if it produces at
least a 20 percent end-to-end p99, throughput, or CPU advantage over
`ssd_resident` under one named workload without violating memory, recovery, or
cost gates. A local-engine-only win is insufficient.

Keep `ram_turbo` only if its faster acknowledgement curve is material and every
receipt, API response, and operator surface exposes the `BUFFERED` result and
current `C - O` loss window.

Prefer SSD serving alone if one bounded RAM iteration cannot clear those gates.
Preserve the `ServingImage` boundary so a future memory engine can re-enter
without changing object or transaction formats.

## Alternatives

- SSD serving plus a regional durable txLog is the default capacity-oriented
  preset. RAM serving plus the same txLog is the durable low-latency preset.
- A separate durable journal allows objectKV compute nodes to remain RAM-only,
  but introduces another operated dependency.
- Synchronous object acknowledgement removes local durable media but imports
  object PUT latency, availability, packing, and control-root serialization
  into commit.
- Persistent memory or RDMA-backed durability remains a replaceable
  `DurableLog` research profile, not a portable OSS assumption.

## Unresolved questions

- How should the placement system decide when a range moves between SSD and RAM
  without oscillation?
- How should RAM replicas balance instant failover against replicated memory
  cost?
- How should a simple in-memory ordered index evolve after the reference lane
  measures concurrency and snapshot cost?
- How should `volatile_buffered` surface progress from `BUFFERED` to
  object-durable without implying an eventual durable commit callback?
- How can an external durable journal retain the open and self-hostable
  deployment properties that motivate objectKV?
