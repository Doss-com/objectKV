# RFC-0056: KV Runtime resource envelope

- Status: active-work, accounted contract accepted; physical gate open
- Authors: DOSS
- Created: 2026-08-24
- Depends on: RFC-0001, RFC-0011, RFC-0036

## Decision under test

`[PROPOSED]` Name the disposable RAM and NVMe serving process a **KV
Runtime**. A KV Runtime hosts many logical **Range Engines**, but applies one
process-wide cache and pressure envelope. Admit this topology only if an
accounted-resource contract demonstrates 1, 100, and 1,000 Range Engine
assignments without reserving a private RAM or NVMe cache for every range.

This contract is a semantic and configured-resource proof. It is not physical
RSS, allocator, file-descriptor, thread, task, or SlateDB database-instance
evidence. A later physical-density gate must measure those quantities in the
real process before 1,000 Range Engines becomes an operating claim.

## Runtime hierarchy

```text
Cell
  |
  +-- transaction services
  |     read-version proxy, commit proxy, resolver, txLog
  |
  +-- KV Runtime process
        |
        +-- one process-wide RAM cache
        +-- one process-wide NVMe cache
        +-- one pressure controller
        +-- Range Engine assignment 1
        +-- Range Engine assignment 2
        `-- Range Engine assignment N
```

A Range Engine is an ownership and serving assignment for one ordered key
range. It is not required to be one OS process, one thread, one private cache,
or one durable database. The KV Runtime owns no authoritative durable bytes.
It can reconstruct assigned ranges from object manifests plus the durable
txLog tail.

## Frozen accounted-resource model

The `accounted-v0` profile fixes:

| Resource | Bound |
|---|---:|
| process RAM | 512 MiB |
| process NVMe cache | 8 GiB |
| Range Engines | 1,000 maximum |
| fixed metadata per Range Engine | 512 bytes |
| recent MVCC bytes per Range Engine | 4 KiB |
| requested process RAM cache | 128 MiB |
| requested process NVMe cache | 2 GiB |
| soft objectification debt | 64 MiB |
| hard objectification debt | 128 MiB |

The fixed density points are 1, 100, and 1,000 assigned Range Engines. The
accounted quantities are:

```text
fixed range bytes
  = sum(range metadata + recent MVCC overlay)

accounted RAM
  = fixed range bytes + admitted process RAM cache

accounted NVMe
  = admitted process NVMe cache
```

The process cache request is constant across density points. Multiplying it by
the Range Engine count is a contract violation.

## Pressure ordering

The frozen controller order is:

```text
evict disposable cache
  -> request objectification
  -> request range movement
  -> rate-limit new work
  -> refuse commits that would cross a hard bound
```

Not every decision emits every action. The ordering rule means an action may
not appear after a later action in the same decision.

Cache-only pressure evicts cache and can continue admitting work. Crossing the
soft objectification-debt bound requests objectification and rate-limits new
work. Crossing the hard debt bound refuses the commit. Non-evictable RAM above
the process bound requests objectification and range movement, then refuses
additional growth until pressure clears.

## Public interface under test

The candidate surface exposes a deterministic `KvRuntime` with:

- bounded Range Engine assignment;
- process-wide cache demand;
- per-range metadata, recent MVCC, and objectification-debt accounting;
- an immutable resource snapshot;
- an ordered pressure decision with `admit`, `rate_limit`, or `refuse`.

The interface does not allocate cache bytes, flush objects, move ranges, or
claim recovery. Those effects remain the responsibility of later physical
adapters and the existing objectification and serving-recovery contracts.

## Eval plan

Freeze `cell-kv-runtime-resource-envelope-v0`. The primary metric is
`kv_runtime.fixed_ram_bytes_per_range`. Secondary evidence records accounted
RAM, accounted NVMe, evicted cache bytes, objectification debt, assignment
count, and pressure decisions.

Passing hard gates require:

- exact 1, 100, and 1,000 assignment points;
- one process cache request at every point;
- exact linear fixed-range accounting;
- accounted RAM and NVMe at or below their configured bounds;
- cache eviction before refusal for cache-only pressure;
- rate limiting above soft debt and refusal above hard debt;
- objectification and range movement before refusal for non-evictable RAM;
- deterministic replay and no telemetry drops;
- every negative subject is detected and discarded.

The frozen negative subjects independently attempt to:

1. reserve the process cache once per Range Engine;
2. refuse cache-only pressure without first evicting disposable cache;
3. admit a commit above the hard objectification-debt limit;
4. omit range movement under non-evictable RAM pressure.

## Physical follow-up gate

`[FUTURE]` A separate real-process suite must measure 1, 100, and 1,000 actual
Range Engine instances with the selected local engine. It must record physical
RSS, allocated heap, threads, async tasks, file descriptors, open local files,
NVMe bytes, cold reconstruction time, p50 and p99 point reads, and compaction
or objectification debt. That suite decides whether one process can host 1,000
engines, whether engines need grouping, or whether the maximum assignment count
must be lower.

## Tradeoff

This gate optimizes for making ownership and overload semantics explicit before
building a physical runtime. It gives up any claim about the actual memory
overhead of RocksDB, SlateDB, an allocator, or an async task graph. Passing it
means the architecture does not require per-range caches. It does not mean a
particular implementation avoids them.

## Alternatives

### One process per range

This makes isolation simple but multiplies cache, runtime, connection, and
operational overhead. It is not the default topology.

### One embedded database per range with default caches

This is easy to construct and likely to fail the physical-density gate. The
contract permits one embedded engine per range only if caches and background
work are genuinely shared or their measured overhead remains within bounds.

### Skip the accounted model and benchmark immediately

Physical measurement is still required. The accounted contract first freezes
which resources are shared, which grow per range, and what must happen at each
bound, so a benchmark cannot pass while implementing the wrong topology.

## Evaluation outcome

`[EXISTS]` The correct 1, 100, and 1,000 Range Engine points pass every hard
gate. Fixed accounted RAM is 4,608 bytes per range; the process-wide RAM and
NVMe cache demands remain constant. The four frozen fault subjects each fail
their owning gate and discard.

The clean eval runner keeps the correct accounted contract. This is not a
physical performance comparison because the lane has no physical incumbent.
The accepted outcome is the public resource and pressure contract only.
Physical density and latency remain `[ACTIVE-WORK]`.
