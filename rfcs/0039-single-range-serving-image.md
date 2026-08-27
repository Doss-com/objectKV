# RFC-0039: SingleRange serving-image boundary

- Status: `[CODE-COMPLETE]` interface, `[EVALUATING]` performance
- Authors: DOSS
- Created: 2026-08-27
- Scope: public single-range read configuration and disposable hot state

## Decision to test

Put one provider-neutral `ServingImage` boundary below `SingleRange` and above
the immutable object base. Integrate the SSD implementation first. It uses
RocksDB on bounded disposable local media. The later RAM implementation must use
the same activation and read contract.

```text
cell durability profile
  regional quorum txLog
            |
            v
range serving profile
  recent DRAM tail
            |
            v
  ServingImage
    SSD: RocksDB on disposable NVMe
    RAM: ordered in-memory image
            |
            v
  indexed immutable-object fallback
```

The axes remain separate:

```text
DurabilityProfile  owns COMMITTED meaning for the tenant generation
ServingProfile     owns the disposable hot representation for one range
Object layout      owns immutable permanent bytes and cold access economics
```

Changing a serving profile cannot change transaction ordering, acknowledgement
meaning, object identity, or the reconstruction equation:

```text
Database(C) = ObjectState(O) + txLog(O, C]
```

## Minimal interface

The first interface owns only what the current point-read kernel can prove:

1. activate an empty image from a complete decoded object snapshot through `O`;
2. bind that image to one cell generation and object-durable version;
3. return exact value, tombstone, or absence for a point key;
4. report provider ID, installed records, and accounted local bytes;
5. fail closed on stale generation, incomplete activation, or a provider error.

Range iteration, incremental image apply, partial block admission, eviction, and
profile handoff remain outside this interface until their owning gates exist.
The recent MVCC tail remains in `SingleRange` for this slice.

## Configuration

`SingleRangeConfig` accepts either no serving image or one empty provider:

```text
serving_image = none
  -> recent tail, then indexed object read

serving_image = RocksDbServingImage(root, max_local_bytes)
  -> activate from the complete object base during open
  -> recent tail, then local image read
  -> no object read after successful activation
```

The RocksDB provider lives outside the provider-neutral `okv` crate. This keeps
the kernel independent of one local engine and keeps RocksDB compilation behind
an explicit feature in the evaluation binary.

## SSD activation contract

Activation may fetch and decode the complete immutable row closure. Before the
image becomes readable it must:

1. verify every index, object digest, block checksum, and record order;
2. reduce each key to its visible value or tombstone at `O`;
3. install that state with RocksDB WAL disabled because the image is disposable;
4. flush the image, measure local bytes, and reject it above the configured
   local-byte ceiling;
5. publish complete generation and coverage metadata only after all prior steps
   pass.

An image miss is authoritative absence only after complete activation. Any
activation failure leaves the range unopened.

## First performance gate

The first public-kernel SSD gate reuses the G3.1 deterministic dataset and
80/20 point distribution:

```text
keys:                         65,536
value bytes:                   1,024
logical bytes:                64 MiB
warmup reads:                100,000
measured reads:              200,000 per repeat
seeds:                  1103, 2207, 3301
repeats:                           4
local-byte ceiling:          128 MiB
```

Candidate and direct RocksDB control run as separate optimized processes in
ABBA order. Population and activation are outside measured point operations.

Hard gates:

1. every read returns the expected key identity and value checksum;
2. the activated candidate issues zero object operations during warmup and
   measurement;
3. generation and coverage checks remain enabled;
4. the local image stays at or below 128 MiB;
5. candidate median throughput is at least 80 percent of direct RocksDB;
6. candidate aggregate p99 is at most 1.20 times direct RocksDB;
7. a stale-generation or incomplete-coverage poison fails before serving;
8. candidate and control receipts contain at least five samples before a
   performance verdict.

The real-NVMe gate additionally records the provider machine, filesystem,
device, mount, build, binary digest, CPU, memory, and background load. A Mac
local-filesystem result is a mechanism diagnostic, not an NVMe claim.

## Hierarchical follow-on curves

The interface is admitted only through progressively wider curves:

```text
L0 local engine
  -> direct RocksDB versus activated ServingImage

L1 public range
  -> generation + tail + image, one process and one thread

L2 serving process
  -> routing/RPC, 1/4/16/64 clients, mixed overlay and base hits

L3 recovery
  -> empty worker, hydration, txLog catch-up, first correct read

L4 steady objectification
  -> foreground read/write p99 while C - O repeatedly converges

L5 cell
  -> same curves with independent quorum media and host failure
```

No wider layer may hide a regression in a lower layer. Each layer keeps its own
paired control and hard gates.

## Tradeoff

This optimizes for a small provider-neutral kernel and an immediately measurable
SSD path. It gives up partial residency and concurrent range scans in the first
interface. Dynamic dispatch remains in the point path until the benchmark shows
that it is material; if it exceeds the 20 percent envelope, the next candidate
can specialize the profile at process construction without changing the
activation contract.

## Not claimed

- `[EVALUATING]` production NVMe latency or throughput;
- `[PROPOSED]` concurrent-client scaling;
- `[PROPOSED]` incremental RocksDB tail application;
- `[PROPOSED]` SSD to RAM profile handoff;
- `[PROPOSED]` bounded partial residency or eviction;
- `[FUTURE]` PostgreSQL, Redis, search, or DataFusion performance.

## First integrated diagnostic

Run `56535944-86e4-4f31-b1a8-38cce19ea668` activated 256 exact records into
an 86,667-byte RocksDB image, killed the first serving process, recovered a
distinct empty replacement through six OpenRaft authority processes, applied
the txLog suffix, and measured 100,000 public `SingleRange::get` calls. It
reported 824,252 reads/s, 1,583 ns p99, zero correctness failures, and zero
object operations after activation.

This is a dirty-tree arm64 debug diagnostic with one small dataset. Its APFS
scratch volume maps to an internal Apple SSD AP1024Z NVMe device, but the run
has no isolated provider runner or background-load control. It proves the
interface, recovery, and object-exclusion gates compose. It does not pass the
frozen optimized ABBA performance gate.
