# Garnet storage and distribution study

Date: 2026-08-30

Status: `[EVALUATING]` primary-source study. This document changes no objectKV
implementation or performance status.

Source snapshot: Garnet commit
[`c9607605baa4b7eca0e41dc622a0ee3e52f5574c`](https://github.com/microsoft/garnet/commit/c9607605baa4b7eca0e41dc622a0ee3e52f5574c)
and the official documentation retrieved on 2026-08-30.

## Clarity

Question: Which Garnet mechanisms should change objectKV's RAM, log, and
distributed-system design?

Punchline: Current Garnet validates one hot engine with several typed session
views, a narrow operation waist, deterministic operation logging, and explicit
RAM, SSD, and cloud commit depths. Its asynchronous hash-slot cluster cannot
replace objectKV's cell-scoped strict-serializable transaction plane.

Counter: This call is wrong if the product requirement narrows from an ordered
transactional kernel to a Redis-compatible sharded cache whose multi-key work
may be confined to one hash slot.

Next: freeze `read`, `upsert`, `atomic_modify`, `delete`, and `ordered_scan` as
the candidate `okv-fabric` storage waist, then compare direct session execution
with per-core dispatch before selecting a native RAM or NVMe hot-engine shape.

Confidence: High on the architectural boundary, medium on the performance
transfer because the published comparisons are Redis workloads rather than
objectKV's ordered MVCC, object-history, and HTAP workload.

## Primary-source observations

1. Garnet maps its command surface onto five storage operations: `Read`,
   `Upsert`, `Modify`, `Delete`, and `Scan`. The four-operation summary omits
   `Scan`. The paper calls this interface RUMDS and uses `Modify` for atomic
   read-modify-write. [PVLDB paper, sections 3.1 and
   5.1](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)

2. Garnet's architecture has changed since the paper. The paper describes
   separate string and object stores. Current code has one `TsavoriteKV` per
   logical database. String, object, unified, and vector sessions open typed
   views over that same store; the hybrid log uses `ValueIsObject` to
   distinguish inline bytes from heap-backed structured objects. This is
   evidence for one objectKV RangeEngine state with several typed fabric views,
   not one consistency domain per API surface. [Current `GarnetDatabase`](https://github.com/microsoft/garnet/blob/c9607605baa4b7eca0e41dc622a0ee3e52f5574c/libs/server/GarnetDatabase.cs#L27-L55),
   [current `StorageSession`](https://github.com/microsoft/garnet/blob/c9607605baa4b7eca0e41dc622a0ee3e52f5574c/libs/server/Storage/Session/StorageSession.cs#L101-L129),
   [official memory documentation](https://microsoft.github.io/garnet/docs/getting-started/memory)

3. `TieredStorageDevice` orders devices from hot to cold, reads from the
   closest tier that contains the segment, and writes to all tiers in parallel.
   Its callback waits through a configured commit point and every hotter tier;
   colder writes may continue after acknowledgement. The tier order and durable
   commit depth are separate configuration decisions. [Pinned
   `TieredStorageDevice` source](https://github.com/microsoft/garnet/blob/c9607605baa4b7eca0e41dc622a0ee3e52f5574c/libs/storage/Tsavorite/cs/src/core/Device/TieredStorageDevice.cs#L10-L104)

4. The paper's example assigns roughly 100 microseconds to SSD and 5
   milliseconds to cloud. Acknowledging at SSD permits complete restart of the
   same node but only partial survival on a fresh node until the cloud write
   completes. This is a named durability tradeoff, not object durability at SSD
   acknowledgement. [PVLDB paper, section
   6.1](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)

5. Garnet's request path lets the network I/O completion thread parse and run
   storage operations directly. Its design study reports 47 million operations
   per second for Garnet versus 1.3 million with one global lock, 4.4 million
   with one worker, 9 million with 64 workers, and 17.1 million with 64
   processes. The paper attributes much of the difference to routing, data
   movement, and result collation. [PVLDB paper, sections 4 and
   8.4](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)

6. Garnet cluster mode divides keys into 16,384 hash slots and restricts
   multi-key operations to keys in one slot. It has a passive control plane and
   expects an external orchestrator for leader election and failover actions.
   [Official cluster documentation](https://microsoft.github.io/garnet/docs/cluster/overview)

7. The paper reports up to 100x throughput and up to 4x lower high-percentile
   latency in its selected comparisons. In one 256 million key, 6,400-client,
   1:9 SET:GET workload with 8-byte values, Garnet reports at least 20x the
   throughput and 3x lower latency than standalone Valkey. These are
   workload-specific results, not a direct objectKV control. [PVLDB paper,
   abstract and section 8](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)

8. Official documentation describes memory-only and larger-than-memory SSD or
   Azure Storage profiles, checkpointing, replication, failover, transactions,
   and sharded cluster mode. It also reports sub-300-microsecond p99.9 results
   on a named accelerated-network Azure setup. [Official Garnet
   documentation](https://microsoft.github.io/garnet/docs)

9. Current Garnet separates the hybrid-log budget, read-cache budget, index,
   overflow buckets, and I/O buffer-pool budget. A single opaque `cache_bytes`
   number would hide the pressure source. objectKV needs equivalent per-budget
   OTel signals and admission decisions. [Official memory
   documentation](https://microsoft.github.io/garnet/docs/getting-started/memory)

10. Checkpoint, resize, and migration are explicit epoch-protected state
    machines. The driver prevents new transactions at selected transitions,
    tracks transactions by version, waits for participants, and advances a
    common phase. objectKV should use the same structural pattern for object
    publication, compaction, range movement, and recovery generation changes,
    then keep those transitions aligned with the TLA+ action vocabulary.
    [Pinned `StateMachineDriver`
    source](https://github.com/microsoft/garnet/blob/c9607605baa4b7eca0e41dc622a0ee3e52f5574c/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs#L11-L160)

11. Garnet replication is asynchronous log shipping. Its documentation states
    that primary failure can lose writes that replicas have not received. This
    is an explicit incompatibility with objectKV's default `COMMITTED` contract,
    not a mechanism to copy. [Official replication
    documentation](https://microsoft.github.io/garnet/docs/cluster/replication)

## objectKV mapping

| Garnet mechanism | objectKV analogue | Decision boundary |
|---|---|---|
| RUMDS storage waist | `okv-fabric` primitive contract | `[PROPOSED]` Add `modify` and retain ordered `scan`; keep rich commands above the kernel |
| Deterministic operation log | `okv-log` -> `okv-wal` -> quorum `txLog` | `[CODE-COMPLETE]` local primitives exist; distributed integration remains `[EVALUATING]` |
| Memory, SSD, cloud devices | RangeEngine RAM, NVMe or RocksDB, immutable object history | `[EVALUATING]` Profiles must name acknowledgement depth and data-loss envelope |
| One store, typed string/object/unified/vector sessions | One RangeEngine state, typed `okv-fabric` views | `[PROPOSED]` Keep one version and consistency domain; do not add Redis object types to the kernel |
| Direct I/O-thread execution | Future RAM-profile RPC path | Measure only after storage and transaction work stop dominating the curve |
| Epoch-protected maintenance state machine | Publication, compaction, recovery, and range-movement phases | `[PROPOSED]` Align service events with the TLA+ cell actions |
| 16,384 hash-slot cluster | Future multi-range objectKV cell | Reject as the transaction boundary; one tenant transaction must remain able to cross ranges inside one cell |
| External passive control plane | objectKV generation and placement authority | Insufficient for the target self-managing cell without an owning orchestrator |

## Proposed profile language

```text
ram_volatile
  acknowledge after memory
  fastest, acknowledged state may be lost with the memory replicas

ssd_local
  acknowledge after one local persistent device
  fast, host loss may lose acknowledged state

ssd_quorum
  acknowledge after a replicated txLog quorum
  target durable objectKV hot path

object_committed
  acknowledge only after immutable object publication
  portable durability, object-store latency enters the commit path
```

This vocabulary is `[PROPOSED]`. It makes Garnet's useful commit-point idea
explicit without equating a local SSD acknowledgement with object-native
durability.

## Architectural consequences

### D1. One hot state, several fabric views

`[PROPOSED]` The RangeEngine should own one versioned hot state and expose byte
KV, structured object, ordered range, log, and future columnar views through
the same session and transaction boundary.

```text
okv-fabric session
  -> byte KV | structured object | ordered range | log | columnar
  -> read | upsert | atomic_modify | delete | ordered_scan
  -> one RangeEngine versioned state
  -> txLog commit contract
  -> immutable object base
```

This optimizes for direct execution, shared cache locality, and one recovery
history. It gives up independent scaling and implementation freedom for every
view inside one RangeEngine process. Separate compute fleets remain possible
above immutable snapshots and logs, but they do not become separate hot truth.

### D2. Serving tier and commit depth are independent axes

`[PROPOSED]` RAM, native NVMe, and RocksDB describe how a RangeEngine serves
and retains its disposable hot state. RAM, local stable media, quorum txLog,
and object publication describe when a write may be acknowledged. A profile
must name both axes. Garnet's configurable commit point supports this split;
its asynchronous replication does not satisfy the default objectKV depth.

### D3. Maintenance work is a protocol

`[PROPOSED]` Publication, compaction, checkpoint, recovery, and range movement
must be explicit phased state machines with generation fencing and observable
wait conditions. Background work is not an untracked thread pool because it
changes what history may be reclaimed and which serving image may answer.

### D4. Budget every memory class separately

`[PROPOSED]` The eval and OTel model should report at least mutable overlay,
resident base image, ordered index, read cache, I/O buffers, queued writes, and
native allocator bytes separately. Admission and eviction need the same labels.

## Decision-bearing evals

1. Direct session execution versus per-core dispatch versus multiple local
   engines, under uniform and Zipf point traffic. Measure throughput,
   p50/p95/p99/p99.9, CPU, context switches, cache misses, and copied bytes.
2. Deterministic semantic operation log versus physical committed mutation
   log. Crash after every boundary and measure acknowledgement latency,
   bytes per operation, replay throughput, exact state digest, and compatibility
   across engine versions.
3. RAM acknowledgement, local-NVMe acknowledgement, quorum-txLog
   acknowledgement, and object-gated acknowledgement while publication and
   compaction run. Measure the full latency curve, RPO, rebuild time, object
   operations, amplification, and foreground interference.

## Performance-matrix consequence

- Row 8, RAM serving profile: add Garnet as a matched specialist control only
  after objectKV has the same command subset, payloads, durability depth,
  network topology, and client load.
- Row 9, Redis-like fabric surface: use Garnet and Valkey as separate controls;
  compare only implemented commands and report semantic mismatches before
  latency.
- Rows 2 through 7: no control change. Garnet does not test generation-pinned
  object reads, cell-wide strict serializability, immutable object history, or
  cross-range transactions.
- PostgreSQL and HTAP lanes: no direct Garnet claim. The useful transfer is the
  operation waist and tier contract, not its data model.
