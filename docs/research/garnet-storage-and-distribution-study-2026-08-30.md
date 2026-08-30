# Garnet storage and distribution study

Date: 2026-08-30

Status: `[EVALUATING]` primary-source study. This document changes no objectKV
implementation or performance status.

## Clarity

Question: Which Garnet mechanisms should change objectKV's RAM, log, and
distributed-system design?

Punchline: Garnet validates a narrow operation waist, deterministic operation
log, and explicit RAM, SSD, and cloud commit depths, but its hash-slot cluster
and same-slot transaction boundary cannot replace objectKV's cell-scoped
strict-serializable transaction plane.

Counter: This call is wrong if the product requirement narrows from an ordered
transactional kernel to a Redis-compatible sharded cache whose multi-key work
may be confined to one hash slot.

Next: Freeze `read`, `upsert`, `modify`, `delete`, and `scan` as the candidate
`okv-fabric` storage waist, then compare Garnet only in the future RAM and
Redis-like workload lanes.

Confidence: High on the architectural boundary, medium on the performance
transfer because the published comparisons are Redis workloads rather than
objectKV's ordered MVCC, object-history, and HTAP workload.

## Primary-source observations

1. Garnet maps its command surface onto five storage operations: `Read`,
   `Upsert`, `Modify`, `Delete`, and `Scan`. The four-operation summary omits
   `Scan`. The paper calls this interface RUMDS and uses `Modify` for atomic
   read-modify-write. [PVLDB paper, sections 3.1 and
   5.1](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)

2. The Tsavorite-backed main store owns raw string data. An optional object
   store owns richer types. One deterministic operation log records Tsavorite
   operations rather than RESP commands and coordinates durability,
   replication, and checkpoint recovery across both stores. Non-deterministic
   command inputs are captured before logging. [Garnet repository
   architecture](https://github.com/microsoft/garnet#storage), [PVLDB paper,
   sections 5.2 and 6.1](https://www.vldb.org/pvldb/vol19/p224-chandramouli.pdf)

3. `TieredStorageDevice` orders devices from hot to cold, reads from the
   closest tier that contains the segment, and writes to all tiers in parallel.
   Its callback waits through a configured commit point; colder writes may
   continue after acknowledgement. [TieredStorageDevice source](https://github.com/microsoft/garnet/blob/main/libs/storage/Tsavorite/cs/src/core/Device/TieredStorageDevice.cs)

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

## objectKV mapping

| Garnet mechanism | objectKV analogue | Decision boundary |
|---|---|---|
| RUMDS storage waist | `okv-fabric` primitive contract | `[PROPOSED]` Add `modify` and retain ordered `scan`; keep rich commands above the kernel |
| Deterministic operation log | `okv-log` -> `okv-wal` -> quorum `txLog` | `[CODE-COMPLETE]` local primitives exist; distributed integration remains `[EVALUATING]` |
| Memory, SSD, cloud devices | RangeEngine RAM, NVMe or RocksDB, immutable object history | `[EVALUATING]` Profiles must name acknowledgement depth and data-loss envelope |
| Main and object stores | Opaque ordered values plus higher-level adapters | Do not add Redis object types to the kernel |
| Direct I/O-thread execution | Future RAM-profile RPC path | Measure only after storage and transaction work stop dominating the curve |
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
