# RFC-0011: Cell and tenant topology

- Status: draft
- Created: 2026-08-22

## Proposed decision

An objectKV cell is a complete distributed transactional database cluster. It
contains the transaction, durability, storage, recovery, and control roles for
one bounded operating envelope. A cell is not a KV entry, physical block,
segment object, or logical key range.

A tenant database is the normal transaction domain. One transaction may access
arbitrary keys and ranges inside its tenant database, subject to the kernel's
size and duration limits. A transaction cannot span cells. A metacluster routes
tenant databases to cells and owns placement and migration, but does not join
their commit histories.

## Vocabulary and hierarchy

```text
global fabric / metacluster
  -> cell
       -> tenant database / transaction domain
            -> ordered keyspace
                 -> shard / range
                      -> immutable segment object
                           -> block / page
                                -> key/value entry
```

| Concept | Contract |
|---|---|
| Key/value entry | One logical ordered key and value |
| Block/page | Physical read and decompression unit inside a segment |
| Segment/object | Immutable sorted versioned entries |
| Shard/range | Contiguous ordered-key interval for routing and work assignment |
| Tenant database | Keys that one serializable transaction may access |
| Cell | Complete transaction, durability, storage, control, and recovery system |
| Metacluster | Tenant-to-cell placement, routing epoch, and migration authority |

FoundationDB uses a shard for a continuous key range and divides one cluster
into thousands of shards. Its transaction proxies sequence commits, consult
resolver partitions, and make tagged mutations durable in transaction logs
before storage servers pull them. This RFC transfers that transaction shape,
not FoundationDB's replicated-local-disk storage-server ownership. See the
[FoundationDB HA write path](https://apple.github.io/foundationdb/ha-write-path.html).

## Cell boundary

Each cell eventually contains:

- read-version and commit-proxy roles;
- partitionable conflict resolvers;
- cell-scoped commit versions and recovery generations;
- partitionable replicated transaction logs and tagged mutation streams;
- an ordered keyspace split into many ranges;
- disposable serving and materialization workers;
- object segment manifests, objectification watermarks, and GC roots;
- membership, assignments, fencing epochs, ratekeeping, and recovery control.

Cells have independent version spaces, logs, recovery generations, watermarks,
and failure domains. A large tenant may receive a dedicated cell. A shared cell
may host many tenant databases with quotas, cache isolation, encryption policy,
and per-tenant ratekeeping.

FoundationDB's documented tenant feature similarly defines a tenant as a named
transaction domain confined to one keyspace, although that specific feature is
still experimental. See the
[FoundationDB tenant documentation](https://apple.github.io/foundationdb/tenants.html).

## Incremental implementation

The client contract should not depend on the first cell's internal role count.

1. Cell v0 centralizes read-version allocation, commit ordering, logical
   resolution, and one replicated log set. It still supports concurrent clients,
   multi-key transactions across ranges, and direct distributed reads.
2. Cell v1 distributes serving workers, range routing, splitting, tagged
   materialization, and empty-cache recovery.
3. Cell v2 partitions conflict resolution by ordered conflict domain.
4. Cell v3 adds multiple commit and read-version proxies.
5. Cell v4 partitions transaction logs and recovery positions.
6. Cell v5 adds metacluster placement, migration, evacuation, and upgrades.

Logical protocols for read-version allocation, commit, resolution, durable log,
range mapping, storage serving, and distribution must be explicit in v0 even
when several roles share one process. This keeps centralized throughput a staged
implementation choice rather than the permanent architecture.

## Tenant movement

A cross-cell tenant move is snapshot plus tail migration, not a distributed
transaction:

1. establish source snapshot `T` and a destination routing epoch;
2. reference or copy immutable historical objects;
3. stream the committed tenant tail after `T`;
4. enter a bounded write freeze;
5. fence the source and conditionally publish the destination routing epoch;
6. resume writes in the destination and retain rollback roots through the
   declared safety horizon.

No transaction may observe both cells as writable for the same tenant epoch.

## Why bound cells

One global transaction and recovery system maximizes transaction scope but also
expands recovery time, metadata contention, upgrade scope, and blast radius.
FoundationDB documents performance testing up to 500 processes and database
testing up to 100 TB, which is evidence that mature systems still need declared
operating envelopes. It is not a capacity prediction for objectKV. See
[FoundationDB known limitations](https://apple.github.io/foundationdb/known-limitations.html).

## Eval gates

- Cell v0: strict-serializable histories across several ranges with concurrent
  clients; one centralized role may saturate but may not weaken semantics.
- Partitioned resolver: multi-domain transactions commit only when every
  resolver accepts the same transaction version; injected partial acceptance
  must fail.
- Partitioned logs: acknowledgement waits for every required tagged log set;
  missing-tag and recovery-generation faults must fail exact replay.
- Metacluster: tenant movement copies bounded durable bytes when object reuse is
  available, prevents dual writers, and preserves snapshot plus tail exactly.
- Operating envelope: publish throughput, recovery time, metadata size, and
  failure radius against cells of increasing range, process, and tenant count.

## Tradeoff

Optimizes for: FDB-like transaction scope inside a bounded database cluster,
independent fleet recovery, and incremental scaling without redefining the API.

Gives up: cross-cell atomic transactions and the simplicity of treating each
cell as one small single-writer KV partition.

## Unresolved questions

- Exact tenant, transaction, and range limits for Cell v0.
- The bootstrap authority for the first cell and the metacluster directory.
- Whether immutable objects may be shared across tenant or cell encryption
  boundaries during movement.
- The measurement that triggers resolver, proxy, and log partitioning.
- The protocol for restoring a cell when the metacluster is unavailable.
