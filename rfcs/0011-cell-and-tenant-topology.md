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
- disposable KV Runtime and materialization workers;
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

That documentation is vocabulary evidence only. objectKV must separately prove
tenant authentication, authorization, quotas, encryption boundaries, cache
isolation, noisy-neighbor control, and movement safety.

## Cell protocol map

The first complete cell exposes these logical boundaries even when several are
implemented in one process:

```text
MetaclusterDirectory [FUTURE]
  TenantId -> CellId + RoutingEpoch

TenantSession
  -> ReadVersionService.get(min_known_version)
  -> RangeMap.lookup(key, routing_epoch)
  -> KvRuntime.read_at(range, version)
       -> RangeEngine.read_at(version)
  -> CommitProxy.commit(read_conflicts, write_conflicts, mutations)
       -> VersionAuthority.allocate(active_generation)
       -> Resolver.resolve(version, conflict domains)
       -> txLog.append(versioned, tagged commit envelope)
       -> DurableFrontier.advance only after reconstructable publication
       -> Materializer publishes through ManifestAuthority
```

The protocol vocabulary is:

| Boundary | Required identity or result |
|---|---|
| `GenerationAuthority` | `CellId`, active generation, transaction-system membership, WAL root, control root |
| `TenantSession` | `CellId`, `TenantId`, `RoutingEpoch`, causal minimum version |
| `ReadVersionService` | exact cell version at or above the caller's causal minimum, or a fenced/unavailable error |
| `CommitProxy` | one transaction outcome bound to tenant, generation, version, conflict ranges, and mutation fingerprint |
| `Resolver` | accept or reject one declared subset of conflict ranges at one version |
| `txLog` | checksum-protected commit envelope plus required mutation tags and quorum evidence |
| `RangeMap` | versioned range assignment and serving generation |
| `KvRuntime` | disposable process-wide RAM, NVMe, cache, pressure, and assignment envelope |
| `RangeEngine` | exact read at `T`, `version_not_applied`, `version_too_old`, or fenced routing error for one assigned range |
| `ManifestAuthority` | conditional, generation-fenced root or range-manifest publication |
| `DurableFrontier` | conservative cell frontier plus per-range and per-consumer positions |
| `Ratekeeper` | admission from retained-log bytes, objectification debt, role saturation, and recovery state |
| `DataDistributor` | split, placement, movement, and assignment without changing tenant transaction semantics |

No durable commit exists unless every required resolver accepts and every
required log set durably acknowledges the envelope. A resolver may retain
conservative in-memory conflict state for a rejected transaction, causing a
false conflict later, but partial resolver acceptance can never become a
visible or durable commit.

## Bootstrap authority

`[DECIDED]` Each cell has its own statically bootstrapped coordinator quorum as
defined by RFC-0009. It owns that cell's generation and root control pointer.
The cell can recover and serve its existing tenants without the metacluster.

`[FUTURE]` A separate metacluster authority owns tenant placement and routing
epochs across cells. It does not issue cell commit versions, participate in
cell commit quorum, or become necessary for ordinary in-cell recovery. The
metacluster bootstrap and membership-change protocol remain unresolved.

## Incremental implementation

The client contract should not depend on the first cell's internal role count.
The current versioned engine and object-store work is a cell substrate, not a
complete cell claim.

1. Cell substrate v0 proves versioned storage, immutable publication, object
   economics, generation fencing, and the replicated WAL contract. It need not
   expose distributed transactions or call itself a cell implementation.
2. Cell v0 is the first complete transaction cell. It centralizes read-version
   allocation, commit ordering, logical resolution, and one replicated log set.
   It supports concurrent clients, atomic multi-range tenant transactions,
   explicit range routing, direct reads from one or more KV Runtimes, and
   generation recovery. Static range boundaries are acceptable.
3. Cell v1 adds dynamic range splitting, placement, tagged materialization,
   empty-cache worker recovery, and automated distribution.
4. Cell v2 partitions conflict resolution by ordered conflict domain.
5. Cell v3 adds multiple commit and read-version proxies.
6. Cell v4 partitions transaction logs and recovery positions.
7. Cell v5 adds metacluster placement, migration, evacuation, and upgrades.

Logical protocols for read-version allocation, commit, resolution, txLog,
range mapping, KV Runtime serving, and distribution must be explicit in v0 even
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
  clients, stale routes, stale generations, and causal read-version minima. A
  missing conflict and a stale read-version negative subject must fail.
- Commit outcome: crash every boundary before and after leader fsync, quorum
  fsync, reply, and retry. Acknowledged loss, two commits for one retained
  identity, and RAM-only deduplication must fail.
- Objectification: race manifest publication, the cell durable frontier, and
  WAL pop. No required log entry may be popped before every affected range is
  reconstructable.
- Partitioned resolver: one-role and partitioned-role runs must accept the same
  histories. No durable commit exists unless every required resolver accepts;
  injected omission of one conflict domain must fail.
- Partitioned logs: acknowledgement waits for every required tagged log set;
  missing-tag and recovery-generation faults must fail exact replay.
- Metacluster: tenant movement copies bounded durable bytes when object reuse is
  available, prevents dual writers, and preserves snapshot plus tail exactly.
- Tenant isolation: one tenant's workload, key encoding, quota, encryption
  boundary, or movement epoch cannot expose or mutate another tenant's state.
- Operating envelope: publish throughput, recovery time, metadata size, and
  failure radius against cells of increasing range, process, and tenant count.

## Tradeoff

Optimizes for: FDB-like transaction scope inside a bounded database cluster,
independent fleet recovery, and incremental scaling without redefining the API.

Gives up: cross-cell atomic transactions and the simplicity of treating each
cell as one small single-writer KV partition.

## Unresolved questions

- Exact tenant, transaction, and range limits for Cell v0.
- Metacluster bootstrap, membership change, and disaster recovery.
- Whether immutable objects may be shared across tenant or cell encryption
  boundaries during movement.
- The measurement that triggers resolver, proxy, and log partitioning.
- Bounded recovery-root and range-index representation at the cell ceiling.
- Whether strong client-identity deduplication remains part of the public commit
  contract after its durable retention and generation semantics are simulated.
