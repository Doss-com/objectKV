# RFC-0010: OLTP and OLAP snapshot semantics

- Status: proposed
- Created: 2026-08-22

## Proposed contract

OLTP and analytical representations share commit versions, schema versions,
table/record identity, and one authoritative history. They may use different
physical objects.

Columnar lag changes query cost, not required freshness. A query chooses one
cell and tenant snapshot `T`. For each table partition `p`, it reads a columnar
base covered through `W_p`, then overlays every table change in `(W_p, T]`:

```text
LogicalTable_p(T) = ColumnarBase_p(W_p) + RowChanges_p(W_p, T]
```

Every table in one query uses the same `T`, even when its partitions have
different base watermarks.

## Snapshot overlay

For each primary key, the tail is reduced to its latest mutation through `T`.
The overlay suppresses any base key present in that reduced tail, then emits the
tail's latest upsert and omits its deletes. When base and tail are ordered by
primary key, the preferred execution is a bounded-memory streaming merge. A
left anti join plus tail union is the correctness fallback.

DataFusion exposes a custom source as `TableProvider + ExecutionPlan + stream`.
The plan can declare output partitioning and ordering, which makes a
`SnapshotOverlayExec` a direct extension point. See the
[DataFusion custom source guide](https://datafusion.apache.org/library-user-guide/custom-table-providers.html).

## Predicate rule

Predicate pushdown cannot erase tail keys needed to invalidate base rows. If a
base row matches `status = OPEN` and the tail changes it to `CLOSED`, the tail
key must still suppress the old base row even though the new row fails the
predicate. The provider must distinguish:

- tail keys required for invalidation;
- tail rows required for final predicate output.

Projection, partition pruning, and Parquet predicate pushdown remain valid only
after this rule holds. Filters that cannot preserve invalidation are reported
as unsupported or inexact to DataFusion.

## Durable analytical tail

The analytical tail is not identical to the short recovery WAL. It must remain
queryable whenever a columnar base can lag beyond WAL retention. A table-change
index records table, partition, commit version, primary key, operation, schema
version, row movement, and required before/after values. Recent changes may be
served from RAM and MVCC overlays; older uncompacted changes live in immutable
delta objects.

```text
recovery tail: commits in (K, C] retained in transaction logs
analytical tail: table changes in (W_p, T] retained until base advancement
```

## Query lifetime and later writes

An analytical query uses a snapshot lease, not a long-lived OLTP transaction.
The lease pins the base manifests, schema version, and required analytical tail
through `T`, then releases them when execution completes.

An analytical result that influences a later serializable write uses one of
three explicit patterns:

1. Maintain invariant-critical aggregates as transactional projections.
2. Return a snapshot version plus dependency tokens, then validate them in a
   short write transaction.
3. Treat long planning as a proposal and revalidate or reserve resources before
   applying it.

Broad dependencies require broad coordination. A cell-wide version token is a
correct fallback but conflicts with every intervening cell write. The
FoundationDB Record Layer provides prior art for indexes that maintain partial
aggregates and implement core count/sum aggregates with atomic mutations. See
its [aggregate index extension points](https://foundationdb.github.io/fdb-record-layer/Extending.html#aggregate-functions).

## Required evals

- base plus insert, update, delete, and partition-move tails equal the row oracle
  at one exact `T`;
- independently lagging partitions and tables still join at one `T`;
- the predicate-invalidation negative control returns no stale base row;
- query lease expiry never causes silent fallback to an older snapshot;
- WAL truncation does not remove an analytical tail still required by `W_p`;
- dependency-token granularity reports retry rate and certificate size;
- transactional projections remain consistent under conflicting writers.

## Questions to resolve

- Columnar snapshot metadata and covered-through version.
- Schema changes inside a materialization interval.
- Retention roots shared by MVCC, CDC, snapshots, and analytical objects.
- When Parquet evidence justifies a later Vortex experiment.
- Partitioning and ordering requirements for the first streaming overlay.
- Minimum before-image data needed for differential aggregates.
