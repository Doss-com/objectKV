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

For each partition, `A_p` is the highest version through which the durable
analytical change history is known complete. An exact query is admissible only
when:

```text
W_p <= T <= min(C, A_p)
```

Every committed row mutation must atomically create its complete, idempotent
table-change effect in the same transaction. If any required partition lacks
complete analytical coverage through `T`, the query waits within policy or
returns `snapshot_unavailable`. It never returns a mixed or silently older
snapshot.

## Snapshot overlay

For each primary key, the tail is reduced to its latest mutation through `T`.
The overlay suppresses any base key present in that reduced tail, then emits the
tail's latest upsert and omits its deletes. When base and tail are ordered by
primary key, the preferred execution is a bounded-memory streaming merge. A
left anti join plus tail union is the correctness fallback.

The streaming path is valid only when base and tail share the same non-null
primary-key encoding, byte ordering, collation, range partitioning, and
per-execution-partition sort order. DataFusion ordering is not assumed to be
global. The overlay retains primary-key, schema, partition-epoch, operation,
and commit-version columns as hidden inputs even when the SQL projection omits
them. A pushed `LIMIT` remains above the overlay unless an equivalence proof
shows that early limiting cannot change invalidation or ordering.

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

## Partition movement and schema

A row movement creates two atomic effects at the same commit version:

1. invalidate the primary key in the old partition;
2. upsert the normalized row in the new partition.

The rule holds when the two partitions have different `W_p` values and when a
row moves away and back more than once inside one tail interval. Reduction is
by logical row identity and ordered effects, not by independently choosing the
latest row in each physical partition.

Every base and tail row is decoded from its writer schema and partition epoch
into `SchemaAt(T)` before filtering, invalidation, or output. The schema
contract must define defaults, renames, compatible type changes, partitioning
changes, retained transformation code, and rejection of primary-key changes
that cannot preserve identity. An exact query is unavailable when a required
writer schema or transformation is no longer retained.

## Durable analytical tail

The analytical tail is not identical to the short recovery WAL. It must remain
queryable whenever a columnar base can lag beyond WAL retention. A table-change
index records table, partition, commit version, primary key, operation, schema
version, row movement, and required before/after values. Recent changes may be
served from RAM and MVCC overlays; older uncompacted changes live in immutable
delta objects.

```text
recovery tail: commits in (O, C] retained in transaction logs
analytical tail: table changes in (W_p, A_p] retained until safe replacement
```

Advancing `O` or popping recovery WAL never reclaims analytical changes by
itself. Tail reclamation requires a complete replacement base manifest plus all
snapshot leases, historical reads, CDC positions, backups, branches, and schema
transformation roots to have advanced beyond the reclaimed interval.

## Query lifetime and later writes

An analytical query uses a snapshot lease, not a long-lived OLTP transaction.
Lease acquisition atomically pins the complete base, tail, schema, and
partition-map object closure for every queried table at one `T`. The lease
releases those roots when execution completes. Expiry or renewal failure returns
an error. It never rebases the query or substitutes objects from another
snapshot.

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

A dependency certificate binds `CellId`, `TenantId`, `T`, schema version,
partitioning epoch, plan-rule version, and every dependency domain and token
value read at `T`. Relevant mutations update tokens from both before-image and
after-image predicate membership, so inserts and moves cannot create untracked
phantoms. Token validation and the dependent write occur in one serializable
transaction with conflicts on the validated tokens. If the planner cannot
derive complete deterministic dependencies, the result is uncertifiable or
uses the coarse cell-wide token.

## Required evals

- canonical row multisets, values, duplicates, deletes, and aggregates exactly
  equal an independent row oracle at one `T`;
- independently lagging partitions and tables still join at one `T`;
- a predicate plus projection plus `ORDER BY` plus `LIMIT` pushdown poison cannot
  leak a stale base row;
- a schema change plus cross-partition row movement produces one normalized row;
- query lease expiry never causes silent fallback to an older snapshot;
- WAL pop does not remove an analytical tail still required by `W_p`;
- base replacement and GC racing an active lease returns the exact snapshot or
  `snapshot_unavailable`, never mixed objects;
- dependency-token granularity reports retry rate and certificate size;
- a token-validation time-of-check/time-of-use race must retry, not approve;
- transactional projections remain consistent under conflicting writers.

Correctness and cost are separate measurements. The suite records exact result
equality as a hard gate, then tail rows, tail bytes, peak memory, spill bytes,
and latency as `T - W_p` grows. Freshness lag is not a proxy for overlay cost.

## Executable contract model

`[VERIFIED]` `crates/okv-model/src/htap.rs` and
`evals/suites/htap-contract.toml` make the exactness rules executable before a
DataFusion operator exists. Five deterministic seeds cover:

- invalidation before predicate, projection, ordering, and limit;
- schema normalization plus a cross-partition move at unequal watermarks;
- analytical-tail retention after recovery-WAL pop;
- active snapshot closure during a base-publication and GC race;
- atomic dependency-token validation and write conflict;
- two independently lagging table bases combined at one target version.

Five negative subjects each violate one rule and must receive a `discard`
verdict at a bounded step. The runner emits exact-result, anomaly, tail-row,
tail-byte, peak-memory, spill, replay, and trace evidence through the shared
result and OTel path.

`[PROPOSED]` This model does not claim a DataFusion `TableProvider`, Arrow
stream, Parquet reader, snapshot-manifest protocol, or production certificate
implementation. Those implementations must pass the same row oracle before
their latency or memory curves are admitted.

## Questions to resolve

- Complete columnar snapshot-manifest and analytical-coverage encodings.
- Exact allowed schema and primary-key evolution rules.
- Retention roots shared by MVCC, CDC, snapshots, and analytical objects.
- When Parquet evidence justifies a later Vortex experiment.
- Partitioning and ordering requirements for the first streaming overlay.
- Minimum before-image data needed for differential aggregates.
