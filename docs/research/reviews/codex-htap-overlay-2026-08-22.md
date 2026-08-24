# Internal adversarial review: exact HTAP overlay

Status: `[EXISTS]` read-only Codex multi-agent review of commit `07a449a` on
2026-08-22. This is an internal architecture review, not external expert
consensus.

## Verdict

Base plus durable analytical tail is the right decomposition, but RFC-0010 was
not yet an executable exactness contract. Atomic capture, physical ordering,
schema normalization, partition moves, leases, and phantom-safe certificates
must be frozen before T16 or T17 implementation.

## Missing invariants

1. Every committed row mutation atomically creates one gap-free, idempotent
   analytical change effect. An exact partition read requires
   `W_p <= T <= min(C, A_p)`, where `A_p` is complete analytical coverage.
2. A streaming merge requires identical non-null primary-key encoding,
   collation, aligned range partitioning, and per-execution-partition ordering.
   Primary key and invalidation columns remain hidden inputs through projection.
   `LIMIT` stays above the overlay until equivalence is proven.
3. A partition move is an old-partition invalidation plus new-partition upsert
   at one commit version. Differing watermarks and move-away-then-back histories
   must reduce by logical row identity.
4. Base and tail rows normalize from their writer schema and partition epoch to
   `SchemaAt(T)` before filtering and merge. Defaults, renames, type changes,
   partition changes, primary-key changes, and retained transformations need
   explicit rules.
5. Tail GC and snapshot leases are one protocol. Lease acquisition pins the
   complete base, tail, schema, and partition-map closure. Expiry returns an
   error, never a rebase.
6. Certificates bind cell, tenant, `T`, schema, plan rules, domains, and token
   values. Every relevant mutation updates tokens using before and after
   predicate membership. Validation and the dependent write share one
   serializable transaction, so inserts cannot create untracked phantoms.
7. Result recall is not an exactness oracle. Compare canonical row multisets,
   values, duplicates, deletes, and aggregates. Measure tail rows, tail bytes,
   memory, spill, and latency separately from freshness.

DataFusion's `TableProvider` defines projection, filter, and limit pushdown
contracts, while execution ordering is per partition. See the
[`TableProvider` API](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html)
and
[`ExecutionPlanProperties` API](https://docs.rs/datafusion/latest/datafusion/physical_plan/execution_plan/trait.ExecutionPlanProperties.html).

## Minimal negative controls

1. Pushdown poison: a base `OPEN` row changes to `CLOSED`; projection omits the
   primary key and the query uses filter, ordering, and limit. Prefiltering the
   tail or limiting below overlay must leak a stale row and fail.
2. Schema plus move: schema v2 renames a field while one row moves between
   partitions with different watermarks. Exactly one normalized row must remain.
3. WAL-pop conflation: recovery durability advances while the columnar base
   remains behind. Deleting the analytical tail with the recovery WAL must fail.
4. Lease and GC race: a query pins base 10 and tail through 12 while base 15
   publishes. It returns exact version 12 or `snapshot_unavailable`, never mixed
   objects.
5. Certificate time-of-check/time-of-use: a concurrent order changes the token
   after validation begins but before the approval write. Non-atomic validation
   must incorrectly approve and fail the oracle.

## Disposition

RFC-0003 and RFC-0010 own the storage and exactness contracts. The serving-model
suite now uses exact result equality as the hard gate and p99 exact-snapshot
latency as the HTAP primary metric. T16 and T17 remain blocked on their minimal
deterministic models rather than the distributed transaction system.
