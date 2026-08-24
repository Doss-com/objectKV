# RFC-0012: DataFusion snapshot overlay implementation

- Status: proposed
- Created: 2026-08-23

## Decision

The first physical ZebraDB snapshot source pins DataFusion 54.0.0 and its Arrow
and Parquet 58.3.0 line. DataFusion 55 requires Rust 1.94, while the repository
targets Rust 1.88. DataFusion 54 declares Rust 1.88 and is therefore the newest
compatible major at this checkpoint.

`ZebraSnapshotTableProvider` owns snapshot target `T`, per-partition base
watermarks, a Parquet base provider, and an Arrow tail provider. Its scan never
pushes SQL projection, predicates, ordering, or limit below invalidation. It
scans the hidden primary key, operation, commit version, writer schema, previous
partition, and next partition inputs, runs `SnapshotOverlayExec`, then applies
the requested SQL projection above the overlay.

The first execution candidate is a correctness adapter. It may materialize its
bounded fixture inputs before primary-key reduction and merge. It must report
that behavior and cannot produce an admitted memory or latency curve. A later
candidate replaces materialization with a record-batch streaming merge after
the ordering and partition-alignment gates pass.

## Physical row contract

The initial normalized output contains:

```text
id: UInt64, non-null
status: Utf8, non-null
partition: Utf8, non-null
amount_cents: UInt64, non-null
priority: UInt8, non-null
```

The Parquet control fixture uses writer schema v1, where `total_cents` becomes
`amount_cents` and `priority` defaults to zero. The Arrow tail uses writer
schema v2 and includes `commit_version`, `operation`, `previous_partition`, and
the after-image. `DELETE` has no after-image. A move retains the same logical
`id`, invalidates `previous_partition`, and emits the v2 after-image in its new
partition.

The operator rejects null primary keys, unknown operations, unsupported writer
schemas, unsorted input where ordering is declared, a target outside analytical
coverage, and any row whose transformation to `SchemaAt(T)` is unavailable.

## Pushdown contract

`TableProvider::scan` treats all filters as unsupported and ignores an offered
limit. DataFusion keeps them above the custom execution plan. The provider may
accept a final output projection only after it has added every hidden overlay
input to both child scans.

This optimizes for exactness and a small auditable extension point. It gives up
Parquet pruning and early limit until a later equivalence proof shows those
optimizations cannot remove invalidation keys or change result ordering.

## Eval contract

The `zebradb-datafusion-overlay-v1` suite writes a deterministic v1 Parquet base,
builds a v2 Arrow tail, registers `ZebraSnapshotTableProvider`, executes SQL
through DataFusion, and compares canonical rows to the frozen RFC-0010 oracle.
It measures source rows, overlay rows, materialized bytes, and query duration,
but correctness is the only admission metric for this candidate.

Negative subjects must expose:

1. filtering the tail before it invalidates a matching base row;
2. reducing a cross-partition move independently per physical partition;
3. applying output projection before retaining the hidden primary key.

## Unresolved questions

- Exact `ExecutionPlan` ordering declaration for aligned range partitions.
- Streaming state machine across arbitrary record-batch boundaries.
- Parquet row-group pruning that preserves a separate invalidation-key stream.
- Snapshot-manifest and analytical-coverage object encodings.
- Spill policy and memory-pool accounting for the correctness fallback.
