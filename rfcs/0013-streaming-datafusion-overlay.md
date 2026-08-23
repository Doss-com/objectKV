# RFC-0013: Streaming DataFusion snapshot overlay

- Status: proposed
- Created: 2026-08-23

## Decision

The second physical ZebraDB source replaces whole-input materialization with an
incremental ordered merge. For one logical-id execution range, both the
normalized Parquet base stream and Arrow table-change stream are ordered by
non-null primary key. Tail effects for one key are additionally ordered by
commit version. `StreamingSnapshotOverlayExec` validates those orders across
record-batch boundaries before declaring ordered output.

For partition base watermarks `W_p` and target version `T`, the first streaming
implementation reads the logical tail interval:

```text
(min(W_p), T]
```

It groups every base row and table-change effect for one logical identity. A
tail group suppresses every base representation for that identity and emits
only its latest after-image through `T`, unless that effect is a delete. When no
tail group exists, the normalized base group passes through. This handles a row
that appears in bases taken at different physical-partition watermarks without
reducing moves independently per physical partition.

Scanning from the minimum watermark may read changes already reflected in a
newer partition base. Replaying those idempotent effects is correct and keeps
the first stream shape simple. It gives up the lower tail volume of a later
partition-aware invalidation and after-image split.

## Ordering and memory contract

One execution partition owns one disjoint logical-id interval. DataFusion
ordering is per execution partition, not global. The plan declares ascending
`(id, partition)` output only after it has validated:

- base order `(id, partition)` across every input batch;
- tail order `(id, commit_version)` across every input batch;
- identical non-null primary-key encoding and collation;
- no logical-id group larger than the configured resource bound.

The operator may retain at most one base batch, one tail batch, one logical-id
group, and one bounded output batch. It emits output incrementally and advertises
`EmissionType::Incremental`. The fixture caps one logical-id group at 16 input
rows and one output batch at two rows. Exceeding the group bound returns a
resource error instead of silently materializing or spilling.

Spill remains zero for this ordered path. The existing materializing operator
remains the correctness fallback, but it is ineligible for this streaming gate
even when its rows are exact.

## Independent watermark fixture

The physical fixture contains west base data through version 5 and east base
data through version 8. Its globally ordered tail spans versions 6 through 13,
while the query target is 12. It includes:

- two effects for one identity split across Arrow batches;
- an update at version 6 that is required only because west lags;
- an insert at version 7 already present in the east base;
- a delete;
- a west-to-east movement;
- an update at the target version;
- an insert after the target that must remain invisible.

The exact stream scans `(5, 12]`. Starting at the maximum watermark drops the
west update. Treating batches as group boundaries emits duplicate or stale
rows. Both defects must be independently observable.

## Continuation contract

A page boundary does not open or retain an OLTP transaction. Its continuation
identity binds cell, tenant, table, target version `T`, schema version,
partition epoch, plan-rule version, and last emitted `(id, partition)`. The
next page reacquires immutable inputs for the same snapshot and resumes strictly
after that logical key.

The executable fixture proves the target binding by placing a new row at
version 13. Reusing the key boundary while rebasing the second page from version
12 to 13 must fail the continuation gate. Cryptographic token encoding,
snapshot-lease renewal, and manifest acquisition remain separate work.

## Eval contract

`zebradb-datafusion-streaming-v1` freezes three seeds, a 30-event budget, the
two-row output batch size, and five negative subjects:

1. materialize both inputs before overlay;
2. reset a logical-id group at an Arrow batch boundary;
3. start the tail at `max(W_p)` instead of `min(W_p)`;
4. rebase a continuation to a newer target version;
5. accept unsorted input while still claiming ordered output.

The primary metric remains correctness anomalies. Exact result equality,
incremental emission, input-order validation, cross-batch grouping, independent
watermarks, target-bound continuation, bounded buffering, and output-order
declaration are hard gates. Tail rows, tail bytes, input/output batch counts,
peak buffered rows and bytes, spill bytes, and duration are telemetry. This gate
admits a streaming mechanism, not a latency curve.

Candidate surface:

- `crates/okv-htap/src/streaming.rs`;
- the `htap_streaming_contract` adapter in `crates/okv-eval`;
- CI invocation for the frozen suite.

Frozen during the experiment:

- `crates/okv-model` and RFC-0010 oracle semantics;
- this RFC and `evals/suites/htap-streaming.toml`;
- eval seeds, budget, metric registry, aggregation, and result schema.

## Deferred

- snapshot-manifest and lease acquisition;
- multiple logical-id execution ranges and repartitioning;
- partition-aware tail pruning that preserves invalidation;
- spill for an unordered correctness fallback;
- a scalable `T - W_p` performance and economics dataset;
- retained schema transformation objects and primary-key evolution.
