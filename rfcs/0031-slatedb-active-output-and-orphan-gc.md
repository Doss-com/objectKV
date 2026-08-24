# RFC-0031: SlateDB active-output and orphan GC

- Status: accepted for local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0025, RFC-0028, RFC-0030

## Decision

[ACTIVE-WORK] Treat every output named by active compaction authority as a GC
root before it enters the serving manifest. Delete an immutable compacted SST
only after it is old enough and absent from every serving manifest and active
compaction record.

## Question

Can the pinned SlateDB collector preserve completed but unpublished worker
output, then delete an aged immutable SST that no authority root references?

## Frozen contract

For each seed:

1. Write eight overlapping 8 MiB logical snapshots and flush each snapshot.
2. Start separate coordinator and worker processes.
3. Wait until the worker persists compacted output and the job remains active.
4. Stop both processes before the coordinator publishes that output.
5. Run compacted-object GC with zero minimum age.
6. Require every active-job output object to survive.
7. Start a replacement coordinator and require it to commit the preserved output.
8. Create one aged compacted SST absent from every manifest and compaction job.
9. Run compacted-object GC again and require the true orphan to disappear.
10. Reopen a fresh serving handle and verify every latest overwrite exactly.

The negative control runs the second collection as a dry run. It must preserve
data and the active output, but leave the aged orphan and fail only the deletion
gate.

## Hard gates

- eight overlapping L0 SSTs exist before maintenance;
- a worker persists compacted output while the job is active;
- GC preserves every output rooted by active compaction state;
- a replacement coordinator commits the preserved output;
- an aged compacted SST is absent from manifests and active jobs;
- correct GC deletes that true orphan;
- a fresh serving handle returns every latest overwrite exactly;
- fresh open reads at most 1 MiB;
- the first cold point reads at most eight requests and 512 KiB;
- the dry-run control discards for the intended deletion gate.

## Interpretation

A pass admits one local immutable-object reachability boundary for the pinned
SlateDB collector. It does not prove public-cloud listing behavior, delayed or
duplicated object operations, checkpoint and clone roots, multi-tenant roots,
host partitions, or safe retention across an independently implemented OLAP
tail.

## Tradeoff

This optimizes for executable reachability evidence before broadening the root
graph. It gives up aggressive collection because uncertain authority state must
retain objects.

## Result

Candidate `dea0b20` passed the frozen contract across seeds 1103, 2207, and
3301. Runs `8d606761` and `26b19dfb` kept with zero anomalies. GC preserved
every compacted output still named by active compaction state, a replacement
coordinator then committed that exact output, and the second GC deleted every
aged SST absent from manifests and active jobs.

Orphan collection took 1.88 to 1.92 ms. Every fresh serving handle returned the
latest overwrite for all 8,192 keys. Fresh open read 538 bytes and the first
cold point used three to five requests and at most 83,264 bytes.

Dry-run control `161eac32` preserved the same exact data and active output but
discarded only on `aged_unreferenced_sst_deleted`. This distinguishes safe
retention from a collector that can actually reclaim unreachable bytes.

The suite hash is
`b8fa39b88a69f22d1e642f11e2f5f74f5bcc88f1e711add41b7a21ef8e8c67c2`.
All admitted runs exported OTel metrics, traces, and logs through the shared
collector.
