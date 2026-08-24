# RFC-0024: SlateDB bounded physical configuration pass

- Status: accepted for the local Phase 0 serving-worker candidate
- Authors: DOSS
- Created: 2026-08-23
- Supersedes: the RFC-0022 untuned incumbent only if every configured gate passes

## Decision

Run one frozen physical configuration against the 64 MiB RFC-0022 dataset.
Separate serving from maintenance: the serving process uses 64 KiB SST blocks,
Bloom filters on every non-empty SST, no SlateDB object WAL, and no embedded
compactor or garbage collector. The objectKV transaction log remains the recent
durability authority; compaction and garbage collection become separately
scheduled object workers. Do not tune again from the result.

## Context and invariant

The repaired untuned run read 210,773,938 bytes during fresh-instance open and
1,395,893 bytes for its first correct 1 KiB point read. The cliff begins where
the logical dataset crosses SlateDB's default 64 MiB L0 target. The ranked
hypotheses frozen before this pass are:

1. embedded maintenance starts against the oversized L0 state during open and
   performs dataset-proportional reads;
2. 4 KiB SST blocks create an index large enough that a cold point read exceeds
   the product byte budget;
3. WAL replay is secondary because clean close flushes the memtable, but a
   second object WAL duplicates objectKV's transaction-log authority and write
   cost.

The logical oracle, fresh-instance cache gate, and exact ordered scan remain
unchanged. Disabling SlateDB's object WAL is valid only because this profile is
a materialization worker beneath the separately replicated objectKV log. It is
not a standalone SlateDB durability claim.

## Proposed contract

`objectkv-serving-v1` freezes:

| Setting | Value | Reason |
|---|---:|---|
| SST block | 64 KiB | reduce index and request amplification |
| minimum filter keys | 1 | retain whole-key invalidation before data fetch |
| SlateDB object WAL | disabled | objectKV transaction log is authoritative |
| automatic flush | disabled | objectification controls materialization cadence |
| embedded compactor | disabled | serving open cannot start dataset work |
| embedded GC | disabled | serving open cannot delete or enumerate maintenance state |
| L0 target | 64 MiB | change only the four named configuration dimensions |

The raw report carries these settings. The default and configured runs use the
same generator, logical bytes, key count, read samples, scan, and close and
reopen sequence.

## Failure model

This pass covers local filesystem objects and a new in-process cache. It does
not cover remote latency, process death during materialization, the external
compaction worker, garbage-collection fencing, transaction-log loss, or object
provider throttling. A passing serving profile therefore keeps SlateDB only as
a candidate segment implementation.

## Alternatives

- Increase cache capacity. This optimizes the warm path and gives up the empty
  worker invariant, so it is rejected.
- Wait for embedded compaction before close. This may produce a compacted base,
  but couples serving lifecycle to dataset work and masks restart scheduling.
- Disable only the compactor. This isolates the leading hypothesis but leaves
  the known point-read index cost and duplicate WAL authority unchanged.
- Replace SlateDB immediately. This avoids adaptation cost but discards a useful
  pinned reference before the one pass allowed by D30.

## Eval plan

The frozen suite is
`evals/suites/phase0-slate-bounded-configuration.toml`. Run the default profile,
the configured seed 1103 profile, the confirmation seeds 2207 and 3301, and the
warm-instance poison. Every correct run must preserve the logical gates and:

- read at most 1 MiB during fresh-instance open;
- use at most eight object requests for the first correct cold point read;
- fetch at most 512 KiB for that point read.

The primary metric remains open through first correct read. The result is
`keep_candidate`, `replace_slatedb_incumbent`, or `instrumentation_failure`.
There is no second configuration pass.

## Result

Candidate `7567b99` kept the configured profile and both confirmation seeds.
The warm-instance poison discarded on `fresh_db_cache_on_reopen`.

| Metric | Default seed 1103 | Configured seed 1103 | Confirmation seeds 2207, 3301 |
|---|---:|---:|---:|
| Open through first correct read | 415.66 ms | 3.81 ms | 3.81 ms, 4.12 ms |
| Fresh-open read bytes | 210,773,938 | 402 | 402, 402 |
| First-point requests | 3 | 5 | 5, 5 |
| First-point read bytes | 1,395,893 | 210,439 | 210,435, 210,439 |
| Total read bytes | 255,477,624 | 27,390,034 | 27,390,078, 27,390,042 |
| Total written bytes | 141,386,507 | 68,873,267 | 68,873,267 each |
| Total requests | 345 | 455 | 455 each |

The configured profile reduced total read bytes by 89.3 percent and written
bytes by 51.3 percent. It increased total requests by 31.9 percent. This keeps
SlateDB as a local candidate segment implementation and rejects embedded
maintenance in a serving worker. It does not admit external compaction, remote
object-store economics, or standalone durability.

## Compatibility and migration

The Phase 0 raw contract advances from version 2 to version 3 and records the
physical receipt. The logical dataset receipt remains independent of physical
layout. Existing SSTs are not migrated by this experiment. A future production
format change requires an explicit reader compatibility and rewrite plan.

## Unresolved questions

- Can a separate compaction worker maintain the same limits after overwrites?
- What L0 count and segment size minimize remote requests without recovery
  replay?
- Does the profile retain its shape under MinIO and GCS latency and pricing?
