# Fable RFC-0048 pre-implementation review

- Date: 2026-08-30
- Reviewer: Fable adversarial review
- Subject: RFC-0048 generation-pinned typed object-layout curve
- Final outcome: SHIP for implementation

## Review sequence

The first pass blocked implementation on five evaluation-boundary defects:

1. one fixture seed was conflated with three workload trace seeds;
2. child descriptors did not require generation, length, and digest for every
   object, and measured reads did not pass the expected GCS generation;
3. no-retry transport and SDK-attempt accounting were point-only;
4. the scan statistic and end-to-end DataFusion timer were underspecified;
5. C5 scanned sequentially while the proposed C0/C5 plan permitted unmatched
   concurrency.

RFC-0048 and its plan now freeze fixture seed 5699 independently from trace
seeds 5701, 5702, and 5703; bind every child object and measured read to its
GCS generation; share one no-retry transport boundary; use the nearest-rank
median of 15 within-block scan ratios; time SQL parse through final batch
digest; and require observed scan concurrency exactly one for both subjects.

The second pass confirmed those five corrections and found one remaining
binding error. The oracle hash covered the abstract workload but was named as
though it covered the postpublication execution. The final contract separates:

```text
workload_plan_sha256
  -> fixture-independent workload, traces, and SQL

execution_plan_sha256
  -> workload and plan-file digests
  -> oracle and generator digests
  -> published root, children, and every GCS generation
  -> IAM, machine, cache, transport, order, timer, budgets, and gates
```

Every measured position and aggregate receipt must bind both hashes.

## Final reviewer statement

SHIP for implementation. The frozen workload hash is separate from the
postpublication execution hash, all stated hashes verify, and the independent
generator byte-matches the checked-in oracle artifact.

## Tradeoff

Optimizes for: one comparable row-versus-column result whose history,
authority, object revisions, retries, cache state, concurrency, and statistics
cannot drift after publication.

Gives up: parallel scan scheduling in this first curve and reuse of the current
self-publishing GCS runner as admission evidence.
