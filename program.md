# objectKV autonomous research program

Status: `[PROPOSED]`. The open-ended loop is disabled until the admission gate in
`docs/EVALS.md` passes for a specific lane.

## Objective

Improve one objectKV research lane through small, reproducible code experiments.
Correctness and contract gates must remain green. One lane has one primary
metric. Do not optimize a blended system score.

## Setup

1. Read `README.md`, `CONTEXT.md`, `docs/DECISIONS.md`, `docs/EVALS.md`, and the
   RFCs owning the chosen lane.
2. Select one lane and record its primary metric, hard gates, fixed budget,
   practical improvement threshold, candidate edit surface, and frozen surfaces.
3. Verify the baseline command and result-schema validation.
4. Create `research/<lane>/<tag>` from the exact incumbent commit.
5. Run the incumbent in the same environment and append the baseline result.
6. Run a negative control that must fail one hard gate. Stop if it passes.

## Allowed

- Modify only the declared candidate surface.
- Use dependencies already approved for the lane.
- Make one hypothesis-bearing change per commit.
- Fix a trivial compile error in the same experiment, if the hypothesis remains
  unchanged.

## Forbidden

- Modify the reference model, suite, held-out seeds, budgets, result schema,
  aggregator, or this program during an experiment.
- Weaken an assertion or remove a workload to improve a score.
- Increase hardware, concurrency, runtime, cache capacity, or object-store
  privileges beyond the fixed profile.
- Merge separate hypotheses into one candidate.
- delete or rewrite failed experiment history.
- Publish a benchmark without its exact profile and commit identity.

## Loop

Repeat until the run is interrupted or its declared experiment budget is spent:

1. Inspect the incumbent, prior ledger rows, and near misses.
2. State one falsifiable hypothesis.
3. Implement the smallest candidate on top of the incumbent.
4. Commit the candidate.
5. Run the fixed suite with output redirected to its run directory.
6. Validate the result schema and inspect only the compact verdict first.
7. Append an experiment row with `keep`, `discard`, `inconclusive`, or `crash`.
8. Promote only if every hard gate passes and the primary metric clears the
   practical threshold plus noise.
9. Keep discarded commits reachable. Advance the champion pointer without
   rewriting history.

If the global best does not move after the lane's declared stall limit, stop
local tuning. Draft several orthogonal mechanisms before continuing. Do not
produce minor variations of the same failed idea indefinitely.

## Human-visible result

Report:

- lane and hypothesis;
- candidate and parent commit;
- hard-gate result;
- incumbent and candidate metric distributions;
- exact environment/profile;
- keep/discard/inconclusive decision;
- what was learned;
- next orthogonal experiment.

Do not report a faster number as progress when correctness, cost, or the fixed
profile changed.
