# RFC-0054: Transaction-system recovery work and duration curve

- Status: accepted for bounded local process calibration
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0009, RFC-0011, RFC-0040, RFC-0053

## Decision under test

`[PROPOSED]` Retain RFC-0053 full transaction-system generation recovery only
if local recovery work remains linear in the retained transaction-log inventory,
pending-ticket window, and successor role count, and remains independent of
permanent database size. Measure wall duration for failure observation, durable
generation fencing, authenticated tLog inventory, successor role recruitment,
and admission of the first successor request as separate phases.

Do not set a production availability objective from the RFC-0053 end-to-end
history duration. That number includes construction and replay of the complete
semantic fixture. This gate isolates the recovery path and records a repeatable
local-process curve. Independent hosts, network partitions, cloud control-plane
latency, and production failure detection remain later gates.

## Context and invariant

RFC-0053 proves that one conservative generation transition can recover a
commit-proxy failure without filling a nonempty ticket gap, publishing a
partially durable record, or duplicating a fully durable lost-reply mutation.
It does not show whether the outage is operationally acceptable.

The invariant is:

```text
recovery work
  = O(authenticated retained tLog records
      + pending tickets
      + successor transaction-system roles)

recovery permanent database bytes read
  = 0
```

If recovery work or bytes scale with total permanent database size, the cell
architecture is rejected. If the retained-tail curve is linear but too slow,
the next candidate is exact-batch within-generation takeover with full
generation recovery retained as fallback.

## Frozen bounded model

The suite uses seeds `1103`, `2207`, and `3301`, seven timed repetitions per
seed, a three-node replicated recovery authority, authenticated tLog inventory
receipts, and real disposable local role processes. Each recovery produces one
canonical untimed receipt and five timed phase observations:

1. observe the failed old-generation role;
2. durably fence the old generation;
3. load, authenticate, and reduce every required tLog set to the maximal
   contiguous quorum-present prefix;
4. recruit the declared successor commit proxies and resolver roles;
5. durably admit the successor generation above the old issued high watermark.

The curve changes one dimension at a time:

| Curve | Frozen points | Fixed dimensions |
|---|---|---|
| retained tail per tLog | 256, 4,096, 65,536 records | 64 pending, 9 resolvers, 2 sets of 3 |
| pending window | 8, 64, 512 tickets | 4,096 records, 9 resolvers, 2 sets of 3 |
| role and tLog topology | 3 resolvers with 2x3 tLogs; 9 with 2x5; 33 with 4x5 | 64 pending, 4,096 records |
| database independence | 1 GiB and 1 PiB sparse logical database extents | middle point transaction-system state |

The sparse extents are labels and filesystem metadata, not evidence of a
materialized 1 PiB database. Their purpose is to prove that the recovery code
never opens or reads the permanent database path. Public-cloud storage and
large retained-tail economics require physical follow-up runs.

## Negative subjects

The frozen suite independently attempts to:

1. read permanent database bytes during transaction-system recovery;
2. derive a visible boundary from one tLog set instead of every required set;
3. perform a quadratic retained-inventory comparison;
4. admit the successor before all declared transaction-system roles are ready.

Each control must emit the same telemetry surface, violate its owning hard
gate, and discard.

## Eval plan

Freeze `cell-transaction-system-recovery-curve-v0`. Its primary metric is
`recovery.transaction_system_duration`. Each point records seven samples per
seed plus phase histograms, inventory bytes, deterministic work units, and
permanent database bytes read. Report median, median absolute deviation,
minimum, and maximum for every point. A performance result is comparable only
under the same suite hash, profile hash, candidate commit, machine, toolchain,
and backend.

The first candidate is calibration. It may be admitted only when semantic and
algorithmic hard gates pass, all four negative subjects discard, and the
curves are recorded. It does not receive a production recovery SLO. Later
optimization requires at least 10 percent median improvement beyond observed
noise with all frozen surfaces unchanged.

Passing semantic and algorithmic gates require:

- old-generation fencing precedes inventory interpretation;
- every required tLog set has authenticated quorum evidence;
- the recovered boundary is the maximal contiguous common prefix;
- every pending ticket is classified once with linear work;
- every declared successor role is ready before admission;
- the successor version exceeds the old issued high watermark;
- permanent database bytes read equal zero;
- inventory work is linear in records examined, with no pairwise scan;
- all five phase receipts and the total duration distribution are present;
- exact canonical untimed receipts replay across duplicate executions.

## Failure model

- commit-proxy process death after an issued ticket;
- stale old-generation role traffic after a durable fence;
- one tLog node missing the suffix;
- altered or unauthenticated tLog inventory;
- pending tickets beyond the recovered prefix;
- successor role startup failure;
- first-successor admission before role readiness;
- local filesystem and process timing noise.

Network partitions, remote hosts, clock uncertainty, cloud object APIs,
recovery-authority quorum loss, and correlated zone failure are outside this
local calibration gate.

## Alternatives

### Isolated within-generation proxy takeover now

Retaining the exact canonical batch in replicated state can reduce the outage.
It also adds a second transaction-payload durability path and must reconcile
partial resolver and tLog effects. Optimize for the simpler full-generation
fallback until this curve shows a measured need.

### Measure the complete RFC-0053 history

The full fixture is valuable for semantics, but process construction, seeded
transaction execution, replay, and teardown dominate its elapsed time. It
cannot isolate the recovery bottleneck.

### Use a synthetic sleep model

Configured delays can test timeout policy but cannot reveal actual CPU, disk,
signature, serialization, or process-recruitment cost. This gate measures real
local work and labels its topology limits.

## Tradeoff

This gate optimizes for an early falsifiable availability curve without
pretending that a workstation is a distributed production environment. It
gives up a production SLO and remote-failure claim in exchange for separating
algorithmic growth from fixture overhead before more architecture is added.

## Compatibility and migration

The candidate adds evaluation-only receipts and metrics. It changes no public
client API or object format. Unknown receipt versions fail closed. The
RFC-0053 semantic recovery path remains the incumbent and fallback.

## Unresolved questions

1. Which measured phase dominates at the large local point?
2. At what retained-tail size does exact-batch takeover become cheaper than
   full generation recovery?
3. What remote-host and zone topology should set the first production SLO?
4. Should role recruitment be parallelized before or after independent-host
   evidence exists?
5. How should recovery compose with an in-progress resolver split or tLog
   policy transition?

## Evaluation outcome

Candidate `90c1526` kept all ten correct curve points under evaluated suite
hash `06717ac6` and profile hash `004f73d4`. Every point used seeds `1103`,
`2207`, and `3301` with seven repetitions per seed, for 210 isolated recovery
samples. Every untimed receipt replayed exactly, every required tLog set
authenticated, every successor admitted after its declared roles became ready,
and permanent database bytes read remained zero. Workspace tests and
warning-free Clippy passed. OTel exported every total and phase distribution.

The retained-tail curve is the first measured bottleneck:

| Records per tLog | Run | Total median | MAD | Inventory median |
|---:|---|---:|---:|---:|
| 256 | `462f9122` | 0.292 s | 0.007 s | 0.014 s |
| 4,096 | `7923a187` | 0.465 s | 0.009 s | 0.183 s |
| 65,536 | `81c64262` | 3.158 s | 0.021 s | 2.870 s |

The inventory phase grew with authenticated bytes examined. At 65,536 records
it accounted for 91 percent of the total local recovery median. This is linear
work, but it is not an acceptable production availability claim. Retained-tail
checkpointing, compact authenticated summaries, or exact-batch takeover now
have measured justification.

Pending-ticket classification was not the bottleneck. Eight pending tickets
kept at 0.468 seconds in run `7e91dd0d`; 512 kept at 0.459 seconds in run
`9d2dda47`. Their deterministic work differed by exactly 504 classifications
per repetition while the inventory and role topology stayed fixed.

Topology work also scaled in the expected phase. A 2x3 tLog topology with 3
resolvers kept at 0.390 seconds in run `bb8c05b0`; 2x5 with 9 resolvers kept at
0.627 seconds in run `c6099744`; 4x5 with 33 resolvers kept at 1.313 seconds in
run `1a602e65`. At the largest point, tLog inventory took 0.616 seconds and
sequential role recruitment took 0.607 seconds. Parallel recruitment is a
candidate optimization, not a semantic change.

The 1 GiB and 1 PiB sparse logical database controls used identical recovery
work and read zero permanent database bytes. Runs `55137c1e` and `aae31079`
kept at 0.460 and 0.474 seconds. Their overlapping distributions support the
database-size independence invariant for this instrumented local path. They do
not substitute for a physically large remote database.

Four controls discarded: permanent database scan `ba0e9874`, one-set trust
`184488f6`, quadratic inventory work `0333e048`, and early successor admission
`a7a19078`. The scan control recorded 86,016 database bytes across its 21
samples. The quadratic control recorded 3,297,630 work units versus 496,062 for
the middle correct point.

The decision is to retain full generation recovery as the correctness fallback
and move the availability lane to retained-tail summaries, checkpoint cadence,
parallel recruitment, and an independent-host curve. Do not add a second
replicated transaction-payload path until one of those bounded optimizations
fails to meet the later remote-host objective.
