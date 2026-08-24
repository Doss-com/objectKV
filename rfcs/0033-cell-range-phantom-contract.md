# RFC-0033: Cell range-read phantom contract

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0008, RFC-0032

## Decision

[ACTIVE-WORK] Treat every actual range read as a dependency over the complete
half-open key interval, including keys that do not yet exist. A serializability
oracle must build dependency edges from observed ranges rather than trusting a
client declaration or only enumerating returned point keys.

## Question

Does a real three-process Cell v0 reject an insertion phantom after a range read,
including when the leader dies between the dependent commits, and does an
independent dependency graph reject the omitted-range control?

## Frozen history

Each of 100 rounds starts two transactions from the same linearizable snapshot:

1. Transaction R reads one unique empty prefix and plans to write its observed
   row count to a summary key.
2. Transaction I reads that summary key as absent and plans to insert one row
   into R's prefix.
3. I commits first.
4. R then attempts to commit at its original read version.

With the complete range conflict, R must receive a durable conflict. At the
midpoint, the controller kills the leader after I commits and submits R to a
successor. The killed process restarts after all rounds and must converge.

The negative control declares only returned point keys for R. Because the range
was empty, it declares no range dependency. Both transactions then commit:

```text
R reads prefix before I, so R -> I
I reads summary before R, so I -> R
```

The resulting dependency cycle must discard even though final rows are
well-formed and every process converges.

## Hard gates

- 200 transaction identities execute per seed;
- all 100 insertions commit and all 100 range readers conflict;
- 100 range observations and 100 point observations match independent expected
  state;
- 200 dependency edges are checked with no cycle in the correct subject;
- leader death between I and R does not lose the conflict outcome;
- the restarted node converges on exact rows and envelope chain;
- two fresh executions produce the same canonical report;
- the omitted-range control commits both sides and exposes 100 cycles per seed.

## Interpretation

A pass admits one deterministic empty-range phantom shape through the
centralized transaction authority. It does not prove arbitrary range scans,
range clears, multiple overlapping intervals, multiple read-version proxies,
partitioned resolvers, or general history search.

## Admitted evidence

Candidate `5d4427d` kept run `04b84730` with zero anomalies across seeds
`1103`, `2207`, and `3301`. It exercised 600 transaction attempts, 300 exact
range observations, 300 exact point observations, 600 dependency edges, three
leader process kills, and exact restarted-node convergence. All 300 insertion
transactions committed and all 300 dependent range transactions conflicted.

Omitted-range control `f4678cd8` committed all 600 transactions and produced
300 dependency cycles. It discarded on the outcome and acyclicity gates while
read observations, replay, and process convergence remained valid. The OTel
stream records 600 `range-phantom-cycle` constraints as `pass` for the correct
subject and 600 as `fail` for the control.

## Tradeoff

This optimizes for a schedule-controlled dependency cycle that cannot be
explained away by another serialization order. It gives up broad generated
range workloads until the smallest insertion phantom survives failover.
