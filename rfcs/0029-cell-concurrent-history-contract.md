# RFC-0029: Cell v0 concurrent history contract

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0008, RFC-0009, RFC-0011

## Decision

[ACTIVE-WORK] Make a 1,000-transaction concurrent history the next Cell v0
admission gate. The subject is the real three-process OpenRaft path and the
centralized semantic state machine, not a model-only simulation.

This gate is a bounded early indicator, not a complete strict-serializability
proof. It deliberately combines transactions whose correct outcomes are
independent of task scheduling, then validates their durable outcomes and final
state after a leader change.

## Question

Can one bounded cell preserve the intended OCC and retry contract when 1,000
logical transactions are submitted in concurrent batches, one committed reply
is lost, the leader dies, and the killed process later rejoins?

## Frozen history

Each of 100 rounds obtains one read version and concurrently submits ten
transactions:

1. Four transactions read and write the same hot key. Exactly one may commit
   and three must receive durable conflict outcomes.
2. Four transactions atomically write two disjoint keys in distant ordered
   prefixes. Every transaction must commit and both rows must appear together.
3. Two blind transactions write the same key without a read conflict. Both may
   commit, and the final value must belong to the larger commit sequence.

At the midpoint, one disjoint transaction drops its reply only after apply. The
runner kills and reaps the leader, elects a successor, recovers the durable
outcome, retries the same request identity, and requires the original outcome.
The killed node remains absent for the second half of the history, then restarts
from its retained state and converges with the live quorum.

The negative control removes read-conflict declarations from the four hot-key
transactions. It must commit all four and fail the one-winner oracle while the
other transaction shapes remain exact.

## Hard gates

- exactly 1,000 logical transaction identities execute per seed;
- every hot-key round has one commit and three conflicts;
- every disjoint two-key transaction commits with both rows exact;
- both blind writers commit and the greater commit sequence determines the row;
- every committed transaction has one unique commit sequence;
- a dropped reply is recovered after real leader process death;
- an identical retry returns the original durable outcome;
- the restarted process recovers the lost outcome;
- all three nodes converge on exact rows and the same applied position;
- the complete commit-envelope chain remains valid;
- two fresh executions produce the same canonical report;
- the omitted-read-conflict control is discarded for the intended reason.

## Interpretation

A pass is evidence that the centralized Cell v0 transaction path composes its
existing OCC, multi-key atomicity, durable deduplication, Raft failover, and
retained-state recovery contracts under bounded concurrency. It does not prove
arbitrary histories, phantom protection for range reads, multi-proxy real-time
ordering, partitioned resolvers, partitioned transaction logs, long-duration
stability, or production throughput.

The next isolation falsifiers are a general history checker, concurrent range
conflicts, and read-version causality across multiple proxies.

## Tradeoff

This optimizes for a fast, executable architecture signal using outcome shapes
with schedule-independent invariants. It gives up exhaustive model checking and
large-scale load until the composed Cell v0 semantics survive this gate.

## Result

Candidate `1e01b08` passed the frozen contract across seeds 1103, 2207, and
3301. Runs `9616bf69` and `f66bb379` each evaluated 3,000 logical transaction
identities plus an exact fresh-process replay and kept with zero anomalies.
Every run observed 2,100 commits, 900 durable conflict outcomes, three leader
process kills, three lost-reply recoveries, three duplicate retries, and exact
three-node convergence.

The omitted-read-conflict control `c837f980` discarded with two anomalies per
seed. It committed all 3,000 transactions and produced zero conflict outcomes,
while exact replay and operation-coverage gates still passed. This is the
intended falsifier: concurrency alone does not provide isolation when the
caller omits the dependency declaration.

The suite hash is
`9f50bff15af31ca4200c729c2d9eada6aeb0132b800c5b4a288bfab8f5fb4c43`.
Both correct runs and the control exported OTel metrics, traces, and logs
through the shared collector.
