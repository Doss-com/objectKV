# RFC-0032: Cell read-value and real-time witness

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0008, RFC-0029

## Decision

[ACTIVE-WORK] Strengthen the bounded Cell v0 history with actual values from a
linearizable read, actual read dependencies independent of the client's
declared conflict ranges, and a serialization witness that checks real-time
order between non-overlapping rounds.

The kernel still requires clients to declare conflicts. The oracle must not
trust those declarations when deciding whether the observed history was
serializable.

## Question

Can a real three-process cell produce one commit-sequence witness that explains
every observed read value, actual read dependency, and real-time edge across a
seeded 1,000-transaction history with leader death and lost-reply recovery?

## Frozen contract

For each seed:

1. Execute 100 rounds of ten transactions through the real process path.
2. Before each round, obtain a linearizable cell snapshot and its read version.
3. Seed the hot key across 17 keys; four contenders observe its actual value and
   compute writes from the same snapshot.
4. Submit the four hot transactions, four disjoint atomic transactions, and two
   blind writers concurrently.
5. Record every observed key/value, read version, durable outcome, and commit
   sequence.
6. At the midpoint, drop one applied reply, kill the leader, recover and retry
   the exact outcome on its successor, and later restart the killed process.
7. Independently replay committed writes by commit sequence.
8. Require every observed value to match the state at its read version.
9. Require no committed write to an actually read key between that read version
   and the reader's commit sequence.
10. Require commit sequences to respect every real-time edge between completed
    earlier rounds and invoked later rounds.

The negative control omits declared read conflicts for the hot transactions.
All four may commit, but the witness must reject the resulting actual-read
dependency cycle even though the client supplied no dependency declaration.

## Hard gates

- 400 actual read observations per seed are checked;
- every observed value matches independent replay at its read version;
- every committed actual read has no intervening write before its commit;
- every non-overlap real-time edge agrees with commit order;
- one commit-sequence witness satisfies all three constraint classes;
- all RFC-0029 failover, retry, atomicity, convergence, and exact-replay gates
  remain green;
- the omitted-conflict control discards on actual-read dependencies.

## Interpretation

A pass admits a deterministic witness for one seeded history family. It is not
an exhaustive strict-serializability checker. It does not cover range reads,
phantoms, arbitrary operation generation, multiple read-version proxies,
partitioned resolvers, clock-based external consistency, or unbounded history
search.

## Tradeoff

This optimizes for an executable read-value oracle without introducing a large
external checker or hiding dependencies inside the kernel. It gives up general
history search because commit sequences provide the candidate witness in this
version.

## Result

Candidate `a93041f` passed the frozen contract across seeds 1103, 2207, and
3301. Run `56a132c6` kept with zero anomalies after two fresh executions per
seed. The run evaluated 3,000 transaction identities, 1,200 linearizable read
observations, 300 committed actual-read dependencies, and 727,650 real-time
edges. It also retained 2,100 commits, 900 durable conflicts, three leader
kills, three lost replies, exact retry, exact replay, and three-node
convergence.

Omitted-conflict control `aa460aa8` committed all 3,000 transactions and
discarded. Its 1,200 observed values and 1,485,000 real-time edges remained
internally valid, but the gate over 1,200 committed actual-read-dependency
checks failed as intended. This proves the oracle does not trust the client's
conflict declaration when judging the history.

The suite hash is
`466dfd0591ef819941fd4318855ebf25a9f4eddd87940acffebbd538de3bdbfb`.
The correct run and control exported separate read-value, actual-dependency,
and real-time counters plus the normal metrics, traces, and logs through OTel.
