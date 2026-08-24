# RFC-0034: Cell read-version proxy causality contract

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0002, RFC-0008, RFC-0032

## Decision

`[ACTIVE-WORK]` A tenant session carries its minimum known cell version across
read-version proxy handoff. A proxy may return a version only at or above that
minimum and only after a linearizable authority read proves the snapshot is
available. It may return retryable unavailability; it may not answer from an
older cache.

## Question

Can two independent proxy processes preserve read-your-writes and
real-time order through the real three-process Cell v0 authority, including a
leader death after commit acknowledgement and before the next proxy request?

## Frozen history

Each of 100 rounds keeps one tenant session and alternates source and target
proxy instances:

1. Both proxies obtain the same linearizable pre-commit snapshot.
2. The source proxy supplies the transaction read version.
3. One unique write commits and advances the session minimum to its durable
   commit sequence.
4. The session hands off to the other proxy.
5. The target must return a snapshot at or above the session minimum and the
   acknowledged value must be present.

At the midpoint, the authority leader dies after acknowledging the write. A
successor is elected before the target proxy serves the handoff. The killed
authority process restarts after all rounds and must converge exactly.

The negative control returns the target proxy's pre-commit cache and ignores
the session minimum. Its snapshot is internally valid but violates the
write-before-read real-time edge and omits the acknowledged value.

## Hard gates

- two independent proxy processes and caches alternate source and target
  responsibility;
- 300 proxy requests and 100 writes execute per seed;
- every acknowledged commit advances the session minimum;
- every post-commit handoff returns a version at or above that minimum;
- every post-commit observation contains the acknowledged value;
- authority failover does not weaken the causal floor;
- the restarted authority process converges on exact rows and envelope chain;
- two fresh executions produce the same canonical report;
- the ignore-minimum control returns 100 stale versions and values per seed.

## Interpretation

A pass admits the session-token and proxy-cache rule across two independent OS
processes against one centralized transaction authority. The read observation
still comes from the authority snapshot, not a serving worker's historical MVCC
path. This does not prove concurrent request batching, proxy generation
rollover, bounded waiting under lag, multiple transaction generations, direct
storage reads, or partitioned resolver agreement.

## Admitted evidence

Candidate `d910d10` kept run `eec5ca77` with zero anomalies across seeds
`1103`, `2207`, and `3301`. Each execution started two independent proxy
processes per seed; the canonical receipt records six starts and exact
reexecution did the same. It then exercised 900 proxy requests, 300 acknowledged
writes, 300 causal handoffs, three authority leader kills, and exact restarted
authority convergence. Every target view met the session floor and included the
acknowledged value.

Ignore-minimum control `d280df19` returned a valid pre-commit cache on every
handoff. It produced 300 minimum-version violations and 300 stale observations,
then discarded on the causal gates while exact authority convergence remained
green. The OTel stream records 300 `multi-proxy-real-time` constraints as
`pass` for the correct subject and 300 as `fail` for the control.

## Tradeoff

This optimizes for falsifying the smallest cross-proxy stale-read failure through
a real process boundary. It gives up throughput and availability claims until
the service batches requests and has an explicit lag and generation policy.
