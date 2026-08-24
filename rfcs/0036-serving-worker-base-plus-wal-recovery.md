# RFC-0036: Serving worker base plus WAL recovery contract

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0002, RFC-0005, RFC-0007, RFC-0031

## Decision

`[PROPOSED]` A disposable serving worker may answer a read at target version
`T` only after it reconstructs an immutable object base through `O` and applies
the complete quorum-recovered mutation suffix `(O, T]`. A base-only answer at
`O < T` is stale and must never be labeled as version `T`.

```text
Database(T) = ObjectState(O) + RetainedMutations(O, T]
```

## Question

Can a fresh OS process resolve a replicated publication root, open its exact
object closure, reopen a locally quorum-durable retained suffix, validate one
commit-envelope chain across the base and tail, and reconstruct the exact
transaction state at `T` when `O < T`?

## Frozen history

The existing Cell v0 durable-snapshot scenario supplies one admitted committed
history with `C=10` and an authority snapshot through `8`.

1. Encode only committed envelopes through `O=8` in one immutable object
   segment and publish its manifest through the replicated publication
   authority.
2. Write every later committed envelope to a fresh three-file local WAL and
   require a two-file synchronized quorum for each record.
3. Start one serving worker process with empty private state.
4. The worker resolves the published root from the authority, verifies and
   replays the base closure, reopens the retained WAL from disk, validates chain
   continuity, and applies entries through `T=C`.
5. The worker returns its observed frontier and exact ordered rows.

The negative control opens the same valid base and ignores the retained suffix.
It must report frontier `O`, return stale rows, and discard rather than silently
claiming `T`.

## Hard gates

- the source transaction proof has zero anomalies;
- `0 < O < C=T`;
- base and suffix each contain at least one committed envelope;
- the immutable closure is complete before publication;
- every suffix record is synchronized to a quorum before worker start;
- the worker is a distinct OS process with no private state from the writer;
- the worker resolves the exact manifest from replicated authority;
- the base chain ends at `O` and the suffix connects to that exact chain;
- the worker's observed frontier equals `T`;
- the worker's ordered rows equal the transaction oracle exactly;
- two fresh executions produce the same canonical report;
- the ignore-suffix control exposes a stale frontier and stale rows.

## Interpretation

A pass admits the recovery equation through real filesystem objects, replicated
publication authority, a fresh serving process, and a reopened local quorum WAL
suffix. The suffix is a bounded copied recovery fixture, not yet the original
OpenRaft log stream. This does not prove tagged log routing, live tailing,
historical reads at arbitrary `T`, range placement, independent hosts, public
cloud latency, or serving under object-store brownout.

## Evidence

Candidate `9e733e2` kept run `ed0cdfe8-085e-4269-9d2a-6818d1df7b8d` across
seeds `1103`, `2207`, and `3301`. The subject passed 45 of 45 checks, started 21
transaction processes, nine publication-authority processes, and three fresh
serving-worker processes, and reconstructed exact rows at `T=10` from object
state at `O=8` plus one retained suffix record per seed. OTel exported the exact
candidate, suite hash, frontiers, requests, anomalies, and availability result.

Ignore-suffix control `690e0844-b46a-4a93-8868-3f498a99cf23` opened the same
valid base but stopped at frontier `8`, recovered no suffix records, returned
stale rows, produced nine anomalies, and discarded. Both subjects replayed
exactly, so the control isolates the retained-suffix obligation rather than a
fixture failure.

## Tradeoff

This optimizes for testing the complete read recovery equation before building
range routing or cache policy. It gives up a production worker protocol and
throughput claim until the worker consumes the original retained transaction
log and serves concurrent reads.
