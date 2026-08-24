# RFC-0037: Authoritative committed-envelope serving feed

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0007, RFC-0008, RFC-0036

## Decision

`[PROPOSED]` Serving workers consume committed `CommitEnvelope` bytes, never raw
transaction-proposal entries from the consensus journal. Cell v0 exposes the
committed suffix through a linearizable read from the replicated transaction
authority. A later partitioned transaction-log role must retain and route the
same envelope bytes by range tag.

The raw OpenRaft journal is not a serving mutation stream. It also contains
duplicate retries, durable conflict rejections, blank entries, and membership
changes. Replaying its transaction commands in a serving worker would require
resolver state and would collapse transaction and storage roles.

## Question

Can a fresh serving-worker process reconstruct exact `Database(T)` from an
immutable published base through `O<T` plus a suffix fetched directly from the
live three-process transaction authority after an authority leader failure,
without a copied WAL directory or controller-supplied mutation bytes?

## Frozen history

1. Keep the Cell v0 durable-snapshot history alive after it reaches `C=10` with
   an authority snapshot through `8`.
2. Publish only committed envelopes through `O=8` as one verified immutable
   base closure.
3. Kill the current transaction-authority leader and elect a successor.
4. Start one serving worker with empty private state.
5. The worker resolves the published root, opens and validates the object base,
   performs a linearizable committed-envelope request for `(O,T]` against the
   live authority endpoint set, validates chain continuity, and applies the
   exact suffix.
6. The worker returns the authority position, suffix count, observed frontier,
   and ordered rows.

The negative control performs the same authority request but drops its final
envelope before application. It must stop below `T`, return stale rows, and
discard.

## Hard gates

- the source transaction history has zero anomalies;
- `0 < O < C=T`;
- no copied recovery-WAL directory exists;
- the base closure is exact and published through replicated authority;
- the original transaction processes remain the only suffix source;
- the current transaction leader dies after the suffix exists;
- a successor serves the suffix after a linearizability barrier;
- every returned envelope is committed, generation-matched, ordered, and in
  `(O,T]`;
- the base chain connects to the first returned envelope;
- the worker reaches `T` and reconstructs the transaction oracle exactly;
- two fresh executions produce the same canonical report;
- the dropped-final-envelope control exposes the incomplete feed.

## Interpretation

A pass admits the role and wire boundary from replicated transaction authority
to disposable serving worker. It does not yet admit a dedicated partitioned
tLog, push streaming, range tags, backpressure, historical versions, independent
hosts, or serving under transaction-authority brownout.

## Evidence

Candidate `e1c2437` kept run `bf79522d-86a1-40af-ab79-01284e4880e5` across
seeds `1103`, `2207`, and `3301`. The receipt records 48 of 48 checks, 21
transaction-process starts, nine publication-authority starts, three fresh
serving workers, three transaction-leader kills, six verified object reads,
three live committed envelopes, and zero copied WAL directories. Each successor
served a linearizable feed at authority position `11`; every worker rebuilt
exact rows at `T=10` from object state through `O=8`.

Dropped-final-envelope control `3db9c604-d42e-4932-a4b3-09c748afd20b`
contacted the same live authority but applied no suffix. It stopped at `8`,
returned stale rows, produced nine anomalies, and discarded. Both subjects
replayed exactly. OTel exported availability `1` for the admitted subject and
`0` for the control under suite hash `18e2250f`.

## Tradeoff

This optimizes for preserving the transaction and storage role boundary while
removing the controller-copied suffix from RFC-0036. It gives up claiming Cell
v0 has the final high-throughput tLog topology. The linearizable authority feed
is a correctness bridge, not the final scaling design.
