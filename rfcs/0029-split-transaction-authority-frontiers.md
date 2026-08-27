# RFC-0029: Split transaction-authority retention frontiers

- Status: `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 transaction-authority state ownership

## Decision

Replace the monolithic transaction-authority state with four independently
accounted domains. Each domain has one safety frontier and one failure mode.

```text
latest serving values       <- serving coverage S
OCC write-conflict history  <- minimum admitted read R
transaction retry records   <- per-client retry floor Q(client)
recovery commands           <- authenticated object frontier O
```

G4.6 implements the first three ownership boundaries and projects the fourth.
It does not make recovery-command reclamation mutable. The object frontier can
advance only after a later publication proof binds an immutable object closure,
generation, and covered-through version.

## Persisted owners

`TransactionAuthority` becomes a composition of:

```text
TransactionServingState
  values: latest value and version by ordered key

TransactionResolverState
  current_version
  conflict_retention_floor
  committed write-conflict ranges newer than the floor
```

The consensus state machine separately owns:

```text
TransactionRetryState
  exact outcomes and fingerprints newer than Q(client)
  one monotonic retry floor per client

TransactionFrontierState
  latest applied frontier sequence and exact retry response

RetainedTransactionStream
  accepted recovery commands newer than O
```

The first implementation still serializes these owners in one OpenRaft state
machine snapshot. The split is a state and protocol boundary, not yet a process
or Raft-group boundary. Resolver partitioning, disposable serving ownership,
and a distinct txLog group can move the same owners later without changing the
transaction command.

## Read frontier `R`

The resolver may discard committed conflicts with version `<= R` only after the
transaction system promises not to admit a transaction with `read_version < R`.
The authority then rejects such a transaction as `read_version_expired` before
checking conflicts or applying mutations.

The equality is intentional:

```text
read_version = R
  needs conflicts strictly newer than R
  so conflicts at or below R are reclaimable
```

Blind writes use the same admission rule in this first contract. This keeps one
fail-closed rule while the read-version service is still centralized. A future
optimization may prove that a command with no reads can bypass `R`; it cannot be
assumed by this gate.

## Retry frontier `Q(client)`

Transaction request IDs are monotonic within one stable `client_id`. Advancing
`Q(client)` is the client's durable promise that no request ID at or below the
floor will be submitted again. The state machine removes those exact outcomes
and fingerprints. A later command at or below the floor returns
`retry_identity_expired` and never applies again.

This trades an unbounded exact-retry history for an explicit retry window. It
does not bound the number of distinct client IDs. Client-session leases and
client-floor compaction remain a separate cardinality gate.

## Frontier command

`transaction-frontier-advance-v1` is carried inside the existing versioned,
generation-fenced `ClientCommand`. It contains:

```text
sequence
conflict_retention_floor
retry_floors[] = { client_id, through_request_id }
```

Sequences begin at one and increase without gaps. The state retains only the
latest command fingerprint and exact response. An exact replay of the latest
sequence reconstructs the same response. An older sequence fails as expired, a
future gap fails as invalid, and different bytes at the current sequence fail
as a conflicting identity. This bounds frontier-command retry state to one
record rather than one record per advance.

Before mutating anything, apply validates:

1. the sequence transition;
2. canonical, unique client floors;
3. monotonic `R` and every `Q(client)`;
4. `R <= current_version`.

The resolver and retry-domain changes then apply atomically at one replicated
log position.

## Snapshot compatibility

The decoder accepts pre-split `TransactionAuthority` JSON and maps its values,
version, and conflicts into the new serving and resolver owners with floor zero.
Existing generic durable outcomes remain readable as legacy control-plane
state. Fresh transaction commands use `TransactionRetryState`; generation,
publication, and opaque control commands keep their existing outcome tables.

An upgraded snapshot may therefore contain bounded legacy entries from before
the split. G4.6 measures a fresh cluster so the curve represents the new write
path, not an offline migration policy.

## G4.6 falsifier

Reuse the G4.5 fixed-cardinality workload and checkpoints:

```text
live keys: 256
value bytes: 128
commits: 256, 1,024, 4,096
seeds: 1103, 2207, 3301
```

At each checkpoint the candidate advances `R` through the latest commit and
`Q(client)` through the latest request, then computes the non-mutating ideal
projection `O = C`. Complete projected snapshot bytes at 4,096 commits must be
at most 2.0 times bytes at 256 commits.

The same-correctness control advances neither `R` nor `Q` and projects only
`O = C`. It must retain lifetime-sized resolver and transaction retry state.
The poison reports only serving-state bytes. It may appear flat, but the
complete-accounting oracle must reject it.

## Required invariants

1. Fixed live-key cardinality is preserved.
2. Conflicts at or below `R` are absent; conflicts newer than `R` remain.
3. Transactions below `R` fail before mutation.
4. Retry records at or below `Q(client)` are absent.
5. A request at or below `Q(client)` fails and never re-applies.
6. Exact replay of the latest frontier command returns its original response.
7. A stale or gapped frontier sequence fails without partial reclamation.
8. Recovery commands remain readable because `O` is only projected.
9. Complete accounting includes serving, resolver, retry, frontier, legacy
   control, and recovery-stream bytes.
10. Three fresh-process seeds and a fresh-controller replay agree structurally.

## Tradeoff

This optimizes for bounded state by active work, not lifetime commits, while
preserving the strict-serializable transaction command and exact retry inside a
declared window. It gives up unlimited transaction age, unlimited retry age,
and the simplicity of one state owner with no frontier coordination.

## Not claimed

- authenticated mutation of `O` or physical OpenRaft log purge;
- a production read-version lease service;
- bounded distinct-client cardinality;
- resolver or serving ownership in separate processes;
- snapshot installation while frontiers advance;
- independent-machine, cloud, release-build, or latency evidence.
