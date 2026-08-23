# RFC-0015: Replicated publication authority

- Status: proposed
- Authors: objectKV contributors
- Created: 2026-08-23
- Supersedes: none

## Decision

The bootstrap cell's existing three-node `OpenRaft` generation-authority group
also owns publication intents, reader-visible roots, snapshot and query pins,
and deletion reservations. Generation and publication remain separate state
domains in one replicated state machine and one snapshot. They do not become a
second consensus cluster.

Every publication command carries one globally unique `RequestIdentity` and the
active `GenerationCredential`. The authority applies the command only while
that generation is `Active`. Exact retries return the recorded semantic result;
reusing an identity with different bytes is a deterministic conflict.

This RFC freezes the replicated command and recovery contract. It does not yet
claim that publisher or sweeper processes recover safely at every object-store
boundary. That is the next physical-process gate.

## Context and invariant

RFC-0014 proved the publication protocol through a real object client and a
quorum-fsynced local authority prototype. That prototype did not provide
cross-process consensus, independent node journals, leader replacement, or
generation fencing.

For each accepted publication transition `P`:

```text
accepted(P) -> quorum_committed(P)
               and generation(P) == active_generation
               and replay(P) == recorded_outcome(P)
```

For each reader-visible root `R`:

```text
root(R) -> matching_prepared_intent(R) was atomically retired
```

For each accepted deletion reservation `D`:

```text
reserve(D) -> mark_epoch(D) == current_root_intent_epoch
              and no current intent names D.object
              and later intersecting prepares are rejected
```

## Replicated state domains

The coordinator state machine contains:

```text
GenerationAuthorityState
PublicationAuthorityState
DurableRequestOutcomes
RequestFingerprints
```

`PublicationAuthorityState` contains:

- the most recent accepted authority log position;
- a monotonic root and intent epoch;
- prepared publication intents;
- reader-visible roots;
- snapshot and query pins;
- per-object deletion reservations.

Durable request outcomes and fingerprints remain common state-machine
infrastructure. A caller allocates request identities from one cell-wide client
namespace, not separately per command family.

All fields are included in the existing `OpenRaft` state-machine snapshot.
Unknown or missing fields may default only while this unpublished bootstrap
format remains at version `1`; an explicit snapshot format and migration gate
is required before public compatibility is claimed.

## Command contract

The authority accepts six versioned publication actions:

1. `Prepare`, naming the complete immutable object closure and manifest.
2. `Publish`, requiring the matching intent, installing the root, and retiring
   that intent atomically.
3. `Pin`, installing a retained manifest root.
4. `Unpin`, removing one retained manifest root.
5. `ReserveDelete`, requiring an unchanged mark epoch and no intersecting
   current intent, then issuing an opaque permit bound to the exact object
   identity, plan, generation, and applied authority log position.
6. `RetireDelete`, requiring the exact plan and object key before removing the
   reservation.

`Prepare`, `Publish`, `Pin`, and `Unpin` advance the root and intent epoch.
Deletion reservation and retirement do not. A reservation serializes physical
deletion against future publication preparation without making an accounting
counter or object listing authoritative.

Rejected commands still occupy a Raft log position but do not advance the
publication revision or root and intent epoch. Their deterministic rejection is
retained as the request outcome.

## Generation and recovery rules

- Publication commands are rejected while the cell is `Uninitialized`,
  `Fencing`, or `Recovering`.
- A credential from an older or pending generation cannot mutate publication
  state.
- Prepared intents, roots, pins, and deletion reservations survive authority
  leader loss, process restart, and state-machine snapshot recovery.
- A generation transition does not silently discard a deletion reservation.
  The new active generation must reconcile it.
- Resuming or cancelling an abandoned physical delete after a generation
  change requires a separately frozen sweeper-ownership fencing protocol. This
  RFC does not authorize a new generation to retire an old reservation while an
  old sweeper may still issue an unguarded delete.

The last rule is intentionally conservative. An abandoned reservation may
temporarily block one digest from publication. Safety takes priority until the
physical worker gate proves a bounded takeover mechanism.

## Linearizable API

Authority-role nodes expose:

- `publication_write(command, drop_reply_after_commit)`;
- `publication_read()` after `ensure_linearizable`;
- the existing `outcome(request_identity)` lookup.

`drop_reply_after_commit` exists only in the executable contract harness. It
creates a real unknown outcome after committed apply. Production transport may
produce the same condition through connection loss and must resolve it through
the request identity.

Data-role nodes reject publication reads and writes. Authority reads from a
follower either establish linearizability through Raft or return an error; they
never return a local stale snapshot as authoritative.

## Failure model

The first gate covers:

- three authority processes with separate stable-log directories;
- reply loss after quorum commit;
- authority leader process death and successor election;
- retry against a successor;
- restart and catchup of killed nodes;
- same-identity retries and conflicting-payload reuse;
- stale generation credentials;
- stale mark epochs;
- publication and deletion-reservation races.

It does not cover:

- publisher or sweeper process death between object and authority operations;
- authority snapshot compaction and retained-outcome expiry;
- simultaneous independent-disk loss beyond quorum;
- disk-full and repair;
- generation takeover with an in-flight unguarded delete;
- partitioning deletion reservations by key range;
- GCS or S3 provider-specific guarded delete.

## Eval plan

`object-publication-authority-process-v1` runs three fixed seeds through three
real authority processes over Tokio TCP. Eighteen semantic checks per seed must
fit a 54-event budget. The clean subject must prove:

- active-generation authorization;
- durable prepare and exact unknown-outcome recovery;
- intent survival after authority leader loss;
- exact retry deduplication and conflicting identity rejection;
- publish requires a matching intent;
- root installation and intent retirement are atomic;
- a root or intent epoch change rejects a stale delete plan;
- a deletion reservation survives leader loss;
- a reservation rejects intersecting preparation;
- only the exact delete plan can retire its reservation;
- retirement permits a fresh publication attempt;
- restarted nodes catch up to byte-equivalent authority state.

The frozen negative subjects are:

1. `bypass_generation_fence`
2. `publish_without_intent`
3. `ignore_root_epoch`
4. `ignore_delete_reservation`
5. `disable_request_dedup`

Each must produce at least one bounded anomaly and a `discard` verdict. Two
fresh executions of the clean seed `1103` must emit byte-identical semantic
receipts.

## Alternatives

D1. Create a separate publication Raft group. This can partition load later,
but it introduces an atomicity boundary between generation fencing and root
publication before the bootstrap cell has measured a real bottleneck.

D2. Send publication commands through the data transaction log. That couples
control-root availability to the transaction system being recovered and makes
generation bootstrap circular.

D3. Keep the local quorum-WAL authority. It proves bytes on one machine but not
leader replacement, fencing, or one agreed order across processes.

## Compatibility and migration

Publication commands use an objectKV-owned magic prefix and versioned JSON
payload. The bootstrap wire and snapshot format is unpublished. Any incompatible
change before public release creates a new suite and trace identity. Public
compatibility requires a versioned snapshot envelope and migration fixture.

## Unresolved questions

- How request outcomes are retained, compacted, and expired without making an
  old retry ambiguous.
- How reservation ownership is fenced and adopted across generation changes.
- How reservations partition with range movement without a global hot map.
- Whether the publication state remains in the generation group after measured
  root throughput requires partitioning.
- Which provider-native conditional delete tokens are strong enough to replace
  the unguarded-delete reservation fallback.
