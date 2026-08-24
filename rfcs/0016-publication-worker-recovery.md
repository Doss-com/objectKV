# RFC-0016: Publication worker recovery and object-effect fencing

- Status: proposed
- Authors: objectKV contributors
- Created: 2026-08-23
- Supersedes: none

## Decision

The next physical gate composes the replicated publication authority from
RFC-0015 with two independently crashable worker processes and a real Apache
`object_store` client. Publication state remains in the three coordinator
voters. The G1 and G2 voter sets are transaction-system generations and do not
receive a second copy of publication authority.

A publisher or sweeper may recover with an empty local directory. Worker-local
journals are hints only. Stable transition identities, authoritative outcomes,
mark receipts, deletion reservations, and terminal object-effect receipts live
in the coordinator authority. A generation handoff cannot discard or silently
adopt an in-flight deletion reservation.

An unguarded object delete is never safe after its reservation is retired while
an old worker can still exercise the copied delete capability. A backend must
therefore provide either an exact revision-guarded delete or an object-effect
grant whose old-generation use becomes impossible before retirement. Direct
long-lived credentials are not an admissible fallback for unguarded deletion.

## Context and invariant

RFC-0014 proved real object effects beside a local quorum-fsynced authority.
RFC-0015 proved the authority state through three OpenRaft processes. Neither
gate killed the processes issuing object effects or composed an in-flight
reservation with the G1 to G2 recovery protocol from RFC-0009.

For each visible root `R`:

```text
visible(R) -> quorum_committed_intent(R)
              and exact_named_closure_verified(R)
              and quorum_committed_publish(R)
```

For each retired deletion reservation `D`:

```text
retired(D) -> complete_mark(D.mark_id)
              and terminal_effect_receipt(D.object)
              and no stale effect grant remains exercisable
```

For generation replacement `G1 -> G2`:

```text
reservation_owned_by(G1)
  -> remains publication-blocking through recovery
  -> G2 may resolve or adopt only after certified G1 fencing
  -> retirement waits for guarded delete or old effect-grant expiry
```

## Process topology

The bounded process harness uses:

- three coordinator voters containing generation and publication authority;
- three active G1 transaction-system voters;
- three empty-directory G2 learners that become voters;
- one publisher process;
- one sweeper process.

The controller and local object-store fixture are test infrastructure, not
database roles. The coordinator voters remain the only publication-authority
group. This preserves D16 and D24 while exercising the existing certificate-
backed data-voter handoff.

## Authoritative records

### Mark receipt

The sweeper persists one canonical `MarkReceipt` before requesting a deletion
reservation:

```text
mark_id
owner_generation
authority_revision
root_intent_epoch
root_set_digest
closure_digest
inventory_generation
candidate_key
candidate_identity
quarantine_not_before
complete
```

`root_set_digest` covers the exact roots, pins, and intents supplied to the
marker. `closure_digest` covers the canonical reachable-object set.
`inventory_generation` identifies the audit-only inventory used to discover the
candidate. The candidate identity contains exact length, SHA-256 digest, and
backend revision token when present.

`complete = true` means every selected root was readable, checksum-valid,
format-supported, and closed. It does not make the worker Byzantine-safe. The
correctness oracle and negative subjects verify the worker contract. A future
multi-tenant marker may add independently checkable traversal proofs.

Recording a mark receipt does not advance `root_intent_epoch`. Any later root,
pin, or intent mutation makes its epoch stale and prevents reservation.

### Object-effect receipt

The worker resolves every physical delete to one terminal receipt before the
authority may retire its reservation:

```text
reservation_position
object_identity
worker_generation
effect_grant_identity
resolution = deleted | absent | identity_changed
named_read_identity
```

`identity_changed` is terminal for the attempted old identity but cannot
authorize deleting the replacement. An unknown response is not terminal.

## Command changes

The versioned publication command family adds:

1. `RecordMark`, which stores or exactly replays one complete or incomplete
   mark receipt.
2. `ReserveDelete`, which references `mark_id` and requires the stored receipt
   to be complete, current, quarantine-eligible, and an exact match for the
   candidate identity.
3. `RecordDeleteEffect`, which stores a terminal named-resolution receipt bound
   to the exact reservation grant.
4. `AdoptDelete`, which moves an old-generation reservation to the active
   generation only after the generation authority contains the matching
   certified recovered position and the old object-effect grant is fenced.
5. `RetireDelete`, which requires the exact reservation, terminal effect
   receipt, and effect-grant fencing proof.

Stable request identities derive from the publication or sweep identity plus
the named transition. Recovering with an empty scratch directory reconstructs
the next action from authority and named object reads, not from local sequence
counters.

## Object-effect fencing

A `DeletePermit` is an authority capability, not sufficient proof that a stale
process has lost object-store access. The physical adapter supports two safe
paths:

1. Revision-guarded delete. The backend rejects deletion if the named object no
   longer has the permit's exact revision or version.
2. Unguarded fallback. The worker uses a short-lived generation-bound effect
   grant. Publication remains blocked by the reservation until every grant
   issued to the old generation is past its enforceable expiry and the new
   worker has resolved the named object outcome.

The local fixture models the second path with a controller-owned monotonic clock
and a generation-fenced effect proxy. Cloud admission must prove the equivalent
provider or credential boundary. Process death alone is not proof of fencing.

## Worker recovery protocol

### Publisher

1. Read the active generation and publication state linearly.
2. Create or recover the stable publication identity.
3. Commit `Prepare` before the first object PUT.
4. For each object and manifest, issue `put_if_absent` and collapse an unknown
   response through exact named verification.
5. Verify the complete manifest closure by named reads.
6. Commit `Publish` with expected prior root, then resolve a lost response by
   the stable request identity.

### Sweeper

1. Read one linearizable mark snapshot.
2. Walk exact roots and persist `RecordMark`, including incomplete walks.
3. Discover old candidates through inventory or `LIST`, which remains
   audit-only.
4. Commit `ReserveDelete` by exact `mark_id` and candidate identity.
5. Obtain a guarded delete or expiring effect grant.
6. Issue the physical delete and resolve an unknown response by named read.
7. Persist `RecordDeleteEffect`.
8. Commit `RetireDelete` only after the stale-effect boundary is closed.

## G1 to G2 handoff

The coordinator authority enters `Fencing`, then `Recovering`, while the data
group performs the certificate-backed membership transition. Publication
mutations are rejected in both phases. Roots, mark receipts, request outcomes,
and deletion reservations remain present in the coordinator snapshot.

After G2 activation:

- a new publisher retries only G2-owned publication work;
- a G1 intent is not automatically publishable by G2;
- a G1 deletion reservation remains blocking;
- `AdoptDelete` requires the exact G1 reservation, certified recovery identity,
  and object-effect fencing proof;
- every G1 data directory may be destroyed before the first G2 correctness
  read;
- coordinator data is not copied from an old worker or G1 data voter.

This gate uses retained-log empty-directory recovery because OpenRaft snapshots
remain disabled. It does not claim compacted-log or coordinator-quorum rebuild.

## Failure model

The fixed schedule kills a worker after each load-bearing boundary:

- prepared intent committed before the first PUT;
- data PUT succeeded with its response lost;
- manifest PUT succeeded with its response lost;
- full closure verified before publish;
- publish committed with its response lost;
- complete mark receipt committed;
- deletion reservation committed with its response lost;
- DELETE succeeded with its response lost;
- deletion retirement committed with its response lost.

It also kills an authority leader, fences G1, moves the data-group membership to
empty-directory G2 nodes, destroys every G1 data directory, restarts both
workers with empty scratch directories, corrupts inventory and counters, and
requires one exact final state.

Outside this gate are coordinator snapshot compaction, loss of a coordinator
quorum, disk-full repair, provider-native cloud semantics, multi-region IAM
revocation latency, and partitioned marker or reservation throughput.

## Eval plan

`object-publication-worker-process-v1` runs seeds `1103`, `2207`, and `3301`
through a fixed 100-event schedule. Correctness anomalies are the only primary
metric. One event is one labeled controller action; internal Raft traffic and
bounded polling do not consume events.

The clean subject must prove:

- all nine kill boundaries execute exactly once per seed;
- every upload follows a quorum-durable intent;
- every visible root follows exact named closure verification;
- stable retries create one effect and reject conflicting identity reuse;
- incomplete or stale mark receipts cannot reserve deletion;
- a reservation survives worker death, authority-leader death, and G1 to G2
  data-group handoff;
- stale object-effect grants cannot delete after reservation retirement;
- unknown delete outcome resolves before effect receipt and retirement;
- post-delete publication recreates and verifies immutable bytes;
- G2 begins from empty directories and no G1 data directory is read after the
  cut;
- restarted workers need no prior scratch state;
- corrupt inventory and counters cannot alter liveness;
- two fresh controllers emit byte-identical semantic receipts;
- required OTel logs, metrics, and traces have zero drops.

The frozen negative subjects are:

1. `ack_authority_before_quorum`
2. `resume_publisher_without_named_verification`
3. `reuse_stale_mark_after_restart`
4. `drop_reservation_during_generation_handoff`
5. `retire_before_delete_outcome_resolution`
6. `reuse_stale_delete_effect_grant`
7. `reuse_old_data_directory_as_recovery`

Each must emit at least one bounded anomaly and a `discard` verdict. The budget
is exactly 100 events per seed. OTel attributes use bounded role, phase, command,
fault-point, generation-slot, object-kind, recovery-kind, and result enums.
Object keys, digests, request identities, paths, ports, PIDs, and timestamps are
forbidden metric attributes.

## Alternatives

D1. Put publication state in the G1 and G2 data groups. This makes data-group
replacement carry the state, but reintroduces circular recovery and conflicts
with D16 and D24.

D2. Treat killing the old sweeper as capability revocation. This is simple in a
test harness but does not fence a paused process, delayed request, or copied
long-lived credential.

D3. Never delete on a backend without revision-guarded delete. This has the
smallest safety surface but gives up reclamation on common object APIs. The
expiring effect-grant fallback retains safety with a longer publication stall.

D4. Retire the reservation immediately after a named object read returns
absent. This reduces blocking time but permits an old delayed delete to remove a
later recreation of the same digest object.

## Compatibility and migration

The command and state format remain unpublished. Adding mark, effect-receipt,
and adoption records requires a new command version and a state-format fixture.
Unknown commands fail closed. Activation requires homogeneous coordinator
binaries. Existing RFC-0015 reservations without a mark or effect receipt are
not auto-retired; a bootstrap migration must preserve them as blocking until an
operator abandons the local test state or a later explicit reconciliation
transition handles them.

## Unresolved questions

- Which S3, GCS, and Azure primitives provide a usable revision-guarded delete.
- How production effect grants are enforced without placing a proxy on every
  object operation.
- How trusted time, credential expiry, and maximum request lifetime compose in
  the unguarded fallback.
- How mark receipts and reservations partition with range movement.
- How coordinator snapshots retain or expire request outcomes and receipts.
- Whether the longer fallback reservation stall is economically acceptable.
