# RFC-0023: Routine voter reconfiguration

- Status: accepted for the bounded Cell v0 process contract
- Authors: DOSS
- Created: 2026-08-23
- Supersedes:

## Decision

objectKV separates routine voter repair from transaction-system generation
recovery. A healthy surviving quorum admits a replacement under a fresh Raft
node identity and storage incarnation, installs an authority snapshot plus the
retained suffix, promotes it through Raft membership change, and retires the
failed voter without changing the cell generation. The external control
authority records a monotonic `membership_epoch` and authorizes the exact old
and next voter sets. Loss of transaction-system continuity still uses RFC-0009
and allocates a new generation.

## Context and invariant

The fresh-learner process gate proves that a blank node can receive a durable
authority snapshot, replay the suffix, retain exact outcomes, and reopen. The
same-ID repair control fails because a surviving leader can retain replication
progress for an erased node identity and therefore skip the bytes the new disk
needs.

The next ambiguity is whether every replacement should allocate a new cell
generation. It should not. A generation is a discontinuity in the transaction
system and version space. Replacing one voter while a valid quorum retains the
ordered history is an ordinary membership change inside the same transaction
system.

The invariant is:

```text
routine repair:
  generation stays G
  membership_epoch advances E -> E + 1
  Raft log and commit-version sequence remain continuous

transaction-system recovery:
  generation advances G -> G + 1
  RFC-0009 fence, reserve, reconstruct, and activate apply
```

At most one voter reconfiguration may be pending for a cell and membership
epoch. A new voter may not acknowledge writes or serve authoritative reads
until it has installed the admitted snapshot, replayed the required suffix, and
entered the committed voter set.

## Proposed contract

The control authority extends its cell record with:

```text
active_generation
membership_epoch
active_transaction_system_id
active_voters: node identity -> public key and storage incarnation
active_membership_position
pending_reconfiguration:
  reconfiguration_id
  expected_generation
  expected_membership_epoch
  old_voters
  next_voters
  replacement_node
  minimum_snapshot_position
  state: prepared | learner_ready | membership_committed
```

The storage incarnation is a stable random identity created with a blank durable
root. It is not inferred from host, address, disk path, or Raft node ID. A
destroyed root never inherits the prior incarnation.

### State transition

1. The controller submits `PrepareReconfiguration` with exact generation,
   membership epoch, active voter digest, next voter digest, and a stable
   reconfiguration identity. The authority rejects concurrent, stale, or
   conflicting requests.
2. The replacement starts with a fresh node ID and storage incarnation. The
   `AddLearner` RPC requires the prepared reconfiguration credential because a
   learner receives tenant bytes.
3. OpenRaft installs the durable authority snapshot and replays the retained
   suffix. The replacement does not serve or vote yet.
4. The replacement and an existing voter quorum attest a `LearnerReady`
   statement binding cell, generation, membership epoch, reconfiguration ID,
   old and next voter digests, storage incarnation, snapshot position, and
   exact applied Raft position.
5. The authority records `learner_ready` only after verifying the certificate.
6. The active leader performs the Raft membership transition to the exact next
   voter set. Normal writes may continue through Raft's membership rules; no
   version generation changes.
7. The resulting membership-log position is certified by a quorum of the next
   voter set. The authority records `membership_committed`, installs the next
   voter map, advances `membership_epoch`, and clears the pending record.
8. The removed voter loses commit, publication, and recovery authorization. Its
   bytes enter a separate quarantine and deletion process.

Every request and response is idempotent under the stable reconfiguration
identity. A lost response is resolved from the authority and Raft logs, not by
guessing from current membership alone.

### Commit behavior

Routine repair does not append a generation-fence barrier and does not change
`Version.generation`. The surviving quorum may continue to commit while the
learner catches up. Backpressure or commit refusal remains capacity-driven.

If the existing voter set loses quorum before the membership transition is
committed, routine reconfiguration stops. The controller cannot use the pending
record to manufacture a quorum. Recovery proceeds through RFC-0009 with a new
generation and the full fence and reconstruction proof.

### RPC separation

Routine repair receives distinct prepare, learner admission, membership commit,
and finalize operations. The existing generation-recovery membership endpoint
remains restricted to the `Recovering` phase. One endpoint must not infer which
protocol applies from a caller-provided voter set.

Generation-fenced semantic transactions bind the same credential in both the
RPC wrapper and `CellTransactionCommand`. The process state machine rechecks its
replicated generation mirror at apply time so a command authorized before a
full recovery fence cannot apply after that fence.

## Failure model

- the failed voter returns with stale disk or old credentials;
- a blank disk is started under the old Raft node identity;
- snapshot install or suffix replay is partial, corrupt, or interrupted;
- the learner-ready response or membership-change response is lost;
- the old leader dies before, during, or after membership transition;
- the control authority leader dies at every state transition;
- two controllers race with different replacement identities or voter sets;
- the next voter set loses quorum before authority finalization;
- a removed voter attempts a commit, publication, or recovery signature;
- network partitions isolate old, new, or joint voter majorities;
- disk full prevents the learner from durably applying the required position.

## Alternatives

Allocate a new generation for every voter replacement. This reuses the existing
recovery machinery but turns routine maintenance into transaction-system
failover, aborts in-flight work, fragments version history, and increases the
amount of coordinator state that must be correct.

Reuse the same Raft node ID with a new disk. This preserves configuration shape
but permits surviving leaders to reuse stale replication progress. The bounded
process probe already rejected it.

Let the Raft leader change membership without external authorization. This is
mechanically simple but permits unauthorized learner admission, tenant-data
exposure, conflicting repair controllers, and control-plane drift.

Put a storage-incarnation epoch inside the Raft transport and retain the same
node ID. This can be safe if every leader invalidates replication progress when
the incarnation changes. It adds a forked consensus protocol surface and must
pass the same controls before reconsideration.

## Eval plan

The pure contract suite is `cell-routine-reconfiguration-v0`. The admitted
physical follow-up is `cell-routine-reconfiguration-process-v1` over a
three-process external authority quorum, three initial data voters, and one
fresh replacement process.

The positive lane:

1. commits semantic multi-key transactions in generation `G`;
2. snapshots and reclaims the admitted prefix;
3. loses one voter and prepares a fresh node and storage incarnation;
4. commits another transaction while the learner catches up;
5. installs snapshot plus suffix and certifies the learner position;
6. moves membership from `{1,2,3}` to `{2,3,4}`;
7. loses voter `2`, elects from `{3,4}`, and commits again;
8. restarts voter `4` and verifies rows, OCC history, envelope chain, and exact
   retry outcomes;
9. proves every committed version retains generation `G` and monotonic log
   sequence.

Negative controls must reject:

- same-ID blank-disk replacement;
- learner addition without prepared authority state;
- promotion before snapshot and suffix catchup;
- stale membership epoch or mismatched voter digest;
- concurrent reconfiguration identity;
- lost-reply retry that applies membership twice;
- removed-voter commit or recovery signature;
- routine repair after the surviving voters lose quorum.

Hard gates are zero acknowledged loss, exact snapshot and outcome recovery,
one membership transition, no generation change, no unauthorized learner, and
post-repair commit availability. OTel records snapshot bytes and duration,
suffix bytes and entries, repair duration, commit pause, membership epoch,
process kills, retries, and correctness anomalies.

## Compatibility and migration

The authority record gains a versioned optional reconfiguration block and a
membership epoch. Existing records migrate as epoch zero with no pending
reconfiguration. Old nodes may participate only as existing voters; they cannot
be newly admitted or promoted once the authority requires the new credential
and certificate versions.

The generation-recovery protocol and certificates remain valid. Routine repair
uses distinct command and certificate purpose tags so a proof cannot be replayed
between protocols.

## Unresolved questions

- Whether learner readiness requires an existing-voter quorum signature in
  addition to the replacement's exact-position signature.
- Whether one-at-a-time reconfiguration is sufficient for zone evacuation or a
  bounded batch protocol is required.
- How commit authorization leases interact with temporary control-authority
  unavailability during an otherwise healthy routine repair.
- When a caught-up learner may serve snapshot reads before voter promotion.
- The maximum admitted snapshot size and suffix lag before repair must refuse
  and require another snapshot.
