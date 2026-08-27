# RFC-0009: Failure and recovery generations

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

objectKV uses generation-based recovery and builds exact deterministic
simulation before the replicated WAL. A small statically configured coordinator
quorum, outside the objectKV data keyspace, owns the active generation and the
root control-metadata pointer. Every authoritative publication includes the
generation and is conditionally rejected after a newer generation activates.

Turmoil 0.7.2 is the first simulation harness behind an objectKV-owned interface.
The dependency choice is reversible; the seed, canonical trace, invariants, and
replay command are objectKV contracts.

## Context and invariant

Asking stale roles to stop is not fencing. The system is safe only when old
sequencers, WAL leaders, resolvers, storage workers, and compactors cannot make
new state authoritative after recovery activates a higher generation.

For every generation `G`, at most one transaction-system configuration may
acknowledge commits. Every manifest or root transition must satisfy
`publication.generation == active_generation` at its compare-and-publish point.

## Bootstrap authority

The first deployment configures an odd coordinator set through operator-owned
bootstrap configuration. The quorum stores:

- cluster identity;
- monotonically increasing active generation;
- active transaction-system membership and WAL root;
- root control-metadata object identity;
- last completed recovery record.

Bulk range maps and other control records may live in a versioned system
keyspace after boot, but the coordinator's root pointer and generation are
sufficient to locate and fence that keyspace. The object store cannot choose a
latest root through LIST.

## Recovery protocol

1. Detect loss of the active transaction system through coordinator leases and
   quorum evidence. A single observer cannot start recovery.
2. Prepare recovery identity `R` and generation `G + 1` durably at the
   coordinator quorum. The authority enters `Fencing` and stops issuing new G1
   commit authorization.
3. Append a generation-fence barrier to the active data log. The data state
   machine rejects every generation `G` client command ordered after that
   barrier, including a request that passed its authority check before step 2.
   A command ordered before the barrier remains part of the committed recovery
   prefix even if its reply is delayed.
4. Reserve generation `G + 1` as `Recovering` only after the authority records a
   nonzero fence position for `R`. Generation `G` can no longer publish or add
   a semantically accepted client command to the log.
5. Read the last accepted root, its bounded range-map and manifest index,
   object-durable watermark `O_cell`, per-consumer durable frontiers, and the
   retained WAL suffix. Reconcile the control root, WAL chain, and every range
   assigned for immediate service before that range serves a read. Recovery
   does not fetch or validate every data object before any range can serve.
6. Determine the maximum committed WAL index and commit version supported by a
   quorum. Unknown client outcomes remain unknown until replay resolves them.
7. Replay the contiguous committed suffix after `O_cell` into clean generation-owned
   state. Missing or conflicting committed entries halt recovery.
8. Recruit sequencer, resolver, WAL, and worker roles for `G + 1`. In-flight
   transactions from `G` abort and retry; they never cross generations.
9. Conditionally publish the new control root through the coordinator quorum.
10. Activate commits only after the new roles pass the bounded control and active
   assignment invariant scan. Read-only recovery may precede writes only under
   an explicit version ceiling, and each lazily opened range must verify its
   manifest closure before serving.

## Data-quorum recovery certificates

The generation authority accepts a fence or recovered position only through a
versioned quorum certificate. The recovery controller transports certificates
but is not trusted to invent their contents.

Each certificate statement binds:

- certificate purpose, `fence` or `recovered`;
- cell, next generation, and recovery identity;
- active and pending transaction-system identities;
- the exact Raft term and index observed by the signing data node;
- a digest of the authority-pinned voter identity and public-key map.

Every attestation is an Ed25519 signature by one pinned data voter over the
canonical statement bytes. The authority deterministically rejects unknown or
duplicate signers, bad signatures, a membership-digest mismatch, a stale
recovery identity, a zero position, and fewer than a majority of distinct
configured voters.

A data node signs a `fence` statement only when the statement position equals
the exact applied generation-fence barrier in its state machine. It signs a
`recovered` statement only when the position equals the applied voter-set
transition and its local generation mirror is still `Recovering`. Merely having
applied some later index is not sufficient.

The active voter key map is installed at bootstrap. `Prepare` pins the pending
transaction-system voter key map before any certificate can be collected.
`Reserve` verifies a fence certificate against the active map. `Activate`
verifies a recovered certificate against the pending map and requires its log
position to follow the certified fence position.

This protects against a faulty or compromised recovery controller and fewer
than a quorum of compromised data signers. It does not protect against a
compromised authority quorum, a compromised data quorum, unsafe private-key
provisioning, or a signer implementation that violates the local observation
contract. Production key custody and rotation remain separate work.

No rollback reactivates an older generation. Failed recovery attempts allocate
another generation or continue the same reserved attempt under one quorum-owned
recovery identity.

The root and range index must have a declared size and open-cost envelope. A
recovery path that requires scanning all objects or every historical range
manifest before the first correct read violates the disposable-serving thesis.

## Simulation contract

The simulator supplies deterministic variants of:

- clock and timers;
- random choices and unique identities;
- network delivery, latency, drops, partitions, and repair;
- durable log writes, fsync, partial writes, disk-full, and restart;
- object GET/PUT/conditional update, lost success, throttling, and corruption;
- process crash, restart, and stale-role execution;
- operator actions and workload generation.

Every scenario records a canonical trace containing contract version, exact
dependency lock hash, scenario ID, seed, profile hash, logical event index,
virtual time, actor, action, result, invariant snapshots, and final digest. It
contains no wall-clock timestamp, OS-generated UUID, pointer address, hash-map
iteration, or secret.

Exact replay means two fresh processes at the same source commit, toolchain,
profile, and seed emit byte-identical canonical traces. The build fails closed
if Tokio runtime RNG seeding is not enabled. A successful same-process rerun is
insufficient evidence.

## Required invariants

- no acknowledged commit is lost after allowed faults;
- one committed version maps to one batch fingerprint;
- active generation never decreases and committed versions are never reused;
- a stale generation never wins a root or manifest publication;
- `O_cell` never advances beyond reconstructable object state;
- popped WAL is never required to reconstruct any admitted version;
- after faults stop, the system either heals within the declared RTO or reports
  one bounded terminal reason;
- GC never deletes a reachable object.

## Worked failure cases

1. Generation 4's compactor pauses before manifest CAS. Generation 5 activates.
   The old CAS includes generation 4 and fails even if its expected ETag still
   matches an older local read.
2. A sequencer allocates versions after losing coordinator quorum. None can be
   acknowledged because generation authority is no longer provable.
3. Recovery sees object state through 100 and WAL entries 101, 102, and 104 with
   log index 103 missing. Version gaps may be legal, but a missing committed log
   index is not; recovery halts.
4. Resolver memory is lost during generation change. All in-flight transactions
   abort, so the new resolver never guesses at old conflict state.
5. The same seed produces a different trace because one dependency reads OS
   entropy. The determinism gate fails before any correctness result is trusted.
6. Faults are repaired but no fresh write commits within RTO. Safety may hold,
   but the liveness gate fails.

## Alternatives

- Store the generation only inside objectKV. This is circular because locating
  and committing that keyspace already requires a valid generation and root.
- Use object-store conditional metadata as the only coordinator. It lacks
  multi-party failure detection and an ordered quorum log for transaction-system
  activation.
- Adopt MadSim immediately. It covers more dependencies but requires broader
  runtime and crate substitution before objectKV has evidence that Turmoil's
  narrower seam fails.
- Build a scheduler from scratch. This maximizes control but repeats mature
  network, timer, and crash-model work and makes the simulator itself the first
  distributed-systems project.

## Eval plan

- CI runs a fixed seed corpus plus two fresh-process executions of one canonical
  replay probe and compares trace bytes.
- The deterministic `cell-commit-contract-v1` model rebuilds client outcomes
  from quorum-certified envelopes after restart and rejects stale generations,
  partial resolver acceptance, incomplete log tagging, conflicting retry, and
  leader-only durability.
- A deliberate stale-publication bug must be found by a recorded seed and replay
  identically.
- The `fault-recovery` suite grows from the probe to overlapping sequencer,
  resolver, WAL, worker, object-store, and coordinator faults.
- Safety and post-fault liveness are separate hard gates.

## Independent-machine eval boundary

`[CODE-COMPLETE]` The independent-machine controller boundary reuses the exact
G0.4 transaction history and independent oracle on three externally managed
data machines plus a fourth controller machine. It does not replace the
semantic workload with a cloud smoke test. The G5.2 infrastructure execution is
`[PROPOSED]` until the resident and object-leverage gates justify the cloud run.

The machine configuration names exactly three node IDs and binds each to:

- one distinct non-loopback IP endpoint;
- one absolute stable-storage root;
- one distinct machine identity;
- one distinct failure-domain identity.

The controller machine and failure-domain identities must differ from every
data-node identity. These fields are topology attestations recorded in the
receipt, not proof by themselves. The infrastructure receipt must additionally
name the cloud project, zone, instance identity, disk identity and type,
filesystem, source revision, binary digest, and lifecycle-hook digest from
provider observations.

The controller invokes one bounded lifecycle hook with four actions:

```text
prepare NODE_ID PROCESS_NODE_CONFIG_JSON
start   NODE_ID PROCESS_NODE_CONFIG_JSON
kill    NODE_ID
cleanup NODE_ID
```

`prepare` clears only the declared per-run root before a fresh replay. `start`
may start a stopped machine and then starts the exact node binary without
clearing its root. `kill` must remove the declared machine failure domain, not
merely close a client connection. `cleanup` stops residual test processes but
does not delete evidence or unrelated storage. Every action has a declared
timeout and fails closed on a nonzero exit.

The machine runner hashes the complete topology configuration and hook bytes,
runs each seed twice, and requires equal topology and semantic digests. Cloud
credentials never enter the configuration, command history, or receipt.

## Compatibility and migration

Simulation trace schema, scenario ID, dependency lock hash, and source commit are
part of every receipt. Dependency or trace-schema changes establish a new
baseline and do not claim byte comparability with old traces. The production
runtime accesses clock, random, network, durable log, and object operations
through seams that the simulator can replace.

## Unresolved questions

- Coordinator implementation and membership-change protocol.
- Production data-voter key provisioning, rotation, revocation, and custody.
- Automatic recovery detection and the evidence needed before `Prepare`.
- Bounded range-index representation, active recovery set, and maximum root
  size before a second index level is required.
- Whether upstream Turmoil closes all runtime entropy for the eventual WAL and
  object-store dependency graph without a pinned fork.
- Seed corpus size and long-running exploration budget per release phase.
- Which recovery state should be model-checked in TLA+ in addition to simulation.
