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
2. Reserve generation `G + 1` durably at the coordinator quorum. Generation `G`
   can no longer publish or acknowledge.
3. Read the last accepted root, every range manifest, object-durable watermark
   `O`, and the retained WAL suffix. Reconcile identities and checksums before
   serving.
4. Determine the maximum committed WAL index and commit version supported by a
   quorum. Unknown client outcomes remain unknown until replay resolves them.
5. Replay the contiguous committed suffix after `O` into clean generation-owned
   state. Missing or conflicting committed entries halt recovery.
6. Recruit sequencer, resolver, WAL, and worker roles for `G + 1`. In-flight
   transactions from `G` abort and retry; they never cross generations.
7. Conditionally publish the new control root through the coordinator quorum.
8. Activate commits only after the new roles pass the invariant scan. Read-only
   recovery may precede writes only under an explicit version ceiling.

No rollback reactivates an older generation. Failed recovery attempts allocate
another generation or continue the same reserved attempt under one quorum-owned
recovery identity.

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
- `O` never advances beyond reconstructable object state;
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
- A deliberate stale-publication bug must be found by a recorded seed and replay
  identically.
- The `fault-recovery` suite grows from the probe to overlapping sequencer,
  resolver, WAL, worker, object-store, and coordinator faults.
- Safety and post-fault liveness are separate hard gates.

## Compatibility and migration

Simulation trace schema, scenario ID, dependency lock hash, and source commit are
part of every receipt. Dependency or trace-schema changes establish a new
baseline and do not claim byte comparability with old traces. The production
runtime accesses clock, random, network, durable log, and object operations
through seams that the simulator can replace.

## Unresolved questions

- Coordinator implementation and membership-change protocol.
- Whether upstream Turmoil closes all runtime entropy for the eventual WAL and
  object-store dependency graph without a pinned fork.
- Seed corpus size and long-running exploration budget per release phase.
- Which recovery state should be model-checked in TLA+ in addition to simulation.
