# RFC-0030: Authenticated object frontier and crash-safe txLog pop

- Status: `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 publication authority, data authority, and recovery retention

## Decision

Advance the transaction recovery-stream floor `O` only through a two-authority
handshake. The publication authority first retains one exact immutable row
manifest as a pending object frontier. The data authority then validates that
manifest, quorum-commits a physical txLog pop through its covered version, and
returns a quorum certificate over the exact applied frontier. The publication
authority verifies the certificate and promotes the pending frontier to active.

```text
immutable row manifest M, covered through O
                    │
                    ▼
publication authority: prepare pending(M, O)
                    │  M is now a GC root
                    ▼
data authority: validate M, replicate pop through O
                    │  every voter applies floor O
                    ▼
data-voter quorum certificate over (M, O, log position)
                    │
                    ▼
publication authority: pending -> active
```

There is one whole-cell object frontier in Cell v0. Range-local frontiers are a
later scaling change and cannot weaken this handshake.

## Context and invariant

G4.6 proves that serving, conflict, and retry state can be retained by separate
frontiers, but it only projects ideal recovery-stream reclamation. A mutable
`transaction_retention_floor` without an authenticated object closure could
remove the only recoverable copy of committed mutations.

The invariant is:

```text
for every committed version v <= transaction_retention_floor:
  active or pending publication state names an immutable manifest
  whose verified closure reconstructs the cell through at least v
```

The pending state is part of the invariant. A crash after data pop and before
publication activation must still leave the new closure protected from GC.

## Persisted state

The publication authority adds:

```text
pending_object_frontier: ObjectFrontierRecord?
active_object_frontier:  ObjectFrontierRecord?

ObjectFrontierRecord
  owner_generation
  source_root
  manifest: exact key, length, SHA-256
  covered_through
  prepared_at: publication-authority term and index
```

The data authority adds:

```text
AppliedObjectFrontierState
  latest frontier record
  data-authority applied term and index
  exact latest command fingerprint and response

RetainedTransactionStream
  records with commit_version > object_frontier.covered_through
  transaction_retention_floor = object_frontier.covered_through
```

Old snapshots decode both fields as absent. Empty fields are omitted when
serializing so the existing default snapshot shape remains stable.

## Publication transitions

### Prepare

`PrepareObjectFrontier` supplies an exact current root, exact manifest,
covered-through version, and expected active frontier.

The authority accepts only when:

1. no pending frontier exists;
2. the source root currently equals the supplied manifest;
3. the expected active frontier equals current active state;
4. the new coverage is non-zero and strictly greater than active coverage;
5. the manifest is a valid immutable manifest reference;
6. the command generation is the active generation.

Acceptance creates a record using the authority-owned committed position. Both
pending and active manifests are GC roots. Prepare increments the root and
intent epoch, so a concurrent mark cannot sweep either closure.

There is no cancel transition in Cell v0. This gives up automatic liveness after
a permanently failed controller in exchange for removing the ambiguous case
where data may already have popped through the pending frontier. Operator
repair requires completing the handshake or a later independently proved
recovery protocol.

### Activate

`ActivateObjectFrontier` supplies the exact pending record and a data-quorum
certificate. The publication authority verifies the proof against the active
generation's pinned data-voter public keys. It then atomically replaces active
with pending and clears pending.

Activation never precedes data pop. Once active, the prior active manifest is
no longer retained by the object-frontier state, although another root, pin, or
intent may still retain it.

## Data safe-pop transition

`AdvanceObjectFrontier` is a versioned command inside the existing
generation-fenced data command envelope. Before proposing it, the data leader
must perform all of the following:

1. read publication state through a linearizability barrier;
2. match the exact pending record;
3. read the manifest object by exact key, length, and SHA-256;
4. decode and validate the row-manifest envelope and all named object
   references;
5. require manifest generation and covered-through fields to equal the pending
   record;
6. require `transaction_retention_floor < O <= current_commit_version`.

The replicated apply path rechecks monotonic coverage, generation fencing, and
the local commit-version bound. It then atomically:

```text
retained_transactions := records with commit_version > O
transaction_retention_floor := O
applied_object_frontier := (pending record, applied log position)
```

This is a physical state-machine mutation, not a storage-accounting projection.
A stale read cursor at or below the new floor fails closed.

The pending record is immutable and has no cancel path. Therefore a valid
linearizable observation remains safe while the data command is in flight. A
generation change fences the command before apply.

## Data quorum certificate

Every data voter has an Ed25519 key pinned by the generation authority. A voter
signs only after its local state machine exactly matches the proposed
statement:

```text
protocol_version
cell_id
generation
transaction_system_id
frontier record
data-authority applied term and index
data membership SHA-256
```

The publication authority accepts only distinct valid signatures from a data
quorum, with an exact membership digest and exact pending frontier. Duplicate,
unknown, stale-generation, wrong-manifest, wrong-coverage, wrong-position, and
sub-quorum proofs fail without changing publication state.

This is crash-fault tolerance, not Byzantine consensus. The data controller is
trusted code and must validate the immutable manifest before proposing pop.
Signatures prevent stale or fabricated cross-authority acknowledgements; they
do not make a malicious quorum safe.

## Crash matrix

| Crash point | Durable state | Recovery action |
|---|---|---|
| before prepare commit | old active only, txLog intact | retry prepare |
| after prepare | old active plus pending, txLog intact | validate and pop |
| during data quorum commit | pending retained, pop outcome unknown | linearizable data read, retry exact command |
| after data pop | pending retained, txLog physically popped | collect attestations |
| during attestation | pending retained, txLog popped | recollect from voters |
| during activation | pending retained or promoted active | resolve exact publication outcome |
| after activation | new active, txLog popped | next frontier may begin |

At every row, at least one object closure that covers the physical retention
floor remains a publication GC root.

## Failure model

The contract covers controller and worker crashes, lost responses, duplicate
requests, leader failover, stale generation credentials, partial voter
availability, malformed or missing object data, root replacement before
prepare, object-read timeout, duplicate signatures, and snapshot replay.

The contract does not cover object-store loss outside the configured durability
class, Byzantine data voters, compromise of quorum signing keys, or a manifest
whose content-addressed bytes are semantically wrong because trusted publisher
code encoded the wrong logical state.

## Alternatives

### Read root, then pop

This minimizes protocol work. It gives up safety because root replacement and
GC can race the data pop.

### Object upload directly from the txLog quorum

This reduces cross-authority coordination. It puts object-store latency and
credentials on the commit quorum and gives up disposable publication workers.

### Keep the txLog forever

This avoids safe-pop coordination. It reproduces G4.5 lifetime-state growth and
cannot support bounded cell recovery state.

### FoundationDB or TiKV authority plus object history

This buys a mature durability and log-retention implementation. It gives up the
native object frontier but remains the required pivot if objectKV cannot pass
the same-durability performance and operational gates.

## G4.7 eval plan

The candidate runs a real three-data-process OpenRaft cluster, a real
three-process publication authority, and a physical immutable row-object
closure. It executes prepare, validated pop, voter attestation, activation,
process restart, and empty-controller recovery.

Negative controls:

1. pop without a publication pending frontier;
2. forged covered-through version for an otherwise valid manifest;
3. sub-quorum or duplicate-signer activation certificate;
4. stale-generation frontier command;
5. remove pending protection before activation;
6. accounting-only projected pop.

Hard gates:

1. all committed keys recover exactly after physical pop and process restart;
2. stale txLog cursors fail at the persisted floor;
3. the candidate physically removes all retained records at or below `O`;
4. the pending or active manifest remains a GC root through every crash point;
5. every negative control is rejected before unsafe mutation;
6. exact retries return one outcome and do not advance twice;
7. old state-machine and publication snapshots still decode;
8. three deterministic seeds and a fresh-controller replay agree;
9. complete state accounting preserves the G4.6 bounded-state curve;
10. the frozen wall-time budget is reported, not relaxed after measurement.

Primary correctness metrics are recovered-state digest equality, persisted
retention floor, physically retained record count, protected frontier count,
certificate signer count, and rejected unsafe transitions. Latency is recorded
by phase but remains diagnostic until release builds run on independent
machines.

## Compatibility and migration

The publication command remains under its existing versioned envelope and gains
new tagged actions. The data command receives a distinct versioned payload
magic. Old binaries reject unknown command versions before mutation. New
binaries default absent frontier state to empty and continue reading all G4.6
fixtures.

Rolling mixed-version mutation is not supported. A cell upgrades while fenced,
then begins the first object-frontier handshake only after every authority voter
runs the new state-machine code.

## Tradeoff

This optimizes for bounded recovery state without putting object latency on the
transaction commit path. It gives up a one-authority design, adds a signed
cross-quorum handshake, and temporarily retains both old and pending object
closures while a frontier is in flight.

## Not claimed

- OpenRaft internal log purge or snapshot compaction below the application
  recovery-stream floor;
- range-local or tenant-local object frontiers;
- cancellation of a stuck pending frontier;
- cloud or independent-machine evidence;
- Byzantine safety;
- production key management or rotation;
- acceptable commit throughput.
