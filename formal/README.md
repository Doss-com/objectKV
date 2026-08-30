# objectKV executable reference model

Status: `[VERIFIED]` for the two finite TLC scopes recorded below. This is not
a proof of the Rust implementation or an unbounded production cell.

The model is the shortest precise description of how one objectKV cell should
behave across concurrent transactions, the txLog, memory and stable media,
object publication, recovery generations, and disposable serving state.

```text
concurrent client transactions
  -> order and validate conflicts
  -> stage in RAM
  -> persist on a stable-media quorum
  -> acknowledge COMMITTED
  -> retain the exact txLog suffix
  -> build and authenticate an immutable object closure
  -> protect the pending object frontier
  -> pop the covered txLog prefix
  -> activate the object frontier
  -> hydrate a RAM, NVMe, or Rocks serving image
  -> serve only a reconstructable version in the active generation
```

The load-bearing equation is:

```text
O <= C

Database(C) = ObjectState(O) + txLog mutations in (O, C]
```

`C` is the committed cell version. `O` is the authenticated active object
frontier. A committed version is safe only while it is covered by an immutable
object closure or retained on stable txLog media.

## What the model owns

[`ObjectKVCell.tla`](ObjectKVCell.tla) models one bounded cell as one integrated
state machine. It owns these contracts:

| Contract | Representative actions | Safety property |
|---|---|---|
| Concurrent transaction order | `Begin`, `SequenceTxn`, `CommitTxn`, `RejectConflict` | versions are unique and conflicting work cannot commit |
| Commit durability | `StageInRam`, `PersistOnStableMedia`, `DeliverCommitted` | a committed reply requires a stable quorum and a recoverable version |
| Recovery fencing | `AdvanceGeneration`, `InstallGeneration` | stale generations cannot commit or serve |
| Object publication | `BuildObjectClosure`, `PrepareObjectFrontier`, `ActivateObjectFrontier` | active and pending roots name complete closures |
| txLog reclamation | `PopTxLogThroughPending` | a log prefix is removed only after the pending object frontier protects it |
| Disposable compute | `LoseRam`, `LoseStableMedium`, `HydrateServingImage`, `ServeRead` | serving images are reconstructable, generation current, and safe to discard |

RAM, NVMe, and Rocks are serving and staging choices, not separate database
truths. The model permits any serving tier to be recreated from the same
object base and stable txLog suffix. A RAM copy alone never authorizes a
`COMMITTED` response in the default profile.

## The proof and evidence chain

```text
TLA+ reference state machine
  -> defines allowed architecture transitions and invariants

Rust trace refinement                           [VERIFIED: staged txLog prefix]
  -> maps 36 healthy implementation events to the exact model identity
  -> rejects an early-acknowledgement poison at its first assertion

real-infrastructure eval receipt                [EVALUATING]
  -> measures latency, throughput, loss, debt, and bounded state
```

The model answers whether an explored ordering is allowed. The staged txLog
prefix now has one implementation refinement receipt. Transaction commit,
object publication, txLog pop, and serving recovery remain `[EVALUATING]`.
Performance still requires a separate eval receipt.

The retained GCP implementation receipt is documented at
[`docs/artifacts/eval-receipts/cell-trace-refinement-gcp-r0-2026-08-30/`](../docs/artifacts/eval-receipts/cell-trace-refinement-gcp-r0-2026-08-30/README.md).

## Checked scopes

The checks ran with TLC 2.19 from `tla2tools` 1.7.4 on the GCP R0 runner. The
tool jar SHA-1 was `bee4a54f3ee3d4afc347c3240ec2d9e93b075104`.

| Configuration | Scope | Generated | Distinct | Depth | Result |
|---|---|---:|---:|---:|---|
| `ObjectKVCell.cfg` | 3 nodes, 1 transaction, 2 generations, 1 stable-media failure | 2,486,430 | 99,408 | 23 | `[VERIFIED]` no safety violation |
| `ObjectKVConcurrency.cfg` | 3 nodes, 2 conflicting transactions, 2 generations | 4,496,463 | 164,668 | 28 | `[VERIFIED]` no safety violation |

Six negative controls each weaken one named contract. Every control produced a
counterexample:

| Poison | Expected violation | Generated before violation | Distinct | Depth |
|---|---|---:|---:|---:|
| `AckBeforeStableQuorum.cfg` | committed reply without stable quorum | 183 | 88 | 6 |
| `IgnoreGenerationFence.cfg` | stale-generation commit | 12,979 | 1,724 | 9 |
| `PopStalePendingFrontier.cfg` | txLog pop without protected object state | 76,683 | 7,515 | 12 |
| `PublishIncompleteClosure.cfg` | publication of an incomplete closure | 13,752 | 1,736 | 9 |
| `ServeStaleImage.cfg` | read from an unsafe serving image | 2,121 | 275 | 6 |
| `SkipConflictValidation.cfg` | conflicting concurrent commit | 214,655 | 15,098 | 15 |

The exact machine-readable receipt is
[`evidence/gcp-r0-2026-08-30.json`](evidence/gcp-r0-2026-08-30.json).

## Run the model

Use Java 11 or newer and the official TLA+ tools jar:

```bash
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -config formal/ObjectKVCell.cfg formal/ObjectKVCell.tla

java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -config formal/ObjectKVConcurrency.cfg formal/ObjectKVCell.tla

for cfg in formal/poisons/*.cfg; do
  java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
    -config "$cfg" formal/ObjectKVCell.tla
done
```

Healthy configurations must exit successfully. Poison configurations must exit
with an invariant counterexample.

## Deliberate boundary

The model abstracts consensus to a quorum and generation-fencing contract. It
does not prove Raft or Paxos, object-store linearizability, the RocksDB engine,
the wire protocol, or liveness. Conflict tracking is one coarse domain. The
largest checked concurrency scope contains two transactions. Serving images
are version markers rather than physical row or column layouts.

These are refinement obligations, not hidden claims. Add state only when a
design decision cannot be expressed or falsified with the current model.

The modeling structure follows Oswald's useful separation of object store,
log protocol, and system composition, especially its treatment of writers,
tailers, snapshots, and garbage collection as one concurrent system. objectKV
differs at the acknowledgement boundary: the default commit path is a
stable-media quorum, while immutable object publication remains asynchronous.
See [Oswald](https://nvartolomei.com/oswald/) and its
[formal model](https://github.com/nvartolomei/oswald/tree/main/p/Oswald).
