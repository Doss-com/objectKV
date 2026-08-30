# RFC-0050: Executable cell reference model

- Status: `[PROPOSED]`
- Model mechanism: `[VERIFIED]` in two finite scopes
- Implementation refinement: `[EVALUATING]`
- Authors: DOSS
- Created: 2026-08-30

## Decision

Maintain one small executable TLA+ state machine as the architecture contract
for a complete objectKV cell. It composes transaction concurrency, generation
fencing, RAM staging, stable-media quorum acknowledgement, txLog retention,
immutable object publication, safe log reclamation, and disposable serving
images.

Do not create independent formal models that can each pass while violating an
inter-layer invariant. Specialized models may refine one mechanism, but they
must map their externally visible transitions back to this cell model.

```text
                 one cell reference state machine

transactions -> ordering -> RAM -> stable txLog quorum -> committed C
                                            |
                                            v
                                   retained suffix (O,C]
                                            |
object builder -> pending O -> safe pop -> active O
                                            |
                                            v
                              RAM | NVMe | Rocks serving image
```

## Why this boundary

The highest-risk failures sit between subsystems:

1. returning `COMMITTED` before a durable quorum exists;
2. accepting a stale writer after a generation change;
3. reclaiming txLog records before an authenticated object closure protects
   them;
4. activating an incomplete object root;
5. serving from a stale or unreconstructable local image;
6. committing conflicting concurrent transactions.

Unit models of consensus, storage, publication, and serving can miss these
compositions. One reference machine makes the intended ordering reviewable and
falsifiable.

## Reference contract

The model preserves:

```text
O <= C
Database(C) = ObjectState(O) + StableTxLog(O, C]
```

A `COMMITTED` response means the transaction has passed conflict validation,
matches the active generation, and exists on a stable-media quorum. Object
storage is not on this foreground acknowledgement path. Publication advances
`O` asynchronously only after the complete immutable closure exists.

RAM, NVMe, and RocksDB are interchangeable serving profiles under one recovery
contract. RAM can also stage writes, but a volatile copy does not satisfy the
default durable commit contract. A serving image may be discarded at any time
and must be reconstructed from the active object closure plus retained txLog.

## Implementation refinement vocabulary

The implementation should emit a compact trace using these stable event names.
Payloads must include cell, generation, version, transaction or publication
identity, and the relevant media or worker identity.

| TLA+ action | Rust or service boundary | Existing evidence |
|---|---|---|
| `Begin`, `SequenceTxn` | transaction authority and commit proxy | strict-serializability model `[CODE-COMPLETE]` |
| `StageInRam` | staged txLog volatile append | RFC-0045 L0 and L1 `[VERIFIED]` |
| `PersistOnStableMedia` | log-node synchronized local journal | RFC-0045 L1 `[VERIFIED]` on one host |
| `CommitTxn`, `DeliverCommitted` | quorum outcome and client reply | independent-media commit `[EVALUATING]` |
| `BuildObjectClosure` | object writer and authenticated manifest build | RFC-0030 mechanism `[CODE-COMPLETE]` |
| `PrepareObjectFrontier` | replicated pending publication root | G4.10b `[EVALUATING]` |
| `PopTxLogThroughPending` | recovery-stream reclamation | RFC-0030 `[EVALUATING]` |
| `ActivateObjectFrontier` | replicated active publication root | publication recovery `[VERIFIED]` in bounded process profiles |
| `AdvanceGeneration`, `InstallGeneration` | recovery generation authority and writer fence | process recovery `[VERIFIED]` in bounded profiles |
| `HydrateServingImage`, `ServeRead` | `ServingImage` and `SingleRange` | GP3.1 and GP3.1.1 `[VERIFIED]` |
| `LoseRam`, `LoseStableMedium` | worker or media fault injection | independent-host envelope `[EVALUATING]` |

`[PROPOSED]` A small trace checker consumes implementation events and rejects a
sequence that has no corresponding TLA+ transition. This is the next useful
refinement step. It is more valuable than enlarging the finite model before the
implementation can demonstrate conformance to the current one.

## Model-check result

TLC 2.19 exhaustively explored:

| Scope | Generated | Distinct | Depth | Result |
|---|---:|---:|---:|---|
| integrated 3-node cell, 1 transaction, 2 generations, 1 stable-media loss | 2,486,430 | 99,408 | 23 | no invariant violation |
| 3-node concurrency, 2 conflicting transactions, 2 generations | 4,496,463 | 164,668 | 28 | no invariant violation |

Six deliberate contract violations each produced a counterexample: early
acknowledgement, stale-generation commit, unsafe txLog pop, incomplete object
publication, stale serving read, and skipped conflict validation.

The exact model, configurations, limitations, and receipt are under
[`formal/`](../formal/README.md).

## Relationship to Oswald

Oswald demonstrates a useful formal-method pattern: model the object store,
log protocol, writer, tailer, snapshot, and garbage collector together, then
monitor a few load-bearing invariants such as one value per log sequence number
and monotonic snapshot and garbage-collection state.

objectKV adopts that composition pattern. It does not adopt object-only
foreground acknowledgement as the default cell profile. objectKV acknowledges
from a stable-media quorum and uses object storage as the asynchronous,
immutable, open-format recovery base. The design tradeoff is lower commit
latency and multi-writer scaling in exchange for operating a bounded txLog
quorum.

## What this does not decide

This RFC does not select Raft versus Paxos. Both must refine the same quorum,
ordering, and fencing contract. It does not prove liveness, an unbounded node or
transaction count, object-store semantics, RocksDB correctness, SQL semantics,
or performance. It also does not make a finite TLC success a production proof.

Those claims remain owned by mechanism-specific formal refinements, Rust trace
conformance, and the master performance matrix.

## Next decision-bearing work

1. Emit the minimal stable transition vocabulary from the current transaction,
   txLog, publication, and serving paths.
2. Check one retained healthy trace and one poison trace against the reference
   model.
3. Use the same transition identities in the independent-media T29 receipt.
4. Extend the model only when that trace work reveals a missing state or when a
   new architectural decision, such as resolver partitioning, cannot be stated
   with the current contract.

This optimizes for one communicable architecture and early detection of unsafe
cross-layer orderings. It gives up detailed proofs of each chosen algorithm
inside the top-level model.
