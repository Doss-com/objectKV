# objectKV executable cell model

Status: `[VERIFIED]` for the two finite TLC scopes in the R2 receipt.
Hand-written Rust trace conformance is `[CODE-COMPLETE]`. Neither result is an
unbounded proof or a mechanical implementation refinement.

## What the model says

[`ObjectKVCell.tla`](ObjectKVCell.tla) is the shortest executable description
of the ordering contract for one objectKV cell.

```text
┌──────────────────────┐
│ concurrent requests  │
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ order and conflicts  │
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ RAM stage             │
└──────────┬───────────┘
           ↓
┌──────────────────────┐
│ stable txLog quorum   │
└──────────┬───────────┘
           ↓ COMMITTED
┌──────────────────────┐      ┌──────────────────────┐
│ retained suffix      │  →   │ immutable closure    │
└──────────────────────┘      └──────────┬───────────┘
                                        ↓ protected pop
                             ┌────────────────────────┐
                             │ active object frontier │
                             └────────────┬───────────┘
                                          ↓
                             ┌────────────────────────┐
                             │ disposable RAM, NVMe,  │
                             │ or Rocks serving image │
                             └────────────────────────┘
```

The load-bearing relationship is:

```text
O ≤ C

Database(C) = ObjectState(O) + stable txLog mutations in (O, C]
```

`C` is the latest committed cell version. `O` is the authenticated active
object frontier. A committed version remains recoverable only while an
immutable object closure covers it or the stable txLog retains it.

## Contract map

| Contract | Representative actions | Safety property |
|---|---|---|
| Transaction order | `Begin`, `SequenceTxn`, `CommitTxn`, `RejectConflict` | versions are unique; conflicting work cannot commit |
| Commit durability | `StageInRam`, `PersistOnStableMedia`, `DeliverCommitted` | a committed reply requires stable quorum protection |
| Generation fencing | `AdvanceGeneration`, `InstallGeneration` | stale generations cannot commit or serve |
| Object publication | `BuildObjectClosure`, `PrepareObjectFrontier`, `ActivateObjectFrontier` | visible roots name complete current-generation closures |
| txLog reclamation | `PopTxLogThroughPending` | a prefix is removed only behind a protected object frontier |
| Disposable serving | `LoseRam`, `LoseStableMedium`, `HydrateServingImage`, `ServeRead` | the latest read uses a reconstructable, current-generation image |

RAM, NVMe, and RocksDB are disposable serving or staging choices. They are not
independent database truths. Loss clears the affected serving image. Recovery
must hydrate it again from the same object base and stable txLog suffix.

## Evidence chain

```text
TLA+ state machine                    [VERIFIED, finite scopes]
  → allowed transitions and invariants

Rust trace-conformance checker        [CODE-COMPLETE]
  → replays selected emitted events
  → validates model identity and TLA constant assumptions

real-infrastructure evaluation        [EVALUATING by mechanism]
  → latency, throughput, loss, debt, and physical bounds
```

The Rust checker is hand-written. It does not establish a mechanical refinement
mapping from the complete Rust implementation to TLA+. The retained staged
txLog receipt remains useful historical mechanism evidence, but it is bound to
an older model identity. A current-model implementation trace must be captured
before that conformance slice is called verified.

## Checked scopes

R2 ran TLC 2.19 from `tla2tools` 1.7.4 on the GCP R0 runner. The tool jar
SHA-256 is
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The exact model SHA-256 is
`55d5bb137b9e3c37deace42f92b4602b022a7583b0a23a801ef707f40618a3ba`.

| Configuration | Scope | Generated | Distinct | Depth | Result |
|---|---|---:|---:|---:|---|
| `ObjectKVCell.cfg` | 3 nodes, 1 transaction, 2 generations, 1 stable-media failure | 2,484,568 | 99,408 | 23 | `[VERIFIED]` no invariant violation |
| `ObjectKVConcurrency.cfg` | 3 nodes, 2 conflicting transactions, 2 generations | 4,496,463 | 164,668 | 28 | `[VERIFIED]` no invariant violation |

Six negative controls weaken one contract each. Every R2 control produced its
exact named invariant violation, and none exited on a parse, semantic, or
incomplete-successor error.

| Poison | Named invariant | Generated | Distinct | Depth |
|---|---|---:|---:|---:|
| `AckBeforeStableQuorum.cfg` | `RepliesTellTheTruth` | 157 | 71 | 5 |
| `IgnoreGenerationFence.cfg` | `GenerationsAreFenced` | 10,793 | 1,468 | 9 |
| `PopStalePendingFrontier.cfg` | `SafeTxLogPop` | 82,551 | 7,963 | 12 |
| `PublishIncompleteClosure.cfg` | `PublicationsAreComplete` | 13,418 | 1,738 | 9 |
| `ServeStaleImage.cfg` | `ServingReadsAreSafe` | 1,483 | 302 | 6 |
| `SkipConflictValidation.cfg` | `ConflictsAreValidated` | 220,294 | 15,475 | 15 |

Exact evidence:

- machine-readable receipt:
  [`evidence/gcp-r2-2026-08-30.json`](evidence/gcp-r2-2026-08-30.json)
- retained raw model, configs, healthy logs, and poison logs:
  `gs://doss-objectkv-dev-okv-evals/eval-receipts/objectkv-cell-tla-r2-55d5bb1/objectkv-cell-tla-r2-55d5bb1.tar.gz`
- GCS generation: `1788122230581424`
- archive SHA-256:
  `407bbfe489f9bb699a1f33f33031f7c9cdce5697c44524295acd971dba0167c4`

## Run and validate

```bash
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -coverage 1 -config formal/ObjectKVCell.cfg formal/ObjectKVCell.tla

java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC \
  -coverage 1 -config formal/ObjectKVConcurrency.cfg formal/ObjectKVCell.tla
```

For a poison, a nonzero process exit is insufficient. The log must contain the
configured exact `Error: Invariant <name> is violated.` line and must not
contain an incomplete-successor, parse, or semantic error.

## Deliberate boundary

The model abstracts consensus to quorum intersection plus generation fencing.
It does not prove Raft or Paxos, object-store behavior, RocksDB correctness,
wire compatibility, liveness, unbounded scale, SQL semantics, or performance.
Conflict tracking is one coarse domain. The largest checked concurrency scope
contains two transactions. Serving images are version markers, not physical
row or column layouts.

The composition follows Oswald's useful practice of modeling storage, log,
snapshots, garbage collection, and concurrent actors together. objectKV uses a
different acknowledgement boundary: stable-media quorum is on the default
foreground path, while immutable object publication remains asynchronous. See
[Oswald](https://nvartolomei.com/oswald/) and its
[formal model](https://github.com/nvartolomei/oswald/tree/main/p/Oswald).
