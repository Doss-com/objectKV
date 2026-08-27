# G4.7 authenticated object frontier

Status: `[EVALUATING]` on one local machine with dirty source and debug
processes. The protocol and process harness are `[CODE-COMPLETE]`.

## Question

Can objectKV physically discard committed recovery records through object
frontier `O` without creating a crash window in which neither the txLog nor an
authoritative immutable object closure can reconstruct those commits?

## Result

Yes within the frozen six-process local contract. The candidate retained one
exact manifest as pending publication state, validated every manifest, index,
and row-data object, quorum-committed a physical recovery-stream pop, collected
three data-voter attestations, and promoted the pending frontier. It then
recovered all 16 committed values from objects after data-leader failover,
publication-leader failover, and restart of the killed data voter.

This is not a production durability or performance result. It does not purge
OpenRaft's internal journal, run on independent machines, use GCS, exercise key
rotation, or establish acceptable commit throughput.

## Protocol

```text
row-object closure through O
            |
            v
publication quorum: pending(M, O)
            |
            v
controller validates every named byte
            |
            v
data quorum: pop retained txLog through O
            |
            v
data-voter signatures over (M, O, applied position)
            |
            v
publication quorum: pending -> active
```

The pending manifest is a garbage-collection root before the physical pop.
There is no cancel transition in Cell v0. A controller crash can delay the
handshake, but it cannot remove pending protection after the txLog has been
popped.

## Frozen execution shape

```text
publication authority: 3 OpenRaft processes
data authority:        3 OpenRaft processes
transactions:         16
value bytes:          128
object closure:       3,299 bytes, 3 named objects
seeds:                4701, 4702, 4703
suite hash:           40b1d296f09c7694067b670d266e995765567ecc6217c7fde22ecde2d60cb8ad
```

All executions used the local filesystem object backend and local stable
journals. The result is marked inconclusive by the runner because the candidate
revision is dirty and is therefore not comparable as an admitted benchmark.

## Candidate receipt

Run `f314d6e1-386f-4c1d-b46d-9c283a63a144` passed every hard gate:

| Measurement | Result |
| --- | ---: |
| Protocol p99 across seeds | 160.331 ms |
| Protocol median | 146.914 ms |
| Physical pop median | 20.519 ms |
| Wall budget | 31.386 s of 90 s |
| Popped recovery records | 16 |
| Persisted retention floor | 18 |
| Remaining recovery records | 0 |
| Correctness anomalies | 0 |

The protocol duration is off the transaction commit path. Object validation,
safe pop, certification, and activation happen after immutable objectification.
This keeps object latency out of foreground commit acknowledgement while
bounding recovery-state retention.

## Adversarial controls

| Control | Receipt | Observed result |
| --- | --- | --- |
| No pending publication frontier | `e3467af2` | Pop rejected, floor remained 0, all 16 records remained |
| Manifest claims forged coverage | `7bbf3d7e` | Full object validation rejected before pop, all 16 records remained |
| One-signer activation certificate | `5bfbd653` | Physical pop and object recovery succeeded, activation failed, pending frontier remained protected |

Every control reproduced its semantic digest across all three seeds. The
receipt checksums are stored in
`docs/artifacts/eval-receipts/object-frontier-g4.7-v1/SHA256SUMS`.

## Boundaries established

`[CODE-COMPLETE]`:

1. publication state retains separate pending and active object frontiers;
2. the controller validates the complete immutable closure before requesting
   mutation of `O`;
3. the data state machine physically removes recovery records through `O` and
   persists the floor atomically;
4. data voters attest only an exact locally applied frontier and log position;
5. publication activation requires a distinct valid quorum from the active
   generation membership;
6. exact retries, stale cursors, snapshot replay, leader replacement, and voter
   restart fail or recover according to RFC-0030.

`[EVALUATING]`:

1. release and clean-source reproducibility;
2. independent-machine stable-media durability;
3. remote object-store latency and failure behavior;
4. OpenRaft internal log purge and snapshot installation;
5. sustained safe-pop convergence while commits and objectification continue;
6. production signer custody and rotation.

## Decision

Keep the authenticated frontier protocol. It closes the application recovery
stream's correctness gap without adding object I/O to the commit path. It does
not admit the native transaction authority because G4.6 still measured only
about 30 sequential commits per second and missed its fixed wall budget.

The next falsifier is G4.8: compare bounded transaction batching with the
unchanged one-entry-per-transaction path under the same three-process stable
journal durability contract.
