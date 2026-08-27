# G4.6 split transaction-authority state

Status: `[EVALUATING]` local dirty-source diagnostic, 2026-08-26.

## Question

Can objectKV keep complete transaction-authority state bounded by active work,
rather than lifetime commits, when serving values, OCC history, transaction
retry records, and recovery commands have separate retention frontiers?

## Answer

The state-shape answer is yes. The complete projected snapshot grew 1.0029x
from 256 to 4,096 commits with aligned read, retry, and object frontiers. The
object-frontier-only control grew 9.1694x. The incomplete serving-only poison
also appeared flat, but the complete-accounting oracle rejected it with nine
anomalies.

The overall eval did not pass. The candidate took 545.726 seconds against a
frozen 180-second diagnostic budget. This is a `[EVALUATING]` architectural
indicator, not a verified result or an admitted transaction implementation.

## Implemented state split

```text
StateMachineData
|
+-- TransactionAuthority
|   +-- TransactionServingState       latest ordered values
|   `-- TransactionResolverState      current version, R, OCC history
|
+-- TransactionRetryState             Q(client), outcomes, fingerprints
+-- TransactionFrontierState          latest sequence and exact response
`-- retained_transactions             recovery commands, projected O only
```

`transaction-frontier-advance-v1` atomically advances `R` and one or more
`Q(client)` values through Raft. Conflicts at or below `R` and retry records at
or below `Q(client)` are reclaimed. A transaction below `R` is rejected before
mutation. A retry at or below `Q(client)` returns `RetryIdentityExpired` and
never re-applies. The state retains only the latest frontier command response,
so frontier advancement does not create another lifetime-sized outcome table.

The implementation still serializes all owners in one OpenRaft state-machine
snapshot. This is a protocol and retention split, not yet resolver or serving
process separation.

## Frozen experiment

- Suite: `transaction-authority-split-frontiers-v1`
- Evaluator suite hash: `483b0bb149a728829b1edfd1b23e1f80bf3e3e7abc0c39c3e6141fbd074dd4aa`
- Profile hash: `d2f3008217623bf0c5ac02189b7f587bf01bb77a4ddbf3d9cc1faf3fc7c0188c`
- Revision: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`
- Backend: `data-openraft-local-process`
- Build: debug
- Seeds: 1103, 2207, 3301
- Checkpoints: 256, 1,024, and 4,096 commits
- Process boundary: three fresh OpenRaft data processes per seed plus an exact
  fresh-controller replay of seed 1103
- Live state: 256 rotating keys with 128-byte values
- Telemetry: disabled for this dirty local diagnostic

## Results

| Subject | 256 commits | 1,024 commits | 4,096 commits | Maximum growth | Verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| Aligned `R`, `Q`, projected `O` | 130,671 B | 130,807 B | 131,047 B | 1.0029x | discard on time budget |
| Projected `O` only | 280,651 B | 734,857 B | 2,573,392 B | 9.1694x | discard on boundedness and time |
| Serving-only accounting poison | omitted complete state | omitted complete state | omitted complete state | 1.0028x | discard, nine anomalies |

The candidate samples were 1.002877x, 1.002893x, and 1.002809x. The control
samples were 9.169367x, 9.162406x, and 9.166018x. Fresh-controller replay was
byte-structurally exact for every subject.

The candidate's actual unprojected snapshot still grew from 328,445 to
3,310,992 bytes because all recovery commands remain retained and readable.
The retained command field grew from 197,783 to 3,179,955 bytes. This is
intentional: G4.6 does not mutate `O` and does not claim safe pop.

At the final aligned checkpoint:

```text
complete projected snapshot                         131,047 B
|
+-- serving plus empty resolver owners              129,372 B
+-- transaction outcomes, encoded empty maps              4 B
+-- transaction fingerprints, encoded empty maps          4 B
`-- remaining snapshot metadata and frontier state     1,667 B

retained recovery commands before projected O      3,179,955 B
```

The object-only control retained the G4.5 lifetime histories at the final
checkpoint:

```text
serving plus unbounded resolver history               992,491 B
transaction retry outcomes                          1,055,341 B
transaction retry fingerprints                       524,432 B
recovery commands after projected O                         0 B
```

## Performance readout

The candidate executed 16,384 measured transactions across three seeds and one
replay in 545.726 seconds, an effective controller rate of about 30.0 committed
transactions per second including cluster startup, checkpoints, and frontier
probes. This is not a product throughput benchmark, but it is too slow to
ignore. The current client submits one transaction at a time and the bootstrap
Raft path synchronizes each ordered entry. objectKV still needs a commit-proxy
batching or equivalent group-commit mechanism before the transaction hot path
can approach its latency and throughput targets.

The state split fixes retention economics. It does not fix commit economics.

## Decision

Keep the split-frontier architecture. Do not revert to the monolithic state
shape. Do not mark G4.6 verified because every run is dirty-source, local-host,
debug-build evidence and the candidate failed its execution budget.

The next correctness slice is an authenticated object-frontier certificate and
physical recovery-stream pop. The next performance slice is a same-semantics
commit batching control under a separately frozen latency and throughput suite.
Neither should weaken the exact retry, stale-read rejection, or complete-state
accounting proven here.

## Remaining bounds

- `Q(client)` bounds commits per active client, not the number of historical
  client IDs. Client-session leases and floor-map reclamation need their own
  cardinality gate.
- `R` is advanced by the diagnostic controller. A production read-version
  lease authority does not exist.
- Serving values are fixed-cardinality in this curve and still live inside the
  state-machine snapshot. Disposable range ownership is not implemented.
- `O` is non-mutating. No object closure, generation proof, physical Raft-log
  purge, or crash-during-pop result exists.
- There is no independent machine, GCS, OTel, release-build, concurrency, or
  sustained-ingest receipt.

## Receipts

- Candidate: `ed9c894b-531f-40aa-9f5c-cd1ee5f2331d`
- Object-only control: `3e68f55e-eab7-47be-948a-6b88a27ee248`
- Serving-only poison: `b8e1a531-834b-4887-aedb-a3ad03318841`
- Immutable files: `docs/artifacts/eval-receipts/authority-state-g4.6-v1/`
