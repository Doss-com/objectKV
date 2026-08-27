# G4.8 bounded concurrent commit submission

Status: `[EVALUATING]` on one local machine with dirty source. The mechanism,
I/O observations, process harness, and frozen suite are `[CODE-COMPLETE]`.

## Question

Can objectKV obtain an economical quorum-durable commit curve without changing
transaction bytes or retry semantics, by submitting independent transactions
concurrently and allowing OpenRaft plus the node journal to group stable-log
appends?

## Answer

No at the frozen G4.8 gate. Bounded concurrency preserved the strict-
serializable recovery contract and improved median throughput from 38.772 to
153.708 transactions per second, a 3.964x gain. It missed the 200 transaction
per second absolute gate, the 4x paired-improvement gate, and the 250 ms p99
ceiling in one of three seeds.

The durable I/O trace explains the limit. Followers grouped roughly 11 entries
per stable append, but the leader still issued 512 appends for 512 transaction
entries. The mechanism pipelines leader synchronization; it does not perform
leader-side group commit.

## Frozen execution shape

```text
client requests:       512 per seed
live keys:             256
value bytes:           128
candidate window:       32
control window:          1
authority:               3 OpenRaft processes
durability:              sync_all on each stable journal append
build:                   release
seeds:                   4801, 4802, 4803
suite hash:              855f49c190c8710454ef993ed709437b30082596ea99a243e652569cf9e91e77
```

Both normal subjects use the same executable, transaction workload, stable
journal, consensus path, retry checks, leader failover, killed-voter restart,
and fresh-controller replay. Only the maximum number of requests in flight
changes.

## Results

| Measurement | Bounded concurrency | Sequential control | Frozen candidate gate |
| --- | ---: | ---: | ---: |
| Median durable throughput | 153.708 tx/s | 38.772 tx/s | at least 200 tx/s |
| Paired throughput ratio | 3.964x | 1.000x | at least 4x |
| Maximum commit p99 | 264.887 ms | 34.051 ms | at most 250 ms |
| Median voter entries per append | 12.190 | 1.000 | at least 4 |
| Suite wall time | 16.080 s | 55.623 s | at most 120 s |
| Correctness anomalies | 0 | 0 | 0 |

The candidate run `a27fd93c` was discarded. The sequential control run
`8e964ea9` was inconclusive only because the source revision was dirty; all of
its correctness and budget gates passed.

One separately retained release trace for seed 4801 recorded this per-voter
shape:

```text
leader 201:    512 entries / 512 append calls = 1.000 entries per append
follower 202:  512 entries /  48 append calls = 10.667 entries per append
follower 203:  512 entries /  48 append calls = 10.667 entries per append
```

That trace reached 136.012 tx/s and 261.873 ms p99. It is diagnostic evidence
for the mechanism, not a replacement for the three-seed suite result.

## Durability poison

The early-ack subject returned before quorum durability, reached a median
3,400.389 apparent transactions per second, then lost the acknowledged
transaction after the isolated leader was killed and the remaining quorum
recovered. Run `69fa1b90` was discarded with a detected correctness anomaly.

This control bounds the interpretation of the performance result. Removing
quorum durability can make the number look more than 22x faster than the
candidate, but it no longer implements the objectKV contract.

## Decision

Discard bounded concurrent submission as the final group-commit mechanism.
Keep the concurrency window, durable I/O counters, process recovery checks,
and poison because they are useful components of the next experiment.

G4.9 should test one explicit commit-proxy batch entry. The proxy may collect a
bounded set of independently fingerprinted transaction requests, but the state
machine must still assign one ordered result and commit version per transaction,
retain exact retry outcomes, reject conflicts deterministically, and recover
the entire batch from quorum state. The control remains G4.8's one-entry-per-
transaction path at the same durability.

## What this does not conclude

- OpenRaft cannot support an economical objectKV transaction authority.
- A multi-transaction entry will pass the next curve.
- The local filesystem curve predicts independent-machine stable-media latency.
- The current 200 tx/s gate is a production SLO.
- Object storage belongs on the foreground acknowledgement path.

The negative result is narrower: concurrent one-entry submissions cannot be
the last commit-path design because the leader still synchronizes each entry.

## Receipts

- Candidate: `docs/artifacts/eval-receipts/commit-group-g4.8-v1/candidate.json`
- Sequential control: `docs/artifacts/eval-receipts/commit-group-g4.8-v1/sequential-control.json`
- Early-ack poison: `docs/artifacts/eval-receipts/commit-group-g4.8-v1/early-ack-poison.json`
- Per-voter trace: `docs/artifacts/eval-receipts/commit-group-g4.8-v1/candidate-trace-4801.json`
- Checksums: `docs/artifacts/eval-receipts/commit-group-g4.8-v1/SHA256SUMS`
