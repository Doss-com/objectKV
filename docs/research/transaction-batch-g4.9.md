# G4.9 explicit transaction batch entry

Status: `[EVALUATING]` on one local machine with dirty source. The wire format,
versionstamp semantics, exact retry path, recovery cursor, process harness, and
frozen suite are `[CODE-COMPLETE]`.

## Question

Can objectKV remove the measured leader one-sync-per-transaction bottleneck by
placing several independently retryable transactions in one quorum-durable
Raft entry, without weakening snapshot, conflict, replay, or recovery semantics?

## Answer

Yes at the frozen local G4.9 mechanism gate. A 16-transaction batch reached
559.511 median durable transactions per second and 34.016 ms maximum p99. The
same executable and durability path with one Raft entry per transaction reached
151.944 transactions per second and 262.174 ms maximum p99. The 3.682x paired
gain cleared the frozen 2.5x requirement.

Every candidate seed produced 16 logical transactions per leader stable append,
zero correctness anomalies, exact paginated recovery, exact individual and
whole-batch retries, exact failover, and exact killed-voter restart.

This is not a verified production curve. The source revision is dirty, all
three voters share one host, the stable journals share one local filesystem,
and OTel export is disabled.

## Version model

```text
one Raft batch entry
       |
       +-- transaction 0 -> (commit_version, batch_order 0)
       +-- transaction 1 -> (commit_version, batch_order 1)
       `-- transaction N -> (commit_version, batch_order N)
```

The pair is the unique ordered transaction versionstamp. The scalar commit
version remains the database snapshot boundary. A read sees the complete batch
or none of it. Earlier accepted items may conflict later items in the same
batch.

This follows FoundationDB's documented transaction-batch shape: one write
version applies to a commit-proxy batch and the versionstamp carries an
additional batch-order component.

## Frozen execution shape

```text
transactions per seed:          512
candidate batch size:             16
candidate Raft entries:            32
control transactions in flight:    32
live keys:                        256
value bytes:                      128
authority:          3 OpenRaft processes
durability:         sync_all stable journals
build:              release
seeds:              4901, 4902, 4903
suite hash:         c34b47eb5965dedf95d529c9b72a6fe8af4c73adc58bdec45060b6816862f585
```

## Results

| Measurement | Batch candidate | One-entry control | Frozen gate |
| --- | ---: | ---: | ---: |
| Median durable throughput | 559.511 tx/s | 151.944 tx/s | at least 400 tx/s |
| Paired throughput ratio | 3.682x | 1.000x | at least 2.5x |
| Maximum commit p99 | 34.016 ms | 262.174 ms | at most 100 ms |
| Logical transactions per leader append | 16 | 1 | at least 8 |
| Candidate suite wall time | 10.811 s | 15.918 s | at most 120 s |
| Correctness anomalies | 0 | 0 | 0 |

Candidate run `0f50aeae` and control `0a891a4a` both passed their semantic,
release-build, topology, replay, and wall-budget gates. Their final verdict is
inconclusive only because the revision is dirty and therefore not a comparable
benchmark artifact.

## Correctness coverage

`[CODE-COMPLETE]` G4.9 covers:

1. one `OKVB1` entry with at most 32 independently encoded client commands;
2. one shared commit version plus contiguous 16-bit batch order;
3. deterministic in-batch conflict resolution;
4. exact per-item fingerprints and durable outcomes;
5. exact individual retry through the ordinary transaction API;
6. exact whole-batch replay without duplicate mutation;
7. a retained-stream cursor over `(commit_version, batch_order)` so a page may
   end inside a batch;
8. scalar object frontiers that cover complete commit versions only;
9. leader failover and killed-voter restart;
10. per-voter stable-log observations.

## Adversarial controls

| Control | Receipt | Observed result |
| --- | --- | --- |
| Duplicate identity in one batch | `e12401cb` | Entire batch rejected before mutation or retry-state insertion |
| Acknowledge before quorum | `13ab1d24` | Median apparent 13,179.572 tx/s, then all acknowledged outcomes absent after recovered-quorum election |

Both controls reproduced their semantic digest across seeds 4901, 4902, and
4903 and were discarded by the suite.

## Decision

Keep the explicit transaction-batch entry as the Cell v0 commit-proxy primitive.
Do not yet admit the native transaction authority. The next stress gate must
measure batch delay, byte bounds, conflicts, overload, fairness, and safe object
frontier convergence under sustained writes. A clean revision and independent
stable-media hosts are still required before any verified latency, throughput,
or durability claim.

## Remaining bounds

- batch size is fixed, not delay or byte adaptive;
- one commit proxy submits one batch at a time in the candidate;
- control and candidate share one host and filesystem cache;
- state-machine apply remains monolithic and range-unpartitioned;
- internal OpenRaft journal snapshot and purge remain open;
- client-floor cardinality remains unbounded;
- objectification does not yet run concurrently with this admitted write rate;
- no GCS, independent-machine, OTel, overload, or long-duration receipt exists.

## Receipts

- Candidate: `docs/artifacts/eval-receipts/transaction-batch-g4.9-v1/candidate.json`
- One-entry control: `docs/artifacts/eval-receipts/transaction-batch-g4.9-v1/one-entry-control.json`
- Duplicate control: `docs/artifacts/eval-receipts/transaction-batch-g4.9-v1/duplicate-identity-control.json`
- Early-ack poison: `docs/artifacts/eval-receipts/transaction-batch-g4.9-v1/early-ack-poison.json`
- Checksums: `docs/artifacts/eval-receipts/transaction-batch-g4.9-v1/SHA256SUMS`
