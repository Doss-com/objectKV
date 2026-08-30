# RFC-0045 L2a three-machine txLog preflight

- Date: 2026-08-30
- Result: `[VERIFIED]` for the bounded L2a mechanism
- Program row: 4 remains `[EVALUATING]`
- Corrected source: `d3db192e02a197903e6e05e5278b56c5806ed8df`
- Rejected source: `bcafe7b519f6d37561fc5b137d5838d411614a96`
- Corrected Linux binary SHA-256: `a55a41712487003b7b953e6d0395d62b5f02c3714cdf2c61bf3070d268e0668f`
- Source archive SHA-256: `a0b33d24970d8774c38ec821190dbd90aaa1dbe509e8dcee4c3ba72f3d53fd9f`
- Immutable archive: `gs://doss-objectkv-dev-okv-evals/eval-receipts/staged-txlog-l2a-d3db192-r1/staged-txlog-l2a-d3db192-r1.tar.gz`
- Object generation: `1788127487242891`
- Archive SHA-256: `3816dcb20477561e59b6661b20623226ca89e61193afb0c05c4e74292901b3fe`

## Question

Can one client append bounded batches to three independent same-zone local-NVMe
log nodes, acknowledge after a durable quorum, and retain enough headroom to
justify the full open-loop matched-control curve?

## Physical topology

```text
GCE runner, n2-standard-8, 10.77.0.2
  |
  | persistent TCP, one request per node and batch
  +--> node 0, n2-standard-8, 10.77.0.6, 375 GB local NVMe, ext4
  +--> node 1, n2-standard-8, 10.77.0.7, 375 GB local NVMe, ext4
  +--> node 2, n2-standard-8, 10.77.0.8, 375 GB local NVMe, ext4

acknowledgement = second durable node response
final check     = exact full history on all three nodes
object path     = zero operations
```

All four machines ran in `us-central1-a`. Each log node had a distinct GCE
instance, process, local NVMe device, ext4 filesystem, root, and TCP endpoint.
This is independent machine and media evidence, but not an availability-zone
failure test.

## Frozen workload

| Parameter | Value |
|---|---:|
| logical records per run | 65,536 |
| logical payload per record | 128 bytes |
| records per physical batch | 256 |
| batches per run | 256 |
| node requests per run | 768 |
| writer epoch | 7 |
| object operations | 0 |

Every node validates a complete consecutive batch before physical mutation,
writes all new record frames, performs one shared `sync_all`, and advances its
in-memory position only after that sync succeeds. Exact retries do not grow the
journal.

## Results

The first physical run was correct but performance-invalid. Its batch latency
was almost perfectly flat at 47 ms. The response writer sent a four-byte frame
length and JSON body as separate writes while server-side Nagle coalescing was
enabled. The client consumed the length and waited for the body, creating a
delayed-ACK plateau. Commit `d3db192` enabled `TCP_NODELAY` on the accepted
server connection. No transaction, batch, quorum, payload, node, or media
parameter changed.

| Run | Source | Records/s | Batch p50 | Batch p95 | Batch p99 | Exact nodes | Anomalies |
|---|---|---:|---:|---:|---:|---:|---:|
| rejected r0 | `bcafe7b` | 5,321.189 | 47.219 ms | 47.271 ms | 47.336 ms | 3/3 | 0 |
| corrected r1 | `d3db192` | 48,077.570 | 4.343 ms | 4.548 ms | 4.730 ms | 3/3 | 0 |
| corrected r2 | `d3db192` | 49,159.470 | 4.377 ms | 4.535 ms | 4.682 ms | 3/3 | 0 |
| corrected r3 | `d3db192` | 49,028.258 | 4.348 ms | 4.498 ms | 4.570 ms | 3/3 | 0 |

Across all 768 corrected quorum batch acknowledgements:

| Percentile | Latency |
|---|---:|
| p50 | 4.357 ms |
| p95 | 4.535 ms |
| p99 | 4.716 ms |
| p99.9 | 5.343 ms |
| maximum | 5.591 ms |

The corrected median throughput was 49,028.258 records/s. Across three clean
runs, 196,608 of 196,608 records were acknowledged, all nine final node-state
checks were exact, and anomaly count was zero. Each node stored 16,777,300
physical bytes for 8,388,608 logical payload bytes. The current record frame is
therefore approximately 2.0x the logical payload before replica multiplication.
That frame amplification is now an explicit optimization target.

## Claim boundary

`[VERIFIED]` The batched staged txLog can durably synchronize one ordered stream
across three independent same-zone NVMe machines with persistent connections,
exact full-state agreement, zero normal-path object operations, a combined
4.716 ms batch p99, and about 49 thousand 128-byte records/s at 256 records per
sync.

`[EVALUATING]` This is not the row-4 admission result. The preflight has one
client, closed-loop batches, no matched remote-block or incumbent control, no
open-loop queue-depth sweep, no concurrent application writers, no injected
node loss, no cross-zone topology, no transaction resolver, and no OTel
correlation. Per-record latency also includes an unspecified batch-formation
dwell in a real commit proxy.

## Decision

Proceed to the frozen L2 open-loop curve. Preserve the response framing lesson,
add the matched durable control, report queue depth and batching dwell, and
measure the same three-machine path through saturation. Do not integrate this
log into the transaction plane until that comparison clears row 4.
