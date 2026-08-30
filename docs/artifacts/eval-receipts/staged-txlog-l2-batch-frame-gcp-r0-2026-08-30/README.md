# RFC-0045 L2 batch-frame optimization diagnostic

- Date: 2026-08-30
- Result: `[EVALUATING]`
- Correctness within the five named runs: `[VERIFIED]`
- Source: `2a159c3f14eab9bff3d0a37563fa708181e5eae6`
- Source archive SHA-256: `ef547188a1269d21a784bf46032c21e823e59ef89e4057f7eb75b88187c53681`
- Linux binary SHA-256: `0ae0e9861694897cac0d0f10c3237e47bd65b02048a3c23a5097e031e6c9c4a2`
- Immutable archive: `gs://doss-objectkv-dev-okv-evals/eval-receipts/staged-txlog-l2-batch-frame-2a159c3-r0/staged-txlog-l2-batch-frame-2a159c3-r0.tar.gz`
- Object generation: `1788132665292172`
- Archive SHA-256: `31d19eaf0167c17e9f2eb56269a6011b0bded2b5e3f24be43e8b9187c2f233d6`

## Question

Does one checksummed journal frame per batch plus a binary client-node protocol
move the staged txLog software ceiling without weakening quorum durability?

## Change under test

```text
before
  JSON byte arrays -> one OKVT frame per record -> one batch fsync

after
  binary batch request -> one OKVT v2 frame per batch -> one batch fsync
```

The v2 frame shares log identity, writer epoch, first position, count, and
checksum. Each record retains its request identity and payload length. Recovery
still reads v1 frames, exact retries do not grow the journal, and a torn v2
frame is discarded as one indivisible batch. The local process contract passed
all exact-retry, restart, torn-tail, stale-writer, segment-identity, and bounded
state gates before this machine run.

## Topology and matched workload

```text
64 Poisson producer threads, 256 logical streams
                    |
                    v
bounded 32,768-record active-writer queue
                    |
        close at 256 records or 250 us
                    |
      persistent TCP to three log nodes
                    |
          acknowledge after 2 durable
                    |
        exact digest from all 3 nodes
```

The runner and three log nodes were separate `n2-standard-8` GCE instances in
`us-central1-a`. Each log node used a fresh 375 GB local-NVMe device formatted
as ext4. Every point attempted 131,072 128-byte records. The queue, batch close,
producer, stream, acknowledgement, and digest geometry match the admitted v1
frame diagnostic. The node instances were recreated, so the before/after result
is a topology-matched one-repeat diagnostic, not a paired admission run.

## Curve

| Offered | Ack records/s | Batch mean | Record p50 | Record p95 | Record p99 | Record p99.9 | Queue p99 | Refused |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 40k/s | 36,771 | 35.4 | 1.275 ms | 3.434 ms | 3.864 ms | 5.120 ms | 2.576 ms | 0 |
| 60k/s | 54,246 | 57.8 | 1.439 ms | 3.572 ms | 4.194 ms | 5.635 ms | 2.486 ms | 0 |
| 100k/s | 86,852 | 143.6 | 2.318 ms | 3.995 ms | 4.593 ms | 5.768 ms | 2.492 ms | 0 |
| 150k/s | 107,279 | 255.5 | 114.196 ms | 196.542 ms | 201.981 ms | 202.935 ms | 200.076 ms | 0 |
| 200k/s | 106,921 | 255.0 | 202.512 ms | 279.231 ms | 285.191 ms | 286.513 ms | 283.470 ms | 21,924 |

The knee moved from 40k to 60k offered records/s under v1 frames to 100k to
150k under v2 frames. Saturated throughput moved from approximately 45k to 46k
records/s to approximately 107k records/s, about 2.35x. The 100k point remained
below the knee with no refusal, a 405-record maximum queue, 86,852 acknowledged
records/s, and 4.593 ms record p99. Nominal offered load realizes at about 96.3
percent, so the 100k point cannot produce 100k acknowledgements per second.

## Direct before and after

| Offered | v1 ack/s | v2 ack/s | Throughput ratio | v1 p99 | v2 p99 | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 40k/s | 36,890 | 36,771 | 0.997x | 5.434 ms | 3.864 ms | 0.711x |
| 60k/s | 45,451 | 54,246 | 1.194x | 539.902 ms | 4.194 ms | 0.008x |
| 100k/s | 45,638 | 86,852 | 1.903x | 704.950 ms | 4.593 ms | 0.007x |

The large latency ratios at 60k and 100k primarily reflect moving the
saturation knee, not a 100x reduction in individual fsync latency.

## Where the remaining time goes

At 100k offered records/s:

| Stage | p50 | p99 |
|---|---:|---:|
| Binary decode | 0.007 ms | 0.030 ms |
| Batch validation | 0.029 ms | 0.064 ms |
| Journal encoding | 0.143 ms | 0.268 ms |
| Journal write | 0.025 ms | 0.046 ms |
| Journal sync | 0.416 ms | 1.773 ms |
| Quorum request | 0.915 ms | 2.212 ms |
| Active-writer queue dwell | not additive at p50 | 2.492 ms |
| End-to-end record acknowledgement | 2.318 ms | 4.593 ms |

The framing work is no longer the ceiling. Journal sync and quorum service own
the node-side tail, while active-writer queue dwell roughly doubles the 100k
end-to-end p99. A 1 ms durable-ack p99 is below the measured 1.773 ms node-sync
p99 on this media and cannot be reached by another codec optimization alone.

## Byte result

At 100k offered records/s, each node retained 21,583,456 journal-frame bytes
for 16,777,216 payload bytes, or 1.286x before replication. The v1 journal used
approximately 2.0x. The binary protocol sent 70,822,704 bytes across the three
nodes, or 1.407x payload per node. A full 256-record, 128-byte v2 journal frame
is 42,080 bytes instead of 65,536 bytes under v1 framing.

## Correctness and claim boundary

Across the five points:

- 655,360 arrivals were attempted;
- 633,436 accepted records were acknowledged;
- all 15 final node digests matched the exact accepted history;
- anomaly count and foreground object operations were zero;
- queue refusal occurred only at the bounded 200k saturation point.

`[CODE-COMPLETE]` The v2 journal frame, binary request/response protocol, and
stage timing fields are implemented at the recorded source.

`[VERIFIED]` The unchanged local process oracle accepts the v2 format and still
rejects early acknowledgement, stale writers, torn tails, and divergent bytes.
The five real-infra runs preserved every accepted record and exact final state.

`[EVALUATING]` Row 4 remains open. The result has one repeat, recreated rather
than paired machines, no new `pd-ssd` control, no independent OTel or CPU
profile, no failure during load, and no transaction resolver in the path. It
misses the frozen simultaneous 1 ms p99 and 100k records/s gate.

## Decision

Keep the v2 framing. It removes the measured software ceiling and reduces
per-node journal amplification from approximately 2.0x to 1.286x without
changing acknowledged durability. Do not spend the next slice on another
encoding pass. The next txLog work is direct active-writer scheduling and
measured fsync policy, followed by the repeated failure curve. In parallel, the
program can begin one transaction-path integration because the staged log now
has more than 2x the prior throughput headroom.
