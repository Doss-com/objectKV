# G4.10 commit-proxy closure and compact-wire readout

Status: `[CODE-COMPLETE]` implementation, `[EVALUATING]` dirty-source local
receipts.

Date: 2026-08-26.

## Question and call

Can independent client transactions enter a bounded commit proxy, share a
quorum-durable Raft entry, and retain exact per-request transaction meaning
without losing the latency, byte, overload, or recovery properties that G4.9
did not exercise?

The local answer is yes at a 32-item envelope, but not yet as a production
claim. The original 16-item, 64-caller configuration is discarded because it
missed the frozen p99 ceiling. A separately frozen 32-item configuration clears
the local absolute and same-durability gates and advances to G4.10b.

## Implemented pipeline

```text
independent client tasks
          ↓
bounded FIFO try-send admission
          ↓
close on items │ bytes │ delay │ sender shutdown
          ↓
one ordered transaction-batch entry
          ↓
three-process OpenRaft quorum + synchronized journals
          ↓
response count and identity validation
          ↓
independent per-request outcomes
```

The client API submits one transaction and receives one result. Batch identity
does not become application transaction identity. Queue overflow and oversized
requests fail before replication. Accepted request order is FIFO at this commit
proxy. Result count or identity drift fails the complete attempt rather than
delivering a result to the wrong caller.

## Why G4.9 was insufficient

G4.9 began with an already-formed 16-transaction batch. It proved the authority
entry, ordered versionstamp, per-item retry, conflict, pagination, failover, and
restart semantics. It did not measure client queueing or prove how a normal
independent request becomes part of that batch.

G4.10 adds the missing commit-proxy boundary:

- bounded FIFO admission;
- exact encoded-entry byte accounting;
- item, byte, delay, credential, and sender-shutdown closure;
- explicit queue-full and oversized-item rejection;
- per-request outcome demultiplexing;
- policy counters and latency measurement from client admission.

## Result matrix

| Subject | Median throughput | Maximum p99 | Batch | Decision |
|---|---:|---:|---:|---|
| G4.10a, 16 items and 64 callers | 581.791 tx/s | 131.488 ms | 16 | discard |
| G4.10a admission knee, 16 items and 32 callers | 595.440 tx/s | 63.398 ms | 16 | explanatory control |
| G4.10a.1, 32 items and 64 callers | 1,157.369 tx/s | 76.101 ms | 32 | retain for G4.10b |
| Same-durability one-entry control | 182.093 tx/s | 492.355 ms | 1 | baseline |

The retained candidate is 6.356x the one-entry control. Every admitted identity
has one outcome, shared versions use contiguous batch orders, retained-stream
replay reconstructs the authority state, exact retry does not mutate twice, and
leader failover plus killed-voter restart preserve the result.

## Policy controls

Sparse arrival closes on the 2 ms batch deadline instead of waiting for fill.
Each batch contains one request, and maximum client p99 is 30.961 ms including
process and quorum overhead.

The overload control uses a 32-request queue. Of 512 attempted requests, 32 are
admitted and resolved, while 480 receive explicit pre-replication backpressure.
No admitted identity is lost or duplicated.

The oversized-item poison is rejected before admission and mutation. This is an
expected negative outcome, so its receipt verdict is `discard` while the scoped
`oversized_item_poison_detected` gate passes.

## Compact-wire correction

The first byte control found that JSON integer arrays were the bottleneck. One
8 KiB logical value became an 89,097 byte `OKVB1` entry, leaving room for only
one transaction under a 128 KiB cap.

RFC-0034 replaces new writes with backward-readable v2 encodings:

```text
opaque bytes
    ↓
OKVT2 base64 transaction fields
    ↓
OKVQ2 base64 client envelope
    ↓
OKVB2 base64 batch payloads
```

Decoders retain v1 support. Retry fingerprints are semantic, so the exact
transaction committed as v1 can retry through v2 and recover the original
outcome. The corrected byte control fits eight 8 KiB-value transactions in a
119,731 byte entry without crossing 128 KiB.

## What this establishes

- `[CODE-COMPLETE]` Independent requests can use one bounded batcher without
  collapsing application transaction identity.
- `[CODE-COMPLETE]` Queue, item, byte, delay, and oversized bounds fail closed.
- `[CODE-COMPLETE]` New bootstrap wire avoids integer-array amplification while
  remaining backward readable.
- `[EVALUATING]` The local 32-item shape passes its frozen correctness,
  throughput, p99, batching, and paired-control gates.

It does not establish an adaptive production policy, multi-tenant fairness,
multiple commit proxies, independent-media durability, remote-zone latency,
clean-source reproducibility, or OTel-backed causality.

## Next falsifier

G4.10b must preserve the exact 32-item admission path while introducing:

1. controlled point and range conflict rates;
2. a concurrent authenticated object-frontier advance;
3. bounded retry and retained-stream state through objectification;
4. write-rate and `C - O` convergence curves;
5. the same one-entry durability control.

The candidate is discarded if conflict resolution breaks deterministic batch
order, concurrent objectification pops a required outcome, p99 crosses the
frozen ceiling, or the paired advantage disappears. Only after this local
composition passes should one clean revision move to three independent stable
media hosts.

## Receipts

- discarded G4.10a:
  `docs/artifacts/eval-receipts/commit-proxy-g4.10a-v1/`;
- retained G4.10a.1 local candidate:
  `docs/artifacts/eval-receipts/commit-proxy-batch32-g4.10a.1-v1/`;
- suites: `evals/suites/commit-proxy.toml` and
  `evals/suites/commit-proxy-batch32.toml`.
