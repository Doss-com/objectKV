# BtrLog implications for objectKV

Status: `[EVALUATING]` primary-source architecture review. BtrLog's published
results are prior-art evidence, not objectKV performance evidence.

Primary source: Maximilian Kuschewski et al., "BtrLog: Low-Latency Logging for
Cloud Database Systems," PVLDB 19(10), 2026, pages 2894-2907,
[arXiv:2606.27051](https://arxiv.org/pdf/2606.27051), DOI
[10.14778/3828612.3828640](https://doi.org/10.14778/3828612.3828640). The read
PDF has SHA-256 `d45936ecd478bac70c23d8f6c76ed58b5eeeb250dfd9ff7ad10ee25dab057e24`.
The [BtrLog reproducibility package](https://github.com/maxi-k/btrlog) was
inspected at commit `e4140b06e6fcfed955ffec4bb1011eebf61e6a1e`.

## Clarity

Question: Is BtrLog worth studying for the objectKV architecture?

Punchline: Yes, it is the closest public validation yet of `okv-log` as a
reusable RAM plus replicated-NVMe tail that publishes large immutable segments
to object storage, but it validates a single-writer log service rather than the
ordered transactional KV, serving, or HTAP layers.

Counter: Its reported latency comes from three AWS metal log nodes, one writer
per log, a custom Rust and `io_uring` runtime, UDP, instance-local NVMe, and a
failure-free prototype path; those conditions do not transfer automatically to
objectKV's multi-key transaction and read-serving paths.

Next: add a BtrLog-shaped `okv-log` evaluation slice after T27 that measures
one-round-trip quorum append, SSD-off RAM mode, segment publication, fencing,
tail repair, and recovery against remote block storage and direct object WAL.

## What the paper builds

```text
DBMS client, one writer per log
    -> append record with LSN, commit watermark, and segment offset
    -> send concurrently to every log node
    -> acknowledge after one write quorum fsyncs local NVMe

log nodes
    -> bounded in-memory segment
    -> out-of-place local NVMe WAL write
    -> deterministic full segment
    -> asynchronous conditional PUT

object storage
    -> immutable large segments
    -> cold scan and recovery source

metadata store
    -> conditional-write ownership token
    -> writer fencing and failover metadata
```

The API is `append`, `sync`, `scan`, and `read`. A client assigns monotonic
LSNs because each log has one writer. A common three-node deployment commits
after two SSD acknowledgements. Full segments, evaluated at 16 MiB, are
published asynchronously to S3. Hot unflushed data is read from log-node RAM;
cold data is streamed from object storage.

This is not an object-only WAL. Object storage is absent from the foreground
commit path. Local NVMe protects the unflushed tail against process and power
loss, while quorum replication protects node loss and latency variance.

## Observed results

The paper reports the following on AWS c6id.metal log nodes and a c6in.metal
client:

| Measurement | BtrLog result | Comparison |
| --- | ---: | --- |
| Best 128-byte append | 70 us p50, 79 us p99 | 318 us best p50 on EBS io2; 262 us on optimized BookKeeper |
| Half load | 111 us p50 at about 500k appends/s | BtrLog remained SSD-I/O bound |
| Full load | 188 us p50 at about 1M appends/s | EBS io2 reached 503 us p50 and 651 us p99 |
| One node killed at 400k appends/s | 95 -> 115 us p50; 192 -> 221 us p99 | quorum remained available |
| LeanStore YCSB-A | 1.25x EBS io2; 2x BookKeeper; 3x EBS gp3 | WAL backend replacement only |
| RAM mode | about 50 percent lower median append latency | local SSD durability disabled |

The paper's cloud baseline also reports network plus local-SSD latency versus
remote block storage of 76 versus 311 us on AWS, 71 versus 453 us on GCP, and
301 versus 776 us on Azure. These are BtrLog measurements, not objectKV
receipts.

The implementation bounds overload by limiting queues and dropping excess
requests. At peak it served 2,186 active logs and used 71 GiB of RAM. Each log
consumed 32 MiB on average, so inactive-log eviction remains a production
requirement rather than a solved detail.

## Mechanisms to carry into objectKV review

### D1. Keep single-writer ordering below multi-writer APIs

BtrLog does not make one log multi-writer. It assigns one writer to each log
stream, then scales through many independent streams. This fits `okv-log` and
per-range or per-transaction-group WAL streams. A multi-writer `okv-fabric`
still needs transaction ordering and conflict resolution above those streams.

Optimization: one client-to-quorum network round trip per append.

Tradeoff: fencing, reassignment, and cross-stream transaction order remain
explicit system responsibilities.

### D2. Separate durable from committed object data

Failures can publish overlapping, duplicate, or uncommitted records. BtrLog
stores a committed-LSN watermark and a writer-token epoch with objects. Readers
filter by both. objectKV must preserve the same distinction:

```text
bytes exist durably
    !=
transaction version is committed and visible
```

Immutable object publication alone is not a commit proof.

### D3. Make segment identity deterministic

The client sends each record's byte offset within the segment. Nodes therefore
construct byte-identical segments and race one conditional object PUT. This
avoids one object copy per replica. The objectKV analogue is deterministic
`txLog` segment closure plus content identity, with duplicate or overlapping
failure objects resolved by epoch and committed frontier.

### D4. Treat RAM mode as an explicit durability profile

BtrLog can disable local SSD and reports about half the median latency. It also
states that this weakens durability. This supports objectKV's configurable hot
tier only when the receipt makes the tradeoff unambiguous:

```text
RAM plus quorum
    fastest, loses the unflushed tail under correlated memory loss

RAM plus quorum NVMe
    slower, preserves the unflushed tail across process and power loss
```

Both profiles still publish immutable object segments asynchronously.

### D5. Do not claim the failure protocol is implementation-complete

The artifact README identifies `prototype/` as the implementation and `spec/`
as the TLA+ protocol. The paper states that the prototype implements the
latency-critical failure-free append path, while failure cases are modeled in
TLA+. Node replacement and automatic reconfiguration are future work.

For objectKV, a modeled fencing protocol is not `[CODE-COMPLETE]`, and a
failure-free benchmark is not `[VERIFIED]` recovery.

## Artifact surfaces to study next

The repository contains focused implementation seams that map well to
`okv-log`:

```text
prototype/src/client/quorum.rs
prototype/src/client/journal_state.rs
prototype/src/server/journal/
prototype/src/server/wal/
prototype/src/server/blob/s3.rs
prototype/src/runtime/
spec/BtrLogSpec.tla
```

The custom runtime, UDP transport, and `io_uring` block-device path should be
treated as optimization references. The quorum state, watermarks, writer token,
segment offsets, conditional publication, and tail-repair model are protocol
references.

## Proposed objectKV evaluation additions

Do not interrupt T27. Add these after the current admission curve establishes
the read-serving baseline:

1. `L1`, one writer and one log, 2-of-3 quorum append latency versus offered
   load for 128 B, 1 KiB, and 4 KiB records.
2. `L2`, matched RAM-only and RAM plus NVMe profiles, with the durability class
   included in every receipt.
3. `L3`, 256 KiB through 32 MiB object-segment sweep, measuring PUT
   amortization, publication lag, retained tail bytes, and recovery time.
4. `L4`, kill one log node under load, then measure availability, p50, p99, and
   under-replicated tail repair.
5. `L5`, fence the writer during in-flight appends and verify one committed
   contiguous prefix across RAM, NVMe, and object segments.
6. `L6`, compare same-zone quorum NVMe against remote block WAL and direct
   object WAL on GCP, including cost per million appends.

The relevant admission question is not whether BtrLog's published number can
be repeated. It is whether objectKV can retain the same log-path advantage once
transaction ordering, exact recovery, object publication, and read-serving
work share the system.
