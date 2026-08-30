# RFC-0045: Staged quorum txLog and object-log publication

- Status: `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-30
- Scope: `okv-wal`, the Cell v0 durability provider, and T29

## Implementation state

`[VERIFIED]` L0 deterministic protocol semantics now cover quorum durability,
writer epochs, exact retry identity, suffix repair, committed segment
visibility, manifest-only reads, and bounded queues across three seeds and six
negative controls. The clean-source receipt is
`docs/artifacts/eval-receipts/staged-txlog-l0-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` L1 runs three real log-node processes with distinct roots and TCP
listeners. Across three seeds it verifies synchronized `OKVT` appends, exact
retry identity, torn-tail repair, restart recovery, stale-writer fencing, and
byte-identical `OKVL` segment previews. Each of three process poisons is
rejected. The clean-source receipt is
`docs/artifacts/eval-receipts/staged-txlog-l1-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` L2a runs the batched log path on three independent same-zone GCE
machines and local NVMe devices from a fourth client machine. Across three
corrected runs, it acknowledged 196,608 of 196,608 records, reproduced exact
final state on all nine node checks, issued zero object operations, and recorded
zero anomalies. The combined 768-batch distribution was 4.357 ms p50, 4.535 ms
p95, 4.716 ms p99, and 5.343 ms p99.9. Median throughput was 49,028 128-byte
records/s at 256 records per sync. Receipt:
`docs/artifacts/eval-receipts/staged-txlog-l2a-gcp-r1-2026-08-30/README.md`.

`[EVALUATING]` The first L2 open-loop diagnostic ran the same binary and
topology against local NVMe and a dedicated `pd-ssd` control. The candidate
stayed stable through 40,000 offered records/s at 5.434 ms record p99, then
entered queue saturation between 40,000 and 60,000 records/s. Saturated
throughput was 45,000 to 46,000 records/s, approximately 1.18x the control, but
missed the frozen 1 ms p99, 100,000 records/s, and 1.5x throughput gates. Exact
final digests matched on all 39 node checks across 13 named runs with zero
anomalies. This is a one-repeat diagnostic without OTel, CPU attribution,
failure injection, cross-zone coverage, or transaction integration. The
admitted L2 curve and L3 through L6 remain open, and the RFC remains proposed.

`[CODE-COMPLETE]` The first measured optimization replaces per-record journal
frames with one versioned checksummed batch frame and replaces JSON payload
arrays with a binary client-node protocol. `[EVALUATING]` A topology-matched
GCP curve moved the knee to between 100,000 and 150,000 offered records/s and
raised saturated throughput to approximately 107,000 records/s, about 2.35x
the v1 frame. At 100,000 offered records/s, it acknowledged 86,852 records/s at
4.593 ms p99 with no refusal. All 633,436 accepted records across five points
were acknowledged, all 15 node digests matched, and no anomaly or foreground
object operation occurred. The absolute simultaneous 1 ms and 100,000-record/s
gate remains unmet. Receipt:
`docs/artifacts/eval-receipts/staged-txlog-l2-batch-frame-gcp-r0-2026-08-30/README.md`.

## Decision

Evaluate a reusable single-writer staged log service as the next physical
`okv-wal` shape. One active writer assigns consecutive positions for one log
stream, sends each append to a fixed set of log nodes in parallel, and receives
a durable result after a write quorum has persisted the record on local NVMe.
Log nodes retain a bounded hot tail and publish complete immutable segments to
object storage outside the acknowledgement path.

This does not change `okv-log`. It remains the pure ordered-record algebra. It
also does not authorize replacing the current OpenRaft transaction authority.
The staged service must first pass standalone correctness, latency, fencing,
publication, and recovery gates. Integration may replace one physical txLog
path only after it preserves the existing transaction history oracle without
double logging.

The architecture follows the useful part of BtrLog while retaining objectKV's
existing generation and publication authorities:

```text
many application writers
          |
          v
commit proxy and resolvers
  cell order, conflicts, retry identity
          |
          | one writer per txLog stream and epoch
          v
      TxLogClient
      /    |    \
     v     v     v
 LogNode LogNode LogNode
  RAM     RAM     RAM
  NVMe    NVMe    NVMe
     \     |     /
      quorum durable
          |
          +----> SegmentPublisher ----> GCS / S3
                    complete immutable segments
```

## Context and invariant

The current native cell replicates transactions through OpenRaft and persists
each voter's log through `NodeJournal`. That path proves ordering, fencing, and
recovery semantics, but it combines transaction agreement with physical log
storage. BtrLog demonstrates that the common single-writer WAL case can remove
a separate sequencer and commit in one client-to-quorum network round trip,
while asynchronously archiving large segments to object storage.

The load-bearing distinction is:

```text
single writer per physical stream
    !=
single application writer or single-writer database
```

The commit proxy or assigned log writer is the only writer for one stream and
epoch. Many clients may submit transactions concurrently, and a cell may use
many streams. Transaction ordering, conflict resolution, and cross-stream
commit remain above this service.

The invariant is:

> Every acknowledged durable position remains recoverable as the same record
> after any allowed writer or log-node failure, and no stale writer can extend
> the active stream.

Immutable object publication cannot prove transaction commit by itself. Bytes
may be physically present without being committed or reader-visible. The
active publication root and committed frontier remain separate authorities.

## Proposed contract

### Service taxonomy

| Component | Owns | Does not own |
|---|---|---|
| `okv-log` | Consecutive indexes, append planning, suffix replacement, purge, exact reads | Files, network, durability, epochs, transaction meaning |
| `TxLogClient` | Position allocation, bounded in-flight window, ordered acknowledgements, quorum collection | Conflict resolution, object visibility, permanent metadata |
| `LogNode` | Epoch validation, RAM staging, optional NVMe persistence, hot reads | Global transaction order, range state, object-root authority |
| cell generation authority | Active writer epoch and log membership | Per-append payload storage |
| `SegmentPublisher` | Canonical segment construction, immutable PUT, closure verification | Foreground acknowledgement |
| publication authority | Pending and active segment roots, committed coverage, GC roots | Object bytes and hot-tail serving |

### Logical API

The first service API is intentionally log-shaped:

```text
open(log_id, membership, writer_epoch, expected_position)
append(log_id, writer_epoch, first_position, records[]) -> DurableAppend
read(log_id, after_position, limit, read_frontier)       -> RecordPage
recover(log_id, new_writer_epoch)                        -> RecoveredTail
seal(log_id, through_position)                           -> SegmentCandidate
```

`DurableAppend` binds:

```text
log identity
writer epoch
first and last position
record-batch digest
membership digest
acknowledging node identities
durability profile
```

The certificate is diagnostic in the standalone gate. Transaction integration
must define which replicated owner retains the commit outcome and how an exact
retry recovers it.

The service accepts batches because the current commit proxy already closes
bounded transaction batches. One physical append may therefore carry several
logical transactions without making them one application transaction.

### Durability profiles

| Profile | Durable result | Foreground path | Claim |
|---|---|---|---|
| `quorum_nvme` | `COMMITTED` after quorum local-NVMe persistence on declared failure domains | RAM plus local NVMe plus quorum network | Default candidate |
| `quorum_ram` | `BUFFERED` after quorum RAM placement | RAM plus quorum network | Lowest latency, volatile under correlated memory loss |
| `sync_object` | `COMMITTED` after exact object effect and authority publication | Object API and authority | Higher-latency portable alternative |
| `external_journal` | Defined by the named journal contract | Provider specific | Adapter alternative |

`quorum_ram` never reports `COMMITTED` merely because several volatile copies
exist. It may become committed only through later NVMe, synchronous object, or
an admitted external-journal transition.

### Write and acknowledgement path

For `quorum_nvme`:

1. The generation authority assigns one writer epoch and membership digest.
2. The writer installs that epoch on a write quorum before appending.
3. The writer assigns consecutive positions and sends one batch to every log
   node concurrently.
4. Each node validates the epoch and expected position, copies the batch into
   bounded RAM, persists a checksummed record to local NVMe, then responds.
5. The client completes positions in order after the required quorum responds.
6. Missing replicas repair asynchronously. Queue and tail bounds can refuse new
   appends before memory or media become unbounded.
7. Object publication runs independently and cannot delay a normal append.

There is one network round trip between the assigned writer and the write
quorum. Calls from an external application to the commit proxy are a separate
hop and must remain visible in end-to-end transaction measurements.

### Writer recovery

A replacement writer first acquires and installs a higher epoch. It then reads
a quorum, finds the greatest consecutive recoverable prefix, and re-replicates
that exact prefix before appending. Because a read quorum intersects every
prior write quorum, an acknowledged record remains discoverable within the
declared failure model.

Recovery may retain a record whose previous response was unknown. That is
allowed only when the record carries an immutable request identity and exact
retry fingerprint. The successor may complete the same operation, but it may
not substitute a different payload at that position. The transaction layer
must expose `commit_unknown` until the recovered outcome is resolved.

This rule is more conservative than inferring transaction commit from physical
presence. A record on one node may be durable without having reached the old
write quorum.

### Segment publication

The first segment target is 16 MiB, with an evaluation sweep from 256 KiB
through 32 MiB. A segment contains one consecutive stream interval and binds:

```text
format version
log identity and writer epoch
first and last position
committed-through position
record count and logical bytes
per-record length and checksum
segment length and digest
optional sparse read index
```

The publisher constructs canonical bytes for a complete committed prefix. A
named content-addressed PUT is create-only. The publication authority exposes a
segment only after exact identity and closure verification. Object-store LIST
is never authority.

Nodes may race to upload byte-identical segments, but only one named object is
retained. Incomplete, overlapping, prior-epoch, or ambiguous objects remain
unreferenced until reconciled. Readers follow the active manifest, never all
objects that happen to exist.

The initial active-stream path may emit one object per full segment. Cold-log
packing is not part of the first implementation. The evaluation must therefore
report partial-segment count, mean object size, and PUTs per GiB. If cold logs
create an unacceptable small-object curve, a later RFC must add authenticated
multi-stream packs before production admission.

### Read and recovery path

Hot positions come from a log-node quorum or a disposable RAM cache. Cold
positions come from active immutable segments. A recovery scan composes:

```text
active object segments through P
              +
quorum-recovered hot tail (P, C]
              =
one exact consecutive stream through C
```

The segment manifest provides all object names and ranges. Recovery must not
enumerate an object bucket or hydrate unrelated streams.

### Bounds and backpressure

Every profile declares:

```text
maximum in-flight appends and bytes
maximum node queue entries and bytes
maximum RAM staging bytes
maximum local-NVMe tail bytes
soft and hard publication-lag bounds
maximum object-segment age
maximum open streams per node
```

Crossing a soft limit reduces admission. Crossing a hard memory, media, or RPO
limit refuses new durable appends. Switching from bounded RAM to an unbounded
queue is a correctness failure, not a throughput optimization.

## Relationship to the native transaction plane

The current OpenRaft path owns both agreement and stable transaction history.
Adding a second staged WAL beside it would increase latency and media without
proving a product mechanism. The standalone evaluation is therefore isolated.

There are two valid later integration outcomes:

1. Keep OpenRaft as transaction agreement and replace only its per-node stable
   journal implementation after an equivalent recovery proof.
2. Move toward the FoundationDB-shaped transaction plane, where commit proxies
   and resolvers establish accepted order and the staged txLog provides the
   quorum-durable mutation stream.

Outcome 2 can remove a leader replication hop, but it also assumes ownership of
recovery generations, log-set reconfiguration, version authority, resolver
coordination, and ambiguous-commit recovery. L1 through L5 do not authorize
that change. L6 must compare both complete transaction paths.

## Failure model

The contract covers:

- request loss, duplication, reordering, and delayed responses;
- one log-node process or machine loss in a three-node, quorum-two topology;
- stale writer, concurrent takeover, and writer loss after quorum but before
  response;
- incomplete local-NVMe record, complete corruption, disk full, and slow disk;
- bounded queue saturation and publication lag;
- object PUT success with lost response, duplicate upload, corrupt existing
  identity, and object-store unavailability;
- partial, overlapping, and prior-epoch object segments;
- replacement writer and empty-node recovery from objects plus a quorum tail.

The first same-zone profile does not claim availability after zone loss. The
first three-zone profile does not claim availability after one zone plus one
additional node failure. Those stronger envelopes require larger membership.

## Evaluation plan

`evals/suites/staged-txlog.toml` owns this ladder.

### L0. Deterministic protocol contract

Model epochs, write and read quorums, unknown outcomes, suffix repair, segment
visibility, and bounded queues. Negative subjects acknowledge one copy, accept
a stale epoch, overwrite an acknowledged suffix, publish an uncommitted
segment, trust LIST, or remove the hard queue bound.

Primary metric: `correctness.anomalies`, total must be zero.

### L1. One-host process mechanism

Start three real log-node processes and one client. Prove exact frames,
checksums, sync ordering, process restart, and deterministic segment bytes for
128 B, 1 KiB, and 4 KiB records. This is `[VERIFIED]` for the one-host process
mechanics and cannot support an independent-machine durability or cloud
latency claim.

The L1 candidate freezes two experimental byte contracts before implementation:

```text
OKVT v1 node journal
  log identity digest
  writer epoch
  consecutive position
  immutable request identity
  payload length and payload
  frame checksum

OKVL v1 immutable segment preview
  log identity digest
  first and last position
  committed-through position
  ordered OKVT record bodies
  segment checksum
```

`OKVT` and `OKVL` are objectKV formats, not BtrLog compatibility formats. L1
must freeze a fixture for each format. A node acknowledges an append only after
the corresponding `OKVT` frame is fully written and the file synchronization
returns successfully. An exact retry returns the retained outcome without
appending another frame. A conflicting retry, gap, or stale epoch fails before
physical mutation.

For every seed, the candidate must:

1. start three child processes with distinct roots and TCP listeners;
2. install one writer epoch on all three nodes;
3. append 128 B, 1 KiB, and 4 KiB records to a two-of-three write quorum;
4. prove exact retry does not increase any node journal;
5. stop all nodes, inject one incomplete final frame into one stopped node, and
   restart all nodes from disk;
6. prove the torn suffix is removed before the next synchronized append;
7. reject the previous writer epoch after restart;
8. recover one exact consecutive prefix from at least a read quorum; and
9. construct byte-identical `OKVL` segment previews on every complete node.

Hard gates require three distinct process IDs and roots, at least two durable
acknowledgements per accepted append, zero acknowledged-record loss, zero
conflicting or stale mutation, one repaired torn tail, exact restart recovery,
equal segment digests, zero object operations, and bounded physical bytes. The
process workload records append duration as a diagnostic only.

Three process-level negative controls are frozen with the candidate. The first
returns a quorum acknowledgement before stable append, the second accepts a
stale writer epoch after restart, and the third injects node-specific bytes
into the segment preview. Each must be rejected by the unchanged L1 oracle.

### L2. Same-zone independent-machine append curve

Run three log nodes on independent local-NVMe machines and a separate client.
Use open-loop Poisson arrivals, 64 client tasks, 256 streams, three record
sizes, five repeats, and a measured offered-load sweep. Capture the idle
network RTT and local-NVMe persistence distribution as the hardware floor.

`[VERIFIED]` The L2a prerequisite path adds bounded node-level append batches
and persistent client connections. A node validates the complete batch before
physical mutation, writes every new `OKVT` frame together, performs one shared
journal sync, and publishes the in-memory records only after that sync succeeds.
Exact retries may share a batch with new consecutive records without growing
the journal. Every record still becomes acknowledged only after the same batch
has synchronized on a write quorum.

The bounded real-machine preflight used one client, three exact machine and
local-NVMe identities, 128-byte records, 256-record batches, persistent
connections, and final exact-history reads from all nodes. Three corrected
runs reached 48,078 to 49,159 records/s with zero anomalies. Across 768 batches,
acknowledgement was 4.357 ms p50, 4.535 ms p95, 4.716 ms p99, 5.343 ms p99.9,
and 5.591 ms maximum. Every node retained 16,777,300 physical bytes for
8,388,608 logical payload bytes, exposing an approximately 2.0x per-node frame
amplification before replica multiplication.

The rejected first run is also retained. It reached only 5,321 records/s and
47.336 ms p99 because the server wrote the response length and body separately
with Nagle coalescing enabled. Adding server-side `TCP_NODELAY` removed that
delayed-ACK plateau without changing the workload or durability path.

L2a verifies physical headroom and exact final state, not the admitted L2 curve.
Batch formation dwell must remain inside each record's end-to-end
acknowledgement latency. Neither this closed-loop batch distribution nor its
throughput number can satisfy the 1 ms per-record p99 gate by itself. Receipt:
`docs/artifacts/eval-receipts/staged-txlog-l2a-gcp-r1-2026-08-30/README.md`.

`[EVALUATING]` The first bounded open-loop diagnostic used 64 Poisson producer
threads, 256 streams, a 32,768-record queue, a 256-record or 250 us batch close,
and 131,072 attempted records at each 20k, 40k, 60k, 80k, and 100k offered-load
point. The same three machines then reran the curve against dedicated `pd-ssd`
devices. Candidate record p99 was 4.157 ms at 20k and 5.434 ms at 40k. At 60k,
queue dwell accounted for 534.928 ms of 539.902 ms record p99 and the candidate
saturated near 45k to 46k acknowledged records/s. The control saturated near
38.4k records/s. Candidate/control p99 ratios ranged from 0.404x to 0.848x;
saturated throughput ratios ranged from 1.166x to 1.184x.

Increasing the batch cap to 512 and 1,024 raised quorum p99 from approximately
4.8 ms to 9.0 ms and 17.9 ms while acknowledged throughput remained below
48.3k records/s. The current `OKVT` layout writes one 128-byte envelope around
each 128-byte payload, and the JSON protocol represents payload bytes as
numeric arrays. This makes a shared batch journal frame, binary wire framing,
and node-side encode, write, and sync timing the next implementation slice.
The evidence is consistent with per-record software work, but lacks the CPU
profile required to assign the cost to one component. Receipt:
`docs/artifacts/eval-receipts/staged-txlog-l2-open-loop-gcp-r1-2026-08-30/README.md`.

`[CODE-COMPLETE]` The v2 `OKVT` format now writes one indivisible frame per
consecutive batch while preserving request identity, exact retry, v1 recovery,
and whole-batch torn-tail truncation. A full 256 by 128-byte frame is 42,080
bytes rather than 65,536 bytes. The binary request path sends raw payloads and
reports decode, validation, journal encode, write, and sync durations.

`[EVALUATING]` The exact 131,072-record, 32,768-queue workload then ran at 40k,
60k, 100k, 150k, and 200k offered records/s on three recreated
`n2-standard-8` local-NVMe nodes. Record p99 was 3.864, 4.194, and 4.593 ms at
40k, 60k, and 100k, with zero refusal. At 150k, queue dwell raised record p99
to 201.981 ms; saturated throughput was approximately 107k records/s. At the
100k point, node-sync p99 was 1.773 ms, quorum p99 was 2.212 ms, and queue-dwell
p99 was 2.492 ms. Journal amplification fell from approximately 2.0x to 1.286x
per node. This moves the software ceiling but shows that the 1 ms gate is below
the measured media-sync p99 on this profile. Receipt:
`docs/artifacts/eval-receipts/staged-txlog-l2-batch-frame-gcp-r0-2026-08-30/README.md`.

Primary metric: p99 `log.append.ack_duration` at the largest offered load that
keeps success at 100 percent and queue refusal below one percent.

Candidate gates:

- zero acknowledged-record loss and zero ordering anomalies;
- p99 no greater than 1 ms at 100,000 128-byte appends per second;
- p50 no greater than 1.5x the measured quorum hardware floor;
- p99 no greater than 0.75x the same-zone remote-block control at matched load;
- throughput at the 1 ms p99 ceiling at least 1.5x the control;
- no object operation in the foreground measured window;
- all queues and retained bytes remain within the declared bounds.

The absolute load target is tied to the first `n2-standard-8` objectKV-dev
profile. A different machine class requires a separately frozen target.

### L3. Failure and fencing curve

At 40 percent of the admitted load, kill one log node, replace the writer, and
restore the node from active segments plus the repaired tail.

Gates: 100 percent availability inside the declared one-node envelope, zero
acknowledged loss, no stale-writer append, p99 degradation at most 35 percent
after one node loss, and writer recovery within 2 seconds.

### L4. Segment and cold-recovery curve

Sweep 256 KiB, 1 MiB, 4 MiB, 16 MiB, and 32 MiB segment targets under real GCS.
Measure object PUTs, bytes, publication lag, partial segments, cold scan rate,
and exact empty-node recovery.

Gates: no foreground object operations, p99 publication lag at most 5 seconds
under admitted load, at most 128 PUTs per logical GiB after warmup, no LIST
authority, and cold scan throughput at least 50 percent of the same-machine raw
GCS sequential-read control.

### L5. RAM profile

Disable local NVMe while preserving the same RAM, network, record, and stream
shape. Require at least 20 percent lower median append latency than
`quorum_nvme`, but return only `BUFFERED`. Destroying the volatile quorum must
lose or reject the unflushed tail without producing a false committed receipt.

### L6. Transaction integration

Integrate exactly one staged-log candidate into the existing transaction suite.
Run strict serializability, conflict, retry, leader loss, host loss, object
frontier, and empty-recovery histories unchanged. Compare end-to-end commit
latency and throughput with the current OpenRaft path. Reject any design that
double logs, changes versionstamps, weakens `commit_unknown`, or requires
objects in the normal acknowledgement path.

## Alternatives

### Keep the current OpenRaft journal only

Optimizes for: one agreement and durability mechanism with mature Raft safety.

Gives up: the potential one-round-trip physical-log path, reusable WAL service,
and independent scaling of transaction roles and log nodes.

### Use FoundationDB as the transaction plane

Optimizes for: production-tested commit proxies, resolvers, logs, range
placement, and recovery.

Gives up: native control of the hot path and an open object-native txLog format.
FoundationDB remains the semantic oracle and fallback profile.

### Write each WAL append synchronously to object storage

Optimizes for: no local durable-media requirement and simple regional recovery.

Gives up: microsecond-class commit latency, low per-append cost, and independence
from object-service tail latency. It remains a measured control.

### Use remote block storage

Optimizes for: simple lift-and-shift durability and device semantics.

Gives up: local-NVMe latency, pay-as-used object archival, and independent log
service scaling. It remains the same-availability-class control.

### Treat quorum RAM as durable

Optimizes for: the shortest foreground path.

Gives up: honest durability under correlated process, software, or power loss.
This alternative is rejected.

## Compatibility and migration

The standalone gate does not modify `OKVR` or `OKVW` bytes. Segment bytes must
receive a new versioned magic and frozen fixtures before L4. Existing
`NodeJournal` readers continue to read the current format.

Transaction integration requires a quiesced generation handoff or a dual-read
recovery tool. Dual writes are permitted only as an observation mode and cannot
acknowledge from two independent authorities. Rollback returns to the old
generation and old log root before accepting new writes.

## Unresolved questions

- Whether the staged service replaces only `NodeJournal` persistence or opens
  the FoundationDB-shaped commit-proxy, resolver, and txLog architecture.
- Whether one cell uses one log stream, tagged streams, or one stream per range
  group after the single-range gate.
- Whether low-frequency streams require authenticated multi-stream object packs
  to meet the small-object economics gate.
- Whether the production transport remains Tokio TCP, moves to UDP with explicit
  reliability, or uses another kernel-bypass path after profiling.
- Whether three nodes are sufficient for the first regional envelope or the
  availability target requires a larger multi-zone log set.
