# RFC-0036: Independent-media object-frontier convergence

- Status: `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 durability, bounded local media, and remote object publication

## Decision to test

Run the G4.10b transaction and object-frontier protocol as a sustained loop on
three independent data hosts. Each host owns one data-authority voter and one
publication-authority voter with separate stable roots on persistent SSD. One
controller outside those failure domains drives the workload, records OTLP,
and publishes immutable closures to a private single-region GCS bucket.

```text
controller host
  |
  +-> host A: data voter A + publication voter A + SSD roots A
  +-> host B: data voter B + publication voter B + SSD roots B
  +-> host C: data voter C + publication voter C + SSD roots C
  |
  +-> regional GCS: immutable row objects + manifests
  +-> OTLP collector: traces + metrics + logs
```

The two Raft roles are separate processes but share host fate. Loss of one host
must leave both quorums available. GCS is never part of foreground transaction
acknowledgement.

## Why this gate is next

G4.10b proves the transaction and object-frontier composition on one host.
G4.11a then proves crash-safe state snapshots and physical journal reclamation
on three local processes. Neither proves bounded snapshots, independent-media
durability, remote object semantics, sustained frontier convergence, or
host-loss recovery.

Logical txLog pop alone is insufficient. The current state machine can discard
recovery records through `O` while the append-only OpenRaft node journal still
retains the commands that created them. Repeated frontier cycles therefore need
this physical sequence:

```text
publish and validate closure through O
  -> apply authenticated object frontier O
  -> durably snapshot complete state machine at or beyond O
  -> prove snapshot can reopen from stable media
  -> purge covered OpenRaft entries
  -> atomically compact each node journal
  -> retain and measure suffix (O, C]
```

Purging before a durable state-machine snapshot is a data-loss bug. Journal
compaction is not enabled as process maintenance until snapshot reopen and
crash probes pass.

## Frozen implementation stages

### G4.11a: durable snapshot and physical journal compaction

On one real three-process data quorum:

1. commit a prefix and suffix through the 32-item commit proxy;
2. build a checksummed state-machine snapshot on every voter;
3. crash after snapshot write, after file synchronization, and around atomic
   replacement;
4. reopen the snapshot and require the exact applied log identity, membership,
   transaction state, retry state, frontiers, and retained suffix;
5. purge OpenRaft entries covered by the durable snapshot;
6. compact each node journal into its canonical vote, committed marker, purge
   marker, and retained suffix;
7. restart every voter from the snapshot plus retained journal and compare the
   exact semantic digest.

This stage may use local files, but each voter has a distinct root and process.
It admits only the mechanism, not a failure-domain claim.

Observed result: `[CODE-COMPLETE]` for the snapshot, purge guard, and journal
rewrite. Across seeds 5501, 5502, and 5503, the candidate reduced three
journals from at most 6,391,575 bytes to 879 bytes and reopened exact state,
retained stream, retry outcome, and new suffix commit after stopping all three
processes. The purge-before-snapshot poison changed no journal bytes or purge
markers and reopened exactly.

The result remains `[EVALUATING]`. The tree was dirty, OTel was disabled, and
all voters shared one host. More importantly, the unfrontiered snapshots
totaled at most 5,066,472 bytes for 131,072 logical workload bytes, 38.66082x
physical amplification. The journal mechanism passed; the snapshot state
shape did not pass a bounded Cell v0 gate.

### G4.11a.1: bounded frontiered process snapshots

Before independent media, compose the real process maintenance path with all
state frontiers on local files. Run four cycles over 256 live keys. Each cycle:

1. freezes, publishes, and validates an immutable closure through `O = C`;
2. advances resolver retention `R = C`;
3. advances `Q(client)` while retaining the newest 64 request identities;
4. applies and activates the authenticated object frontier;
5. snapshots each voter at its resulting applied log position;
6. purges and canonical-compacts only through that snapshot position;
7. restarts the complete data quorum and verifies object-plus-suffix state.

Freeze these local boundedness gates before measuring the candidate:

- total snapshot plus retained journal bytes are at most 8x one logical copy;
- cycle-four snapshot bytes are at most 1.25x cycle-one bytes;
- the complete six-process media curve remains inside both frozen byte bounds;
- retries below `Q(client)` fail without mutation;
- retries inside the 64-request window remain exact;
- full-quorum restart and a new suffix commit remain exact.

This stage also records commit p99 while snapshot serialization and durable
replacement run. Snapshot work must not remain an unmeasured blocking task on
the transaction executor.

### G4.11b: independent media and remote objects

After G4.11a.1 passes, run its admitted state ownership and maintenance path
from one exact revision and binary digest on three independent hosts plus a
controller. Use GCS for immutable object effects and persistent SSD for both
quorum journals on each host. Repeat frontier advancement under continuous
foreground commits, kill one host during commit, kill another host during a
later publication cycle, and rebuild a replacement data process from the
active object closure plus retained txLog suffix.

## Frozen workload

```text
initial prefix transactions:              2,048
frontier cycles:                               8
suffix attempts per cycle:                 2,048
value bytes:                                  256
concurrent clients:                            64
maximum batch items:                           32
maximum application entry bytes:          262,144
maximum batch delay:                            2 ms
candidate conflict share:                       25%
hot keys:                                      256
row-object target bytes:                 8,388,608
row-block target bytes:                     65,536
seeds:                         6101, 6102, 6103
wall budget per subject:                      900 s
```

At cycle `i`, the controller freezes `O_i` while commits continue, publishes a
complete manifest through `O_i`, advances and activates exactly `O_i`, snapshots
and purges only through a proven durable log position, then starts the next
cycle. The final check reconstructs `Database(C)` from the final active object
closure plus retained transactions `(O_8, C]`.

## Subjects and controls

### Batched candidate

Use the retained 32-item commit path, 25 percent deterministic conflict suffix,
concurrent remote publication, durable snapshots, and physical journal
compaction.

### Same-durability one-entry control

Use one logical transaction per Raft application entry with the same machines,
SSDs, GCS bucket, conflicts, snapshot cadence, failure schedule, and telemetry.

### Publication-disabled control

Run the batched commit workload without object publication or snapshot purge.
This separates foreground network and stable-media cost from publication
competition. It is not a viable permanent-state profile.

### Snapshot-before-sync poison

Advertise snapshot durability before its file and parent directory are
synchronized. Kill the voter and require restart validation to reject the
missing or incomplete snapshot before log purge.

### Purge-before-snapshot poison

Request OpenRaft purge without a reopen-verified snapshot covering the target.
The process control plane must reject the request without moving the purged log
marker or reclaiming bytes.

### Moving-frontier poison

Substitute live `C` for frozen `O_i` during one remote publication cycle. The
closure or frontier authority must reject it before txLog or journal purge.

## Frozen correctness gates

1. all six role processes run from one clean source revision and one recorded
   executable digest;
2. three distinct provider machine IDs, non-loopback addresses, zones, and
   persistent-disk identities are recorded;
3. OTLP metrics, traces, and logs arrive for every measured subject;
4. every admitted request has one exact committed or conflicted outcome and
   matches the ordered conflict oracle;
5. foreground transaction acknowledgements issue zero GCS operations;
6. every active frontier equals its frozen and fully validated `O_i`;
7. every snapshot reopens with the exact state-machine and applied-log digest;
8. OpenRaft purge never exceeds the durable snapshot position;
9. every compacted journal reopens with the exact vote, committed marker, purge
   marker, and retained suffix;
10. final object state through `O_8` plus txLog `(O_8, C]` reconstructs exact
    final `Database(C)`;
11. one data-host loss, one publication-host loss, leader replacement, killed
    voter restart, and empty replacement recovery preserve exact results;
12. all three poisons fail before unsafe mutation.

## Frozen performance and boundedness gates

1. batched candidate reaches at least 500 resolved durable outcomes per second
   on every seed;
2. client-observed commit p99 is no greater than 250 ms;
3. median candidate throughput is at least 4x the one-entry control;
4. median leader density is at least 16 outcomes per stable append;
5. every frontier activates within 10 seconds and `C - O_i` returns below 4,096
   versions before the next cycle ends;
6. physical node-journal bytes after each compaction are no greater than the
   durable snapshot bytes plus 2x the retained suffix bytes plus 1 MiB;
7. no node-journal or snapshot curve grows monotonically with already
   objectified history across all eight cycles;
8. zero backpressure rejections occur at the frozen offered load;
9. every subject completes within 900 seconds.

The first independent-media receipt records GCS request counts, transferred
bytes, stored bytes, and provider-observed resource identities. It does not set
a product cost ceiling for the row-object layout. A later larger-state object
economics gate must compare stable object reuse, compaction, and cold reads.

## Go or redesign rule

Keep native transaction authority only if G4.11a.1 first bounds local state,
then every G4.11b correctness gate passes and the candidate clears the absolute
and paired curves without unbounded local media. The result admits the first
Cell v0 vertical slice, not a production SLO.

Redesign the snapshot, journal, or objectification boundary if permanent-state
advancement cannot converge while commits continue. Pivot transaction authority
to TiKV or FoundationDB if the same-durability commit curve misses the frozen
floor after independent-media effects are isolated. In either case retain
okv-log, branching, immutable publication, and version-aligned history work.

## Tradeoff

This optimizes for the smallest experiment that can falsify the core separation
of quorum-durable hot commits, bounded local recovery state, and permanent
objects. It gives up multi-region claims, range-partitioned scaling, and final
object-format economics.

## Not claimed

- multiple commit proxies, resolver partitions, or range groups;
- cross-cell transactions or cell synchronization;
- production GCS cost, availability, or multi-region durability;
- admitted PostgreSQL, Redis, search, or DataFusion engines;
- a final object compaction or columnar layout;
- a production latency, throughput, or capacity SLO.
