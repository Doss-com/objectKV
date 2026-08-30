# objectKV log architecture

Status: `[EVALUATING]` architecture and first implementation slice. RFC-0024
owns the boundary. The pure `okv-log` crate and `NodeJournal` delegation exist;
Tetris and Chess action-delta reducers are code complete over volatile
`okv-log`. Transactional application-log append and object-retained segments
remain proposed.

## Punchline

objectKV should be built from a reusable ordered-log algebra, but it should not
pretend the WAL and an application stream are the same product surface.

```text
                    okv-log
         ordered opaque-record semantics
             /                     \
            /                       \
     okv-wal                     future log formats
 quorum, tx envelopes,           sealed object segments
 Raft state, fencing                      |
            |                             |
     objectKV transactions          future adapters
            |                    task / CDC / event log
            +-------------+---------------+
                          |
                 application consumers
```

The reusable waist is an ordered sequence of opaque indexed records with exact
append, suffix replacement, prefix purge, range-read, and replay semantics.
Durability and application meaning are layered above it.

## Vocabulary

| Term | Meaning |
|---|---|
| `okv-log` | Lowest ordered opaque-record contract and reference state machine |
| `okv-wal` | Cell recovery and consensus durability layer using `okv-log` semantics |
| `txLog` | Replicated WAL history required between commit version `C` and object-durable version `O` |
| Application log | User-visible retained events with partitions, cursors, and independent retention |
| Action delta | Versioned application operation that a deterministic reducer applies to prior state |
| State checkpoint | Materialized application state bound to a log position, reducer identity, schema, and checksum |
| Change feed | Projection of committed mutations or schema-aware changes for a named consumer |
| Log partition | Independent append and serving unit with its own cursor order |
| Log position | Partition-local consecutive index; not a wall-clock timestamp |
| Commit version | Cell-scoped transaction order, used to merge partitioned logical changes |
| Sealed segment | Immutable contiguous log run published under a fenced manifest |

## BIDEC workstreams

### Level-1

- W1. Log algebra: indexes, append, suffix replacement, purge, replay, and
  errors.
- W2. Physical durability: framing, checksum, sync, torn-tail handling, and
  compatibility.
- W3. WAL policy: envelopes, votes, commit positions, quorum, fencing, and
  acknowledgement.
- W4. Transactional logs: atomic data plus record plus cursor behavior.
- W5. Object retention: sealed segments, manifests, indexing, compaction, and
  GC roots.
- W6. Evaluation: semantic poisons, crash histories, append curves, catch-up,
  and cost.

### Merged workstreams

1. Define the ordered-log core, covering W1 and the semantic part of W6.
2. Adapt the existing WAL without changing bytes, covering W2, W3, and WAL
   conformance.
3. Add transactional and object log products later, covering W4, W5, and the
   performance part of W6.

### Sequence

1. RFC and independent cross-examination.
2. Pure `okv-log` state machine through red-green behavioral slices.
3. `NodeJournal` integration with compatibility fixtures unchanged.
4. Transactional task-stream RFC and deterministic crash histories.
5. Sealed object-log segment RFC, catch-up path, and economics curves.

Recalibration trigger: if the WAL adapter needs consensus, vote, filesystem, or
codec knowledge inside `okv-log`, stop. The boundary is wrong.

## Bottom-up operation

### 1. Ordered-log core

The core receives commands and returns a new logical retained state:

```text
state: purge marker 41, retained entries 42..57

append 58..60             -> retained 42..60
replace suffix from 55    -> truncate 55, append 55..
purge through 52          -> marker 52, retained 53..
read_exact [48, 56)       -> position_expired
read_clamped [48, 56)     -> retained 53..55
```

The core never opens a file or acknowledges durability. It is deterministic and
can be reused by local files, object segment builders, a simulator, or another
storage engine.

A fresh log accepts any consumer-valid first index. Index zero is legal, but
not required. Suffix replacement remains two or more commands: one truncate,
then consecutive appends. This means every durable command prefix is a valid
recovery point if a physical batch tears between records.

Planning filters append entries at or below the purge marker. A fully purged
batch produces no commands or bytes. A straddling batch begins its live suffix
at exactly `purge + 1`. Truncating beyond the retained tail is a no-op; purging
beyond it is legal. Neither rule allows a later gap above the current frontier.

### 2. WAL durability

`okv-wal` plans commands, applies them to a cloned state, and rejects invalid
history before encoding any bytes. It then encodes the commands in existing
`OKVR` records, writes and synchronizes them, and commits the validated clone to
in-process state before completing the caller. OpenRaft still owns when to
request append, truncate, purge, vote, and committed-position changes. The cell
transaction protocol still owns acknowledgement.

The ordering is load-bearing:

```text
plan -> dry-apply clone -> encode -> write -> sync -> commit state
```

Encoding before validation could durably create a self-unreadable journal. Vote
and committed-position frames may interleave with entry commands; WAL replay
routes those metadata records to `okv-wal` state and only entry, truncate, and
purge commands to `LogState`.

The pure state transition and the persisted replay must agree exactly. This is
the first conformance gate.

### 3. Cell transaction log

The cell encodes canonical transaction envelopes as opaque log payloads. A
successful regional commit requires every required log group and resolver
result. `okv-log` does not know transactions, tenants, ranges, or consensus.

Once immutable database state is reconstructable through version `O`, the
recovery layer may purge the corresponding `txLog` prefix. The purge is driven
by the durable frontier, never by an application consumer.

### 4. Transactional application log

A later adapter represents a log record as transactionally written keys:

```text
/log-record/{log-id}/{partition}/{commit-version}/{ordinal} -> record
/log-cursor/{log-id}/{group}/{partition}                    -> position
/log-dedupe/{producer}/{request-id}                          -> outcome
```

The exact key layout, versionstamp mechanism, and ordinal allocation remain
unaccepted. The semantic requirements are:

- record append is atomic with business mutations;
- records carry a commit version and deterministic ordinal;
- a cursor advance may be atomic with derived objectKV writes;
- a cursor cannot read below its retention lease;
- duplicate producer identity returns one retained outcome;
- external effects use an outbox or effect idempotency.

Producer retry guarantees require the deduplication outcome to remain retained
for a declared lease. Garbage collection cannot discard it while the producer
may legally retry.

#### 4.1 Action deltas and reducer checkpoints

`[CODE-COMPLETE]` The paired playground uses `okv-log` as an in-memory reference
state machine for a compact application history:

```text
checkpoint at P + deltas (P, T] -> deterministic reducer -> state at T
```

Tetris writes a two-byte versioned action and a 205-byte checkpoint every 256
actions. Chess writes a four-byte versioned move and an 81-byte checkpoint
every 64 moves. The paired harness compares the reconstructed fingerprint with
the materialized-KV implementation, so byte reduction is admitted only after
state equivalence passes.

These action records are not the `txLog`. The `txLog` must recover objectKV
without loading a game-specific reducer. `[PROPOSED]` A transactional adapter
will atomically commit business mutations and action-log append through
`okv-wal`; a later materializer will seal retained action runs and checkpoints
to objects. Durable checkpoints must bind log position, reducer identity,
schema version, and checksum. Unsupported reducers or schemas must fail closed.

### 5. Object-retained log

Hot records are served from replicated fast media and disposable serving state.
A materializer seals contiguous partition runs into immutable objects. The
manifest publishes only complete verified runs. Cold consumers range-read the
sparse index and selected blocks, then join the hot tail.

This layout can optimize sequential replay without forcing the OLTP row-object
format to serve stream scans. Both layouts retain the same logical commit
identity where they overlap.

## Ordering promises

| Scope | Promise |
|---|---|
| One log partition | Consecutive logical positions after its purge marker |
| Several partitions in one cell | Deterministic merge by commit version and ordinal; gaps legal |
| One transaction | All emitted records and data become visible atomically |
| Several cells | No total order or atomic transaction |
| Wall clock | Observability mapping only, never ordering authority |

A single global physical tail is deliberately not promised. It would create a
hot range and bind throughput to one owner. Consumers that need a global view
acquire one fixed cell read version, read every partition at that version, then
merge records by cell commit version and ordinal.

## Delivery guarantees

| Claim | Mechanism | Bound |
|---|---|---|
| Durable WAL append | Consensus and `okv-wal` sync policy | Declared replica topology |
| At-least-once application delivery | Durable record plus retryable cursor | One retained partition history |
| Exactly-once logical objectKV effect | Input cursor and output written in one transaction | One tenant database and cell |
| Exactly-once external effect | Not provided directly | Requires effect idempotency or outbox acknowledgement |
| Long replay | Sealed object segments and retention manifest | Declared consumer lease and GC policy |

## API direction

The likely later application interface is intentionally small:

```text
append(log, partition, producer_identity, bytes) -> commit_version + ordinal
read(log, partition, after, limit, snapshot) -> records + continuation
transact(input_cursor, output_mutations, output_records) -> committed outcome
watch(log, partition, after) -> notification only
lease_cursor(group, partition, position, expiry) -> retained root
```

This is not part of the first implementation. Versionstamps, watches, and the
transaction client contract remain proposed elsewhere.

## Initial tests

The first implementation is admitted only if public-interface tests prove:

1. fresh bases at indexes `0`, `1`, and `7`, followed by consecutive appends;
2. overlapping append plans suffix replacement rather than duplicate indexes;
3. purged entries never reappear;
4. truncation cannot cross a purge marker;
5. a purge marker cannot regress or change identity at one index;
6. replay of the command sequence produces identical state;
7. `NodeJournal` reopen matches the reference state;
8. exact reads below purge fail while compatibility reads clamp;
9. a below-purge batch emits no commands or bytes and a straddling batch starts
   at `purge + 1`;
10. a torn suffix replacement reopens as a valid command prefix;
11. the accepted and rejected raw `OKVR` history corpus is byte-identical after
    integration.

## Explicit nonclaims

- `[CODE-COMPLETE]` `okv-log` provides the pure ordered state machine, planner, exact
  and clamped reads, and semantic poison tests.
- `[CODE-COMPLETE]` `okv-wal::NodeJournal` delegates append, truncate, and purge
  semantics while preserving `OKVR` framing and raw accepted/rejected fixtures.
- `[PROPOSED]` No object log-segment format exists yet.
- `[PROPOSED]` No consumer-group coordinator exists yet.
- `[PROPOSED]` No cross-cell log order exists.
- `[PROPOSED]` No exactly-once external effect claim exists.
- `[PROPOSED]` No WAL or stream performance result exists.

## Staged quorum service research lane

`[PROPOSED]` RFC-0045 freezes the next physical `okv-wal` candidate without
changing the pure `okv-log` waist:

```text
okv-log commands
      |
      v
one assigned writer per stream and epoch
      |
      +---- parallel append ----> LogNode RAM + optional NVMe
      |                           LogNode RAM + optional NVMe
      |                           LogNode RAM + optional NVMe
      |                                      |
      <------------- quorum result ----------+
                                             |
                                   asynchronous segment seal
                                             |
                                             v
                                     immutable GCS / S3
```

The candidate borrows BtrLog's useful physical split, not its proof status.
`quorum_nvme` may report `COMMITTED`; `quorum_ram` may report only `BUFFERED`.
The cell generation authority owns writer epochs, and the existing publication
authority owns active object roots. Object storage remains outside the normal
append acknowledgement path and is not a coordination system.

`[VERIFIED]` The L0 deterministic model now preserves acknowledged records
through node loss and takeover, recovers unknown outcomes by immutable request
identity, fences stale writers, rejects suffix overwrite, exposes only
committed segments through the manifest, and bounds the publication queue.
Each of six targeted poisons was detected across three seeds. The receipt is
`docs/artifacts/eval-receipts/staged-txlog-l0-gcp-r0-2026-08-30/README.md`.

`[VERIFIED]` L1 runs three real child processes with distinct roots and TCP
listeners. It verifies synchronized `OKVT` frames, exact retry without journal
growth, restart and torn-tail repair, stale-epoch fencing, and byte-identical
`OKVL` segment previews across three seeds. The unchanged evaluator rejects
early acknowledgement, stale-writer acceptance, and divergent-segment poisons.
Its receipt is
`docs/artifacts/eval-receipts/staged-txlog-l1-gcp-r0-2026-08-30/README.md`.

The standalone service does not replace OpenRaft or verify transaction commit.
L1 uses one machine and local files, sends no object operation, and makes no
latency or throughput claim. T29 may integrate it only after independent-media
quorum, fencing, unknown-outcome repair, bounded queues, segment economics, and
recovery pass, and only if the complete transaction path does not double log.
