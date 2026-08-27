# RFC-0024: Ordered log substrate and WAL layering

- Status: proposed, implementation active
- Authors: DOSS
- Created: 2026-08-25

## Decision under review

Create `okv-log` as the lowest reusable ordered-log contract in the workspace.
`okv-wal` depends on `okv-log` for indexed opaque-record semantics, then adds
transaction-envelope codecs, consensus metadata, quorum acknowledgement,
generation fencing, and recovery policy.

The dependency direction is:

```text
okv-log
   ^
   |
okv-wal
   ^
   |
objectKV transaction cell
   ^
   |
okv transactional log / task stream / CDC adapters
```

The internal recovery `txLog` is not the public application-log API. Its
retention ends after the covered database state is reconstructable from
authoritative objects. Application logs, CDC feeds, and task streams have
independent retention, cursor, partition, and object-materialization policy.

## Why this layering

The current `okv-wal` crate contains two reusable ideas mixed with WAL policy:

1. an indexed opaque record sequence with append, suffix replacement, prefix
   purge, exact range reads, and recovery;
2. transaction and Raft-specific decisions about votes, committed positions,
   quorum evidence, envelopes, fencing, and acknowledgement.

The first idea is useful for transaction logs, application logs, queues, CDC,
replication feeds, and materializer inputs. The second belongs to the WAL and
consensus layers. Separating them makes the reusable primitive smaller without
pretending every log has WAL durability or transaction semantics.

## `okv-log` contract

The first contract is synchronous and byte-opaque:

```rust
pub struct LogEntry {
    pub index: u64,
    pub payload: Vec<u8>,
}

pub struct PurgeMarker {
    pub index: u64,
    pub payload: Vec<u8>,
}

pub enum LogCommand {
    Append(LogEntry),
    TruncateSuffix { from: u64 },
    PurgePrefix(PurgeMarker),
}
```

`LogState` applies these commands and exposes both strict and compatibility
reads. The required semantics are:

- retained indexes are strictly consecutive after the purge marker;
- suffix replacement is planned as durable truncate followed by consecutive
  append commands, never as one opaque replacement record;
- every valid prefix of a planned command sequence is itself a valid state, so
  replay after a torn physical batch cannot manufacture an invalid history;
- truncation cannot cross the purged prefix;
- truncation beyond the retained tail is a legal no-op;
- purge markers never regress or change identity at one index;
- a purge may advance beyond the retained tail;
- an append batch entirely at or below the purge marker emits no commands;
- an append batch straddling the marker drops the purged entries and begins at
  exactly `purge + 1`;
- replaying the same command sequence reconstructs the same state;
- gaps, conflicting purge identity, and invalid truncation fail closed;
- payload bytes remain uninterpreted.

Index zero is legal in the core because consensus logs may begin at zero. A
consumer that reserves zero enforces that policy in its adapter.

Index arithmetic is checked. An append after `u64::MAX` fails with
`IndexExhausted`. The pre-refactor journal used saturating arithmetic and could
overwrite one max-index entry with another. Rejecting that theoretical history
is an intentional bootstrap compatibility break because the overwrite violates
the consecutive-index invariant.

A fresh state has no implied first index. Its first append may establish base
index `0`, `1`, `7`, or another consumer-valid value. Every subsequent retained
append must be consecutive.

The core exposes two read contracts because the current Raft adapter and future
application logs need different behavior:

- `entries_clamped(range)` silently starts after the purge marker. This
  preserves current `NodeJournal` and OpenRaft behavior.
- `entries_exact(from, to)` returns `PositionExpired` when `from` is at or
  below the purge marker. This is the fail-closed contract required by retained
  application streams.

The core does not claim `fsync`, quorum durability, consensus, global ordering
across partitions, wall-clock ordering, exactly-once external effects, or object
retention.

## Durability boundary

`okv-log` first owns the deterministic state transition and conformance model,
not a new stable file format. This avoids replacing or double-framing the
existing frozen bytes:

- `OKVW` remains the version-1 quorum-frame format;
- `OKVR` remains the version-1 per-node Raft journal format;
- compatibility fixtures remain byte-identical;
- `NodeJournal` retains file creation, checksums, torn-tail repair, `sync_all`,
  vote state, committed state, and record encoding;
- only its append, truncate, purge, and retained-entry algebra delegates to
  `okv-log`.

A later RFC may add sealed local or object log segments. That format does not
enter this implementation slice.

## How `okv-wal` uses it

```text
OpenRaft request
  -> okv-wal validates WAL and consensus policy
  -> okv-log plans ordered entry commands
  -> apply commands to a cloned LogState and reject invalid history
  -> okv-wal encodes commands as existing OKVR records
  -> append bytes
  -> sync_all
  -> commit the already-validated cloned state in memory
  -> return durable completion
```

Validation after write is forbidden. It could append a checksummed record that
the same implementation then refuses to replay, leaving a durable journal that
cannot reopen.

On reopen:

```text
read OKVR frames
  -> validate framing and checksum
  -> route vote and committed records to WAL metadata state
  -> route append / truncate / purge commands through LogState
  -> truncate an incomplete final frame
  -> fail on complete corruption or invalid command history
```

Vote and committed-position frames may interleave with log commands. They are
not `okv-log` state and remain owned by `okv-wal` during replay.

The WAL remains responsible for deciding when a durable local append is enough
to satisfy a consensus callback and when a cell commit may be acknowledged.

## Transactional log abstractions above objectKV

`[FUTURE]` An application-log adapter writes a versionstamped record in the same
objectKV transaction as business data, index intent, or an outbox record:

```text
transaction
  -> mutate business keys
  -> append /logs/{log}/{partition}/{versionstamp}/{ordinal}
  -> commit once
```

A consumer transaction reads after its partition cursor, updates derived state,
and advances the cursor atomically. This can provide exactly-once logical
effects inside one tenant database. External side effects still require an
idempotency key or transactional outbox.

Cell commit versions supply a deterministic merge order, with legal gaps. A
multi-partition read must first acquire one fixed cell read version and read
every partition at that same version. Storage and serving scale through log
partitions. There is no cross-cell total order or atomic cursor-plus-output
transaction.

The versionstamped key shape and ordinal allocation remain unproven. They must
not become public contracts until a transaction-layer experiment proves stable
ordering under concurrent writers. Producer deduplication also needs an
explicit retained-outcome lease before it can support retry guarantees.

Watches wake consumers but never replace durable range scans and cursors.
Consumer positions, CDC positions, and snapshot leases become explicit GC roots
with bounded lease and expiry policy.

## Object tier

`[FUTURE]` Long-retention application logs materialize sealed immutable log
segments to object storage. A segment declares:

- log and partition identity;
- first and last logical position;
- minimum and maximum cell commit version;
- record count, byte length, checksum, and codec version;
- sparse offset index and block checksums;
- previous segment or manifest identity.

The hot tail remains on replicated fast media. A fenced manifest publishes a
complete sealed prefix. Consumer reads merge the hot tail with manifest-selected
object segments. `LIST` is never an ordering or retention authority.

The transaction `txLog` may be reclaimed when database state is reconstructable
without it. An application log may retain its sealed objects for days or years.
Those are different frontiers.

## Failure semantics

| Failure | Required result |
|---|---|
| Duplicate append request | Consumer policy either returns the retained outcome or rejects conflicting identity |
| Gap in retained positions | Core rejects the history |
| Complete corrupt record | WAL or segment reader fails closed |
| Incomplete final frame | Physical adapter truncates only the incomplete suffix |
| Truncate through purge marker | Core rejects the operation |
| Same purge index, different identity | Core rejects the operation |
| Worker dies after external effect | Adapter retries with effect idempotency; core alone cannot promise exactly once |
| Consumer cursor expires | Retention policy fences the cursor and returns `position_expired` |
| Object segment PUT is ambiguous | Publisher resolves exact named identity before manifest advance |
| Cross-cell processing | No atomicity or global order is implied |

## First implementation slice

0. Freeze an accepted and rejected raw `OKVR` history corpus against the
   pre-refactor `NodeJournal`. Cover all five record kinds: vote, committed,
   append, truncate, and purge.
1. Add `crates/okv-log` with the pure `LogState`, command planner, exact and
   clamped reads, errors, and prefix-closure tests.
2. Replay the frozen corpus through both the pre-refactor behavior and the new
   core, with identical accepted state and rejection results.
3. Make `okv-wal::NodeJournal` use `LogState` for entry, truncate, and purge
   transitions while preserving validate-clone-before-write and exact `OKVR`
   bytes.
4. Run the existing `okv-wal`, OpenRaft storage, reopen, negative recovery, and
   workspace suites.
5. Do not change `LocalReplicatedWal` or `OKVW`, and do not add object segments,
   networking, asynchronous I/O, consumer groups, versionstamps, or a
   transactional task runtime in this slice.

## Evaluation plan

The first gate is semantic, not performance:

- reference and WAL-adapter histories produce identical retained entries and
  purge markers;
- reopen reproduces the same state;
- the accepted and rejected raw `OKVR` history corpus remains byte-identical;
- gap, purge regression, conflicting purge identity, truncate-through-purge,
  torn tail, and complete corruption subjects are independently detected.

Load-bearing poison cases also cover arbitrary fresh bases, truncation beyond
the tail, purge beyond the tail, a below-purge zero-byte append plan, a
straddling purge boundary, exact versus clamped reads, and a torn suffix
replacement whose durable command prefix still reopens.

Later performance lanes measure append throughput, sync p99, partition scaling,
publish-to-consume latency, object catch-up throughput, GETs and bytes per
record, retention cost, and consumer recovery time.

## Alternatives

### Keep all log mechanics in `okv-wal`

This avoids a crate but makes every non-WAL log either depend on consensus
terminology or duplicate the state machine.

### Make the public transactional log the primitive under the WAL

This creates a dependency cycle because objectKV transactions need the WAL to
commit. It also confuses application retention with recovery retention.

### Change `OKVR` to a new generic `OKVL` frame now

This creates a storage migration before the semantic boundary is proven. The
first slice preserves existing formats and delegates only the algebra.

### Use the raw Raft log as the public stream

This couples users to consensus compaction, membership entries, recovery
generations, and range topology. It prevents independent retention and makes
partitioning a public compatibility break.

## Tradeoff

Optimizes for: a reusable ordered-log core, stable WAL bytes, explicit
durability ownership, and a clean path to task, CDC, and long-retention logs.

Gives up: presenting one magical log abstraction that simultaneously provides
consensus durability, transactional application semantics, object retention,
and exactly-once external effects.

## Questions for review

1. Is `LogState` sufficiently deep to justify a crate, or should the first
   boundary include a durable store interface?
2. Does allowing index zero in the core simplify consensus without weakening
   application-log contracts?
3. Should suffix replacement be one atomic core operation or an explicit
   truncate-plus-append command sequence?
4. Can `NodeJournal` delegate semantics without making `OKVR` recovery accept a
   history previously rejected?
5. Which identities must be shared now so a later object-log segment does not
   require a semantic migration?
