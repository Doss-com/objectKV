# RFC-0027: Authority-owned retained transaction stream

- Status: `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: one-cell recovery suffix reads

## Decision

The replicated data state machine owns a product-facing retained transaction
stream. A `ServingWorker` reads this stream through a linearizable RPC. It never
opens an OpenRaft journal, depends on a journal file layout, or treats one Raft
node's local bytes as the committed transaction history.

Each retained record contains:

```text
commit_version
transaction command
```

Only accepted transactions enter the stream. Conflict and validation failures
do not. Commit versions are strictly increasing but need not be contiguous,
because membership and control entries may consume Raft log indexes.

## Read contract

The first wire format is `retained-transaction-read-v1`:

```text
request
  after_version_exclusive
  through_version_inclusive: optional
  max_records: 1..4096

response
  format_version = 1
  retention_floor
  high_watermark
  target_version
  next_after_version
  complete
  records[]
```

The server executes a Raft linearizability barrier before reading state. An
omitted target freezes `target_version` at the transaction high watermark seen
after that barrier. Subsequent pages name the frozen target explicitly, so
commits after the first page do not change the result set.

`retention_floor` means every accepted transaction with a commit version
strictly greater than the floor is available. A request whose cursor is below
the floor fails closed. A target above the high watermark fails closed.

If a page is incomplete, `next_after_version` is its final record's commit
version. If complete, it equals `target_version`, including when Raft-index gaps
leave no record at the target value.

## Concurrent replacement-worker catch-up

One bounded recovery attempt uses two frozen targets:

```text
objects through O
  -> catch up retained transactions (O, C0]
  -> writers continue and commit through C1
  -> catch up retained transactions (C0, C1]
  -> activate reads at exactly C1
```

The worker may require additional rounds in a future admission protocol when
the second suffix exceeds its activation budget. This gate proves that the
cursor and frozen-target semantics remain exact while commits continue. It does
not claim a complete serving lease or unlimited convergence under overload.

## Snapshot compatibility

The retained stream and retention floor are serialized into OpenRaft state
machine snapshots. Their fields use default-on-read and omit-empty encoding so
the frozen pre-stream snapshot remains readable and its empty-state bytes do
not change. A new non-empty fixture freezes the first retained-record encoding.

An implementation may later replace the in-snapshot vector with object-backed
segments or a separate replicated log state machine. It must preserve this read
contract, retention-floor meaning, and record identities.

## Required invariants

1. Only a transaction response with `Committed { commit_version }` creates a
   record at that exact version.
2. Deduplicated request replay never creates a second record.
3. Records are strictly ordered by commit version.
4. A snapshot installation retains the same readable suffix.
5. Pagination returns each record at most once and cannot cross its frozen
   target.
6. Reads below the retention floor and above the current high watermark fail
   closed.
7. A worker can reconstruct `Database(C) = ObjectState(O) + Stream(O, C]`
   without reading any physical consensus journal path.

## Evaluation plan

G4.4 starts three real OpenRaft data-authority processes and two disposable
serving processes. The first worker is killed after its initial catch-up. The
replacement catches up through `C0`, pauses, observes deterministic concurrent
commits, catches up through `C1`, and serves exact held-out point reads at `C1`.

The same-correctness control fully hydrates the object base. The poison stops
after `C0`; its held-out concurrent update, deletion, insertion, and range clear
must be detected. Receipts record stream requests, returned records and bytes,
catch-up rounds, concurrent commits, target lag, object I/O, process kills, and
semantic replay identity.

## Tradeoffs

D1. Retain logical committed commands instead of exporting raw Raft entries.
This decouples recovery clients from consensus storage and excludes rejected
commands. It temporarily duplicates command bytes in state-machine snapshots.

D2. Freeze targets per read sequence instead of streaming an unbounded moving
head. This gives exact pagination and bounded work. It requires an explicit
activation loop when writes outrun recovery.

D3. Use commit versions as cursors instead of dense offsets. This matches the
cell version model. Consumers must not interpret numeric gaps as missing data.

## Not claimed

- safe-pop coordination with every object-durable frontier;
- independent-machine or regional durability;
- bounded retained bytes under production ingest;
- recovery convergence under sustained overload;
- a production serving-lease handoff;
- range routing or multi-range recovery scheduling.
