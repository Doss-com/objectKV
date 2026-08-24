# PostgreSQL 18.6 objectKV storage bridge

PostgreSQL can use objectKV as a relation-page substrate without creating a
second commit authority. The first compatible implementation requires a small
PostgreSQL fork, keeps PostgreSQL WAL, LSN, tuple MVCC, and transaction status
authoritative, and makes objectKV page persistence subordinate to PostgreSQL's
WAL and checkpoint barriers.

## Abstract

This memo pins the first PostgreSQL bridge to upstream tag `REL_18_6`, commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`. It identifies the minimum relation
storage boundary, the state outside that boundary, the boot and recovery
sequence, and the evaluation gates that must pass before a maintained fork is
admitted.

Status: `[COMPLETE]` source inventory and bridge decision. `[EXISTS]` literal
read, existing-page write, block count, WAL-before-page ordering, bounded local
sidecar recovery, checkpointer-driven replicated stable-root selection,
lagging-base plus certified-tail recovery, checkpoint-captured complete
single-relation objectification outside the foreground mutex, and root-pinned
txLog pop. `[ACTIVE-WORK]` lifecycle, authority recovery, incremental
objectification, database-wide roots, remote recovery, production AIO, and
PostgreSQL compatibility.

## Implementation update, 2026-08-24

The maintained fork now uses PostgreSQL's own dirty-file lifecycle. Selected
permanent writes enter a subordinate Cell transaction and two required signed
txLog sets, then register a deduplicated `SYNC_HANDLER_OKV` tag. During
`ProcessSyncRequests`, the checkpointer asks the sidecar to publish its exact
recoverable frontier through a three-process publication authority.

The first literal run selected objectKV version 13 at authority term 3, index 4
before PostgreSQL completed the checkpoint. A page-service restart recovered
that version without a source heap and reconciled the exact root. Removing the
authority before the next checkpoint allowed hot txLog state to reach version
14, but PostgreSQL refused the checkpoint and the stable root stayed at version
13.

The next follow-up separates the Cell transaction authority from disposable
page compute. The synchronous precursor materialized the complete relation at
stable `B`, published that closure, and used its capability to pop both
required txLog sets. It proved zero-tail recovery and the deletion protocol,
but rejected full relation rewrite on the checkpoint critical path.

Candidate `171b14c` now lets stable target `B` publish from immutable base
`O <= B` plus the complete certified suffix `(O, B]`; pop is capped at `O`.
Checkpoint capture schedules complete relation objectification from immutable
owned inputs outside the foreground mutex, and a later checkpoint may activate
the ready base. A three-page proof published `B=9/O=5`, then `B=10/O=9`, and
recovered from base 9 plus one record without a source heap. Materialization
took 90 ms and 75 ms. During authority outage, base 11 completed in 26 ms while
stable sync waited 6.044 seconds to fail. Both authority harnesses remain
ephemeral and same-host; objectification remains a complete relation rewrite
per captured checkpoint. This is a bounded semantic proof, not a production
WAL-recycling boundary.

## Context

The target is full PostgreSQL behavior with durable bytes on object storage,
not a PostgreSQL-wire-compatible database. A table access method changes tuple
and scan semantics, while a storage-manager bridge preserves PostgreSQL's heap,
index, buffer, and MVCC implementation. The bridge is still only one stage
because PostgreSQL stores critical non-relation state outside the relation
storage manager.

## Verified upstream surface

The following findings are from the exact pinned source revision.

| Surface | Verified fact | Consequence |
| --- | --- | --- |
| Storage manager | [`f_smgr`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/smgr/smgr.c#L78-L126) covers create, unlink, extend, vectored read and write, truncate, sync, and PG18 asynchronous I/O. | This is the smallest physical relation-page boundary. |
| Registration | [`smgrsw`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/smgr/smgr.c#L128-L154) is static and contains only `md`; there is no runtime storage-manager registration hook. | A normal extension cannot install the bridge. A fork is unavoidable. |
| Relation selection | The [storage-manager README](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/smgr/README#L6-L34) says the old per-relation manager ID is gone and suggests tablespace association if multiple managers return. | The first spike should use one explicit test switch. A production fork should select by tablespace. |
| Write ordering | [`FlushBuffer`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/buffer/bufmgr.c#L4288-L4397) flushes WAL through the page LSN before calling `smgrwrite` for permanent pages. | The bridge must never make a page durable ahead of the WAL record that explains it. |
| Commit authority | [`RecordTransactionCommit`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xact.c#L1418-L1529) writes the commit record, flushes `XactLastRecEnd` for synchronous commit, then marks `pg_xact`. | PostgreSQL WAL remains the sole commit acknowledgement authority in the first bridge. |
| Checkpoint | [`CreateCheckPoint`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L6929-L6973) prepares storage sync; [`BufferSync`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/buffer/bufmgr.c#L3357-L3405) writes the selected dirty pages. | WAL retention may advance only after objectKV has honored the corresponding stable-storage barrier. |
| AIO | [`smgrfd`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/smgr/smgr.c#L981-L1097) reopens an operating-system file descriptor for AIO workers. | An object backend needs its own PG18 AIO target behavior or an explicit synchronous fallback. Returning a fake descriptor is unsafe. |
| Table AM | [`TableAmRoutine`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/access/tableam.h#L277-L345) owns scans and tuples, plus [DDL and VACUUM callbacks](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/include/access/tableam.h#L579-L654). Heap DDL still calls relation storage in [`heapam_handler.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/heap/heapam_handler.c#L582-L654). | A table AM does not replace relation storage, indexes, catalogs, WAL, or transaction status. It is not the full PostgreSQL path. |

## Definitions

- **PostgreSQL authority**: WAL, LSN, transaction IDs, tuple MVCC, `pg_xact`,
  checkpoints, and recovery rules that decide whether a SQL transaction
  committed.
- **Page bridge**: the forked `smgr` implementation that maps PostgreSQL
  relation forks and blocks onto objectKV storage operations.
- **Page frontier**: the highest PostgreSQL LSN for which the page service has
  durably closed every required relation write behind the relevant barrier.
- **System state**: durable PostgreSQL state outside relation forks, including
  WAL, control state, SLRUs, two-phase state, and replication slots.

## Proposed bridge

### One authority

`[DECIDED]` PostgreSQL owns SQL commit. objectKV may run its subordinate OCC,
resolver, commit-version, and txLog path for physical page writes, but that
receipt cannot decide SQL visibility or PostgreSQL transaction commit.

```text
SQL transaction
  -> PostgreSQL heap and index changes
  -> PostgreSQL WAL record and LSN
  -> PostgreSQL WAL durable flush
  -> objectKV page write or later page flush
  -> objectKV stable barrier at checkpoint
  -> PostgreSQL checkpoint may advance
```

This optimizes for PostgreSQL correctness and compatibility. It gives up using
the native objectKV transaction protocol as the SQL commit protocol in the
first phase.

### Page identity and operation contract

The logical page identity is:

```text
(cluster, tablespace_oid, database_oid, rel_number,
 temp_backend, fork_number, block_number)
```

Every stored page also carries its PostgreSQL page LSN, checksum metadata, and
bridge format version. The bridge must implement the full `f_smgr` behavior,
including create, exists, unlink, extend, zero-extend, vectored reads and writes,
block count, truncate, immediate sync, registered sync, temporary relations,
unlogged init forks, and recovery-tolerant idempotence.

One 8 KiB page must not become one object-store object. The serving path writes
to a hot overlay, batches pages into immutable segments, and publishes verified
manifests behind the storage-manager contract. Object layout is invisible to
PostgreSQL.

`[EXISTS]` Candidate `0871dec` implements the first read-only form of this
contract in `okv-postgres`. The ordered key is fixed width after a versioned
prefix. The value authenticates exactly 8 KiB of bytes and retains PostgreSQL
page LSN and checksum metadata. The reader keeps the objectKV snapshot version
and maximum admitted PostgreSQL page LSN as separate fields. Unit controls
refuse payload corruption, missing vector blocks, and a page LSN beyond the
selected frontier. Candidate `8fb20e5` then runs three encoded pages through a
real authority-bound Range Engine, independent KV Runtime, stale-route refresh,
and two-range vector read at fixed objectKV version 2. Four missing, corruption,
version-drift, and LSN-frontier controls discard.

Candidate `b04b128` invokes this reader from the pinned PostgreSQL fork. One
actual 148-page heap is imported into an immutable objectKV view. After restart,
PostgreSQL reads every block through a separate service, the routed KV Runtime,
and `smgr_startreadv`, then returns the exact 2,000-row aggregate. The selected
relation has no `mdreadv` or `mdstartreadv` fallback. Service-unavailable and
changed-frontier controls refuse.

Candidate `c3c5df9` adds the first write-side admission boundary below the
future callback. A permanent page batch carries the exact expected objectKV
version, PostgreSQL's observed durable WAL frontier, and a stable request
identity. The gate refuses zero version, more than 128 pages, block overflow,
or any page LSN above the WAL frontier before it produces a mutation. The
admitted mutation bytes receive a domain-separated SHA-256. This is not yet a
transaction-system commit or a PostgreSQL write callback.

Candidate `7de5c4e` adds the next boundary. A versioned extent key stores one
relation fork's authoritative block count. Extend plans require the page batch
to begin exactly at the prior extent, and existing writes cannot change or
cross it. Pages and extent enter one canonical Cell transaction whose retry
identity derives from the PostgreSQL request. A real three-process Cell commit,
duplicate retry, leader death, successor election, and exact post-failover
state now pass. This still sits below the literal PostgreSQL write callback and
above no fresh Range Engine reconstruction or checkpoint barrier.

### Stable-storage barrier

`smgrwrite` may acknowledge buffered acceptance, as the filesystem manager does,
because PostgreSQL can redo pre-checkpoint pages from WAL. `smgrimmedsync` and
checkpoint sync cannot acknowledge until the exact required page set is durable
and readable after an empty-cache restart. A false barrier can let PostgreSQL
advance its redo point and recycle the only WAL that could repair a missing page.

The minimum receipt binds:

```text
cluster identity
PostgreSQL timeline
checkpoint or immediate-sync request identity
covered relation-fork writes and truncations
maximum covered page LSN
published object root
checksum
```

The bridge rejects an older page write over a newer page LSN and prevents a
pre-truncate segment from resurrecting blocks.

## State outside `smgr`

A relation-page bridge is not a full object-native PostgreSQL cluster. The
following state uses separate PostgreSQL paths:

| State | Pinned source | First-stage treatment |
| --- | --- | --- |
| WAL and timelines | [`xlog.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L2773-L2913) | Local or replicated durable volume. It remains authoritative. |
| `pg_control` and checkpoint metadata | [`xlog.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/xlog.c#L573-L623) | Local durable state with an explicit backup and restore contract. |
| Transaction status | [`clog.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/clog.c#L800-L820) | Keep PostgreSQL SLRU behavior. |
| Multi-transaction status | [`multixact.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/multixact.c#L2125-L2150) | Keep PostgreSQL SLRU behavior. |
| Subtransactions, commit timestamps, and serializable state | [`subtrans.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/subtrans.c#L235-L255), [`commit_ts.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/commit_ts.c#L545-L565), [`predicate.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/storage/lmgr/predicate.c#L800-L825) | Keep PostgreSQL behavior and inventory recovery requirements separately. |
| Prepared transactions | [`twophase.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/access/transam/twophase.c#L34-L55) | Keep `pg_twophase` durable or disable prepared transactions in the first spike. |
| Replication slots | [`slot.c`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/replication/slot.c#L2280-L2462) | Keep durable locally or disable persistent slots in the first spike. |

`[FUTURE]` A stateless-compute PostgreSQL cell must remote-durably store WAL and
publish a consistent system-state recovery bundle. That still does not give
objectKV independent commit authority. The bundle is a physical representation
of PostgreSQL authority.

## Boot and recovery sequence

1. `initdb` creates PostgreSQL system state and bootstraps catalogs.
2. The fork loads the objectKV bridge configuration before relation access.
3. The startup process calls [`StartupXLOG`](https://github.com/postgres/postgres/blob/724edf9bde9d356724ad384a2e196edc3c9f80f7/src/backend/postmaster/startup.c#L211-L264).
4. Recovery reads relation pages through the bridge and applies PostgreSQL WAL.
5. Recovery completes its checkpoint and makes normal backends available.
6. Buffer reads and writes flow through `smgr`; commit acknowledgements continue
   to flow through PostgreSQL WAL.
7. Checkpoint completion waits for the bridge's durable-page receipt.

The bridge must be available before catalog relation reads. Therefore the first
implementation uses static configuration and credentials, not catalog-defined
configuration that requires the bridge to read.

## Evaluation plan

The configurable suite is `evals/suites/postgres-page-bridge.toml`. Every run
records exact PostgreSQL, objectKV, and fork revisions; profile and backend
hashes; seed; object request and byte counts; compatibility cases; operation
latencies; WAL, page, and checkpoint frontiers; and OTel trace, metric, and log
receipts.

The first executable lane must include these positive cases:

- `initdb`, server boot, catalog reads, and clean restart.
- Heap and B-tree create, insert, update, delete, rollback, truncate, drop, and
  vacuum.
- Temporary relation isolation and unlogged relation reset.
- Checkpoint, immediate sync, kill during write, kill during checkpoint, and WAL
  replay from an older object root.
- Selected upstream regression and recovery tests with zero unexpected failures.
- Empty-cache reconstruction from an exact published root.

The suite must discard at least these controls:

- page durability acknowledged before the page's WAL is durable;
- false checkpoint barrier;
- stale page LSN overwrites a newer page;
- truncated blocks resurrect from an older segment;
- relation unlink or create is applied on the wrong transaction outcome;
- one required fork is omitted;
- empty-cache recovery depends on a process-local cache;
- objectKV transaction status disagrees with PostgreSQL commit status.

Performance is recorded but does not select a design until the semantic and
recovery gates pass.

## Dispatch-seam probe

`[COMPLETE]` The exact pinned source compiles and boots with a second static
`f_smgr` slot selected by an explicit test-only environment switch. The probe
passed `initdb`, catalog bootstrap, heap and B-tree mutation, rollback,
checkpoint, clean shutdown, restart, and exact row recovery. The patch and
reproduction record are in `experiments/postgres-smgr-probe/`.

Every callback in the probe delegates to PostgreSQL's existing `md` manager.
The result admits the maintained-fork dispatch seam only. It is not evidence of
objectKV persistence, remote barriers, empty-cache recovery, AIO behavior, or
state recovery outside `smgr`.

## Literal read-callback probe

`[COMPLETE]` Candidate `b04b128` replaces the selected relation's synchronous
read callback family with a bounded binary page-service request. Exact
tablespace, database, relation, fork, block range, objectKV version, and maximum
page LSN cross the process boundary. The service authenticates page values and
returns only native PostgreSQL page bytes.

The first live query exposed a PostgreSQL 18 constraint that the source
inventory alone did not settle: `io_method=sync` still enters
`smgr_startreadv`. The probe fork therefore adds
`pgaio_io_complete_readv_synchronously`, which completes the existing upper
buffer AIO callback chain after the non-file storage manager has synchronously
filled the buffers. This is a narrow proof seam. Production objectKV reads need
their own asynchronous operation and cancellation behavior.

The cold debug scan took 233.045 ms for 148 buffers through 13 callback
requests. The immediate shared-buffer repeat took 0.299 ms. The wide gap is the
expected architectural shape, but the cold number is not acceptable as a
target. It includes fresh outer TCP, fresh inner TCP plus JSON, a debug Rust
binary, and no remote object store or concurrent load.

Patch, protocol, reproduction, controls, exact limits, and early performance
evidence are in `experiments/postgres-smgr-read-probe/`.

## Convictions

1. PostgreSQL WAL is the only commit authority in the page-bridge phase.
2. The first full-compatibility path is a maintained `smgr` fork, not a table AM
   and not a PostgreSQL-wire reimplementation.
3. A relation bridge is an intermediate milestone, not evidence that all
   PostgreSQL durable state is object-native.

## Open questions

1. How should a production fork bind a storage manager to tablespaces without
   changing relation identity or bootstrap behavior?
2. How should PG18 asynchronous I/O execute when the physical target is an
   objectKV request rather than an operating-system file descriptor?
3. How should checkpoint receipts encode a bounded page set without producing a
   receipt proportional to every dirty buffer?
4. How should PostgreSQL WAL become remotely durable while retaining PostgreSQL
   LSN and commit authority?
5. How should system-state bundles bind WAL, `pg_control`, SLRUs, two-phase state,
   and replication slots to one recoverable timeline?

## Milestones

- `[COMPLETE]` M0, compile and boot a pinned PG18.6 fork through a second `smgr`
  dispatch slot that delegates to `md`.
- `[COMPLETE]` M1, map and decode relation pages through `okv-postgres`, replace
  the pinned fork's selected read callback family, execute one real relation
  scan, and discard unavailable-service plus changed-frontier controls.
- `[COMPLETE]` M1b-a, admit deterministic page mutations only after PostgreSQL
  WAL reaches every page LSN, with four poison controls.
- `[COMPLETE]` M1b-b, commit pages and authoritative relation extent atomically
  through a real Cell, with duplicate retry and leader handoff.
- `[ACTIVE-WORK]` M1b-c, add literal write and block-count callbacks, a tagged
  txLog certificate, and fresh Range Engine reconstruction for one relation.
- `[PROPOSED]` M2, pass the semantic and crash controls on local filesystem-backed
  objectKV segments.
- `[PROPOSED]` M3, run the same admitted suite against MinIO and protected GCS.
- `[FUTURE]` M4, remote-durable PostgreSQL WAL and a consistent system-state
  recovery bundle.

## Decisions log

| ID | Audit | Decision | Tradeoff |
| --- | --- | --- | --- |
| P1 | unaudited | Keep PostgreSQL WAL and MVCC as the sole commit authority. | Preserves PostgreSQL correctness, gives up native objectKV transaction commits for page writes. |
| P2 | unaudited | Use a maintained `smgr` fork for the first literal PostgreSQL milestone. | Preserves heap and index behavior, accepts an upstream maintenance burden. |
| P3 | unaudited | Treat non-`smgr` state as a separate object-native recovery phase. | Prevents a false full-object-native claim, delays stateless compute. |
| P4 | candidates `0871dec`, `8fb20e5` | Keep objectKV snapshot version and PostgreSQL page LSN as separate frontiers in the page reader. | Prevents clock conflation, requires a bridge root that binds both frontiers. |
| P5 | candidate `b04b128` | Complete synchronous non-file reads through PostgreSQL's existing upper AIO callback chain; do not delegate selected reads to `md`. | Proves the literal seam now, accepts a fork-only helper and defers production asynchronous submission. |
| P6 | candidate `c3c5df9` | Carry PostgreSQL's durable WAL frontier into the objectKV effect boundary and refuse the complete page batch before producing mutations when WAL is behind. | Makes WAL-before-page testable, adds protocol metadata and leaves commit plus stable storage as separate receipts. |
| P7 | candidate `7de5c4e` | Change relation-fork extent and pages in one Cell transaction, and derive Cell retry identity from the PostgreSQL request identity. | Prevents extent/page split state and resolves lost replies, while leaving tagged txLog and stable object publication as later receipts. |
