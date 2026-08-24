# Full PostgreSQL on objectKV

Status: `[EXISTS]` PostgreSQL 18.6 read, existing-page write, objectKV block
count, WAL-before-page admission, atomic page-plus-extent Cell commit, fresh
Range Engine reconstruction, checkpoint callback, PostgreSQL process restart,
immutable certified delta objectification above one complete relation base,
authority-pinned txLog pop, and zero-tail local page-service recovery while an
external Cell authority remains live. `[ACTIVE-WORK]` compact binary delta
encoding, provider-bound replacement-worker integrity, delta compaction, authority
recovery, database-wide roots, concurrent version publication, lifecycle,
remote objects, and crash recovery.

## Target

Run a real PostgreSQL compute process whose durable storage is provided by
objectKV, while preserving PostgreSQL protocol and behavior closely enough to use
PostgreSQL's own regression suite as the compatibility oracle.

## Three implementation shapes

### A. Page or storage-manager bridge

Map PostgreSQL relation/page identifiers onto ordered objectKV keys. PostgreSQL
keeps its parser, planner, executor, catalogs, heap/index formats, and most MVCC
behavior. objectKV supplies versioned durable storage and eventually remote
serving.

Optimizes for: fastest path to actual PostgreSQL semantics and extension
compatibility.

Gives up: immediate kernel-native rows/indexes and some of objectKV's transaction
model. This likely needs a PostgreSQL fork because the storage and WAL boundaries
are not fully replaceable by an extension.

### B. Table/index access method over objectKV

Implement custom table and index access methods while leaving system catalogs and
remaining PostgreSQL internals on conventional storage.

Optimizes for: a smaller fork and earlier logical key/value mapping.

Gives up: a complete objectKV-backed PostgreSQL instance. This is a hybrid, not
the full target.

### C. PostgreSQL-compatible SQL layer

Build or reuse a PostgreSQL-wire SQL frontend and map its logical transactions
directly to objectKV.

Optimizes for: a kernel-native architecture without page baggage.

Gives up: actual PostgreSQL implementation and extension semantics. Compatibility
becomes a large independent database project.

## Decided first choice

Prototype A first at pinned PostgreSQL 18.6 commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`. The source inventory proves that
this needs a maintained `smgr` fork because PostgreSQL has an internal storage
manager switch but no public registration hook.

PostgreSQL WAL, LSN, tuple MVCC, transaction status, checkpoint, and recovery
remain the sole commit authority. objectKV is a subordinate versioned page and
fork store. Never run PostgreSQL WAL authority and objectKV serializable commit
authority as peers for the same SQL transaction.

The exact boundary, non-relation state inventory, boot sequence, controls, and
staged plan are in
[`research/postgres-18-6-storage-bridge.md`](research/postgres-18-6-storage-bridge.md).
Treat B as a targeted experiment and C as a separate later decision.

## Direct-read prerequisite

`[EXISTS]` objectKV candidates `17d5a5d` and `b068256` provide the first
tenant-scoped direct-read client below this bridge. A point or ordered scan is
bound to one caller-selected `T`; stale routing causes a bounded RangeMap
refresh and a complete multi-range restart without obtaining a new version.
The process gate returns rows from both sides of a real split and discards a
control that changes `T` during retry.

`[EXISTS]` This direct-read substrate is sufficient for the bounded page-read
adapter below, not for a production storage manager. Writes, buffer
invalidation, PostgreSQL WAL ordering, checkpoint, fsync, and crash recovery
remain separate gates.

`[EXISTS]` Candidate `0871dec` adds `okv-postgres`. It maps cluster,
tablespace, database, relation, temporary backend, fork, and block identity to
one fixed-width ordered key. Page values carry format version, PostgreSQL page
LSN, PostgreSQL checksum metadata, payload length, SHA-256, and exactly 8 KiB
of page bytes. `PostgresPageReader` issues point and consecutive-block reads
through `KvReadClient` at one exact objectKV version and rejects page LSNs
above the independently selected PostgreSQL page frontier.

`[EXISTS]` Candidate `8fb20e5` closes that first end-to-end gate. One real
authority-bound view stores three encoded pages at objectKV version 1 and
advances block 8 through a certified txLog record at version 2. The client
begins with a stale unsplit route, refreshes to two ranges, preserves version
2, and returns all three authenticated pages under PostgreSQL page-LSN frontier
800. Correct run `977b368d` kept. Missing page `7256f045`, corrupted payload
`d8d0a2a5`, changed objectKV version `3332607a`, and page LSN ahead
`7dd9189d` discarded.

Three direct release samples put the process-warm 8 KiB point read at 247 to
277 microseconds, median 254 microseconds. The three-page vector read, including
one stale-route refusal, one map refresh, and two subrange scans, took 0.83 to
1.03 milliseconds, median 0.93 milliseconds. The fixture uses an in-memory
object store and fresh TCP plus JSON requests. It is not a cloud, concurrency,
or PostgreSQL executor performance result.

`[EXISTS]` Candidate `b04b128` closes the literal callback seam for one real
heap relation. A separate page-service process imports 148 native PostgreSQL
pages into an authority-bound objectKV view. A fresh PostgreSQL 18.6 process
uses its selected `smgr_startreadv` callback, `PostgresPageReader`, routed
`KvReadClient`, KV Runtime, and Range Engine to return the exact 2,000-row
aggregate. The selected callback never calls `mdreadv` or `mdstartreadv`.

The live controls stop the page service and change the fixed frontier. The
first fails with connection refusal despite the original relation file still
existing; the second fails with a typed frontier mismatch. PostgreSQL 18 uses
the AIO callback path even for `io_method=sync`, so the fork adds one narrow
immediate-completion helper for an already-finished non-file read.

The cold debug scan read 148 shared buffers in 233.045 ms through 13 outer TCP
requests and fresh inner TCP plus JSON routed calls. Its immediate repeat took
0.299 ms from PostgreSQL shared buffers. This identifies transport reuse,
binary batching, release builds, Range Engine caching, and real asynchronous
submission as the next read-curve work. It is not a production ceiling.

`[EXISTS]` Candidate `c3c5df9` closes the standalone WAL-before-page admission
gate. It requires a nonzero expected objectKV version, bounds each batch to 128
pages, refuses the complete batch if any page LSN exceeds PostgreSQL's durable
WAL frontier, and emits deterministic page mutations plus a domain-separated
SHA-256. Correct run `0bf18a75` keeps six mutations across three seeds. The
WAL-behind `118ba54b`, zero-version `ee71a5b4`, oversized-batch `b14da383`, and
wrong-digest `c74e05ad` subjects discard.

`[EXISTS]` Candidate `7de5c4e` binds each admitted page batch to one versioned
relation-fork extent key and one canonical Cell transaction. Extend begins at
the prior extent and writes its pages plus new block count atomically. Existing
writes cannot change the extent or reach beyond it. The request identity maps to
the Cell retry identity, and the verified response must preserve identity,
generation, a strictly advancing commit version, and a committed envelope.

Correct run `bb7e18fa` executes across 12 Cell process starts and three leader
handoffs. It commits six pages plus three extent values, resolves every duplicate
retry to the original response, and reads the exact state from each successor.
Missing extent `5816809e`, changed retry identity `247a6cdb`, wrong receipt
identity `68282231`, and non-advancing commit version `71d18d48` discard.

`[EXISTS]` Candidates `f89f8c1` and `402e0ae` route the selected PostgreSQL
18.6 main fork's `smgr_writev` and `smgr_nblocks` through a versioned mutable
service. The callback reads PostgreSQL's flushed WAL pointer, sends the native
8 KiB page, commits page plus unchanged extent through the real Cell, requires
a strictly advancing receipt, and publishes a fresh Range Engine only after
the committed page and `nblocks` are exact.

The literal run updated row 3, checkpointed block 0, advanced objectKV version
5 to 9, restarted PostgreSQL, and returned
`objectkv-final-cell-write-v1` with MD5
`b28339449960a8fb027b080f2294a886`. The local heap file remained byte-exact,
`nblocks=148` remained authoritative, a stale version 5 request refused, and a
forced WAL-behind checkpoint returned typed `WalBehindPage` without advancing
version 5. The one-page debug checkpoint took 678 ms.

`[EXISTS]` Atomic current-view selection now distinguishes objectKV's physical
page-flush version from PostgreSQL's logical transaction snapshot. Expected
version 0 selects the service's current immutable reader, physical version, and
page-LSN frontier as one operation. One backend began at version 5,
checkpointed through version 9, selected version 9 on its next block-count
callback, and returned the same-session result. No discovery request occurs.
Nonzero versions remain exact and fail stale. PostgreSQL remains the MVCC and
visibility authority.

`[EXISTS]` Candidate `3bb2783` makes that sidecar state recoverable in a bounded
local-process proof. First start imports the frozen relation at objectKV version
5, flushes and closes a SlateDB base, and fsyncs a descriptor naming the exact
manifest and live-SST closure. Each later Cell envelope is appended to signed
txLog sets 10 and 20. Each set has three independent local processes and
requires two matching durable records plus two valid attestations.

On restart, the service skips the source heap when `postgres-root.json` exists.
It verifies every named base object, requires one unique quorum history from
every required txLog set, rebuilds the certificates, authenticates the complete
base-plus-tail chain, and replays the exact envelopes into the bounded Cell
baseline. The live proof recovered version 10 from a nonexistent source path,
accepted a post-recovery checkpoint through version 11, then recovered four
tail records through version 12 on a second service restart. The local heap
file remained byte-exact. Removing the txLog quorum or the live SST from
separate disposable roots made startup fail closed.

`[EXISTS]` The pinned fork now adds `SYNC_HANDLER_OKV`. A successful permanent
page write registers one relation tag in PostgreSQL's normal sync-request queue.
The checkpointer's handler and explicit `smgr_immedsync` both request operation
4 from the sidecar. The sidecar captures current physical version `B`, requires
PostgreSQL WAL through the maximum page LSN, persists a content-addressed root
over the exact immutable base and certified txLog frontier, and waits for
replicated prepare, publish, and linearizable read-back.

The literal run published version 13 at authority term 3, index 4, then
completed a checkpoint in 829 ms, including 160 ms in sync. A fresh page
service recovered version 13 with no source heap and reconciled the same root
while PostgreSQL stayed up. When the authority stopped, a later page flush
reached hot version 14 but PostgreSQL refused the checkpoint and stable version
13 did not advance.

`[EXISTS]` Stable sync now requires a separate Cell transaction-authority
harness that outlives the disposable page service. At `B`, the page service
reads the complete relation at one physical version, materializes a new
versioned SlateDB base, binds its visible-row digest and PostgreSQL WAL
frontier, and atomically replaces one local root pointer. The replicated stable
manifest is also a relation-domain Cell snapshot, so the existing publication
capability protocol can authorize exact txLog deletion.

The proof published and popped both three-node txLog sets through version 11.
A new page service used a nonexistent source heap, recovered base 11 with zero
tail records, accepted later writes, published and popped versions 12 and 13,
and survived a second zero-tail restart. After the publication authority was
stopped, hot version 14 and its local object base were built, but PostgreSQL
checkpoint failed, stable remained 13, and every txLog node remained popped
only through 13.

`[EXISTS]` Candidate `e2c9dd5` separates replacement-worker view readiness from
the complete byte audit without weakening the existing production helper. In a
fresh process, an OS-warm 555.04 MB immutable closure became manifest-bound and
readable in 4.75 ms p50. Its first base point took 0.142 ms, its first
eight-page range took 0.621 ms, and bounded worker RSS was 61.9 MiB. The same
closure's full audit took 1.046 seconds and its complete oracle scan took 4.493
seconds. At 1.09 MB, readiness was 2.33 ms. The result shows that the previous
4.549-second restart number was dominated by deliberate whole-relation work,
not by the first OLTP read.

`[ACTIVE-WORK]` This does not admit lazy production serving. The current eager
helper still verifies the complete physical closure before returning. objectKV
must next bind GCS generation and checksum identity into the selected root, or
authenticate touched blocks before returning rows, then replay metadata-warm,
persistent-cache-warm, and cold-cache worker curves on GCS.

`[ACTIVE-WORK]` Complete relation rewrite remains on every stable-sync critical
path and under one page-service state lock. Historical bases are not collected.
Both authority harnesses are ephemeral and same-host, txLog ownership is still
inside the page-service harness, and only one existing main-fork relation is
represented. Authority restart, incremental background objectification,
database-wide checkpoint closure, and remote empty-cache recovery remain open.

`smgr_extend`, truncate, unlink, crash recovery, remote object I/O, production
AIO, and OTel export remain unadmitted.

## Bridge questions to answer

1. How do PostgreSQL relation, fork, block, and tablespace identities map to keys?
2. How are PostgreSQL LSNs related to objectKV commit versions without pretending
   they are the same clock?
3. Which WAL remains PostgreSQL recovery state, and which durability belongs to
   objectKV?
4. Does one transaction system own commit, or is one strictly subordinate?
5. How are page flush, checkpoint, fsync, and crash-recovery callbacks redirected?
6. Which catalog/bootstrap files must remain local before objectKV is available?
7. How do temporary and unlogged relations avoid unnecessary object persistence?
8. What extension and replication features become unsupported in the first
   bridge?

## Compatibility eval

Each result records the exact PostgreSQL revision, objectKV revision, bridge
revision, backend, seed, and machine profile.

Hard gates:

- PostgreSQL boots and initializes a cluster.
- `CREATE TABLE`, insert, update, delete, index creation, transaction rollback,
  and restart preserve expected results.
- Kill/restart never exposes acknowledged data loss or impossible state.
- The selected PostgreSQL regression subset has zero unexpected failures.
- objectKV correctness evals remain green.

Later suites add concurrency, vacuum, crash recovery, extensions, logical
replication, PITR, and upgrade compatibility. Passing a small subset must never be
described as full compatibility.

The full proposed machine-readable gate is
`evals/suites/postgres-page-bridge.toml`. The admitted callback slice is frozen
separately in `evals/suites/postgres-smgr-write-process.toml`. Both preserve
separate PostgreSQL WAL, objectKV page, and checkpoint frontiers; OTel export is
still unexecuted for the literal local run.

## HTAP bridge

The first analytical representation remains Parquet. A materializer consumes an
authoritative commit/version interval and publishes a columnar snapshot with an
explicit covered-through version. Queries combine that base with the complete
durable analytical tail through one target `T`, wait within policy, or return
`snapshot_unavailable`. They never substitute the recovery WAL or return a
mixed-version result.

Vortex becomes an experiment only after this version contract works with Parquet.
