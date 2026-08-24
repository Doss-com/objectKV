# PostgreSQL 18.6 objectKV mutable `smgr` probe

Status: `[COMPLETE]` first literal PostgreSQL existing-page write, objectKV
`nblocks`, bounded local page-service recovery, and checkpointer-driven stable
root publication through a replicated authority on 2026-08-24. This is not a
remote-object, authority-restart, host-loss, or production checkpoint result.

## Result

The probe pins PostgreSQL tag `REL_18_6`, exact commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`, and applies the cumulative
`postgres-18.6-smgr-write.patch`. One selected permanent relation routes its
main-fork `smgr_readv`, `smgr_startreadv`, `smgr_writev`, and `smgr_nblocks`
callbacks through objectKV. Those callbacks have no main-fork `md` fallback.
Non-main forks and relation lifecycle callbacks remain on `md`.

The mutable service bootstrapped the frozen 148-block, 1,212,416-byte relation
into an authority-bound Range Engine view at objectKV version 5. PostgreSQL
then:

1. read all 2,000 rows through objectKV;
2. changed row 3 to `objectkv-final-cell-write-v1`;
3. flushed PostgreSQL WAL;
4. invoked `smgr_writev` for native heap block 0 during `CHECKPOINT`;
5. committed that page plus the unchanged authoritative `nblocks=148` extent
   through the real three-process Cell;
6. advanced objectKV from version 5 to version 9;
7. rebuilt a fresh Range Engine from the verified committed mutation history;
8. restarted PostgreSQL at version 9; and
9. returned the changed row with MD5 `b28339449960a8fb027b080f2294a886`.

A follow-on run replaced the explicit version-discovery handshake with atomic
current-view selection. A zero expected version means "select the current
physical page-store view while beginning this operation". One backend started
at version 5, checkpointed row 6 through the Cell to version 9, then continued
in the same session. Its next block-count callback atomically selected version
9 and returned the exact 1,212,416-byte relation plus
`objectkv-atomic-current-v2` without a PostgreSQL restart or discovery request.
An explicit pinned version 5 request still refused at version 9.

A third run enabled the durable sidecar path. The service materialized the
version-5 relation into a closed SlateDB object base, recorded the exact
manifest and live-SST closure, and started two required signed txLog sets. Each
set used three independent local processes and required two matching records.
The first durable checkpoint changed row 7 to
`objectkv-durable-recovery-v1`, reached page-store version 9, and took
465.720 ms. Stopping PostgreSQL advanced the retained suffix to version 10.

The page service then stopped completely. Its restart configuration named a
nonexistent source heap, so recovery could only use the durable root. It
verified the full object closure, reconstructed four required-log
certificates for the two retained commits, opened a fresh Range Engine at
version 10, and returned row 7. The recovered service accepted another
PostgreSQL checkpoint, changed row 8 to
`objectkv-post-recovery-write-v1`, and advanced to version 11 in 561.758 ms.
After PostgreSQL shutdown produced version 12, a second complete service
restart recovered four authenticated txLog records and returned both changed
rows. This is empty process-memory recovery, not an empty OS or NVMe cache
measurement.

The local relation file retained SHA-256
`3770217fa7ca29da2d79580fa5fd68616a9257d6460801f0a1ade6cfc078d7e8`
before the update, after checkpoint, and after restart. The changed row was
therefore read from objectKV state, not a local heap-page write.

The next run connected PostgreSQL's native sync-request lifecycle to a
three-process publication authority. A dirty selected relation registers one
deduplicated objectKV sync request. After the checkpointer wrote row 9 through
physical version 13, it called the objectKV sync handler, selected the exact
recoverable base-plus-txLog frontier, wrote a content-addressed stable-root
manifest, and waited for replicated prepare, publish, and linearizable
read-back. PostgreSQL completed the checkpoint only after authority term 3,
index 4 selected manifest
`193e84d3aec75b94a7098de8c20520197597552f6d629f482ad137ba8cecf070`.

The page service then stopped while PostgreSQL and the authority stayed up. A
new service process recovered version 13 without a source heap, verified the
authority-selected root, and the still-running PostgreSQL process returned rows
7, 8, and 9. With the authority subsequently unavailable, the next checkpoint
wrote row 10 to the signed txLogs at hot version 14 but timed out in the sync
handler. PostgreSQL reported `checkpoint request failed`; the stable frontier
remained exactly version 13 at the same authority revision and manifest. This
is the required hot-versus-stable separation.

## Executed write path

```text
PostgreSQL UPDATE
  -> PostgreSQL WAL record
  -> PostgreSQL WAL flush
  -> CHECKPOINT selects dirty heap block 0
  -> smgr_nblocks
  -> expected version 0 atomically selects current objectKV view
  -> objectKV extent read returns 148 and selected version
  -> smgr_writev
  -> GetFlushRecPtr() at the callback boundary
  -> fixed-width binary request plus native 8 KiB page
  -> WAL-before-page admission
  -> atomic page plus unchanged extent Cell transaction
  -> advancing Cell commit receipt
  -> append the exact committed Cell envelope to both required txLog sets
  -> require 2-of-3 durable records and signed attestations in each set
  -> authenticate immutable object base plus certified txLog suffix
  -> construct a fresh authority-bound Range Engine
  -> verify nblocks=148 through that new view
  -> publish version 9 and page-LSN frontier
  -> acknowledge PostgreSQL page write
  -> register one deduplicated objectKV relation sync request
  -> PostgreSQL checkpointer processes the sync request
  -> derive exact recoverable base plus certified txLog frontier B
  -> require PostgreSQL WAL flush LSN >= maximum page LSN through B
  -> persist content-addressed stable-root manifest
  -> replicated authority prepare + publish + linearizable read-back
  -> acknowledge PostgreSQL checkpoint sync
```

The service bootstrap is deliberately not sent through the JSON-encoded Cell
process fixture. Doing so exposed extreme prototype journal write amplification
for a 1.2 MB relation. The frozen base is materialized directly at version 5;
every PostgreSQL mutation under test crosses the real Cell commit path and both
required txLog sets. This avoids mistaking a debug JSON transport artifact for
the intended txLog and object data path.

## Durable sidecar recovery

```text
first start
  -> import frozen heap pages at objectKV version 5
  -> flush and close one SlateDB base
  -> inspect and fsync exact manifest plus live-SST closure
  -> persist local PostgreSQL relation/base descriptor
  -> start signed txLog sets 10 and 20, each 3 nodes with quorum 2

accepted page write
  -> Cell returns one exact committed envelope
  -> append that envelope to every node in both required txLog sets
  -> require quorum durability and quorum signatures in both sets
  -> authenticate base plus complete certified suffix
  -> publish the fresh in-process Range Engine view

service restart
  -> do not read the source heap
  -> verify every object named by the frozen physical closure
  -> recover one unique quorum record at each retained txLog position
  -> rebuild and verify every required-log certificate
  -> authenticate the exact Cell envelope chain from base through target
  -> replay those envelopes byte-for-byte into the bounded Cell baseline
  -> open the recovered page-store view and accept subsequent writes
```

The local `postgres-root.json` and `range-base.json` files are bootstrap inputs,
not replicated publication-authority receipts. Replaying envelopes into a
deterministic Cell baseline is also not a production transaction-generation
recovery protocol. Those two boundaries are why this result admits bounded
local sidecar restart but not a checkpoint-stable production root.

## Version meaning and atomic current selection

The objectKV version in this bridge is a physical page-store flush version. It
is not PostgreSQL's SQL snapshot, transaction ID, tuple MVCC horizon, or WAL
LSN. PostgreSQL remains the logical transaction and visibility authority.

Selected main-fork callbacks send expected version 0. The sidecar resolves zero
to its current immutable reader, physical version, and page-LSN frontier under
one state lock. Reads and block counts retain that selected reader after the
lock is released. Existing-page writes retain the lock through admission, Cell
commit, fresh Range Engine construction, and publication. There is no
discovery-to-operation interval.

Nonzero expected versions remain exact pinned requests. A nonzero request that
does not equal current is refused before reading or mutating state. PostgreSQL
updates its process-local physical version from each successful response for
diagnostics, but correctness does not depend on that cached value.

This is the correct probe contract, not the final concurrency design. Holding
one service mutex across a write commit makes selection atomic by serializing
writes. Production needs an immutable generation pointer for reads and a
short publication critical section around concurrently prepared commits.

## Controls

- `[COMPLETE]` stale objectKV version: version 5 was refused after the service
  advanced to version 9. The refusal reported the current version and did not
  change state.
- `[COMPLETE]` page before WAL: a fork-only test switch forced the callback WAL
  frontier to one less than the dirty page LSN. PostgreSQL's checkpoint failed
  with typed `WalBehindPage { block_number: 0, ... }`; the service stayed at
  version 5 with zero committed write batches and one Range Engine view.
- `[COMPLETE]` no local heap fallback: the selected main-fork heap file kept
  the same SHA-256 across the accepted checkpoint and restart.
- `[COMPLETE]` authoritative extent: PostgreSQL reported 1,212,416 bytes from
  objectKV `nblocks=148` after restart.
- `[COMPLETE]` atomic current selection: a backend that began at version 5
  selected the checkpointer's version 9 in its next block-count operation and
  completed the same-session query. The earlier discovery operation was
  removed; protocol operation 4 now has the distinct stable-sync contract.
- `[COMPLETE]` source-independent service restart: two service restarts used
  `/tmp/objectkv-source-must-not-be-read`, which did not exist. The second
  restart recovered page-store version 12, four authenticated txLog records,
  authoritative `nblocks=148`, and both post-base row changes.
- `[COMPLETE]` required txLog quorum: a disposable durable-root copy retained
  only one historical node in txLog set 10. Startup refused with
  `durable PostgreSQL recovery found no unique txLog quorum`.
- `[COMPLETE]` complete object closure: a second disposable copy omitted its
  exact live SST. Startup refused while verifying that named physical object.
- `[COMPLETE]` checkpoint stable publication: the PostgreSQL checkpointer
  published hot version 13 as a recoverable authority root at term 3, index 4,
  then completed its sync phase.
- `[COMPLETE]` authority-selected restart: a new page-service process recovered
  version 13 from a nonexistent source path and reconciled the exact authority
  revision and manifest before serving the still-running PostgreSQL process.
- `[COMPLETE]` false checkpoint refusal: after authority loss, hot txLog state
  advanced to version 14 but `CHECKPOINT` failed and stable version 13 did not
  advance.

## Early performance indicator

The final atomic-current one-page checkpoint took 688 ms in a Rust debug build.
The preceding exact-version run took 678 ms. The durable checkpoint before
service restart took 465.720 ms; the first checkpoint after recovery took
561.758 ms. These are shape measurements, not a target or ceiling. They include
a fresh TCP connection, JSON-encoded Cell command, prototype Raft journal,
six synchronous txLog appends and attestations, full in-memory Range Engine
reconstruction, and debug binaries.

The first authority-published checkpoint took 829 ms end to end: 669 ms in the
write phase and 160 ms in PostgreSQL's sync phase. The authority-outage control
failed after the prototype's five-second socket timeout. These are useful
pipeline decompositions, not performance targets. The stable path currently
performs serial local process and fsync work and holds the page-service state
lock across authority I/O.

The result says the semantics can be joined. It does not yet say the design is
fast. The next performance curve must separately measure:

- resident overlay write and read latency without full view reconstruction;
- binary txLog transport and group commit at 1, 8, 32, and 128 pages;
- checkpoint throughput with concurrent backends;
- immutable generation publication and concurrent writer cost;
- current-view selection under read and write contention;
- warm RAM, warm NVMe, and empty-cache object-read latency; and
- objectification lag versus retained txLog bytes.

## Reproduction inputs

Build and run the service with an exact JSON configuration:

```text
target/debug/okv-postgres-write-service --config-json '{...}'
```

The PostgreSQL fork receives:

```text
OKV_SMGR_TABLESPACE
OKV_SMGR_DATABASE
OKV_SMGR_RELATION
OKV_SMGR_PAGE_HOST
OKV_SMGR_PAGE_PORT
OKV_SMGR_OBJECTKV_VERSION
OKV_SMGR_MAX_PAGE_LSN
```

The sidecar JSON adds optional `durable_root`. On first start it imports the
source relation and creates the object base. When `postgres-root.json` already
exists, startup skips the source file and requires the authenticated object
base and txLog suffix instead.

An optional `publication_authority` block supplies the replicated endpoints,
transaction-system generation, transaction-system identity, and destination
root. `okv-postgres-stable-authority` is the bounded external harness used by
this proof. Its optional `txlog_pop` block pins the authority signer keys,
quorum, authority Cell identity, and pop epoch.

Stable sync also requires a `transaction_authority` block naming a live
external Cell endpoint set and exact Cell, tenant, and generation identity.
`okv-postgres-transaction-authority` supplies the bounded harness. It remains
alive across page-service restart so selected object bases can replace popped
txLog history without resetting the transaction version.

`OKV_SMGR_FORCE_WAL_BEHIND` exists only for the negative control. Run this
probe with `io_method=sync`. Apply the patch only to the pinned PostgreSQL
revision. The cumulative patch SHA-256 is
`2357715c8a165131afe9e14b5496093ee843976bd459ef075db65ef76a1c1e4f`.

## Admission boundary

This admits the literal existing-page write and `nblocks` seams, PostgreSQL
process restart, PostgreSQL sync-handler wiring, complete single-relation
objectification, replicated stable-root selection, authority-pinned pop across
both required txLog sets, and bounded zero-tail local page-service restart. It
does not admit:

- authority process restart, authority state persistence outside its scratch
  harness, remote object storage, host loss, or empty OS/NVMe-cache recovery;
- incremental objectification, database-wide checkpoint closure, object
  collection, or production WAL recycling safety;
- `smgr_extend`, truncate, unlink, create, or zero-extend;
- non-serial concurrent writers and retained historical page-store views;
- txLog repair, policy rotation, independent txLog ownership, or
  independent-host failure domains;
- production binary transport, group commit, shared overlay mutation, or AIO;
- multi-relation, multi-database, temporary, or unlogged semantics;
- crash recovery, PostgreSQL regression compatibility, or OTel export; or
- a performance claim beyond the measured debug callback shape.

The next blocker is replacing checkpoint-captured full-relation rewrites with
incremental range or delta objectification, aggregating every relation fork in
one database root, persisting and recovering both authorities, and verifying an
empty-cache remote restore. In parallel, replace serialized write publication
with immutable-generation compare-and-swap before measuring concurrent
PostgreSQL curves.

The objectified proof used a three-page relation. Its first full checkpoint
took 1.980 seconds, including 703 ms in sync. A no-new-page checkpoint that
rebuilt and published base 12 took 440 ms, including 435 ms in sync. A later
one-dirty-page checkpoint through base 13 took 810 ms, including 448 ms in
sync. These are debug full-rewrite measurements, not a performance target.

## Checkpoint-captured objectification follow-up

`[EXISTS]` Candidate `54d2510` first allowed stable target `B` to publish from
older immutable base `O` plus the complete certified txLog suffix `(O, B]`.
txLog pop remained capped at `O`, and replacement page compute recovered from
that root without the source heap. The candidate was retained as safety
evidence and rejected as the performance shape because each page write could
schedule another full relation base. Publication-authority timeout also held
the bridge-state mutex and inflated one background materialization from about
400 ms to 6.4 seconds.

`[EXISTS]` Candidate `171b14c` captures the durable planner and immutable
reader only from PostgreSQL stable sync. Relation scanning and complete base
materialization then run without reacquiring the bridge-state mutex. The
single-flight worker keeps one running capture and at most one newest pending
capture. Page writes no longer schedule objectification.

The fresh three-page result published `B=9/O=5` in a 780 ms checkpoint and
materialized base 9 in 90 ms. The next 910 ms checkpoint activated base 9,
published `B=10/O=9`, and materialized base 10 in 75 ms. A replacement page
service whose source path did not exist recovered base 9 plus the one certified
version-10 record and returned both changed rows. The local heap SHA-256 stayed
`68fb78de5c5698e5b8ac78f85b70c1aa533861b80807771d3bca21c0c6f4f21c`.

With the publication authority unavailable, a separately committed row reached
hot version 11. PostgreSQL checkpoint failed after 5.41 seconds, the service
recorded a 6.044-second stable timeout, stable stayed at 10, pop stayed at 9,
and captured base 11 completed in 26 ms. This removes the prior objectifier to
authority-timeout coupling. A separate failed checkpoint committed three page
writes through hot version 14 but never reached stable capture and created no
new base. PostgreSQL also reported an out-of-extent page read in that control,
so it is not a successful multi-page compatibility result.

The current implementation remains a complete relation rewrite per captured
checkpoint, not incremental objectification. It does not collect bases 9, 10,
and 11, and stable authority I/O still holds the bridge-state mutex. The next
performance work is range or delta objectification, unreachable-base
collection, and immutable publication-state exchange under concurrent
multi-relation checkpoints.
