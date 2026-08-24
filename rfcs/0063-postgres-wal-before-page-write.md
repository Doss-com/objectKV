# RFC-0063: PostgreSQL WAL-before-page objectKV writes

- Status: active work, literal write callback, checkpoint-captured single-relation objectification, lagging-base stable roots, root-pinned txLog pop, and bounded base-plus-tail local recovery admitted; incremental objectification, authority restart, database-wide roots, and remote recovery remain open
- Created: 2026-08-24
- Depends on: RFC-0005, RFC-0039, RFC-0058, RFC-0062

## Decision

`[DECIDED]` PostgreSQL WAL remains the only SQL commit and recovery authority
during the page-bridge phase. A permanent page mutation can enter objectKV only
after PostgreSQL reports WAL durable through that page's LSN. objectKV assigns
its own subordinate commit version after this check. The two clocks are never
converted or compared as if they shared a number line.

```text
PostgreSQL FlushBuffer(page)
  -> page_lsn = PageGetLSN(page)
  -> XLogFlush(page_lsn)
  -> wal_flush_lsn = GetFlushRecPtr()
  -> require wal_flush_lsn >= page_lsn
  -> smgr_writev(page batch, expected objectKV T, request identity)
  -> objectKV transaction commit at T+N
  -> hot page accepted receipt

PostgreSQL checkpoint sync
  -> capture all accepted page versions through B
  -> derive a recoverable immutable-base plus certified-txLog frontier at B
  -> persist a content-addressed bridge-root manifest
  -> replicated prepare + publish + linearizable read-back
  -> stable barrier receipt for the retained local frontier
  -> checkpoint may complete
```

`smgr_writev` acknowledgement means only that a WAL-covered page batch entered
the subordinate durable transaction path. It does not mean a checkpoint-stable
root exists. `[EXISTS]` `smgr_immedsync` and PostgreSQL's normal checkpointer
sync callback now invoke the stronger authority-selected stable barrier.

## Context and invariant

The literal PostgreSQL 18.6 probe now fills real shared buffers, routes selected
existing-page writes and block counts through objectKV, and registers selected
dirty relations with PostgreSQL's normal sync-request queue. PostgreSQL's
upstream `FlushBuffer` already calls `XLogFlush(page_lsn)` before `smgrwrite`,
but the bridge still carries and verifies that fact at its own effect boundary.

The invariant is:

```text
For every permanent page value accepted by objectKV:

postgres_wal_flush_lsn >= page_lsn

For every PostgreSQL stable-storage acknowledgement:

every required page mutation and extent change is readable from the
published bridge root after empty-cache restart.
```

PostgreSQL transaction commit does not wait for the page mutation. WAL is the
redo source until a later checkpoint establishes the stable root.

## Proposed contract

### Write admission

`[EXISTS]` `PostgresPageWriteBatch` carries:

- the first physical page identity and consecutive native pages;
- the exact objectKV version against which the batch was prepared;
- PostgreSQL's observed durable WAL frontier;
- a caller-stable 128-bit retry identity.

`admit_postgres_page_write` refuses the complete batch before producing any
mutation when:

- the expected objectKV version is zero;
- the page batch is empty or exceeds 128 pages;
- the block range overflows;
- any page LSN exceeds the PostgreSQL WAL flush frontier.

On success it produces deterministic `CellMutation::Set` values in page order,
the maximum page LSN, and a SHA-256 over the full admitted mutation batch. It
does not assign an objectKV commit version or claim persistence.

### Subordinate commit

`[EXISTS]` The page service submits the admitted mutation batch through the
normal objectKV commit proxy and txLog path. The request identity makes retry
after a lost reply idempotent. The commit receipt adds:

```text
cell and tenant
transaction-system generation
request identity
prior objectKV version
committed objectKV version
maximum PostgreSQL page LSN
tagged txLog durability certificate
mutation SHA-256
```

A conflict on the expected objectKV version forces the bridge to reread the
current bridge root and retry. PostgreSQL does not become a second objectKV
sequencer.

### Relation extent

`[EXISTS]` One transaction changes pages and the authoritative relation-fork
extent together:

```text
/pg/page/{physical relation and fork}/{block}
/pg/extent/{physical relation and fork} -> nblocks
```

`smgr_nblocks` reads the extent key from the same objectKV snapshot as page
reads. Extend requires the expected prior extent or an idempotent retry of the
same request. Truncate changes the extent and clears the removed block interval
in one transaction. Older page objects can remain physically present, but no
root may expose them beyond the current extent.

### Bridge root and stable barrier

`[EXISTS]` The bounded local bridge root binds:

```text
PostgreSQL relation and fork identity
PostgreSQL WAL flush LSN
maximum admitted page LSN
objectKV transaction-system generation
accepted-through objectKV version
exact immutable base descriptor and object closure
certified txLog suffix digest and per-set durable positions
publication authority term and index
```

Checkpoint sync captures the page service's current version `B`, selects an
immutable base frontier `O <= B`, requires every mutation in `(O, B]` from every
required signed txLog set, persists a content-addressed root, and publishes it
through the replicated authority. A linearizable read must return the exact
manifest before PostgreSQL sync completes. Later page commits may continue
above `B`; they do not extend the completed checkpoint receipt.

`[EXISTS]` The bounded proof publishes the exact base `O` plus certified suffix
through `B`, then permits txLog pop only through `O`. Checkpoint capture also
schedules one complete relation base outside the bridge-state mutex. A later
checkpoint may atomically select that base and shorten the retained suffix.
Page writes do not schedule full relation rewrites.

`[ACTIVE-WORK]` This is safe only inside the current bounded proof because the
Cell and publication authority harnesses are ephemeral and same-host, complete
relation rewrite remains checkpoint-triggered rather than incremental, and the
root covers only one relation.
It does not yet name PostgreSQL system identity or timeline, aggregate a
database checkpoint, survive authority-process restart, or prove empty-cache
remote restore. Those fields and behaviors remain mandatory before production
WAL recycling is admitted.

## Failure model

| Failure | Required outcome |
| --- | --- |
| page LSN above WAL flush frontier | refuse the complete batch before objectKV mutation |
| lost objectKV commit reply | retry the same request identity and return the original outcome |
| stale expected objectKV version | conflict and reread the current bridge root |
| worker death after txLog durability | replacement worker reconstructs the hot page from certified txLog |
| object upload succeeds but root publication fails | object is unreachable garbage; barrier remains incomplete |
| root publishes before one required object | publication or empty-cache verification refuses |
| checkpoint races later writes | receipt covers fixed `B`; later writes remain outside it |
| truncate races stale page write | expected-version conflict prevents resurrection |
| PostgreSQL restart before checkpoint | PostgreSQL WAL redo is authoritative; objectKV hot overlay may be rebuilt or discarded according to the bridge root |
| objectKV unavailable during page flush | PostgreSQL buffer write fails; no false page or checkpoint acknowledgement |
| publication authority unavailable during checkpoint sync | hot txLog state may advance; PostgreSQL checkpoint fails and stable root remains unchanged |

Temporary and unlogged relations need separate semantics. Their fake or absent
page LSNs cannot use this permanent-page gate unchanged.

## Alternatives

### Treat objectKV commit as SQL commit

Optimizes for one native objectKV transaction model. Gives up PostgreSQL WAL,
recovery, replication, and extension semantics. Rejected for the page bridge.

### Trust PostgreSQL's upstream `XLogFlush` without carrying a frontier

Optimizes for a smaller protocol. Gives up a testable WAL-before-page effect
boundary and makes callback regressions invisible. Rejected.

### Write each page directly as one object

Optimizes for implementation simplicity. Gives up request economics, range
locality, compaction, and bounded object count. Rejected. Page mutations enter
the hot txLog and Range Engine overlay, then objectify in batches.

### Make every `smgr_writev` checkpoint-stable

Optimizes for simple recovery reasoning. Gives up write batching and incurs
remote object latency on buffer eviction. Rejected. Buffered acceptance and
stable barrier are separate receipts.

## Eval plan

`[EXISTS]` Candidate `c3c5df9` freezes the first lane as
`postgres-page-write-gate-v0`. Correct run `0bf18a75` passes 15 checks across
three seeds and emits six deterministic mutations. WAL-behind `118ba54b`,
zero-version `ee71a5b4`, oversized-batch `b14da383`, and wrong-digest
`c74e05ad` discard. The remaining three lanes below are proposed.

`[EXISTS]` Candidate `7de5c4e` freezes the subordinate commit and extent lane as
`postgres-page-commit-process-v0`. Correct run `bb7e18fa` commits six pages and
three extent values through 12 Cell process starts, exact duplicate retries,
and three leader handoffs. Missing extent `5816809e`, changed retry identity
`247a6cdb`, wrong receipt identity `68282231`, and non-advancing version
`71d18d48` discard. Tagged txLog durability, fresh Range Engine
reconstruction, and stable publication remain proposed.

The first frozen suite separates four lanes:

1. write admission, WAL frontier exactness and deterministic mutation bytes;
2. subordinate commit, retry deduplication and stale-version conflict;
3. relation extent, extend, truncate and resurrection controls;
4. stable barrier, empty-cache restart and false-checkpoint controls.

`[EXISTS]` The manual literal stable-barrier run published version 13 through a
three-process authority at term 3, index 4. A fresh page-service process
recovered version 13 without its source heap and reconciled the same root before
serving. Removing the authority before the next checkpoint allowed hot txLog
state to reach version 14, but PostgreSQL refused the checkpoint and the stable
frontier stayed at version 13. The end-to-end debug checkpoint took 829 ms,
including 160 ms in sync. This is semantic and pipeline-shape evidence only.

`[EXISTS]` The objectified follow-up published and popped both required txLog
sets through version 11, recovered the complete relation from that base with
zero tail records and no source heap, then accepted and stabilized later writes
through version 13. It established the deletion protocol and rejected
synchronous full-relation rewrite as the intended performance shape.

`[EXISTS]` Candidate `171b14c` now publishes stable `B` from base `O` plus the
certified suffix `(O, B]` and objectifies only checkpoint captures. A fresh
three-page run published `B=9/O=5`, then `B=10/O=9`; restart without a source
heap recovered base 9 plus one record. Materialization took 90 ms and 75 ms.
During publication-authority outage, base 11 completed in 26 ms while stable
sync waited 6.044 seconds to fail; stable stayed 10 and pop stayed 9.

Hard negative controls:

- accept a page whose LSN is one byte above durable WAL;
- acknowledge a lost commit reply as a new version;
- accept a stale expected version;
- publish a barrier whose base plus certified suffix omits a page or extent mutation;
- restore a truncated block from an older segment;
- omit one relation fork from the checkpoint root;
- pass while OTel lacks PostgreSQL LSN, objectKV version, txLog, objectification,
  or publication spans.

The compatibility lane then runs a pinned PostgreSQL relation update,
checkpoint, process restart, and exact query through the literal callbacks.
Only after local filesystem crash controls pass does the same suite run on
MinIO and `objectKV-dev` GCS.

## Compatibility and migration

Page keys and values retain their existing versioned formats. The write
admission digest uses domain `objectkv/postgres/page-write-admission/v1`.
Bridge-root and extent values require their own explicit format versions before
remote persistence.

The first fork selects one exact relation tuple by environment only for the
probe. Production rollout uses a cataloged tablespace policy and keeps `md` as
the rollback target until a verified bridge root exists. A rollback may use
`md` only if PostgreSQL has continued to write it deliberately; the selected
read or write callback must never silently fall back after an objectKV error.

## Unresolved questions

1. Should buffered page acceptance wait for replicated txLog durability only,
   or also for a local NVMe journal when the cell loses quorum?
2. How should a database-wide PostgreSQL checkpoint aggregate fixed `B` across
   many objectKV relation forks without publishing one root per file tag?
3. How does restart select the exact bridge root before catalog tablespaces are
   readable?
4. How are unlogged and temporary relation extents represented without applying
   permanent-page WAL rules?
5. Does the first production AIO implementation use a PostgreSQL-native custom
   AIO operation, a bounded worker pool, or a multiplexed sidecar transport?
