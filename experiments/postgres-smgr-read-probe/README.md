# PostgreSQL 18.6 objectKV `smgr` read probe

Status: `[COMPLETE]` first literal PostgreSQL relation read through objectKV on
2026-08-24. This is a synchronous read-callback seam, not a write, durability,
checkpoint, recovery, or production AIO result.

## Result

The probe pins PostgreSQL tag `REL_18_6`, exact commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`, and applies
`postgres-18.6-smgr-read.patch`. The fork selects one relation by exact
tablespace, database, and relfilenode. Its `smgr_readv` and
`smgr_startreadv` callbacks have no `mdreadv` or `mdstartreadv` fallback.
Every other callback still delegates to `md`.

One actual PostgreSQL heap relation was created with 2,000 rows, checkpointed,
and stopped. Its 148 native 8 KiB pages, 1,212,416 bytes, were imported into an
authority-bound objectKV view at version 1. A separate
`okv-postgres-page-service` process served the relation through
`PostgresPageReader`, the routed `KvReadClient`, a KV Runtime listener, and a
Range Engine over a pinned SlateDB base.

After a PostgreSQL restart cleared shared buffers, an exact sequential scan
returned:

```text
count = 2000
sum(id) = 2001000
```

The PostgreSQL log records 13 objectKV callback reads covering every block from
0 through 147. The first callback requested block 0; later read-ahead requests
grew through 2, 4, 8, and 16 blocks. Every request carried objectKV version 1
and the same maximum PostgreSQL page-LSN frontier.

## Executed path

```text
PostgreSQL shared-buffer miss
  -> smgr_startreadv
  -> synchronous fixed-width TCP request
  -> okv-postgres-page-service
  -> PostgresPageReader
  -> KvReadClient
  -> KV Runtime routed read
  -> authority-bound Range Engine view
  -> pinned SlateDB base
  -> authenticated 8 KiB page decode
  -> PostgreSQL buffer
  -> fork-only synchronous AIO-handle completion
  -> PostgreSQL page verification and executor
```

PostgreSQL 18 routes the read through `smgr_startreadv` even with
`io_method=sync`. The patch therefore adds a narrow
`pgaio_io_complete_readv_synchronously` helper so a non-file storage manager
can fill the buffers synchronously and still complete the existing upper AIO
callback chain. This does not implement asynchronous objectKV I/O.

## Controls

Two live controls discarded:

- `[COMPLETE]` page service unavailable: a fresh PostgreSQL restart reached the
  selected relation and failed with `Connection refused`. It did not read the
  still-present `md` relation file.
- `[COMPLETE]` changed read frontier: the service was fixed at one page-LSN
  frontier while PostgreSQL requested another. The service refused with
  `storage-manager request changed the fixed read frontier`.

The service also rejects relation identity mismatch, zero or oversized block
ranges, missing pages, malformed page values, payload SHA-256 mismatch, and a
page LSN beyond the selected frontier through the existing adapter contracts.

## Early performance indicator

This is a shape measurement, not a target result:

| Read state | PostgreSQL execution | Buffers | Notes |
| --- | ---: | ---: | --- |
| cold after restart | 233.045 ms | 148 shared reads | 13 outer TCP calls, inner fresh TCP plus JSON routed reads, Rust debug build, in-memory object store |
| immediate repeat | 0.299 ms | 148 shared hits | PostgreSQL shared buffers satisfy the scan; objectKV is not called |

The cold result is intentionally expensive and identifies the next performance
work: persistent multiplexed transport, a binary KV Runtime protocol, direct
page batches, release builds, shared Range Engine RAM/NVMe caching, and true
PG18 asynchronous submission. It does show that PostgreSQL's existing buffer
cache preserves the intended hot-read shape once pages are resident.

## Reproduction inputs

The Rust service accepts one JSON configuration:

```text
target/debug/okv-postgres-page-service --config-json '{...}'
```

Required fields bind the source relation file, full physical relation identity,
objectKV version, maximum page LSN, and maximum blocks per request. The service
prints a machine-readable readiness line only after the source has been
imported and the listeners are bound.

The patched PostgreSQL process receives:

```text
OKV_SMGR_READ_TABLESPACE
OKV_SMGR_READ_DATABASE
OKV_SMGR_READ_RELATION
OKV_SMGR_PAGE_HOST
OKV_SMGR_PAGE_PORT
OKV_SMGR_OBJECTKV_VERSION
OKV_SMGR_MAX_PAGE_LSN
```

Run PostgreSQL with `io_method=sync` for this probe. Apply the patch only to the
exact pinned source revision.

## Admission boundary

This admits the literal relation-read seam and establishes that the first
PostgreSQL integration must handle the PG18 AIO API. It does not admit:

- page create, extend, write, truncate, unlink, or objectKV block count;
- WAL-before-page ordering;
- checkpoint or immediate-sync stable barriers;
- page changes after the immutable import;
- asynchronous objectKV reads or cancellation;
- object-store durability, empty-cache recovery, or remote latency;
- PostgreSQL WAL, `pg_control`, SLRU, prepared-state, or replication-state
  recovery;
- production relation selection, security, pooling, backpressure, or OTel
  export.

The next milestone is a write-through overlay with an explicit WAL-before-page
gate and an objectKV-backed `nblocks` contract, followed by a real stable
checkpoint receipt.
