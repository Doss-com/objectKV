# Full PostgreSQL on objectKV

Status: `[PROPOSED]` research program. No PostgreSQL integration exists yet.

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

## Proposed choice

Prototype A first. It is the only shape that tests the literal goal, full
PostgreSQL compute on objectKV, without first rebuilding PostgreSQL. Treat B as a
targeted experiment and C as a separate later decision.

## pgRust findings

[pgRust](https://github.com/malisper/pgrust) is an external AGPL-3.0 Rust
reimplementation of PostgreSQL 18.3. Its stated target includes the same wire
protocol, SQL semantics, error behavior, and on-disk format as PostgreSQL. The
current project reports the complete default PostgreSQL regression-query corpus,
but also says it is not production-ready and does not provide a stable extension
ABI.

Its source preserves PostgreSQL's heap, index, WAL, buffer-manager,
storage-manager, and page boundaries. That makes pgRust a candidate compute
process for shape A, not evidence that shape C already exists. Its vectorized
push executor, thread-based concurrency, query scheduler, pipelined fsync,
columnar layout, crash simulator, differential oracle, and exact benchmark
receipts are useful research references.

`[PROPOSED]` Add a pgRust evaluation lane beside the upstream PostgreSQL
control:

1. map the pgRust storage-manager and VFS seams needed for an objectKV page
   bridge;
2. run the same PostgreSQL regression and crash subset against upstream
   PostgreSQL and pgRust;
3. measure indexed point reads, contended writes, WAL/page amplification, and
   restart against identical ObjectKV revisions;
4. separately test whether pgRust's columnar and vectorized execution can
   consume version-aligned ObjectKV analytical artifacts;
5. keep pgRust code outside the proposed Apache-2.0 kernel unless the project
   deliberately accepts AGPL licensing.

Learn from pgRust's architecture and evidence discipline. Do not copy its AGPL
source into ObjectKV.

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

## HTAP bridge

The first analytical representation remains Parquet. A materializer consumes an
authoritative commit/version interval and publishes a columnar snapshot with an
explicit covered-through version. Queries combine that base with the complete
durable analytical tail through one target `T`, wait within policy, or return
`snapshot_unavailable`. They never substitute the recovery WAL or return a
mixed-version result.

Vortex becomes an experiment only after this version contract works with Parquet.
