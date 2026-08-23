# Tigris codebase study and objectKV feasibility read

Status: `[EXISTS]` primary-source study completed on 2026-08-22.

## Answer

The product concept is possible as a staged systems program. Tigris strengthens
the case for an immutable-byte plane, transactional metadata, version-addressed
caches, append-only change records, and exact snapshot cutoffs. It does not
prove the novel objectKV claim.

Tigris still uses FoundationDB as the authoritative transaction, metadata,
index, and work-queue substrate. Its block stores hold object bytes. Replacing
that FoundationDB substrate with a bounded, FoundationDB-like kernel whose
permanent bytes live in object storage remains our main research problem.

The decision is therefore:

> Continue. Treat objectKV v0, a bounded cell with short serializable
> transactions, replicated WAL, versioned serving workers, and immutable object
> segments, as technically credible. Do not claim a full FoundationDB,
> PostgreSQL, or HTAP replacement until the recovery, continuation, cache, and
> objectification gates below pass.

This optimizes for learning against the actual missing proof. It gives up an
early claim that S3-compatible storage by itself makes a transactional database.

## Source boundary and pins

The original Tigris database repository is no longer available from its former
GitHub URL. Its last published Go module is still reproducible through the Go
module proxy.

| Source | Pin inspected | Boundary |
|---|---|---|
| Original Tigris database | `v1.0.0-beta.122`, origin commit `7156e157821b1c1f4b4c337cf3cbbfa876c791d2`, published 2023-06-28 | Full archived Go module, zip SHA-256 `5ebc674a6fb61de01326030c8c35c6b2abe8e84d59fb9b2e73e5754e450483d0` |
| Current Tigris architecture docs | `ed6bc01ee6ed744ed155db8299d9bc49b56aaebc` | Public product documentation, not the data-plane implementation |
| Current Tigris engineering blog | `e298dadb3396176ceaae6be02f04c2ab2ad41ca1` | Public architecture and failure-analysis material |
| Tigris Acceleration Gateway, TAG | `40414b783b5143adc9b886a4c9a39993d3b2e8e6` | Public caching proxy, not authoritative object storage |
| OCache | `9f7dd99e584339285d9d4945ae53021350c991c5` | Public local and distributed cache implementation |
| TigrisFS | `d3e466141a3154b8199f8d6d32a7759d66605331` | Public FUSE client for S3-compatible storage |

The current authoritative Tigris object-storage data plane was not present in
the public repositories inspected. Findings about it below are limited to
official documentation and published engineering reports. No inference about
why the earlier database product changed direction is used as technical
evidence.

Primary pins:

- [Original module origin receipt](https://proxy.golang.org/github.com/tigrisdata/tigris/@v/v1.0.0-beta.122.info)
- [Original module manifest](https://proxy.golang.org/github.com/tigrisdata/tigris/@v/v1.0.0-beta.122.mod)
- [Current architecture](https://github.com/tigrisdata/tigris-os-docs/blob/ed6bc01ee6ed744ed155db8299d9bc49b56aaebc/docs/concepts/architecture.md)
- [Current FoundationDB architecture talk](https://github.com/tigrisdata/tigris-blog/blob/e298dadb3396176ceaae6be02f04c2ab2ad41ca1/blog/2026-08-18-fdb-krea-talk/index.mdx)
- [TAG architecture](https://github.com/tigrisdata/tag/blob/40414b783b5143adc9b886a4c9a39993d3b2e8e6/docs/architecture.md)
- [OCache RFC index](https://github.com/tigrisdata/ocache/tree/9f7dd99e584339285d9d4945ae53021350c991c5/docs/rfcs)
- [TigrisFS limitations](https://github.com/tigrisdata/tigrisfs/blob/d3e466141a3154b8199f8d6d32a7759d66605331/README.md)

## What the code and current architecture establish

### 1. The useful separation is transactional metadata plus immutable bytes

`[EXISTS]` Current Tigris writes the data block before opening a FoundationDB
transaction. That transaction writes object metadata, indices, and durable
background-work intent. Reads use metadata to choose a local cache, local block
store, or remote block source. Rename changes metadata without copying the data
block. Snapshots and forks use append-only object versions and a version cutoff.

objectKV implication:

- immutable payload identity and transactional logical identity should remain
  separate;
- segment bytes are published before a small authoritative pointer;
- an unreachable uploaded object is safe but needs bounded orphan collection;
- metadata, derived-index intent, and asynchronous task intent must commit
  together;
- snapshot creation should be a versioned root operation, not an object scan.

This supports the objectKV objectification shape. It does not show that object
storage can replace the transaction system. Tigris depends on FoundationDB to
make the separation safe.

Sources: [write and read paths](https://github.com/tigrisdata/tigris-blog/blob/e298dadb3396176ceaae6be02f04c2ab2ad41ca1/blog/2026-08-18-fdb-krea-talk/index.mdx),
[metadata-only rename](https://github.com/tigrisdata/tigris-blog/blob/e298dadb3396176ceaae6be02f04c2ab2ad41ca1/blog/2025-04-15-renames/index.mdx),
[append-only snapshots and forks](https://github.com/tigrisdata/tigris-blog/blob/e298dadb3396176ceaae6be02f04c2ab2ad41ca1/blog/2025-10-27-append-only-storage/index.mdx).

### 2. The transaction substrate is the product multiplier

`[EXISTS]` The original database implemented a typed document layer, explicit
transactions, primary and secondary indices, schema migration, CDC, and a
search bridge above FoundationDB. Its `store/kv` package exposes ordered point
and range reads, snapshot reads, versionstamped keys and values, atomic adds,
and explicit transactions. Its secondary indexer updates primary data and
ordered index keys inside one FoundationDB transaction.

The current Tigris product repeats the same pattern for object metadata,
indices, and work queues. The queue is an ordered FoundationDB keyspace;
metadata changes and at-least-once task intent commit together, then idempotent
workers perform remote side effects outside the transaction.

objectKV implication:

- the minimal kernel API remains the correct public waist;
- transactional projections are the right mechanism for invariant-critical
  aggregates and indexes;
- a durable, versionstamped task stream belongs in the kernel contract before
  global replication or columnar materialization;
- worker leases and idempotency belong above the transaction engine.

This is strong evidence that an objectKV kernel would unlock several products.
It is also evidence that we are taking on the part Tigris deliberately delegated
to FoundationDB.

Sources: [original module source archive](https://proxy.golang.org/github.com/tigrisdata/tigris/@v/v1.0.0-beta.122.zip),
[original secondary-index RFC](https://proxy.golang.org/github.com/tigrisdata/tigris/@v/v1.0.0-beta.122.zip),
[current queue design](https://github.com/tigrisdata/tigris-blog/blob/e298dadb3396176ceaae6be02f04c2ab2ad41ca1/blog/2026-08-18-fdb-krea-talk/index.mdx).

### 3. Long reads cannot inherit the short transaction model

`[EXISTS]` The original code maps FoundationDB's five-second transaction limit
into explicit errors. Its streaming query runner restarts a read in a new
transaction from the last key when a transaction becomes too old. The project's
secondary-index RFC explicitly notes that streaming across transactions can
return inconsistent or duplicate results if documents change between those
transactions.

objectKV implication:

- an OLAP query cannot stay inside an OLTP transaction;
- continuation by last key alone is not exact;
- every long read needs one target version `T`, a lease that retains all roots
  needed for `T`, and continuation tokens bound to `T`, schema version, tenant,
  range epoch, plan, and last logical key;
- the exact DataFusion base-plus-tail overlay is a semantic requirement, not an
  optimization.

This directly supports RFC-0010 and the frozen physical HTAP suite.

### 4. Search has two materially different consistency paths

`[EXISTS]` The original Tigris code contains both transactional FoundationDB
secondary indices and a Typesense-backed search path. Transactional secondary
indices are updated with the primary document. The Typesense `SearchIndexer`
runs after the FoundationDB commit and sends create, replace, update, or delete
operations to Typesense.

objectKV implication:

- exact lookup and invariant-bearing indices should be transactional
  projections in the ordered keyspace;
- broad text search should publish immutable inverted segments asynchronously;
- query semantics must expose or bridge the search watermark;
- a search result cannot silently be treated as the same snapshot as an OLTP
  read unless it overlays a complete tail through `T`.

This supports distributed inverted search as an early consumer, but not as a
claim that every posting update should be a synchronous objectKV mutation.

### 5. Value chunking does not remove transaction limits

`[EXISTS]` The original database splits payloads larger than 99 KB into several
FoundationDB keys inside the same transaction. This bypasses the single-value
limit but expands transaction bytes and the conflict surface. Bulk secondary
index construction backs off its batch size on conflict, transaction-age, and
transaction-size errors.

objectKV implication:

- public values and transactions need separate hard limits;
- large values should normally become immutable payload objects referenced by a
  small transactional record;
- chunking is a compatibility escape hatch, not an unlimited-value contract;
- evals must vary document size, derived-index fanout, and conflict rate
  together.

### 6. Cache correctness is a database property when cache hits bypass authority

`[EXISTS]` Tigris reports that fault injection found delete-then-read,
rename-invalidation, and deleted-object-resurrection bugs across the cache,
metadata, and regional replication seams. The remediation uses eager
invalidation, versioned bodies, tombstone barriers, and tombstone-aware regional
reconciliation.

Current TAG code makes the invariant concrete:

- body keys include ETag, so overwrites do not clobber bytes used by an
  in-flight reader;
- the body is written before metadata, so visible metadata grants access only
  after bytes exist;
- invalidation writes a tombstone, then removes only metadata;
- stale bodies remain unreachable and expire later instead of being deleted
  under an active reader;
- a slow population checks the tombstone immediately before publishing
  metadata;
- tombstone TTL is derived from maximum fetch plus write time and margin, not a
  fixed guess.

objectKV implication:

- serving-cache hits must be included in linearizability histories;
- cache identity should be `(cell, tenant, range epoch, segment identity,
  block)` or another immutable version key;
- a manifest or range-map pointer is the visibility gate and is published last;
- stale generation, delayed populate, deletion, overwrite, and regional fallback
  need deterministic fault workloads;
- a cache outage must not become a commit outage, but stale cache state must be
  fenced from visibility.

Sources: [Tigris fault analysis](https://github.com/tigrisdata/tigris-blog/blob/e298dadb3396176ceaae6be02f04c2ab2ad41ca1/blog/2026-04-16-cache-coherence-antithesis/index.mdx),
[TAG versioned cache](https://github.com/tigrisdata/tag/blob/40414b783b5143adc9b886a4c9a39993d3b2e8e6/cache/cache.go),
[TAG ETag fixtures](https://github.com/tigrisdata/tag/blob/40414b783b5143adc9b886a4c9a39993d3b2e8e6/cache/etag_versioning_test.go).

### 7. Derived accounting should not own reclamation safety

`[EXISTS]` OCache's current recompaction RFC documents production disk growth
caused by missed incremental delete credits. Reclamation had been gated on a
counter that could drift, so dead bytes became unreachable. The replacement
walks immutable segment headers and checks each entry against authoritative
metadata. The counter remains a prioritization hint; ground truth decides
liveness.

objectKV implication:

- reference counts, byte counters, and compaction debt are useful scheduling
  hints, not safe GC roots by themselves;
- object deletion eligibility must be re-derived from retained manifests,
  snapshot leases, WAL/object watermarks, migration roots, and range epochs;
- an incomplete liveness walk must fail closed;
- a closed object's metadata publication must be structural, not inferred from
  age.

Source: [OCache walk-gated recompaction RFC](https://github.com/tigrisdata/ocache/blob/9f7dd99e584339285d9d4945ae53021350c991c5/docs/rfcs/RFC-009-walk-gated-recompaction.md).

OCache itself is not durability prior art. Its published cluster RFC starts
with replication factor one and eventually consistent gossip membership. It is
a disposable serving cache, which is exactly how objectKV should use comparable
mechanisms.

### 8. S3-compatible POSIX translation is not a PostgreSQL storage layer

`[EXISTS]` TigrisFS uses parallelism, asynchronous buffering, and caching to
make object storage look filesystem-like. Its documented limits include no safe
concurrent updates of one file across hosts, asynchronous write failures unless
the caller explicitly `fsync`s, cache invalidation delays, and alignment and
read-modify-write costs for patched object parts.

objectKV implication:

- PostgreSQL should not run directly on an S3 FUSE mount;
- the upstream PostgreSQL bridge must target logical pages, relations, or WAL
  through an objectKV API;
- local NVMe can be a cache, but durable ordering, conflict handling, and crash
  recovery stay in the database protocol.

## Feasibility by product layer

| Layer | Read after Tigris study | Missing proof |
|---|---|---|
| Single-cell ordered transactional KV | `[ACTIVE-WORK]` credible | serializable multi-range commit, quorum WAL, generation recovery, direct reads, split/move, objectification |
| Object-native permanent storage | `[ACTIVE-WORK]` credible with WAL ahead of objects | bounded brownout, unknown PUT recovery, fenced manifest publication, orphan and retained-root GC |
| Distributed Redis consumer | `[PROPOSED]` feasible as a bounded semantic subset | hot-key ceiling, watches, expirations, atomic command mapping, tail latency |
| Distributed inverted search | `[PROPOSED]` feasible with immutable search segments and transactional catalog | exact watermark semantics, segment publication, compaction, cursor continuity |
| Upstream PostgreSQL bridge | `[PROPOSED]` feasible if PostgreSQL WAL/LSN remains initial authority | page and relation mapping, crash matrix, extension behavior, fsync latency, write amplification |
| objectKV as sole PostgreSQL commit authority | `[FUTURE]` unproven | eliminate double authority without weakening PostgreSQL durability or recovery semantics |
| ZebraDB exact HTAP | `[PROPOSED]` semantically credible | complete table-change index, exact leases, schema-at-`T`, base-plus-tail operator, tail-cost envelope |
| Full FoundationDB replacement | `[FUTURE]` possible in principle, not yet earned | partitioned resolvers, proxy scaling, tagged logs, range movement, generation recovery, fleet operations |

Tigris reduces uncertainty around the layers above the transaction kernel. It
increases confidence that the kernel would be valuable. It does not reduce the
coordination and recovery difficulty inside that kernel.

## New eval obligations

The following become required suites or workloads. Each needs a correct subject
and a deliberately unsafe negative control.

1. **Block-before-metadata publication**
   - block write succeeds, metadata commit fails;
   - block write returns unknown, identity probe resolves success or retry;
   - metadata never points at absent bytes;
   - orphan collection is bounded and cannot delete a late-published block.
2. **Transactional task intent**
   - data, index intent, and task record commit together;
   - worker crash after side effect but before acknowledgement;
   - lease expiry and duplicate delivery;
   - idempotent replay with no missed task.
3. **Exact long-read continuation**
   - forced transaction rollover while concurrent inserts, updates, deletes,
     splits, and schema changes execute;
   - no duplicates, gaps, or snapshot drift at target `T`;
   - stale continuation token fails closed.
4. **Cache visibility and resurrection**
   - delete followed by immediate read;
   - overwrite while an old body streams;
   - delayed populate crossing invalidation;
   - tombstone expiry below the maximum populate window;
   - cache-node and region loss during fallback.
5. **Ground-truth GC**
   - missed and doubled accounting credits;
   - crash between segment close and manifest publication;
   - snapshot and query lease retention;
   - incomplete liveness scan fails closed;
   - derived walk converges while accounting remains wrong.
6. **Consistency-profile conformance**
   - same-region and cross-region histories are scored separately;
   - strong profiles reject stale reads and stale conditional writes;
   - eventual profiles measure bounded replication lag without calling it
     serializable.

## Implementation plan changes

### P0, before a distributed product claim

1. Finish generation recovery and quorum-authenticated takeover. `[EXISTS]`
2. Add an ordered, versionstamped durable task record to the kernel model and
   simulator. `[PROPOSED]`
3. Prove block-before-pointer publication, ambiguous object-write recovery, and
   ground-truth orphan collection. `[PROPOSED]`
4. Finish the exact DataFusion physical overlay and continuation contract.
   `[ACTIVE-WORK]`
5. Add cache-resurrection workloads to deterministic simulation before direct
   storage reads are called complete. `[PROPOSED]`

### P1, after the first cell is credible

1. Add Tigris as a separate object-store conformance backend when a development
   account is available. Do not infer compatibility from its S3 endpoint.
2. Build an immutable inverted-segment prototype whose catalog and watermark
   are objectKV transactions.
3. Build the PostgreSQL bridge with PostgreSQL WAL and LSN as the initial
   authority, then run the crash matrix before considering a sole-authority
   design.
4. Compare Parquet and Vortex only behind the analytical artifact contract.

### P2, only after the preceding evidence

1. Partition conflict resolution.
2. Add multiple read-version and commit proxies.
3. Partition tagged durable logs.
4. Add metacluster placement and tenant migration.

## What would reverse the decision

Stop or materially narrow the product if any of these remain true after their
fixed experiment budgets:

- objectification cannot stay off the acknowledgement critical path without an
  unbounded or operationally unacceptable WAL suffix;
- recovery cannot select one safe generation under repeated partitions and
  exact replay;
- direct reads require an authority round trip that erases the object-native
  cost and scaling benefit;
- exact long reads cannot retain and resume one version without unbounded tail
  or lease cost;
- PostgreSQL's crash and fsync semantics require permanent duplication of two
  independent commit authorities;
- cache coherence needs global synchronous validation on ordinary reads.

Until a falsifier is observed, the engineering direction remains coherent:
FoundationDB-like semantics inside bounded cells, replicated recent durability,
immutable object-native history, disposable versioned serving workers, and
derived row and column layouts over one commit history.
