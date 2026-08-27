# Incumbent transaction-plane selection

Status: `[EVALUATING]` until the frozen semantic preflight runs against both
providers. Source inspection and RFC-0041 are `[CODE-COMPLETE]`.

## Clarity

Question: which incumbent can supply objectKV's transaction plane without
objectKV rebuilding distributed serializable validation?

Punchline: FoundationDB is the only current candidate because it provides
strict serializability and commit versionstamps, while TiKV documents snapshot
isolation and write skew.

Counter: this call is wrong if the real TiKV 8.5.7 preflight rejects the frozen
write-skew history without objectKV adding predicate locks, range locks, or a
serializable certifier.

Next: implement the provider-neutral adapter contract and run the same semantic
preflight before provisioning a broader lifecycle benchmark.

## Primary-source observations

- FoundationDB states that ordinary transactions are globally ACID and strictly
  serializable. It assigns one read version, tracks read and write conflict
  ranges, and aborts a transaction when a committed write intersects a later
  transaction's reads. [Developer guide](https://apple.github.io/foundationdb/developer-guide.html)
- FoundationDB 7.4.6 exposes `get_read_version`, `set_read_version`,
  `get_committed_version`, `get_versionstamp`, ordered range reads, and Blob
  Granule range and read functions through its C API. [Pinned C header](https://github.com/apple/foundationdb/blob/e77b64d4c5d01d240931c08c5384a834cae27337/bindings/c/foundationdb/fdb_c.h)
- FoundationDB backup creates a point-in-time restore from range snapshots plus
  ordered mutation logs in local disk, another FoundationDB database, or blob
  storage. [Backup and restore documentation](https://apple.github.io/foundationdb/backups.html)
- FoundationDB's partitioned backup design records `(commit_version,
  subsequence)` and merges partitioned mutation files into one total order.
  The design is current source material, but its command path remains marked
  experimental. [Partitioned backup-log design](https://github.com/apple/foundationdb/blob/main/design/backup_v2_partitioned_logs.md)
- FoundationDB lists ChangeFeed as experimental through its published feature
  matrix. [Experimental features](https://apple.github.io/foundationdb/experimental-features.html)
- TiKV describes its transaction model as Percolator-like snapshot isolation
  with externally consistent reads and writes. [TiKV repository](https://github.com/tikv/tikv/tree/3f446cfa9eb1d5c653031d261e185911495d0359)
- TiDB's transaction documentation states that its TiKV-backed repeatable-read
  mode is snapshot isolation and explicitly permits write skew. [Isolation documentation](https://docs.pingcap.com/tidb/stable/transaction-isolation-levels/)
- TiKV's Rust client exposes optimistic and pessimistic transactions, ordered
  scans, explicit snapshots at a `Timestamp`, and the commit timestamp returned
  by `commit`. The client repository warns that its current API is not yet
  suitable for production use. [Pinned Rust client](https://github.com/tikv/client-rust/tree/88688d6eb3a55a864885d7bccc8abf428dce076c)
- TiKV 8.5.7 contains CDC, resolved timestamp, backup, log-backup, external
  storage, and SST import surfaces. [Repository maintenance map](https://github.com/tikv/tikv/blob/3f446cfa9eb1d5c653031d261e185911495d0359/doc/maintenance-guides/repo-overview.md)
- Tigris uses FoundationDB for transactionally aligned metadata, indices, and
  work queues while object storage owns immutable payload bytes. The public
  Tigris sources inspected do not replace FoundationDB with object storage.
  [Existing Tigris study](tigris-codebase-study.md)

## Not observed

- No documented TiKV strict-serializable transaction mode was found in the
  TiKV repository, TiKV client, or current TiDB isolation documentation.
- No stable public FoundationDB ChangeFeed contract was found. The first adapter
  therefore cannot depend on ChangeFeed.
- No provider surface was found that can preserve source commit-version numbers
  while restoring into a new cluster. RFC-0041 makes restore start a new
  objectKV generation.

## BIDEC breadth

- W1. Transaction semantics: isolation, ordered ranges, conflicts, limits, and
  unknown commit outcomes.
- W2. Version mapping: provider stamps, generation boundaries, cursors, and
  historical reads.
- W3. Change capture: atomic retained commands, pagination, and reclamation.
- W4. Objectification: immutable closure construction and frontier authority.
- W5. Reconstruction: chunk idempotency, exact digest, activation, and fencing.
- W6. Hot-path economics: extra writes, latency, throughput, bytes, and CPU.
- W7. Lifecycle leverage: branching, empty-generation recovery, and exact HTAP.
- W8. Operations: binaries, client libraries, upgrades, failure domains,
  telemetry, and licensing.

## Depth findings

### W1. Transaction semantics

FoundationDB maps directly to the declared read-conflict and write-conflict
ranges already present in `TransactionCommand`. TiKV fails the declared
strict-serializable shape at its documented isolation boundary. Failure mode:
both transactions in a write-skew pair commit and violate a cross-key
invariant.

### W2. Version mapping

FoundationDB's ten-byte versionstamp maps to the current objectKV
`(commit_version, batch_order)` cursor. A restored provider cannot reuse those
stamps. The logical version must therefore include an objectKV generation.
Failure mode: versions from two restored clusters compare as though they shared
one provider history.

### W3. Change capture

The first FoundationDB mechanism should write one versionstamped retained
command in the same transaction as user data and the request outcome. It avoids
an experimental ChangeFeed dependency but duplicates mutation bytes. Failure
mode: the objectifier misses a committed change or reclaim advances ahead of
the object frontier.

### W4. Objectification

The objectifier consumes immutable log keys, so it does not need a long-lived
FoundationDB read transaction. It builds and verifies objects outside the
transaction, then compare-and-advances the frontier. Failure mode: a visible
frontier names an incomplete object closure.

### W5. Reconstruction

Restore writes deterministic chunks into a fenced destination generation and
activates only after exact digest comparison. Failure mode: a replayed chunk
applies twice or a partially restored generation accepts traffic.

### W6. Hot-path economics

Every logical commit adds at least one retained-change value and one request
outcome. This may double small-write bytes even when latency remains batched.
Failure mode: the lifecycle layer exceeds the frozen 25 percent p99 or
throughput overhead ceiling.

### W7. Lifecycle leverage

Main-branch reads stay entirely in the provider. Historical reads and lazy
branches compose object state with a provider overlay. Failure mode: branch
first-read cost or full hydration removes the metadata-scale advantage.

### W8. Operations

FoundationDB requires its native client library and compatible cluster binary.
TiKV requires PD and TiKV services, and its Rust client carries an explicit
production-readiness warning. Both are Apache 2.0. Failure mode: the adapter's
version or deployment coupling costs more operationally than the lifecycle
features save.

## Merged workstreams

- M1. Admit transaction semantics, covering W1 and the hot portion of W2.
- M2. Prove object continuity, covering W2 through W5.
- M3. Measure product leverage, covering W6 and W7.
- M4. Bound provider operations, covering W8.

## Sequence

1. M1 first, because semantic failure removes a provider before integration
   cost.
2. M2 next, because object continuity is the retained product thesis.
3. M3 after correctness, because latency and lifecycle wins need the exact same
   history.
4. M4 throughout R0 and R1, with deployment and teardown receipts attached to
   each provider run.

## Recalibration check

The D52 intent still holds. The implementation sequence changes from "build two
complete adapters" to "run both semantic preflights, then build lifecycle work
only for providers that pass." This removes work while preserving the provider
decision and the strict-serializable product requirement.
