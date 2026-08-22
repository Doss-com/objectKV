# objectKV bootstrap and implementation plan

## Original intent

Launch objectKV as a contributor-ready open-source project, then use an eval-led
research program to progress from a versioned object engine to an object-native
transaction kernel, full PostgreSQL compute, and ZebraDB HTAP over one logical
history.

## Workstreams, Level 1

- W1. Physical object engine: prove object-store read, write, cache, compaction,
  and reopen economics.
- W2. Version and correctness model: define visibility, replay, deletion,
  snapshots, and storage-engine contracts.
- W3. Durability boundary: add a low-latency replicated WAL and object durable
  watermark without acknowledging impossible states.
- W4. Disposable serving: recover correct reads from metadata, recent WAL, and
  object storage with empty local state.
- W5. Logical distribution: route, split, move, fence, and compact logical
  ranges without copying their durable bytes.
- W6. Transaction semantics: global snapshots, OCC conflict ranges, retries,
  and strict serializability.
- W7. Eval and research system: freeze oracles, budgets, suites, result records,
  and keep/discard rules.
- W8. PostgreSQL and HTAP path: preserve PostgreSQL behavior, then connect one
  commit/version history to OLTP and columnar execution.
- W9. OSS operations: establish naming, license, RFCs, governance, contributor
  tasks, release contracts, and public claims.

## Depth findings

### W1. Physical object engine

- Mechanism: Apache `object_store` for providers, a SlateDB adapter or extracted
  segment layer for the first implementation, RAM/NVMe cache, and immutable
  segment publication.
- First files: `crates/okv-segment`, `crates/okv-object`,
  `evals/suites/phase0.toml`.
- Open question: how can externally assigned versions be introduced without
  inheriting SlateDB writer ownership and manifest authority?
- Failure mode: the engine looks fast when warm but cold point reads or
  compaction generate uneconomic object requests and bytes.
- Rough scope: medium, 1 to 2 weeks for a comparable baseline, 2 to 4 weeks for
  a versioned adapter.

### W2. Version and correctness model

- Mechanism: a single-threaded executable specification, generated histories,
  differential checks, and an immutable-segment interface.
- First files: `crates/okv-model`, RFC-0002, RFC-0003, RFC-0004.
- Open question: how are exact replay, gaps, range tombstones, large values, and
  oldest-readable-version represented?
- Failure mode: implementation and oracle share the same bug or the version
  contract changes silently under benchmarks.
- Rough scope: small for point operations, large once range deletes, snapshots,
  and concurrent histories enter.

### W3. Durability boundary

- Mechanism: one three-node Raft log per cell, quorum fsync before acknowledge,
  asynchronous materialization, and conservative global watermarking.
- First files: future `crates/okv-wal-api`, `crates/okv-wal-raft`, RFC-0005,
  RFC-0007, RFC-0009.
- Open question: which persisted state surrounds `raft-rs`, and how are commit
  versions fenced across generation recovery?
- Failure mode: an acknowledged commit exists in neither reconstructable object
  state nor the retained WAL.
- Rough scope: large, 3 to 6 weeks after the versioned engine is credible.

### W4. Disposable serving

- Mechanism: serving workers subscribe to committed mutations, publish segments,
  maintain applied versions, and rebuild caches lazily.
- First files: future `crates/okv-storage-worker`, objectification evals, and
  empty-cache recovery scenarios.
- Open question: how much metadata must be eagerly loaded for bounded time to
  first correct read?
- Failure mode: logical readiness waits on copying or scanning the full dataset.
- Rough scope: large, 3 to 6 weeks after the WAL path.

### W5. Logical distribution

- Mechanism: ordered key ranges, generation-fenced assignments, metadata-only
  split/move, shared historical segments, and background physical realignment.
- First files: future `crates/okv-router`, `crates/okv-control`, RFC-0006.
- Open question: how are segment references shared safely across child ranges
  while watermarking and GC remain correct?
- Failure mode: a stale owner publishes state or range movement triggers bulk
  durable-byte transfer.
- Rough scope: extra large, multi-month with fault testing.

### W6. Transaction semantics

- Mechanism: read versions, explicit read/write conflict ranges, partitionable
  resolver state, one ordered commit stream first, then measured scaling.
- First files: future `crates/okv-txn`, `crates/okv-resolver`, RFC-0008.
- Open question: ordered versus hashed resolver domains, transaction lifetime,
  commit-unknown handling, and log partitioning threshold.
- Failure mode: a generated concurrent history observes a result the reference
  serializable model cannot produce.
- Rough scope: extra large, begins after ranges and global snapshots.

### W7. Eval and research system

- Mechanism: frozen correctness gates, lane-specific fixed-budget benchmarks,
  public and held-out seeds, machine profiles, noise measurement, and an
  append-only experiment ledger.
- First files: `program.md`, `docs/EVALS.md`, `evals/`, `experiments/`.
- Open question: which stable machine/backend profiles produce results worth
  comparing across contributors?
- Failure mode: an optimizer changes the test, buys speed with correctness, or
  overfits one visible workload.
- Rough scope: small for the chassis, ongoing for each system phase.

### W8. PostgreSQL and HTAP path

- Mechanism: first prototype PostgreSQL page/storage bridging over objectKV;
  later decide whether to deepen that fork or move logical relations/indexes
  directly onto objectKV. Materialize version-aligned Parquet before evaluating
  Vortex.
- First files: `docs/POSTGRES-PATH.md`, RFC-0010, a future isolated bridge crate
  or PostgreSQL fork repository.
- Open question: which PostgreSQL durability and MVCC responsibilities remain in
  PostgreSQL versus move into objectKV?
- Failure mode: two independent transaction/MVCC systems duplicate work and
  cannot agree on snapshot visibility.
- Rough scope: one to two weeks per thin bridge prototype, multi-quarter for full
  compatibility and HTAP.

### W9. OSS operations

- Mechanism: Apache-2.0, vendor-led RFC governance, issue templates, claim-safe
  README, public benchmark receipts, and one adopter-independent API.
- First files: `README.md`, `CONTRIBUTING.md`, `rfcs/`, `.github/`.
- Open question: whether and when public crates need a separate namespace from
  the `okv` shorthand.
- Failure mode: launch claims distribution or performance before evidence, or
  contributors implement conflicting invariants.
- Rough scope: small bootstrap, then continuous maintenance.

## Merged workstreams

- M1. Prove the object engine, covers W1 + early W2 + early W7. Produce a
  versioned engine and Gate 1 report.
- M2. Prove durable objectification, covers W3 + W4 + fault portions of W7.
  Produce low-latency commits and empty-cache recovery.
- M3. Prove the distributed transaction kernel, covers W5 + W6. Produce
  metadata-only range movement, global snapshots, and serializable transactions.
- M4. Prove PostgreSQL and HTAP consumption, covers W8. Produce real PostgreSQL
  regression evidence and version-aligned analytical snapshots.
- M5. Operate the OSS project, covers W9 + research governance from W7. Keep the
  public contract, RFC queue, and contribution lanes coherent through all phases.

## Sequence

### S0. Bootstrap the project

Status: `[ACTIVE-WORK]`.

Deliverables:

- public naming and OSS-boundary decisions;
- compiling Rust workspace;
- executable reference-model smoke eval;
- eval result contract and autonomous research program;
- RFC queue and contributor-ready tasks.

Exit: all local checks pass and one human can select a task without reconstructing
the architecture memo.

### S1. Establish the Phase 0 baseline

Status: `[PROPOSED]`.

Implement the same fixed dataset/workloads against:

1. SlateDB with filesystem storage;
2. SlateDB with local S3/MinIO;
3. SlateDB with one real cloud object-store profile;
4. the in-memory oracle for correctness only.

Gate 1 passes only if hot/cold latency, object request amplification, rewritten
bytes, compaction cost, and empty-cache reopen are measured and acceptable for a
named target workload. A blended average cannot pass the gate.

### S2. Build the versioned object engine

Status: `[PROPOSED]`.

Introduce externally assigned versions, point reads, range reads, exact replay,
immutable segment publication, checksums, and manifest inspection behind a small
storage-engine contract. Differentially test every generated history against
`okv-model`.

Exit: Gate 1 re-runs against the objectKV adapter with no correctness failures.

### S3. Add the fast durability tier

Status: `[PROPOSED]`.

Add one ordered replicated WAL, acknowledge after quorum durability, consume it
into immutable objects, and advance the conservative object durable watermark.

Gate 2: low-millisecond local-region commit is demonstrated while kill/restart
scenarios preserve every acknowledged commit.

### S4. Make serving disposable

Status: `[PROPOSED]`.

Separate read/materialization workers from permanent bytes. Start with an empty
cache and demonstrate bounded logical readiness independent of dataset size.

Gate 3: complete worker loss does not require durable dataset copy.

### S5. Add ranges, snapshots, and OCC

Status: `[PROPOSED]`.

Add logical ranges, assignment generations, metadata-only split/move, global
read versions, conflict ranges, and resolver checks in that order.

Gates 4 and 5: range movement copies approximately zero durable database bytes;
generated histories remain strictly serializable at useful measured throughput.

### S6. Put full PostgreSQL compute on objectKV

Status: `[PROPOSED]`.

Begin the page/storage bridge spike once S2 establishes stable versioned storage.
Do not claim the PostgreSQL path until S3 provides a credible durability boundary.
Use PostgreSQL's own regression suite as the compatibility oracle.

Gate 6A: a real PostgreSQL server boots, creates a database, survives restart,
and passes a declared regression subset with objectKV as its durable backing.

### S7. Build the ZebraDB HTAP path

Status: `[FUTURE]`.

Map records and indexes to atomic objectKV transactions. Materialize Parquet from
the authoritative commit history with explicit coverage versions. Query a
columnar base plus bounded OLTP delta, or wait for a declared analytical
watermark.

Gate 6B: a representative ZebraDB workload is simpler or materially better than
the current dual-system path enough to justify owning the substrate.

## Recalibration check

The core intent still holds, but the north star is sharper than the source memo:
full PostgreSQL compute is now an explicit consumer program. It does not change
the first gate. If Gate 1 fails, stop rather than compensating with distribution
or PostgreSQL work.
