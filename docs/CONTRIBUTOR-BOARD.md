# objectKV contributor board

Status: `[PROPOSED]` initial tasks. Each item is intentionally bounded enough to
become one GitHub issue.

## Ready now

### T1. Complete RFC-0002, version and MVCC model

- Scope: commit ordering, exact replay, read-version availability, tombstones,
  and oldest-readable-version.
- Done when: examples and failure cases are precise enough to extend
  `okv-model` without guessing.
- Dependency: none.

### T2. Add generated differential histories

- Scope: produce deterministic sequences of set, clear, replay, and read; compare
  a candidate engine contract to `okv-model`.
- Done when: a deliberately incorrect engine fails with a minimized seed.
- Dependency: T1 for semantics beyond the current point model.

### T3. Inventory SlateDB adaptation seams `[COMPLETE]`

- Scope: locate sequence assignment, transaction visibility, SST builder/reader,
  manifest publication, cache, compaction, checkpoint, and GC boundaries in the
  exact pinned SlateDB revision.
- Done when: an evidence table classifies each seam as public API, internal reuse,
  upstream change, fork, or rewrite, with file/line links.
- Dependency: none. Read-only research.
- Evidence: `docs/research/slatedb-seams-e016197.md`.

### T4. Implement object-store conformance fixtures

- Scope: memory, filesystem, and MinIO backends; conditional create/update,
  range GET, lost response, retry, checksum, and LIST non-authority behavior.
- Done when: the same contract suite runs against all three local backends and a
  negative store implementation fails.
- Dependency: RFC-0004 draft.

### T5. Build the Phase 0 benchmark runner `[ACTIVE-WORK]`

- Scope: parse `evals/suites/phase0.toml`, pin seeds/profile, emit schema-valid
  JSON, repeat runs, and calculate median/MAD without choosing a champion.
- Done when: an in-memory fake produces a reproducible result and profile drift
  invalidates comparison.
- Dependency: result schema and E0 smoke, both present.
- Exists: configuration validation, dynamic metric instruments, OTel export,
  schema-valid smoke results, median, and MAD.
- Remaining: repeat orchestration, incumbent pairing, noise verdicts, and Phase
  0 workload executors.

### T6. Establish the SlateDB baseline

- Scope: run fixed Phase 0 workloads through unmodified SlateDB on filesystem and
  MinIO.
- Done when: request count, bytes, latency distribution, cache state, compaction,
  and reopen results are captured with exact revision/profile identity.
- Dependency: T4 and T5.

### T7. PostgreSQL bridge surface spike

- Scope: trace PostgreSQL storage manager, buffer manager, WAL/checkpoint, and
  bootstrap paths for one pinned upstream revision. Do not implement a fork yet.
- Done when: the smallest page/storage bridge boundary and unavoidable fork
  surface are documented with source links and a boot sequence.
- Dependency: none. Read-only research.

### T8. Define the Redis semantic subset

- Scope: classify RESP commands by single-key, multi-key, temporal, blocking,
  streaming, scripting, pub/sub, eviction, and cluster semantics.
- Done when: every accepted command has a model invariant and every deferred
  command has a named missing kernel primitive or operational reason.
- Dependency: RFC-0002 for version and time semantics. Read-only research can
  start immediately.

### T9. Specify versioned inverted-index segments

- Scope: term dictionary, postings, document values, deletes, merge generations,
  and snapshot visibility over immutable objects.
- Done when: a deterministic oracle covers update, delete, concurrent query,
  merge, crash, and skewed-term histories with one explicit freshness contract.
- Dependency: RFC-0002 and RFC-0003 drafts.

### T10. Review FoundationDB pattern transfer

- Scope: map read versions, conflict ranges, resolvers, proxies, storage ranges,
  recruitment, failure generations, and deterministic simulation onto the
  objectKV shape.
- Done when: each pattern is marked transfer, adapt, reject, or defer with one
  primary source and one falsifiable experiment.
- Dependency: none. Independent expert review is preferred.

### T11. Draft the physical segment capability contract

- Scope: separate the transactional segment contract from the analytical
  artifact contract. Define their shared sorted versioned-entry stream and
  fenced publication protocol, then locate tombstones, range deletes, merge
  operands, statistics, pruning, and compaction planning explicitly.
- Done when: a row-block transactional segment preserves the full MVCC algebra;
  Parquet and Vortex artifacts preserve covered-through visibility without
  adding schemas to the kernel; an intentionally collapsed one-trait design
  fails a written capability case.
- Dependency: RFC-0003. Design only, no format implementation yet.

### T12. Build serving-model eval oracles

- Scope: differential Redis subset histories, inverted-index result histories,
  PostgreSQL regression manifests, and version-aligned DataFusion delta checks.
- Done when: each suite contains a deliberate semantic break that its hard gate
  rejects.
- Dependency: T8, T9, and the PostgreSQL bridge inventory.

### T13. Build the exact deterministic simulation substrate

- Scope: single logical thread, seeded random source, virtual time, deterministic
  network, durable log, object store, and crash/restart scheduling. Evaluate
  madsim and turmoil before adding a local scheduler.
- Done when: a deliberately injected generation-recovery bug fails under one
  seed, minimizes, and replays exactly in CI with the same event trace.
- Dependency: RFC-0002 generation/version position. This blocks replicated WAL
  implementation.

### T14. Specify acknowledgement, RPO, and lag backpressure

- Scope: `COMMITTED`, `commit_unknown`, WAL topology and placement, regional
  loss model, `C` and `O`, retained-WAL bounds, ratekeeper thresholds, refusal,
  repair, and operator-visible telemetry.
- Done when: a 30-minute object PUT brownout has one bounded state transition
  table and one falsifiable `fault-recovery` suite configuration.
- Dependency: RFC-0005 and RFC-0007. Design can start immediately.

## Opens after Gate 1

- Promote the externally versioned SlateDB spike into the stable engine contract.
- Add immutable segment compatibility fixtures.
- Add the manifest inspection CLI.
- Start the PostgreSQL bridge prototype against the stable versioned engine.

## Opens after Gate 2

- Empty-cache serving worker.
- Kill/restart and lost-ack fault scenarios.
- PostgreSQL restart durability suite over objectKV.

## Not ready

- Multi-WAL partitioning.
- Partitioned resolvers.
- Multi-region writes.
- Vortex in the transactional path.
- New SQL optimizer or PostgreSQL-compatible frontend.
