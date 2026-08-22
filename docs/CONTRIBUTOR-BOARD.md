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

### T4. Implement object-store conformance fixtures `[ACTIVE-WORK]`

- Scope: memory, filesystem, and MinIO backends; conditional create/update,
  range GET, lost response, retry, checksum, and LIST non-authority behavior.
- Done when: one capability-profiled suite runs against memory, filesystem,
  pinned MinIO, and GCS; every published support row records exact versions;
  immutable-overwrite and LIST-authority negative stores fail.
- Dependency: RFC-0004 draft.
- Exists: memory passes `authority`; filesystem passes `segment` and fails
  `authority`; pinned MinIO passes `authority`; short-range, checksum, lost
  response, overwrite, and stale-LIST fixtures execute; results flow through
  the shared schema and OTel path.
- Remaining: run the same accepted suite against the protected `objectKV-dev`
  GCS bucket, add a provider-specific generation-guarded delete adapter, and
  publish a clean-commit cloud receipt.

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

### T13. Build the exact deterministic simulation substrate `[ACTIVE-WORK]`

- Scope: single logical thread, seeded random source, virtual time, deterministic
  network, durable log, object store, and crash/restart scheduling. Evaluate
  madsim and turmoil before adding a local scheduler.
- Done when: a deliberately injected generation-recovery bug fails under one
  seed, minimizes, and replays exactly in CI with the same event trace.
- Dependency: RFC-0002 generation/version position. This blocks replicated WAL
  implementation.
- Exists: Turmoil 0.7.2 is pinned behind `okv-sim`; the build fails closed
  without Tokio runtime RNG seeding; two local fresh processes produced
  byte-identical canonical traces; CI is configured to repeat that comparison;
  and a stale-publication negative control fails.
- Remaining: seed minimization, deterministic object API, WAL and coordinator
  seams, overlapping role failures, and a retained corpus.

### T14. Specify acknowledgement, RPO, and lag backpressure `[COMPLETE]`

- Scope: `COMMITTED`, `commit_unknown`, WAL topology and placement, regional
  loss model, `C` and `O`, retained-WAL bounds, ratekeeper thresholds, refusal,
  repair, and operator-visible telemetry.
- Done when: a 30-minute object PUT brownout has one bounded state transition
  table and one falsifiable `fault-recovery` suite configuration.
- Dependency: RFC-0005 and RFC-0007. Design can start immediately.
- Evidence: RFC-0005 defines `COMMITTED`, single-region RPO, `C/O`, and the
  normal, rate-limited, commit-refused, and recovery-only states;
  `evals/suites/fault-recovery.toml` owns the brownout lane. The workload
  executor remains gated on WAL and objectification components.

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
