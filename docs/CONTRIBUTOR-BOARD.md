# objectKV contributor board

Status: `[ACTIVE-WORK]` initial tasks. Each item is intentionally bounded enough to
become one GitHub issue.

## Ready now

### T1. Complete RFC-0002, version and MVCC model `[COMPLETE]`

- Scope: commit ordering, exact replay, read-version availability, tombstones,
  and oldest-readable-version.
- Done when: examples and failure cases are precise enough to extend
  `okv-model` without guessing.
- Dependency: none.
- Evidence: RFC-0002 fixes the two-`u64` ordered encoding, canonical replay,
  range-clear precedence, exact read errors, inclusive retention boundary, and
  read-your-writes. `okv-model` executes each invariant.

### T2. Add generated differential histories `[ACTIVE-WORK]`

- Scope: produce deterministic sequences of set, clear, replay, and read; compare
  a candidate engine contract to `okv-model`.
- Done when: a deliberately incorrect engine fails with a minimized seed.
- Dependency: T1 for semantics beyond the current point model.
- Exists: five 1,000-event deterministic histories compare `okv-model` with an
  independently normalized full-snapshot oracle. Generated operations include
  canonical and conflicting replay, future and expired reads, inclusive
  retention, historical/gap reads, range clears, and stale generations. Seven
  negative subjects fail at deterministic steps 2 through 9; the clean subject
  has zero anomalies.
- Remaining: run the same contract against a storage-engine adapter after its
  range-clear, explicit-version read, and retention seams exist.

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
  loss model, `C_cell` and `O_cell`, retained-WAL bounds, ratekeeper thresholds, refusal,
  repair, and operator-visible telemetry.
- Done when: a 30-minute object PUT brownout has one bounded state transition
  table and one falsifiable `fault-recovery` suite configuration.
- Dependency: RFC-0005 and RFC-0007. Design can start immediately.
- Evidence: RFC-0005 defines `COMMITTED`, single-region RPO, `C_cell/O_cell`, and the
  normal, rate-limited, commit-refused, and recovery-only states;
  `evals/suites/fault-recovery.toml` owns the brownout lane. The workload
  executor remains gated on WAL and objectification components.

### T15. Adversarially review cell and tenant topology `[ACTIVE-WORK]`

- Scope: RFC-0011 cell boundary, tenant transaction domain, role-partitioning
  sequence, metacluster authority, and snapshot-plus-tail tenant movement.
- Done when: each invariant has a failure case; reviewers identify the first
  throughput, recovery, and control-plane ceiling; and one alternative topology
  is compared with the same workload and failure assumptions.
- Dependency: none. Read-only architecture review can start immediately.
- Exists: an internal Codex multi-agent review mapped the protocol roles,
  identified eight contract gaps, and produced five minimal negative controls
  in `docs/research/reviews/codex-cell-topology-2026-08-22.md`. RFC-0011 now
  separates the pre-cell substrate from complete Cell v0 and freezes one
  coordinator quorum per bootstrap cell.
- Remaining: one external database reviewer, alternative-topology comparison,
  and executable commit, frontier, resolver-partition, and tenant-move models.

### T16. Prototype exact DataFusion base-plus-tail semantics `[ACTIVE-WORK]`

- Scope: RFC-0010 `TableProvider`, ordered `SnapshotOverlayExec`, insert/update/
  delete and row-move tail, per-partition watermarks, and predicate invalidation.
- Done when: a row oracle and injected predicate-pushdown bug prove exact results
  at one target version across differently lagging partitions. Measure tail rows,
  bytes, memory, and query latency as `T - W_p` grows.
- Dependency: RFC-0010 review and a synthetic table-change fixture. Freeze
  primary-key encoding and ordering, atomic change coverage, schema-at-`T`,
  partition-move, lease, and exact-or-error rules first. It does not depend on
  the distributed transaction implementation.
- Exists: the internal review and five minimal negative controls are recorded in
  `docs/research/reviews/codex-htap-overlay-2026-08-22.md`. The configured hard
  gate requires exact canonical results rather than recall. The pure
  `okv-model` contract executes all five controls plus one multi-table,
  single-version case through `zebradb-htap-contract-v1` and OTel.
- Remaining: implement the DataFusion `TableProvider` and ordered physical
  operator, replace model rows with Arrow and Parquet fixtures, force a
  continuation across short physical transactions at one target `T`, and
  measure the cost curve as `T - W_p` grows.

### T17. Specify analytical dependency certificates

- Scope: transactional projections, dependency-token granularity, snapshot
  leases, validation retries, and uncertifiable query classes.
- Done when: one credit-exposure invariant is expressed both as a transactional
  projection and as a certified query; conflicting writers cannot commit from a
  stale result; retry and certificate-size tradeoffs are measured.
- Dependency: RFC-0008 and RFC-0010 drafts. The certificate must bind cell,
  tenant, snapshot, schema, plan rules, and phantom-safe dependency tokens;
  validation and write occur in one serializable transaction. Model work can
  begin independently.

### T18. Freeze and implement the Cell v0 commit envelope `[ACTIVE-WORK]`

- Scope: canonical conflict and mutation bytes, request identity, resolver-set
  acceptance, log tagging, generation fencing, quorum evidence, durable outcome
  recovery, and exact retry behavior.
- Done when: the pure contract model and a real replicated-log implementation
  pass the same normal and negative suite, including process crash between
  quorum commit and client reply.
- Dependency: RFC-0005, RFC-0008, and RFC-0009.
- Exists: `okv-sim` has a checksummed, length-delimited model envelope and
  reconstructs retained request outcomes from quorum-certified records. The
  `cell-commit-contract-v1` suite runs five seeds and six bounded negative
  controls through the shared result and OTel path. `okv-wal` now persists the
  envelope through checksummed frames on three local files and reconstructs a
  matching two-copy prefix after fresh opens. Its suite rejects six persistence
  violations through the same result and OTel path. `okv-consensus` pins
  OpenRaft `0.9.25` and passes its upstream storage conformance suite over an
  objectKV per-node journal. The `openraft-storage-contract-v1` gate reopens
  vote, committed index, append, truncate, purge, torn-tail, and corruption
  state and discards six negative subjects. The `openraft-cluster-contract-v1`
  gate now runs three actual nodes over a seeded Turmoil TCP network. It proves
  quorum commit through two leader changes, isolated-leader non-acknowledgement,
  stale-suffix replacement after repair, simulated process crash and bounce,
  and exact restarted-node catchup. Three unsafe cluster subjects discard.
  The `openraft-process-contract-v1` gate now starts three child processes over
  normal Tokio TCP, drops one reply after apply, kills the leader process,
  recovers the request outcome on its successor, retries exactly once, and
  restarts the killed node from its retained log. CI rejects disabled
  deduplication, acknowledgement without quorum, and skipped restart/catchup.
  The `generation-takeover-process-v1` gate adds a separate three-node authority
  quorum, G1 voters, and G2 learners. It proves a replicated data-log fence
  against a preauthorized in-flight write, recovery reservation, authority
  leader loss, competing-recovery rejection, quiesced voter-set handoff,
  recovery-phase write rejection, nonzero activation position, and exact G2
  continuation across nine real processes. Four unsafe subjects discard. The
  `generation-recovery-certificates-v1` gate pins active and pending data-voter
  public keys, collects exact-position Ed25519 attestations, verifies a distinct
  signer majority in replicated authority state, and rejects five certificate
  defects with exact replay.
- Remaining: load and rotate signing keys through a production secret boundary,
  separately persist or snapshot retained request outcomes, define
  retained-outcome expiry and compaction, and prove disk-full, replica repair,
  independent-disk failure, object-root reconciliation, and replacement
  recovery.

### T19. Audit Tigris architecture and codebase `[COMPLETE]`

- Scope: inspect the original FoundationDB-backed database module, current
  object-storage architecture, TAG, OCache, and TigrisFS at exact source pins.
- Done when: the study separates mechanisms that support objectKV from claims
  Tigris does not prove, records limitations, and turns findings into bounded
  eval and implementation work.
- Dependency: none. Read-only research.
- Evidence: `docs/research/tigris-codebase-study.md`.

### T20. Prove block-before-pointer publication and ground-truth GC

- Scope: model immutable block upload, authoritative pointer publication,
  ambiguous writes, unreachable objects, retained manifests, snapshot/query
  leases, and object deletion eligibility.
- Done when: crash and unknown-outcome histories never expose absent bytes or
  delete reachable bytes; a deliberately wrong accounting counter cannot make
  GC unsafe; an incomplete liveness walk fails closed.
- Dependency: RFC-0003, RFC-0004, and RFC-0007. The model can start before the
  storage-worker implementation.

### T21. Specify a transactional task stream

- Scope: versionstamped task records committed with data and index intent,
  worker claims, leases, retry, acknowledgement, and idempotent side effects.
- Done when: commit, crash-after-effect, duplicate-delivery, lease-expiry, and
  poison-task histories prove no missed work and no duplicate logical effect.
- Dependency: RFC-0002 and the Cell v0 commit envelope.

### T22. Build cache visibility and resurrection oracles

- Scope: version-addressed bodies/blocks, metadata-last visibility, stale
  generation, delayed populate, delete, overwrite, cache loss, and regional
  fallback.
- Done when: cache hits participate in the linearizability history; a
  delete-then-read, overwrite-while-streaming, or expired-barrier resurrection
  fails deterministically and replays exactly.
- Dependency: T13 simulation substrate. It blocks a complete direct-read claim,
  not the first immutable segment builder.

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
