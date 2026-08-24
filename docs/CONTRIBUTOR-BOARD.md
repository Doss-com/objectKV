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
- Exists: `phase0-slate-filesystem-v1` executes deterministic ingest, point
  reads, ordered scans, fresh-instance reopen, per-API request/byte accounting,
  hard gates, raw report artifacts, and OTel export through pinned SlateDB.
- Exists: raw reports are run-scoped, measurement phases no longer blend close
  with reopen, and the 12-hour fixed-cadence audit emits append-only receipts.
- Remaining: generic repeat orchestration inside `okv-eval`, incumbent pairing,
  noise verdicts, and the broader `phase0.toml` workload executors.

### T6. Establish the SlateDB baseline

- Scope: run fixed Phase 0 workloads through unmodified SlateDB on filesystem and
  MinIO.
- Done when: request count, bytes, latency distribution, cache state, compaction,
  and reopen results are captured with exact revision/profile identity.
- Dependency: T4 and T5.
- Exists: the first filesystem incumbent at candidate `12df9f8`. Clean run
  `84410878` kept with zero anomalies across three seeds. OTel run `794c45da`
  recorded the same logical receipt and bounded physical series. The
  warm-instance poison `e53a01c4` discarded despite returning the correct value.
- Exists: candidate `361a0fd` repairs the phase boundaries. Its 1, 8, and 64 MiB
  curve kept exact values, but 64 MiB open read 210,773,938 bytes before the
  first point read. D30 stops the untuned SlateDB incumbent and permits one
  bounded configuration pass. The overnight audit is collecting repeated
  noise and failure receipts.
- Exists: the bounded pass is candidate `7567b99`. Configured and confirmation
  runs `07dad330` and `5a9846fc` opened with 402 read bytes and kept the first
  cold point below five requests and 210,439 bytes across three seeds. The
  warm-instance control `c0affb91` discarded. Total requests rose 31.9 percent.
- Exists: candidate `b240b38` adds the local separate-role compaction gate.
  Runs `d6425f5e` and `5431c0fe` compacted eight L0 SSTs to one sorted run
  across three seeds, preserved every row, measured 1.027x maintenance write
  amplification, and retained bounded reopen and point reads. Missing-worker
  control `af37279a` discarded on four maintenance gates.
- Exists: candidate `803de76` adds eight overwrite rounds and a real worker
  process kill. Runs `238de077` and `882b1fcf` reclaim and complete through a
  fresh worker identity across three seeds in 576 to 618 ms, then verify every
  latest value. Missing-replacement control `af904d02` discards on two gates.
- Exists: candidate `abb2c64` runs the same format-compatible serving and
  compaction contract through pinned MinIO. Runs `229bfced` and `6f0e194b`
  keep three seeds with exact rows, 1.027x maintenance write amplification,
  538 fresh-open bytes, and five first-point requests. Missing-worker control
  `d1125f50` discards on the four intended maintenance gates.
- Exists: candidate `851decb` kills a real coordinator after a worker persists
  output but before manifest publication. Runs `ab8b22d4` and `e73b3458`
  prove distinct replacement coordinators commit the same output without a
  worker rerun in 29.4 to 30.5 ms. Missing-restart control `b2045e82` discards
  only on replacement identity and completion while L0 data remains exact.
- Exists: candidate `2c6a854` overlaps two real coordinator processes. Runs
  `aaaecbb6` and `85672759` advance compactor epoch 0 -> 1 -> 2, self-fence the
  stale processes in 13.56 to 21.61 ms, and complete exact compaction through
  the epoch-2 coordinators. External-kill control `2899bb28` reaches the same
  data but discards on the self-fencing gate.
- Exists: candidate `dea0b20` preserves completed but unpublished output while
  active compaction state roots it, then deletes a true aged orphan. Runs
  `8d606761` and `26b19dfb` keep three seeds; dry-run control `161eac32`
  discards only on deletion.
- Exists: candidate `d1ce1ec` registers checkpoint, clone, backup, analytical
  lease, and tenant-move roots durably, reclaims only an unpinned unique
  closure, and invalidates stale deletion when a lease is pinned after mark.
- Exists: candidate `9e733e2` starts a fresh serving process and reconstructs
  exact rows at `T=10` from object state through `O=8` plus a retained local
  quorum-WAL suffix. Its ignore-suffix control returns stale rows at `8`.
- Exists: candidate `e1c2437` removes the copied suffix, kills the transaction
  leader, and lets fresh workers fetch committed envelopes directly from live
  successor authorities. Its dropped-envelope control returns stale rows at
  `8`. Raw transaction proposals are explicitly not the serving feed.
- Exists: candidate `beec908` copies one committed envelope to three dedicated
  range-tagged tLog processes, enforces a hard retained-byte limit, kills one
  process, and reconstructs exact `T=10` from both survivors. Its missing-tag
  control remains stale at `8`.
- Exists: candidate `c549587` stages one transaction before visibility, waits
  for quorums from two three-process tagged log sets, survives proxy death
  before and after complete log durability, publishes once at `T=11`, returns
  the retained retry outcome, and reconstructs exact state in a fresh worker.
  Its one-set acknowledgement control stays visibly at `10` and discards.
- Exists: candidate `6a81821` replaces proxy-asserted receipts with
  policy-bound Ed25519 quorum certificates. Its five controls reject unsigned,
  duplicate-signer, wrong-log-set, tampered-statement, and obsolete-policy
  evidence before visibility.
- Exists: candidate `f350a12` carries one fully certified staged head through
  data-log fencing, voter-set handoff, successor activation, a lost takeover
  reply, exact old-envelope publication, and successor transaction 12. Five
  controls reject early takeover, incomplete or tampered evidence, skipped
  head, and generation rewrite.
- Exists: candidate `341beb9` durably fences every old tLog set, proves quorum
  absence in the incomplete set, aborts transaction 11 through the active
  successor, rejects late old-generation appends after process restart, and
  commits successor transaction 12. Six controls reject early, under-signed,
  incomplete, forged, volatile, and sequence-reuse subjects.
- Exists: candidate `900b646` recovers the longest quorum-present prefix from
  one four-record, 16 KiB staged window, aborts the first quorum-absent record
  and its dependent suffix, survives a lost reply and tLog restart, and commits
  the successor after the entire window. Six controls falsify unsafe boundary,
  ordering, limit, and inventory policies.
- Exists: candidate `868c3de` ratekeeps exact projected tLog frame bytes from
  fresh signed quorums before sequence allocation, survives objectification lag,
  durably pops both required sets, restarts a tLog, and reconstructs exact state
  through transaction 16. Six controls falsify partial, stale, single-node,
  unauthorized-pop, unreplicated-pop, and allocate-first policies. Each tLog
  now verifies a quorum-signed publication root and exact embedded snapshot
  frontier before local deletion.
- Exists: candidate `670ef0a` rebuilds one failed tLog as an empty non-voting
  learner from a quorum-certified retained snapshot, survives a learner
  restart, and requires a second active quorum to certify readiness. Fresh
  serving workers and capacity checks count only the active survivors. Six
  one-source, tamper, stale, incarnation, premature-counting, and duplicate
  live-identity controls discard.
- Exists: candidate `b69714c` moves the repaired learner through one replicated
  log-set policy epoch, persists successor activation, fences the removed root,
  commits transaction `17` after another member fails, and serves from only the
  active E2 quorum. Seven frozen policy controls discard.
- Exists: candidate `254cf421` resumes a failed tLog through durable base
  chunks, exact retry after restart, and an ordered tail while transactions
  `15` and `16` continue to commit. The learner and a fresh serving worker reach
  transaction `16`, but capacity and serving still count only active survivors.
  Seven volatile, missing, conflicting, gapped, stale, premature-counting, and
  full-recopy controls discard.
- Remaining: signer key custody and fence authorization, GCS, root expiry and
  abandonment, coordinator election and host-partition behavior, remote and
  multi-repair scheduling, transfer cleanup, concurrent range serving,
  proxy-failure gap recovery, online resolver-map movement, ratekeeping on the
  partitioned path, metadata propagation, price snapshots, and named Gate 1
  ceilings. Composed stateless-resolver and authenticated tLog recovery plus
  bounded multiple commit-proxy ordering now have local semantic receipts.

### T7. PostgreSQL bridge surface spike `[COMPLETE]`

- Scope: trace PostgreSQL storage manager, buffer manager, WAL/checkpoint, and
  bootstrap paths for one pinned upstream revision. Do not implement a fork yet.
- Done when: the smallest page/storage bridge boundary and unavoidable fork
  surface are documented with source links and a boot sequence.
- Dependency: none. Read-only research.
- Evidence: pinned PostgreSQL 18.6 commit `724edf9` has a static internal
  `f_smgr` switch with no runtime registration hook. The page bridge therefore
  requires a maintained fork. PostgreSQL WAL, LSN, tuple MVCC, transaction
  status, checkpoint, and recovery remain the sole authority; objectKV is a
  subordinate page store. The full source inventory, non-`smgr` state, boot
  sequence, controls, and staged milestones are in
  `docs/research/postgres-18-6-storage-bridge.md`. The proposed configurable gate
  is `evals/suites/postgres-page-bridge.toml`.
  A compile-and-boot probe now selects a second `f_smgr` slot for `initdb`,
  relation lifecycle, checkpoint, and restart. It delegates to `md`, so it
  admits only the maintained-fork seam. Evidence is in
  `experiments/postgres-smgr-probe/`.

### T7a. PostgreSQL read-only page adapter `[COMPLETE]`

- Scope: map PostgreSQL physical page identity and bytes onto the admitted
  routed-read client without introducing a second commit clock.
- Exists: candidate `0871dec` adds `okv-postgres`, fixed-width ordered page
  keys, authenticated 8 KiB page values, independent objectKV-version and
  PostgreSQL-LSN frontiers, and point plus consecutive-page reader methods.
- Process proof: candidate `8fb20e5`, correct run `977b368d`, routes three real
  encoded pages across two ranges at fixed objectKV version 2. Missing page,
  payload corruption, changed version, and page LSN beyond frontier controls
  discard.
- Callback proof: candidate `b04b128` imports one actual 148-page PostgreSQL
  heap into an authority-bound objectKV view. A fresh PostgreSQL 18.6 process
  reads all blocks through a separate page service, the routed KV Runtime, and
  `smgr_startreadv`, returning the exact 2,000-row aggregate. Thirteen callback
  reads cover blocks 0 through 147 with no `md` read fallback.
- Controls: stopping the page service causes connection refusal; changing the
  fixed frontier causes a typed refusal. PostgreSQL's sync I/O mode still uses
  the AIO callback path, so the probe includes a fork-only synchronous
  completion helper.
- Not included: page writes, objectKV block count, WAL ordering, buffer
  invalidation, sync, checkpoint, asynchronous objectKV I/O, or crash recovery.

### T7b. PostgreSQL page-write and stable-barrier contract `[ACTIVE-WORK]`

- Scope: replace `smgr_extend`, `smgr_writev`, `smgr_nblocks`, and the stable
  sync family for one selected relation without creating a second commit
  authority.
- First proof `[COMPLETE]`: the write path refuses a page unless PostgreSQL WAL
  is durable through that page LSN, then reconstructs the accepted mutation in
  a fresh Range Engine and returns it after PostgreSQL process restart.
- Local durability proof `[COMPLETE]`: objectify the base, retain an
  authenticated txLog suffix, restart the page service with empty process
  memory and no readable source heap, return the exact accepted page, accept a
  new checkpoint, and recover it after a second restart.
- Admission proof: candidate `c3c5df9`, suite `postgres-page-write-gate-v0`,
  correct run `0bf18a75`, admits three bounded two-page batches only after WAL
  reaches page LSN 900. All 15 semantic checks pass and all three replays are
  exact. WAL-behind `118ba54b`, zero-version `ee71a5b4`, oversized-batch
  `b14da383`, and wrong-digest `c74e05ad` discard.
- Current boundary: the gate emits deterministic `CellMutation` batches and a
  digest.
- Commit proof: candidate `7de5c4e`, suite
  `postgres-page-commit-process-v0`, correct run `bb7e18fa`, commits two pages
  plus one extent in one Cell transaction per seed. Duplicate retry returns the
  original outcome without advancing the version. After three leader handoffs,
  all six pages and all three `nblocks=2` extent values remain exact. Missing
  extent `5816809e`, changed retry identity `247a6cdb`, wrong receipt identity
  `68282231`, and non-advancing version `71d18d48` discard.
- Callback proof: candidates `f89f8c1` and `402e0ae`, suite
  `postgres-smgr-write-process-v0`, route one selected PostgreSQL 18.6 main
  fork's read, existing-page write, and block-count callbacks through objectKV.
  A real checkpoint committed block 0 and unchanged `nblocks=148` through the
  Cell, advanced version 5 to 9, constructed a fresh Range Engine, and returned
  the changed row after PostgreSQL restart. The local heap SHA-256 did not
  change. Stale version and forced WAL-behind controls refused; the latter kept
  zero committed batches and returned typed `WalBehindPage`.
- Atomic current-view proof: a PostgreSQL backend began at physical page-store
  version 5, its checkpointer advanced version 9, and the same backend selected
  version 9 inside the next `smgr_nblocks` operation. The following query
  returned the exact relation size and changed row. There is no discovery
  round trip, and an explicit pinned version 5 still refused. PostgreSQL's
  logical MVCC snapshot did not move with this subordinate storage version.
- Sidecar-recovery proof: candidate `3bb2783` materializes the version-5 base
  into one exact SlateDB object closure and retains Cell envelopes in two
  required signed txLog sets, each three local processes with quorum two. A
  restart using a nonexistent source path recovered version 10 and row 7,
  accepted row 8 through version 11, and a second restart recovered four tail
  records through version 12. Removing the txLog quorum or live SST from
  disposable roots made startup fail closed.
- Stable-sync proof: PostgreSQL's native sync queue now dispatches selected
  relation tags through operation 4. Version 13 was selected by a three-process
  authority at term 3, index 4 before checkpoint completion. A page-service
  restart reconciled that exact root. With the authority down, hot txLog state
  reached version 14 but PostgreSQL refused the checkpoint and stable version
  13 remained fixed.
- Current boundary: the publication-authority harness does not survive restart,
  replay into the bounded Cell baseline is not production generation recovery,
  objectification does not reach stable `B`, txLog pop is not root-pinned, and
  non-serial concurrent view publication remains unproved. `smgr_extend`,
  truncate, unlink, host-loss recovery, remote objects, production AIO, and
  OTel remain out of scope. The first authority-published debug checkpoint took
  829 ms, including 160 ms in sync, and is not a performance target.
- Next proof: objectify and restore the authority-selected root through `B`,
  bind txLog retention and pop to it, survive authority restart, then replace
  the serialized write critical section with immutable generation publication
  and conflict retry.
- Hard controls: page before WAL, false checkpoint barrier, stale-page
  overwrite, truncate resurrection, missing fork, and acknowledged transaction
  loss.
- Not included: remote performance tuning until the local semantic and crash
  matrix passes.

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
  coordinator quorum per bootstrap cell. The first vertical process proof now
  composes the transaction quorum, immutable envelope closure, and separate
  publication quorum under one generation fence; it reconstructs an
  empty-cache worker at `C_cell` and holds log pop to
  `min(O_cell, S_authority)`.
- Remaining: one external database reviewer, alternative-topology comparison,
  multi-range frontier, resolver-partition, and tenant-move models.

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
  single-version case through `zebradb-htap-contract-v1` and OTel. Candidate
  `4eddce5` adds a real DataFusion 54 `TableProvider` and custom
  `SnapshotOverlayExec`, reads a schema-v1 Parquet base, overlays a schema-v2
  Arrow tail, preserves hidden keys below SQL projection and filtering, and
  normalizes a cross-partition move. Run `2b487a75` kept all physical gates at
  zero anomalies; three poisoned subjects discarded; OTel run `4cf2f747`
  exported the result, tail, materialization, spill, and duration series.
  Candidate `95d57b7` adds an incremental ordered merge that holds one base
  batch, one tail batch, one bounded logical-id group, and one output batch. It
  validates order across batch boundaries, scans the tail from `min(W_p)` for
  independently lagging west and east bases, and binds continuation to one
  target version. Run `b239a722` kept 24 checks at zero anomalies with four
  peak buffered rows and no spill. Five materialization, grouping, watermark,
  continuation, and ordering subjects discarded exactly. OTel run `1fa53987`
  exported the bounded streaming series.
- Remaining: add snapshot-manifest and lease acquisition, split execution into
  multiple logical-id ranges, prove partition-aware pruning without losing
  invalidation, and measure the cost curve as `T - W_p` grows. The current
  memory receipt covers the overlay operator, not the complete query plan.

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
  defects with exact replay. Candidate `693cf26` adds the first
  fresh-incarnation repair lane: blank node `4` receives authority snapshot
  `8`, replays the retained suffix through transaction position `10` and
  membership position `11`, preserves exact retained outcomes, and restarts
  exactly. Run `799804f8` kept 60 checks at zero anomalies; the log-only control
  `671b6db8` discarded with two anomalies per seed. Candidate `22f2b09` adds
  the RFC-0023 routine-reconfiguration model. Run `486e5799` kept 95 checks at
  zero anomalies while preserving generation `7` and advancing membership
  epoch `4` to `5`; eight unsafe controls discarded same-identity replacement,
  unauthorized learner admission, early promotion, stale epoch, competing
  controller, double finalize, removed-voter commit, and repair without a data
  quorum. Candidate `76116dc` then executes the admitted protocol through a
  three-process authority quorum and four data processes. Run `bfcfe002` kept
  57 checks at zero anomalies across three seeds, advanced membership epoch
  once without changing generation, rejected 15 controls, survived 15 process
  kills, committed after the voter swap, and restarted the replacement exactly.
  Candidate `1e01b08` adds the first real-process concurrent Cell v0 history.
  Runs `9616bf69` and `f66bb379` each evaluated 3,000 logical transactions
  across three seeds with 2,100 commits, 900 durable conflicts, three leader
  kills, exact lost-reply retry, exact replay, and zero anomalies. The
  omitted-read-conflict control `c837f980` committed every transaction and
  discarded with two intended anomalies per seed.
- Exists: candidate `a93041f` adds linearizable read values and non-overlap
  order to that history. Run `56a132c6` checks 1,200 observed values, 300
  committed actual-read dependencies, and 727,650 real-time edges with zero
  anomalies. Omitted-conflict control `aa460aa8` commits every transaction but
  fails the actual-read-dependency class.
- Exists: candidate `65664bf` partitions conflict checking across three
  ordered resolver processes while preserving cross-range transactions. OTel
  run `8be62401` matches the centralized oracle across 1,800 attempts, 1,200
  commits, 600 conflicts, 3,003 signed decisions, 3,000 finalizations, exact
  rows, exact envelope chains, and three restart replays. Seven routing,
  partial, identity, epoch, durability, ordering, and split controls discard.
- Remaining: load and rotate signing keys through a production secret boundary;
  measure remote snapshot and suffix transfer, commit pause, and authority
  lease behavior; define retained-outcome expiry and
  compaction; and prove disk-full, independent-disk failure, object-root
  reconciliation, replacement recovery, bounded history search, range phantoms,
  online resolver-map movement, proxy batching, and hot-range behavior.
  Same-ID, file-copy-only repair is explicitly rejected by D36.

### T19. Audit Tigris architecture and codebase `[COMPLETE]`

- Scope: inspect the original FoundationDB-backed database module, current
  object-storage architecture, TAG, OCache, and TigrisFS at exact source pins.
- Done when: the study separates mechanisms that support objectKV from claims
  Tigris does not prove, records limitations, and turns findings into bounded
  eval and implementation work.
- Dependency: none. Read-only research.
- Evidence: `docs/research/tigris-codebase-study.md`. Current public heads were
  revalidated on 2026-08-23. The follow-up found that Tigris's public
  consistency scripts poll for convergence but do not prove serializability,
  acknowledgement durability, or exact retry outcomes. Those are separate
  objectKV gates.

### T20. Prove block-before-pointer publication and ground-truth GC `[EXISTS]`

- Scope: model immutable block upload, authoritative pointer publication,
  ambiguous writes, unreachable objects, retained manifests, snapshot/query
  leases, and object deletion eligibility.
- Done when: crash and unknown-outcome histories never expose absent bytes or
  delete reachable bytes; a deliberately wrong accounting counter cannot make
  GC unsafe; an incomplete liveness walk fails closed.
- Dependency: RFC-0003, RFC-0004, and RFC-0007. The model can start before the
  storage-worker implementation.
- Exists: RFC-0007 now freezes intent-before-upload, block verification before
  root publication, complete reachability walks, counter and `LIST`
  non-authority, quarantine, and delete-time root revalidation. The
  `object-publication-gc-v1` suite declares one correct subject and six bounded
  unsafe subjects against five deterministic seeds. Candidate
  `2b71dc8a3735b10e0ff94ad593d0c8df3fab21ed` passed the clean contract run
  `ed897a5f-02c4-4abe-997a-a06ea63bbb8e` with zero anomalies, all six unsafe
  subjects discarded exactly, and OTel logs, metrics, and traces verified in
  run `09e4760d-c125-41da-bf98-adb816385629`.
  Candidate `602b317` then executes the protocol through Apache `object_store`
  filesystem bytes and a checksummed, quorum-fsynced local authority. Run
  `e83eeb60` passed 48 checks at zero anomalies across three seeds. It recovered
  lost PUT, authority, and DELETE responses, reopened authority nine times,
  deferred three stale delete plans, and blocked three publications behind
  durable deletion reservations. Seven physical boundary violations discarded;
  OTel run `beaa7904` exported logs, metrics, and traces.
  Candidate `b530321` then moved intents, roots, pins, reservations, and durable
  outcomes into the three-node OpenRaft generation authority. Run `550e5585`
  passed 72 process checks across three seeds with two leader losses per seed,
  exact fresh-process replay, and zero anomalies. Ten unsafe authority subjects
  discarded; OTel run `8071bc8a` exported logs, metrics, and traces.
  Candidate `ffc0c84` then adds the first dedicated publisher boundary. Run
  `3b5cb41f` committed `Prepare` through three real authority processes, killed
  the publisher before its first object PUT, removed its scratch directory,
  and completed exact closure verification plus atomic root publication from a
  replacement process. It kept 30 checks at zero anomalies. The poisoned
  upload-before-Prepare run `26bde1fa` discarded with eight anomalies per seed;
  OTel run `ce7692da` exported logs, metrics, and traces.
  Candidate `a6dfeed` crosses the first object-effect boundary. Run `a4a1aec5`
  retained the first successful PUT while replacing its response with unknown,
  killed the publisher, and recovered from replicated intent plus exact named
  object identity in a replacement with empty scratch. It kept 36 checks at
  zero anomalies. Partial-closure run `fa9d729b` discarded with four anomalies
  per seed; OTel run `b57f141f` exported logs, metrics, and traces.
  Candidate `57e28d4` crosses the manifest-effect boundary. Run `2660e09d`
  retained the successful manifest PUT while replacing its response with
  unknown, killed the publisher, and made an empty-scratch replacement replay
  all data identities, recover the exact manifest, and walk the complete named
  closure before publishing. It kept 39 checks at zero anomalies.
  Manifest-only run `7ace2812` discarded with four anomalies per seed; OTel run
  `5fd6240e` exported logs, metrics, and traces.
  Candidate `72df70c` crosses the replicated `Publish` outcome boundary. Run
  `a544deff` dropped the successful reply after the root transition, killed the
  publisher and accepting authority leader, and made an empty-scratch
  replacement recover and exactly replay the retained outcome without another
  authority transition or object PUT. It kept 42 checks at zero anomalies.
  Convergence-only run `82698bdb` reached the same root and closure but
  discarded with four anomalies per seed, two `Publish` applications per seed,
  and no recovered outcomes. OTel run `50ad5d86` exported logs, metrics, and
  traces.
- Remaining: model partial multipart residue and repeated unknown responses,
  then kill the
  sweeper around complete mark receipt, delete reservation, object effect, and
  retirement. Prove effect-grant fencing, generation handoff, old-root
  deletion, and independent empty-disk recovery; add cloud-native
  guarded-delete receipts, partition reservations, and publication and
  reclamation cost curves.

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

### T23. Implement the replicated snapshot-lease authority `[ACTIVE-WORK]`

- Scope: extend `okv-publication` with lease admission, renewal, release,
  replicated logical expiry, monotonic `F`, prepared collection jobs, exact
  publication receipts, and `G`; then carry it through the existing OpenRaft
  publication authority.
- Done when: RFC-0060's three-process history survives lost replies, leader
  replacement, snapshot restore, renewal versus expiry, worker death, exact
  replacement publication, and root-aware deletion; every negative subject
  discards with OTel receipts.
- Dependency: RFC-0059 local collection curve, complete at candidate `3c9f008`;
  RFC-0015 publication authority and RFC-0035 root graph, both existing.
- First files: `crates/okv-publication/src/lib.rs`,
  `crates/okv-consensus/src/publication.rs`,
  `crates/okv-consensus/src/state_machine.rs`, a new process contract, and a
  frozen eval suite.
- Failure mode: lease state and object roots commit in different histories, or
  failover loses a lease, job, frontier, root, or retained request outcome.
- Tradeoff: one authority serializes long-read admission and publication. It
  gives up an independently scalable lease service to remove an unsafe
  cross-authority gap.
- Current slice: pure transitions, checksummed snapshot restore, and the correct
  three-process lost-response history exist. Candidate `87794a6` adds five
  authority faults and process controls. Together with missing retained
  outcomes, six subjects discard independently on all three seeds: backdated
  admission, omitted lease-root epoch changes, stale range epoch, premature
  `G`, stale input root, and missing retry outcome.
- Physical slice: candidate `3c8a52e` binds an issued `CollectionJobToken` to
  the exact SlateDB input manifest plus live SSTs before compaction, re-reads
  the exact output closure, replaces the authority leader, and publishes
  through the successor. Correct run `a9d1b1f8` kept; omit-SST, semantic-root,
  and skipped-failover controls discarded in `0f0232da`, `15ecd6ac`, and
  `ad93d32a`.
- Serving-root slice: candidate `b228bd3` verifies the authority manifest path,
  length, and SHA-256, hides newer SlateDB manifests, disables WAL replay, and
  keeps both M0 and M1 retained MVCC reads exact after internal latest moves.
  Correct run `49d4d445` kept 27 checks on three seeds.
- Base-tail admission: `fc30e59` binds the manifest, base frontier, retention
  floor, generation, and base commit-chain digest into one `AuthorityRangeRoot`.
  It accepts only a gap-free commit-chain suffix with increasing versions,
  exact target coverage, and quorum certificates for every required txLog set.
  Numeric versions may skip non-commit log positions. Correct run `da53cee9`
  composes a three-node publication authority, two signed txLog sets, and two
  disposable workers across M0=3, M1=5, and T=10. Six controls discard.
- Collector process: candidate `c79e099`, run `3a0e5bfb`, moves the physical
  collector into a child process and re-hashes both closures after it exits.
- Old-root reclamation: candidate `2742400`, run `7805dd6d`, protects M0 through
  a snapshot lease, deletes only after a fresh post-release mark and exact
  permits, and keeps the post-GC M1 worker exact.
- Cache slice: candidate `7071e33` injects shared decoded RAM and bounded NVMe
  caches below authority-root filtering. Repeated backend point requests fell
  from 64 to zero and scan requests from 80 to one on the 16K release pair.
- Streaming scan slice: candidate `20899e7` replaces
  `limit + affected_tail_keys` materialization with an authority-bound base
  cursor and ordered resident-tail merge. Zero-tail and 1,024-record-tail raw
  scans now both make 80 backend range GETs. The long-tail scan improves from
  91K to 186K rows/s while preserving exact ordered results.
- Persistent-NVMe slice: candidate `79afb08` reopens a fresh decoded cache over
  the existing local block-cache directory. First-point data and scans transfer
  zero backend bytes and make zero successful range GETs. View open still
  transfers 788 bytes of manifest metadata, so this is local data reuse rather
  than offline worker bootstrap.
- Corruption slice: candidate `63c9531` overwrites every cached data part and
  rejects any non-exact value after reopen. The current focused fixture detects
  corruption and repairs exact bytes from the backend.
- Historical-authority slice: candidate `7eae670` binds historical cache opens
  to the exact active lease, outer published root, inner immutable-base
  manifest closure, and target version in a supplied authority snapshot.
  Release or mismatch refuses before any storage request.
- Process-freshness slice: candidate `e06a159`, correct run `2b1bdc6a`, makes a
  fourth worker read live authority after M0 lease release and refuse the warm
  persistent-cache reopen. Stale-snapshot run `93773b96` reopens M0 in every
  seed and discards. M0 compaction also makes old data physically reclaimable.
- Authority-unavailable slice: candidate `52ca95e`, correct run `805cc0cf`,
  makes a fifth worker persist a fail-closed receipt when its bounded authority
  read fails. Stale-fallback run `1c769733` reopens M0 in every seed and
  discards.
- Cache-byte fault slice: candidate `505c997`, correct run `83a36734`, prepares
  real persistent caches, exits, overwrites or truncates every cache part, and
  reopens through fresh workers and decoded RAM. It kept 24 checks across 12
  workers, repaired all 30 damaged parts exactly from the backend, and returned
  zero wrong values. Four omitted-fault and accepted-wrong controls discard.
- Multi-range eviction slice: candidate `5f7bf82`, correct run `9375c874`,
  forces eight logical ranges through one 192 KiB cache. Reverse rereads remain
  exact, the cache settles below 132 KiB, and 130 backend range refills prove
  eviction. Unbounded-cache, skipped-reread, and accepted-wrong controls
  discard.
- Atomic publication slice: candidate `e0f1b12` replaces manifest-only serving
  compare with `{full authority root, target version, final txLog chain}`.
  Sixteen retained readers remain exact at `T=5`, later readers are exact at
  `T=8`, and a stale same-manifest publisher is fenced. This is a focused
  in-process regression. Candidate `e3866b2`, correct run `0aa7c992`, promotes
  it into three child processes with 18 publications, 288 exact cross-generation
  reads, and zero mixed results. Four controls discard. Sustained throughput,
  slow-reader memory, worker failure, and OTel export remain open.
- Routed-read slice: candidate `6d0cf63` adds one bounded KV Runtime TCP router
  above many local Range Engine assignments. Requests carry cell, tenant,
  range, epoch, and exact `T`. The focused real-TCP path returns exact point and
  scan answers, fences stale routing, returns a crossing split, and refuses an
  unapplied snapshot. Follow-on `6361695` routes two non-overlapping local
  assignments. Next freeze independent-process latency, route refresh,
  saturation, failure, security, and telemetry controls. Candidate `bd9d959`,
  correct run `740e7111`, now admits the independent-process sequential gate:
  192 exact points, 48 exact scans, three worker kills, and p99 below 0.24 ms on
  process-warm loopback. Four controls discard. Candidate `b068256`, correct
  run `7636b6fc`, then refreshes a stale map once per seed, preserves `T=8`,
  and returns 21/21 ordered rows across a split. Three controls discard.
  Replicated RangeMap publication, replacement routing, sustained multi-tenant
  load, remote misses, security, and OTel remain open.
- Next slice: replay the frozen cache-state matrix on `objectKV-dev` GCS, then
  freeze continuous mixed reads plus repeated tail publication, then measure
  routed network reads and remote rebuild.
  Candidate `f496e8d` adds the GCS profile, unique scratch prefixes, and cleanup
  gate for cache eviction. Candidate `be78904` removes the provider-bound
  range suite's cloud discard stub and adds exact-generation reads, guarded
  per-process prefixes, controller cleanup after worker failure, a pinned
  request-cost snapshot, and the same local/cloud receipt. Blocker:
  reauthenticate the authorized gcloud operator, verify the project and bucket,
  export `OKV_GCP_PROJECT` plus `OKV_GCS_BUCKET`, and provide an OTLP endpoint. The
  cloud run must record the bucket's retained-generation and soft-delete cost,
  not only zero live objects after cleanup. In parallel,
  complete controls for
  worker-local expiry, renewal resurrection, incomplete snapshot restore,
  stale generation, and stale delete marks.
- Bound found: the current collector uses SlateDB's database namespace and a
  broad `kv-runtime/` reservation. Per-range concurrent collection needs either
  explicit output namespaces or isolated staging plus promotion.

## Opens after Gate 1

- Promote the externally versioned SlateDB spike into the stable engine contract.
- Add immutable segment compatibility fixtures.
- Add the manifest inspection CLI.
- Replace the serialized atomic-current write path with immutable generation
  publication and bounded conflicting-writer retry.
- Move PostgreSQL complete-relation objectification off synchronous stable sync,
  aggregate every relation fork into one database root, and preserve the
  existing publication-authorized txLog pop invariant.
- Persist and recover the external Cell and publication authorities across
  independent hosts, then prove remote empty-cache page-service reconstruction.

## Opens after Gate 2

- Empty-cache serving worker.
- Kill/restart and lost-ack fault scenarios.
- PostgreSQL restart durability suite over objectKV.

## Not ready

- Multi-WAL partitioning.
- Online resolver split and merge, pipelining, and hot-range balancing.
- Multi-region writes.
- Vortex in the transactional path.
- New SQL optimizer or PostgreSQL-compatible frontend.
