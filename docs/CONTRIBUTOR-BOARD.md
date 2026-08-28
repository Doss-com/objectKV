# objectKV contributor board

Status: `[EVALUATING]` initial tasks. Each item is intentionally bounded enough to
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

### T2. Add generated differential histories `[EVALUATING]`

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

### T4. Implement object-store conformance fixtures `[EVALUATING]`

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

### T5. Build the Phase 0 benchmark runner `[EVALUATING]`

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
- `[CODE-COMPLETE]` The product program now resolves candidate/control metrics,
  rejects semantic or environment identity mismatches, and emits a signed
  threshold-plus-noise comparison receipt.
- Remaining: generic alternating repeat orchestration inside `okv-eval` and the
  broader `phase0.toml` workload executors.

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
- Remaining: one bounded SlateDB layout/compaction pass, a physical MinIO
  adapter, GCS profile, price snapshots, and named Gate 1 ceilings.

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

### T13. Build the exact deterministic simulation substrate `[EVALUATING]`

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

### T15. Adversarially review cell and tenant topology `[EVALUATING]`

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

### T16. Prototype exact DataFusion base-plus-tail semantics `[EVALUATING]`

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

### T18. Freeze and implement the Cell v0 commit envelope `[EVALUATING]`

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
- Evidence: `docs/research/tigris-codebase-study.md`. Current public heads were
  revalidated on 2026-08-23. The follow-up found that Tigris's public
  consistency scripts poll for convergence but do not prove serializability,
  acknowledgement durability, or exact retry outcomes. Those are separate
  objectKV gates.

### T20. Prove block-before-pointer publication and ground-truth GC `[VERIFIED]`

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

### T23. Replace the G4.3 local tail adapter `[EVALUATING]`

- Scope: expose retained committed transaction records from the real OpenRaft
  data authority or through one frozen `DurableLog` streaming interface.
- Done when: the G4.3 first-worker kill and distinct empty-scratch replacement
  return exact base, update, delete, and tail-only insert reads while commits
  continue, without trusting a local physical path or hydrating the full range.
- Dependency: RFC-0026 and `serving-worker-process-recovery-v1`.
- `[CODE-COMPLETE]`: the same process boundary, authority-selected logical root,
  object-base reader, tail overlay, full-hydration control, and skip-tail poison
  run against a three-file same-machine diagnostic adapter.
- `[CODE-COMPLETE]`: RFC-0027 exposes accepted transaction commands through
  linearizable frozen-target pages owned by the OpenRaft data state machine.
  G4.4 performs two catch-up rounds around four concurrent commits, applies
  `ClearRange`, and accesses no physical Raft journal path.
- `[EVALUATING]`: G4.5 measured the complete serialized state at 256, 1,024,
  and 4,096 lifetime commits with 256 live keys. Ideal txLog pop still produced
  9.172x snapshot growth, rejecting the current monolithic authority layout.
  The 12.005x no-pop control and rejected flat retained-only poison closed the
  causal and oracle checks.
- `[CODE-COMPLETE]`: RFC-0029 splits latest values, OCC history, transaction
  retry records, frontier-command retry, and recovery commands into explicit
  persisted owners. Replicated `R` and `Q(client)` advancement rejects stale
  reads and expired retries before mutation while `O` remains projected.
- `[EVALUATING]`: G4.6 held complete projected state to 1.0029x growth versus
  9.1694x for the object-only control. The serving-only poison was rejected
  with nine anomalies. The candidate still took 545.726 seconds against its
  180-second diagnostic budget.
- `[CODE-COMPLETE]`: RFC-0030 retains an exact pending immutable closure before
  the data authority physically pops through `O`, then requires a distinct
  data-voter quorum certificate before publication activation. G4.7 recovered
  exact object state after popping all 16 records, data and publication leader
  failover, and data-voter restart. Missing-pending and forged-coverage controls
  failed before pop; a subquorum activation left pending protection intact.
- `[EVALUATING]`: G4.7 passed every hard gate across seeds 4701, 4702, and 4703
  with 160.331 ms protocol p99 and 20.519 ms median physical-pop time. Dirty
  source, one machine, debug processes, and local object files prevent a
  verified durability or performance claim.
- `[CODE-COMPLETE]`: G4.8 adds bounded concurrent submission and per-voter
  stable-I/O observations without changing transaction bytes or retry meaning.
- `[EVALUATING]`: G4.8 preserved the correctness and recovery contract but was
  discarded as final group commit. It reached 153.708 median transactions per
  second versus 38.772 for the sequential control, a 3.964x gain, and reached
  264.887 ms maximum p99. The frozen gates were 200 transactions per second,
  4x, and 250 ms. Followers grouped entries; the leader remained one append per
  transaction. The early-ack poison was lost after quorum recovery and rejected.
- `[CODE-COMPLETE]`: RFC-0032 adds one explicit batch entry, shared commit
  version plus 16-bit batch order, independent request outcomes, exact retry,
  deterministic in-batch conflict resolution, and pair-valued recovery cursors.
- `[EVALUATING]`: G4.9 reached 559.511 median durable transactions per second
  and 34.016 ms maximum p99 versus 151.944 transactions per second for the one-
  entry control, a 3.682x gain. Every absolute and paired gate passed. Duplicate
  identities and early acknowledgement were rejected. Dirty source and one
  host prevent a verified performance or durability claim.
- `[CODE-COMPLETE]`: RFC-0033 adds bounded FIFO admission for independent
  requests, closure on item count, exact encoded bytes, delay, or sender
  shutdown, explicit queue-full and oversized rejection, and exact per-request
  result demultiplexing.
- `[EVALUATING]`: G4.10 discarded the 16-item, 64-caller configuration at
  131.488 ms maximum p99. The separately frozen 32-item candidate reached
  1,157.369 median transactions per second, 76.101 ms maximum p99, and 6.356x
  the same-durability one-entry control. Sparse, byte, overload, and oversized
  controls passed their scoped gates.
- `[CODE-COMPLETE]`: RFC-0034 emits backward-readable `OKVT2`, `OKVQ2`, and
  `OKVB2`. The 128 KiB byte control now fits eight 8 KiB-value transactions in
  a 119,731 byte entry instead of one transaction in an 89,097 byte v1 entry.
- `[CODE-COMPLETE]`, local receipts `[EVALUATING]`: RFC-0035 and
  `commit-proxy-object-frontier-v1` compose the 32-item path with a 25%
  deterministic conflict suffix and authenticated frontier advancement through
  frozen `O`. The candidate reached 1,075.343 resolved outcomes per second,
  104.274 ms maximum p99, and 28.776x the one-entry control while exact
  object-plus-suffix recovery and both poisons passed.
- `[PROPOSED]`: RFC-0036 freezes one clean independent-media and remote-object
  gate with eight frontier cycles, durable state snapshots, bounded physical
  journals, host-loss recovery, and required OTLP.
- `[CODE-COMPLETE]`, local receipts `[EVALUATING]`: G4.11a writes checksummed
  state snapshots through synchronized atomic replacement, rejects purge above
  snapshot coverage, canonical-compacts each journal, and reopens all three
  voters exactly. Journals fell from at most 6,391,575 bytes to 879 bytes.
- `[CODE-COMPLETE]`, local receipts `[EVALUATING]`: G4.11a.1 aligned `R`, a
  64-request `Q(client)` window, and authenticated `O` across four complete
  cycles. Snapshot growth passed at 1.091759x, but complete physical media
  failed at 19.692719x against the frozen 8x gate. The frontier mechanism
  remains; the replicated snapshot representation is discarded.
- `[CODE-COMPLETE]`, preflight `[EVALUATING]`: the same-history runner compares
  indexed row objects, indexed Parquet, and a hybrid row-capsule layout with
  exact point, scan, compaction, media, and branch accounting. The small local
  preflight rejects plain Parquet as the generic point path. Add the isolated
  Vortex subject, checksum-block request coalescing, and an honestly accounted
  typed sidecar before freezing admission thresholds.
- `[CODE-COMPLETE]`, local admission `[EVALUATING]`: the coalesced reader and
  split typed run are implemented. Run `f5dbba62` passed every frozen
  three-seed, three-repeat release-local gate. The split subject preserved the
  row control's point request and byte costs, reached 9.124x projected-scan
  throughput, and added 3.040% stored/live amplification. It is eligible for
  clean GCS evaluation. It is not the opaque KV default.
- `[CODE-COMPLETE]`: namespaced GCS execution and the frozen
  `storage-layout-gcs-admission-v1` suite.
- Remaining: restore and verify objectKV-dev GCP access; run remote point and
  scan admission; prove split-closure recovery and DataFusion
  overlay exactness; run G4.11b with clean source,
  independent media, remote GCS, and required OTLP; bound distinct-client
  cardinality; then decide Cell v0 admission before serving leases, scans, GCS
  product claims, PostgreSQL, or HTAP expansion.

### T24. Implement the native resident-engine boundary `[VERIFIED]`

- Scope: RFC-0040 empty activation, base plus retained-suffix materialization,
  atomic live advancement, version-bound engine snapshots, and direct resident
  point reads.
- Done when: every RFC-0040 correctness poison fails, the clean candidate and
  matched RocksDB snapshot control run in AB and BA order, throughput is at
  least 0.80x control, p99 is at most 1.20x control, local bytes remain bounded,
  and the measured resident window issues zero object operations.
- Dependency: RFC-0038 batch-aware retained cursor and RFC-0039 activation
  receipt. This task blocks RAM admission, multi-range work, PostgreSQL, and
  HTAP performance claims.
- Review focus: MVCC key encoding, RocksDB snapshot to objectKV read-version
  mapping, atomic applied-frontier publication, range-clear representation,
  value ownership in candidate and control, and process-death recovery.
- Stop: if both reversed-order receipts fail p99 again, use TiKV or
  FoundationDB for the resident and transaction data plane. Keep `okv-log`,
  publication, branching, reconstruction, and historical views above it.
- Result: the first unmatched-topology control produced 0.8411x and 0.8268x
  throughput with 1.210x and 1.272x p99. D56 reopened the native lane because
  the control omitted the six-process recovery topology. The matched AB and BA
  rerun passed at 0.9089x and 0.9197x throughput with 0.913x p99 in both
  orders. All four runs passed 16 hard gates and emitted all three OTel
  signals. The single-range read boundary is admitted; replicated commit and
  MultiRaft are separate gates.

### T25. Maintain the incumbent fallback transaction plane `[EVALUATING]`

- Scope: keep one minimal adapter that supports ordered commits, versioned
  reads, retained change capture, objectification frontier, and empty-worker
  restore. Use it as the semantic oracle, lifecycle control, and fallback while
  the native path advances.
- Done when: both source-pinned candidates have executable semantic receipts,
  every survivor runs the same frozen history and lifecycle suite on R0,
  limitations are explicit, and one fallback profile is selected by a recorded
  decision without weakening native-path gates.
- Dependency: D56, RFC-0040, and the native GP3.1 receipt.
- Review focus: transaction semantics, version mapping, change-feed retention,
  snapshot/export seams, restore ownership, operational complexity, license,
  and whether objectKV can stay off the incumbent hot path.
- Stop: P1 strict serializability is a knockout gate. Do not add a resolver,
  predicate-lock service, or serializable certifier above a provider to make it
  pass. Do not build lifecycle shims for a provider that fails semantic
  preflight. Remove the losing adapter after its evidence and decision record
  are durable.
- Current result: RFC-0041, `okv-plane`, and the source-pinned preflight are
  `[CODE-COMPLETE]`. `[VERIFIED]` the R0 FoundationDB probe rejected write skew
  and aligned commit, change, and outcome stamps. The R0 TiKV probe committed
  both disjoint writers and is removed from lifecycle work. FoundationDB is the
  sole remaining fallback profile. `[VERIFIED]`
  GP2.5.2 repeated exact closure, named GET, object-frontier CAS,
  empty-generation restore, chunk replay, digest, and stale-generation gates
  under the frozen `ca919518` evaluator and private GCP machine receipt. All
  three poisons were discarded, and all final run IDs occur in OTel logs,
  metrics, and traces. `[VERIFIED]` GP2.5.3 then deleted the source VM, boot
  disk, and provider SSD, observed all three absent, and reconstructed the
  exact 950-record digest on a fresh FoundationDB cluster. Its positive run
  passed 16 gates; the hidden-source-media control was discarded. Next are
  GP2.5.4 external cell-incarnation authority and the GP3.1
  direct-FoundationDB overhead pair. `[VERIFIED]` GP2.5.4's local authority
  composition now rejects stale commit, route, and publication operations and
  detects the corresponding three-surface poison. `[CODE-COMPLETE]` the
  dual-provider GCP resurrection harness. The next owned fallback action is its
  real receipt. Native concurrent-read and replicated-commit work proceed under
  separate gates.

### T26. Verify the native concurrent-read curve `[VERIFIED]`

- Scope: GP3.1.1 native and matched direct RocksDB reads at 1, 8, and 32
  clients, with the exact GP3.1 recovery topology and owned-value boundary.
- Done when: both process orders run from one clean revision on GCP R0; every
  8-client and 32-client pair retains at least 0.80x control throughput and at
  most 1.20x control p99; every run reports its exact client and operation
  counts, zero wrong values, zero measured object operations, and all OTel
  signals; all leased resources are removed.
- Dependency: T24, D56, D57, and RFC-0042.
- Review focus: synchronized start, exact operation partitioning, percentile
  aggregation, scheduler oversubscription, paired order, and whether native and
  control use the same RocksDB and value-ownership boundary.
- Current result: `[VERIFIED]` the deterministic runner and clean GCP R0
  receipt. At 8 clients, native throughput was 0.8798x and 0.8734x control;
  p99 was 1.1842x and 1.1220x. At 32 clients, throughput was 0.8803x and
  0.8906x; p99 was 1.1072x and 1.1478x. All explicit constraints passed in both
  orders. The eight results contain 120 samples, 24,000,000 measured reads,
  zero wrong values, zero measured object operations, correlated OTel signals,
  and complete scratch and lease teardown.

### T27. Freeze the native cache-pressure curve `[PROPOSED]`

- Scope: one explicit block-cache budget shared by native and direct control,
  a reusable immutable object fixture larger than that budget, and metrics for
  CPU time, cache hit ratio, physical bytes read, read amplification,
  throughput, and p99.
- Done when: cache configuration is part of resident activation and the direct
  control, the fixture is content-addressed and reused across samples, and the
  suite separates warm, mixed, and eviction-heavy points without rebuilding
  fixture state for every measurement.
- Dependency: T26 clean receipt. Do not combine this with the first concurrency
  gate.

### T28. Verify GCS cold-point and object-layout geometry `[EVALUATING]`

- Scope: reuse one content-addressed larger-than-cache fixture for indexed cold
  points, cache refill, row control, typed projection, and direct DataFusion
  scan lanes on the R0 runner.
- Done when: cold reads use bounded named requests and bytes independent of
  database size; point and projected-scan lanes each receive a clean paired
  GCS receipt with required OTel; complete split-closure recovery passes.
- Dependency: T27 freezes the cache budget and fixture identity. The two
  performance lanes remain separate even when they share provisioning.

### T29. Verify native replicated commit on independent media `[EVALUATING]`

- Scope: move the retained 32-item commit-proxy mechanism onto three machines
  with independent stable media and compare it with a same-durability control.
- Done when: one-range commit p99 is at most 1.25x control, acknowledged retries
  and conflicts recover exactly after leader and host loss, normal commits issue
  zero object operations, and all workload identities appear in OTel.
- Dependency: T28 fixes the permanent object representation used by recovery.

### T30. Bound objectification debt and local recovery media `[EVALUATING]`

- Scope: overlap sustained commit load with object publication, object-store
  slowdown and outage, state snapshots, txLog reclamation, and one-host loss.
- Done when: `C - O` converges after recovery, backpressure activates at the
  declared bound, acknowledged state remains exact, and complete local physical
  state stays at or below 8x logical live bytes.
- Dependency: T29 independent-media commit receipt.

### T31. Verify metadata branching and lazy empty-worker reopen `[EVALUATING]`

- Scope: compose authenticated object roots, txLog suffixes, copy-on-write
  branches, and empty serving workers under the admitted durability topology.
- Done when: branch time and initial durable bytes do not scale with parent
  database size; the first exact read fetches bounded metadata and requested
  blocks instead of hydrating the complete range.
- Dependency: T30 establishes safe frontier advancement and media reclamation.

### T32. Verify one multi-range cell `[PROPOSED]`

- Scope: add range routing and independent range groups, then execute bounded
  tenant transactions across multiple groups through the strict-serializable
  oracle.
- Done when: throughput rises with additional groups until a named resource
  saturates; failover never exposes a partial transaction; routing and
  generation changes fence stale owners.
- Dependency: T31 closes the single-range lifecycle and recovery contract.

### T33. Admit or reshape the RAM serving profile `[PROPOSED]`

- Scope: implement DRAM serving behind the same `ServingImage` contract and
  compare it with the admitted SSD profile under identical history, durability,
  RPC, concurrency, and recovery conditions.
- Done when: RAM improves one predeclared end-to-end metric by at least 20
  percent, respects its byte budget, survives bidirectional handoff, and never
  reports volatile replication as durable commit.
- Dependency: T32 produces the stable cell boundary. RAM is optional and does
  not block the SSD-backed cell.

### T34. Verify `okv-fabric` workload lanes `[PROPOSED]`

- Scope: expose application logs, the declared Redis subset, version-aligned
  inverted search, and object-catalog or virtual-filesystem metadata through one
  version and transaction fabric.
- Done when: each surface passes its independent semantic oracle and one frozen
  performance lane against its appropriate specialist control. No blended
  adapter score is allowed.
- Dependency: T32. T33 is required only for a lane that explicitly selects RAM.

### T35. Verify PostgreSQL page-storage OLTP `[PROPOSED]`

- Scope: run the pinned upstream PostgreSQL behavior and crash suites through a
  page-storage adapter over `okv-fabric`.
- Done when: the first prototype is within 2x local PostgreSQL, the resident
  target is within 1.25x, page and synchronization amplification are bounded,
  and restart does not double-apply WAL or page state.
- Dependency: T34 proves that the fabric boundary supports real consumers.

### T36. Verify exact DataFusion HTAP `[EVALUATING]`

- Scope: execute one PostgreSQL-derived columnar base plus its complete durable
  analytical tail at one target version, including mixed OLTP load.
- Done when: results are exact; a tail at or below 1 percent adds at most 20
  percent over base-only DataFusion; materialization intervenes before 10
  percent; OLTP and OLAP interference remains inside declared budgets.
- Dependency: T35 supplies the relational history and schema boundary.

### T37. Publish the comparative production envelope `[PROPOSED]`

- Scope: compare the complete path with TiKV or PostgreSQL plus object tier and
  with the relevant specialist for every `okv-fabric` workload.
- Done when: the report includes latency, throughput, tail behavior, local and
  object bytes, recovery, branch, operational failure surface, and cost for
  every lane, including losses. It selects implementation profiles rather than
  deciding whether objectKV continues.
- Dependency: T36 and the complete immutable receipt chain.

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
