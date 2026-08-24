# objectKV domain context

This file defines vocabulary and current facts. Behavioral policy lives in
`AGENTS.md`.

## Names

- **objectKV**: `[EXISTS]` the open-source object-native transactional
  ordered KV kernel and public repository name.
- **okv**: the CLI, Rust package/module, configuration-prefix, and local
  shorthand namespace for objectKV.
- **ZebraDB**: `[FUTURE]` the DOSS database product intended to consume
  objectKV through PostgreSQL and version-aligned DataFusion execution.
- **serving model**: Redis, inverted search, PostgreSQL, or another semantic
  consumer implemented above the ordered transaction contract.
- **transactional segment**: an immutable row-oriented encoding of ordered MVCC
  entries. Its format may change access economics but not key ordering, version
  visibility, tombstones, range deletes, merge operands, or transaction
  atomicity.
- **analytical artifact**: a schema-aware Parquet or Vortex materialization with
  an explicit covered-through version. It is derived from the logical history
  and is not authoritative for newer commits.
- **objectification**: publishing committed mutations into immutable object
  segments and advancing an authoritative durability reference.
- **KV Runtime**: `[ACTIVE-WORK]` one disposable RAM and NVMe serving process.
  It hosts many Range Engine assignments under one process-wide cache and
  pressure envelope. The accounted envelope exists; a routed concurrent
  production role does not.
- **Range Engine**: `[ACTIVE-WORK]` one logical ordered-range assignment inside
  a KV Runtime. It carries routing identity, applied frontier, object base,
  recent MVCC overlay, and resource accounting. It is not one OS process,
  private cache, or permanent durable replica.
- **serving worker**: legacy name retained in accepted RFCs, eval identifiers,
  and process fixtures. New architecture prose uses KV Runtime for the process
  and Range Engine for its logical assignment.
- **txLog**: `[ACTIVE-WORK]` public architecture term for a replicated
  transaction log. Existing Rust symbols, eval IDs, and historical receipts use
  `tlog`; those exact compatibility identifiers are not mechanically renamed.
- **object durable version**: the highest commit version reconstructable from
  authoritative object metadata and immutable objects without older WAL.
- **commit version**: a cell-scoped total-order identifier assigned to an
  accepted commit. Different cells have independent version spaces.
- **range**: `[FUTURE]` a contiguous interval of one cell's ordered keyspace and
  a routing/work-assignment unit, not a permanent data replica.
- **tenant database**: `[PROPOSED]` the normal transaction domain. One bounded
  transaction may span its keys and ranges but cannot cross cells.
- **cell**: `[PROPOSED]` one complete distributed transaction, durability,
  storage, control, and recovery cluster with its own versions and generations.
- **metacluster**: `[FUTURE]` the tenant-to-cell placement, routing-epoch, and
  migration authority. It does not join cell transaction histories.
- **analytical tail**: `[PROPOSED]` durable table changes after a partition's
  columnar base watermark, retained independently when required beyond recovery
  WAL retention.
- **snapshot lease**: `[PROPOSED]` a bounded pin on base manifests, schema, and
  analytical tail through one query version, not an open OLTP transaction.
- **eval lane**: one research objective with one primary metric and frozen hard
  gates.

## Current facts

- `[EXISTS]` The repository contains a Rust workspace, an in-memory reference
  model, a pinned SlateDB adapter spike, a configurable eval runner, an OTel
  path, an exact seeded generation-fencing probe, an object-store conformance
  runner, local persisted-WAL contracts, a pinned OpenRaft per-node storage
  adapter, a deterministic three-node OpenRaft failover contract, and
  planning/RFC scaffolding.
- `[EXISTS]` `okv-object` exposes a deterministic KV Runtime resource envelope.
  The frozen RFC-0056 suite assigns 1, 100, and 1,000 Range Engines with 4,608
  fixed accounted RAM bytes per range and one shared 128 MiB RAM plus 2 GiB
  NVMe cache demand. All hard gates pass; four per-range-cache, early-refusal,
  hard-debt, and missing-movement controls discard. This is configured resource
  accounting, not physical RSS, task, file-descriptor, local-file, or SlateDB
  instance density evidence.
- `[EXISTS]` The RFC-0057 physical-density gate selects one pinned SlateDB
  database with logical range prefixes as the KV Runtime default. The exact
  committed child-process harness passed all nine correct points and discarded
  all four controls. At 1,000 assignments the selected layout held 9 live
  tasks and 9 object files, compared with 8,001 tasks and 9,000 object files for
  both database-per-range layouts. This is a cardinality result, not an
  accepted capacity or production-performance claim.
- `[EXISTS]` The SlateDB spike can apply externally assigned versions and reject
  conflicting replay. Its objectKV-owned MVCC layout supports exact point and
  ordered range reads at `T`, retained-window physical collection, and a
  read-only manifest-pinned base. A Range Engine view now
  combines that base with a gap-free commit-chain txLog suffix whose increasing
  commit versions may skip non-commit log positions and whose required log sets
  each carry a valid quorum certificate. `[EXISTS]` The process handoff gate
  resolves outer published roots through replicated authority, serves M0 and
  M1 through real signed txLog quorums, warms persistent cache, refuses an old
  root after lease release using live authority, reclaims the compacted M0
  closure, and keeps M1 exact. Its stale-authority negative reopens M0 and
  discards. A fifth worker also refuses when authority is unavailable, while
  the stale-fallback negative reopens M0 and discards. A dedicated process gate
  now overwrites and truncates real persistent-cache parts, reopens each with a
  fresh process and decoded cache, and accepts only exact backend repair or
  refusal. Its four exercise and wrong-value controls discard. Another process
  gate forces eight logical Range Engines through one 192 KiB cache, proves
  exact reverse-order rereads with backend refill, and rejects an unbounded
  cache, skipped rereads, and accepted wrong values. `[ACTIVE-WORK]` Concurrent
  reads, remote object storage, atomic range clear, and the GCS cache-state
  matrix remain open. The eviction worker and suite now have a scratch-isolated
  `gcs-dev` profile with cleanup gating, but live execution is blocked by an
  expired interactive credential and an application-default identity without
  access to the declared project.
- `[EXISTS]` The generation-aware reference model and `mvcc-semantics-v1` eval
  cover canonical replay, range clears, scans, retention errors,
  read-your-writes, exact seeded replay, and seven independently detectable
  negative subjects.
- `[EXISTS]` The pure Cell v0 commit contract model freezes replayable conflict
  and mutation payloads, request identity, resolver and log-tag coverage,
  generation fencing, quorum acknowledgement, durable outcome reconstruction,
  and six negative controls. It is not a production WAL or consensus system.
- `[EXISTS]` The local persisted-WAL prototype writes opaque commit envelopes in
  checksummed versioned frames to three ordinary files, calls `sync_all` on each
  selected replica, and reconstructs only a contiguous matching two-file
  quorum after fresh opens. It rebuilds retained retry outcomes, ignores only
  incomplete final frames, rejects bad envelope chains, and fails closed when
  complete corruption leaves no quorum. The six negative subjects cover
  RAM-only deduplication, early acknowledgement, single-copy trust, torn-tail
  promotion, skipped chain checks, and ignored corruption.
- `[EXISTS]` Three OpenRaft nodes communicate through a length-framed JSON TCP
  protocol in Turmoil. Across three fixed seeds the cluster gate commits on a
  quorum, isolates the first leader, elects two successors, replaces an
  uncommitted stale suffix, survives a simulated node crash and bounce with the
  real per-node journal, and catches every node up to the same applied payloads.
  Three unsafe subjects are rejected.
- `[PROPOSED]` Real OS process crash, unsynced-disk loss, generation takeover,
  durable request-outcome integration, disk repair, and independent
  failure-domain placement remain ahead of this cluster contract.
- `[EXISTS]` A centralized Cell v0 semantic state machine now applies bounded
  multi-key OCC transactions through three real OpenRaft processes, assigns
  commit versions from applied log positions, emits exact commit envelopes,
  and retains request outcomes. Its checksummed version-1 authority snapshot
  restores all three voters after their covered journals are removed and
  preserves exact retry plus continued commit behavior. Immutable object-data
  closure, `O_cell`, snapshot transfer, independent disks, and performance
  curves remain unproven.
- `[EXISTS]` A seven-process routine-reconfiguration gate preserves generation
  `1` while replacing voter `201` with fresh voter `204`, advances membership
  epoch `0` to `1` once, installs snapshot plus suffix, verifies purpose-bound
  learner and membership certificates, fences the removed voter, commits after
  failover, and restarts the replacement exactly. The receipt covers three
  local-file seeds on one machine, not independent failure domains, remote
  snapshot cost, production key management, or control-authority leases.
- `[EXISTS]` The pure ZebraDB HTAP contract model compares base-plus-tail output
  with a logical row oracle at one target version. It covers pushdown
  invalidation, schema and partition movement, analytical-tail retention,
  snapshot-lease GC, certificate races, unequal table watermarks, and five
  negative controls. It is not a DataFusion or Parquet implementation.
- `[EXISTS]` The physical ZebraDB path pins DataFusion 54.0.0 and Arrow/Parquet
  58.3.0. Its first adapter reads a schema-v1 Parquet base and schema-v2 Arrow
  tail through a custom `TableProvider`. The admitted streaming candidate
  validates base and tail ordering across record-batch boundaries, scans from
  the minimum of independently lagging partition watermarks, reduces by logical
  identity, declares ordered incremental output, and binds continuation to one
  target version. The receipt covers bounded operator buffering, not manifests,
  leases, multiple execution ranges, full-query memory, or a performance curve.
- `[EXISTS]` The `objectKV-dev` Terraform configuration validates locally.
- `[EXISTS]` The public `Doss-com/objectKV` `main` branch passes hosted Linux
  format, strict all-target clippy, workspace tests, eval and negative-control
  contracts, MinIO conformance, and deterministic replay at candidate
  `a1ada58`.
- `[EXISTS]` Two fresh `okv-sim` processes at seed 1103 emit byte-identical
  canonical traces across synced control state, crash/restart, network
  partition/repair, generation change, and stale-publication rejection. The
  deliberate stale-publication bug fails its oracle.
- `[EXISTS]` Memory passes the `authority` object-store profile, filesystem
  passes only the `segment` profile, and pinned MinIO
  `RELEASE.2025-09-07T16-13-09Z` passes the local `authority` profile through
  Apache `object_store 0.14.1`. GCS has not run because cloud authentication is
  unavailable.
- `[EXISTS]` The physical publication adapter writes digest-addressed bytes
  through Apache `object_store` on local filesystem, reopens publication
  authority from a checksummed three-file synchronized quorum, resolves lost
  PUT, authority, and DELETE responses, walks exact manifests, and holds a
  durable per-object deletion reservation across unguarded delete. This is a
  same-machine adapter proof, not production authority consensus or cloud
  evidence.
- `[EXISTS]` The first disposable publisher gate commits `Prepare` through
  three real OpenRaft authority processes, kills the dedicated publisher before
  its first object PUT, removes its scratch directory, and completes exact
  named-object verification plus atomic root publication from a replacement
  process with empty scratch.
- `[EXISTS]` The next publisher gate injects a real first-object effect with a
  retryable-unknown response, kills that publisher, and starts a replacement
  with empty scratch. The replacement recovers the canonical job from the
  replicated intent, identifies the first immutable object as exact through a
  named read, completes and verifies the closure, and atomically publishes the
  root. A partial-closure publisher is rejected deterministically.
- `[EXISTS]` The ambiguous-manifest gate retains the successful immutable
  manifest effect while replacing its response with retryable-unknown, kills
  that publisher, and starts a replacement with empty scratch. The replacement
  replays every data identity, verifies the existing manifest, and walks the
  complete named closure before root visibility. A manifest-only replacement
  that omits a child is rejected deterministically.
- `[EXISTS]` The lost-`Publish`-response gate drops a successful reply after the
  replicated root transition applies, kills the publisher and accepting
  authority leader, and starts a replacement with empty scratch. The successor
  returns the retained outcome and exact retry causes no second authority or
  object effect. A convergence-only authority reaches the same root and closure
  but is rejected for losing the original outcome and applying `Publish` twice.
  Multipart residue, repeated unknown responses, abandoned intents, sweeper
  recovery, and generation-bound object-effect fencing are not admitted.
- `[EXISTS]` A fresh serving-worker process resolves a replicated publication
  root, verifies an immutable base through `O=8`, reopens a two-of-three local
  quorum WAL, validates the commit-envelope chain, and reconstructs exact rows
  at `T=10`. The ignore-suffix control stops at `8` and returns stale rows. This
  proves the bounded recovery equation with a copied WAL fixture, not original
  OpenRaft log consumption, range routing, concurrent reads, independent hosts,
  or cloud failure behavior.
- `[EXISTS]` A second fresh-worker gate removes the controller-copied WAL. The
  worker reads committed `CommitEnvelope` bytes through a linearizable request
  to the live three-process transaction authority after its leader dies. It
  rebuilds exact `T=10` state from `O=8`; dropping the final envelope exposes a
  stale read at `8`. Raw OpenRaft transaction proposals are not a serving
  mutation stream because they also contain retries, rejections, blank entries,
  and membership changes.
- `[EXISTS]` A third gate copies that same committed envelope to three dedicated
  range-tagged tLog processes with private synchronized roots. After one tLog
  dies, a fresh worker requires matching tag-`10` records from both survivors
  and reaches exact `T=10` from `O=8`. All nodes reject a retained-byte overflow;
  omitting tag `10` leaves the worker stale at `8`. Commit acknowledgement
  integration is proven separately; multi-record streaming, repair, and
  partitioned log sets remain `[PROPOSED]`.
- `[EXISTS]` A fourth gate stages one transaction in the replicated authority,
  records quorums from two required three-process tagged log sets, survives
  commit-proxy death after the first and second log sets, then publishes and
  acknowledges the exact transaction once. A fresh worker reconstructs exact
  visible state from both log-set quorums. The process-derived receipt binds
  the envelope and configured identities but is not authenticated against a
  malicious proxy. Authenticated certificates, staged-head abort, generation
  takeover, sustained lag and backpressure, repair, and partitioned routing
  remain `[PROPOSED]`.
- `[EXISTS]` A fifth gate replaces the unauthenticated receipt with Ed25519
  quorum certificates over the exact cell, tenant, transaction generation,
  staged envelope digest, log set, policy epoch, and durable position. The
  authority installs signer policy independently and rejects unsigned,
  duplicate-signer, wrong-log-set, tampered-statement, and obsolete-epoch
  controls. Key custody, policy rotation, staged-head abort, generation
  takeover, sustained lag and backpressure, repair, and partitioned routing
  remain `[PROPOSED]`.
- `[EXISTS]` A sixth gate carries one fully certified staged head through a
  real transaction-system generation fence, voter-set handoff, successor
  activation, lost takeover reply, and exact retry. Only the active successor
  may publish the original old-generation envelope, and successor transaction
  12 follows visible transaction 11. A head missing one certificate remains
  safely blocked.
- `[EXISTS]` A seventh gate durably fences every old-generation tLog set,
  proves quorum absence in the incomplete set, aborts transaction 11 through
  the active successor, retains the abort outcome across a lost reply, rejects
  old-generation appends after a tLog process restart, and commits successor
  transaction 12 from the last committed chain. Multi-record recovery, fence
  authorization, signer key custody, sustained lag and backpressure, repair,
  and partitioned routing remain `[PROPOSED]`.
- `[EXISTS]` An eighth gate classifies one bounded four-record staged window
  from authenticated prefix-fence inventories. It recovers the longest prefix
  present at quorum in every required log set, aborts the first record absent
  at quorum and its dependent suffix, consumes every sequence, replays the
  retained disposition, and commits the successor after the entire window.
  Six unsafe boundary, ordering, limit, and inventory subjects discard.
  Production fence authorization, signer key custody, sustained lag,
  backpressure, repair, moving log sets, and partitioned routing remain
  `[PROPOSED]`.
- `[EXISTS]` A ninth gate keeps the tagged-log suffix below a soft byte
  limit during a frozen objectification interval. Fresh signed quorum capacity
  samples reserve exact frame bytes before sequence allocation; object
  publication through 12 permits durable quorum pop; restarted tLogs retain
  the pop marker; and a fresh worker reconstructs exact state through 16. Six
  unsafe ratekeeping and pop subjects discard. A publication-authority process
  quorum signs the exact replicated root. Each tLog pins that membership,
  verifies the capability and referenced manifest bytes, and matches the
  embedded cell snapshot frontier before durable deletion.
- `[EXISTS]` A tenth gate replaces one failed tLog as an empty non-voting
  learner while objectification remains behind. An active survivor quorum
  certifies the exact retained snapshot, the learner proves possession of a
  distinct key and storage incarnation, restart preserves the installed
  records, and a second active quorum certifies readiness. Capacity and serving
  still count only active policy members. Six one-source, tamper, stale,
  identity, and premature-counting subjects discard. The admitted correct path
  transfers one complete four-record suffix without concurrent appends.
  Chunked or live-tail catch-up, promotion through a moving log-set policy,
  independent hosts, external machine identity, and production key custody
  remain `[PROPOSED]`.
- `[EXISTS]` An eleventh gate moves that repair-ready learner through one
  replicated log-set policy epoch. The authority prepares and commits the
  exact one-member replacement, successor tLogs stage one policy, an authority
  quorum certifies activation, and activation survives restart. The removed
  root cannot contribute to transaction, capacity, or serving quorums. After a
  second member fails, the new E2 quorum commits and serves exact transaction
  `17` while the unchanged log set remains at E1. Seven readiness, stage,
  epoch, activation, rejoin, and replay controls discard. Concurrent live-tail
  catch-up, chunked transfer, independent hosts, production key custody, and
  concurrent policy movement remain `[PROPOSED]`.
- `[EXISTS]` A twelfth gate keeps the active policy committing while a failed
  tLog is rebuilt through a resumable three-chunk base and a separate two-chunk
  ordered tail. Every acknowledged chunk and immutable descriptor survives
  learner restart; exact retries are idempotent; conflicting retries, missing
  chunks, gapped tails, stale readiness, premature quorum counting, and full
  base recopy discard. The learner and a fresh serving worker reach exact
  transaction `16`, while the learner remains non-voting. Remote transfer,
  multiple simultaneous repairs, unbounded append, lease expiry, orphan chunk
  collection, independent hosts, and production key custody remain
  `[PROPOSED]`.
- `[EXISTS]` A thirteenth gate partitions conflict checking across three
  ordered resolver processes without partitioning tenant atomicity. Every
  overlapped read and write conflict is clipped to its owner; the replicated
  authority requires one distinct, epoch-bound, durable signed decision from
  every touched process. Across 1,800 attempts, status, rows, and envelope
  chains match the centralized Cell v0 oracle. Seven routing, partial,
  identity, epoch, durability, finalization, and split controls discard.
  Online map movement, multiple in-flight touching transactions, batching,
  hotspot curves, independent hosts, and production key custody remain
  `[PROPOSED]`.
- `[EXISTS]` A fourteenth gate removes resolver filesystem synchronization and
  per-transaction finalization from the intended normal path. Three
  memory-only ordered resolvers process batches of eight. Resolver loss stops
  the old transaction-system generation; a replicated authority fence records
  the durable floor; three empty successor resolvers reject old-generation
  traffic and continue from that floor. Across 1,800 attempts, every commit is
  allowed by the centralized oracle, every centralized conflict is rejected,
  rows and envelope chains remain exact, and three conservative false conflicts
  remain safely invisible. Six generation, fence, reply, floor, visibility,
  and durable-head controls discard. The composed authenticated tLog fence,
  multiple commit proxies, recovery-time availability, online resolver-map
  movement, independent hosts, and production key custody remain
  `[PROPOSED]`.
- `[EXISTS]` A fifteenth gate composes memory-only resolvers with real
  authenticated tLog processes and generation recovery. Resolver agreement may
  stage an exact envelope but cannot publish it. After resolver loss, durable
  signed inventories recover the maximal prefix present at quorum in both
  required sets, including one envelope whose certificate never reached the
  proxy. The first quorum-absent record and its dependent suffix abort. Empty
  successor resolvers start at the authenticated frontier and publish a new
  crossing-range transaction only after real successor tLog certificates.
  Seven visibility, inventory, ordering, generation, reply, floor, and prefix
  controls discard. Multiple commit proxies, recovery-time curves, online
  resolver-map movement, ratekeeping on the partitioned path, independent
  hosts, and production key custody remain `[PROPOSED]`.
- `[EXISTS]` A sixteenth gate runs three commit-proxy processes under one
  replicated sequencer ticket chain. Three resolvers and six durable tLog
  workers receive different batch arrival permutations but process the same 24
  `(previous, current]` links. Across 288 transactions, rows, evaluation
  envelope bytes, conflict outcomes, tLog roots, and acknowledgement sets match
  the sequencer-order oracle. Four conflict-only batches per seed advance every
  tLog through explicit progress frames. Eight duplicate, gap, arrival-order,
  mutation, acknowledgement, identity, and omitted-progress controls discard.
  This is a semantic result, not throughput evidence. Proxy-failure gap
  recovery, online resolver-map movement, metadata propagation, independent
  hosts, performance curves, and production key custody remain `[PROPOSED]`.
- `[EXISTS]` A seventeenth gate replaces resolver `2` for `[0x50, 0xa0)`
  with fresh child resolvers `4` and `5` inside one active transaction-system
  generation. The children install an exact clipped batch-8 history, shadow
  batches 9 through 15 while the source remains authoritative, and activate
  only after all proxies and both required tLog sets process the batch-16 map
  mutation. Across 360 attempts, 261 commit, 87 conflict, and 12 old-map
  requests abandon and retry under map epoch `2`. Eight lag, omission, epoch,
  retired-reply, routing, proxy-map, durability, and descriptor controls
  discard. This moves resolver conflict metadata only. Concurrent movements,
  controller recovery, merge, serving-range movement, hotspot curves,
  independent hosts, and production key custody remain `[PROPOSED]`.
- `[EXISTS]` An eighteenth gate kills one commit proxy at three distinct
  boundaries across four sequential transaction-system generations: before
  resolver delivery, after one required tLog set reaches quorum, and after all
  required sets reach quorum but before the client reply. Recovery fences every
  old role, authenticates all required tLog inventories, abandons incomplete
  ticket suffixes, preserves the fully durable unknown result exactly once, and
  starts the successor above every old issued version. Across 432 attempts, 336
  commit and 24 abandoned batches retry through stable identities. Nine
  generation, no-op, durability, prefix, inventory, version, fencing, and
  duplicate-outcome controls discard. The next unknown is measured recovery
  duration under larger pending windows, tLog topologies, and retained tails.
  Independent hosts, simultaneous controller failure, production authorization,
  and signer custody remain `[PROPOSED]`.
- `[EXISTS]` A nineteenth gate isolates the five local transaction-system
  recovery phases behind a live three-process authority. Across 210 samples,
  retained-tail inventory scales from 0.014 seconds at 256 records per tLog to
  2.870 seconds at 65,536; total recovery rises from 0.292 to 3.158 seconds.
  Pending-ticket work from 8 to 512 is flat, role and tLog topology scales in
  its declared phases, and 1 GiB versus 1 PiB logical database extents use
  identical work with zero database bytes read. Four database-scan,
  incomplete-inventory, quadratic-work, and early-admission controls discard.
  Retained-tail summaries, checkpoint cadence, parallel recruitment,
  independent hosts, network partitions, and a production SLO remain
  `[PROPOSED]`.
- `[ACTIVE-WORK]` The actual Google Cloud project and GCS bucket await interactive
  gcloud reauthentication and exact organization/billing verification.
- `[EXISTS]` The `okv` package name is already occupied on crates.io.
- `[EXISTS]` The public GitHub repository is
  `Doss-com/objectKV`.
- `[PROPOSED]` Packages remain `publish = false` until crate naming and API
  boundaries are decided.
- `[EXISTS]` Apache License 2.0 is the public license.
- `[PROPOSED]` Distributed Redis, inverted search, and PostgreSQL are initial
  serving-model consumers. DataFusion over the same version history becomes the
  ZebraDB analytical path.
- `[PROPOSED]` A cell bounds fleet and recovery topology. It does not restrict a
  tenant transaction to one range or one atomic KV partition.
- `[PROPOSED]` Exact ZebraDB queries merge each columnar base with its durable
  analytical tail through one target version. Materialization lag affects cost,
  not required freshness.

## Load-bearing invariant

For one cell's latest committed version `C` and object durable version `O`:

```text
O <= C

Database(C) = ObjectState(O) + WAL mutations in (O, C]
```

Every acknowledged mutation in `(O, C]` must remain in the replicated durability
log. WAL through `X` may be reclaimed only after object state is reconstructable
through `X`.

Object storage is the permanent tier. During `(O, C]`, the retained WAL suffix
is part of the authoritative state and its declared failure topology determines
the system's RPO.
