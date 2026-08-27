# objectKV domain context

This file defines vocabulary and current facts. Behavioral policy lives in
`AGENTS.md`.

## Names

- **objectKV**: `[PROPOSED]` the open-source object-native transactional ordered
  KV kernel and public repository name.
- **okv**: the CLI, Rust package/module, configuration-prefix, and local
  shorthand namespace for objectKV.
- **okv-fabric**: `[PROPOSED]` the unified application API above the objectKV
  kernel. Applications use one version, transaction, log, snapshot, branch,
  projection, and lifecycle fabric instead of integrating separate application
  adapters. PostgreSQL, Redis, search, filesystem, and DataFusion compatibility
  surfaces bind to this fabric without moving their semantics into the kernel.
- **ZebraDB**: `[FUTURE]` the DOSS database product intended to consume
  objectKV through PostgreSQL and version-aligned DataFusion execution.
- **serving model**: Redis, inverted search, PostgreSQL, or another semantic
  surface implemented through `okv-fabric` above the ordered transaction
  contract.
- **value-native kernel**: objectKV owns ordered keys, opaque value bytes,
  versions, transactions, and recovery. A serving model decides whether a value
  represents a PostgreSQL page, logical row, index entry, posting, or another
  consumer structure.
- **page-native PostgreSQL adapter**: `[PROPOSED]` a consumer mapping
  PostgreSQL relation pages to objectKV values while PostgreSQL retains page,
  heap, index, MVCC, catalog, and recovery meaning.
- **row-native PostgreSQL adapter**: `[FUTURE]` a consumer mapping logical rows
  and index entries directly to objectKV keys and values while a
  PostgreSQL-compatible compute layer owns SQL meaning.
- **transactional segment**: an immutable row-oriented encoding of ordered MVCC
  entries. Its format may change access economics but not key ordering, version
  visibility, tombstones, range deletes, merge operands, or transaction
  atomicity.
- **analytical artifact**: a schema-aware Parquet or Vortex materialization with
  an explicit covered-through version. It is derived from the logical history
  and is not authoritative for newer commits.
- **objectification**: publishing committed mutations into immutable object
  segments and advancing an authoritative durability reference.
- **serving worker**: `[FUTURE]` disposable compute that applies recent txLog,
  caches immutable objects, and serves versioned reads.
- **txLog**: a replicated transaction log. Use `txLog`, not `tLog`, in prose,
  CLI output, metrics, and code names.
- **commit proxy**: `[CODE-COMPLETE]` for Cell v0, the bounded FIFO admission
  and batching service in front of the transaction authority. It accepts
  independent requests, closes a batch on item count, encoded bytes, delay, or
  sender shutdown, and demultiplexes exact per-request outcomes.
- **transaction versionstamp**: `[CODE-COMPLETE]` the ordered pair
  `(commit_version, batch_order)`. Transactions in one authority batch share a
  scalar commit version and retain distinct ordered identities.
- **retained transaction stream**: `[CODE-COMPLETE]` the journal-independent,
  linearizable recovery API over accepted transaction commands. It freezes one
  target version, pages by commit version, rejects cursors below a retention
  floor, and does not expose physical OpenRaft entries or files.
- **okv-log**: `[CODE-COMPLETE]` the pure partition-local ordered opaque-record state
  machine. It owns append planning, explicit suffix replacement, prefix purge,
  exact and clamped reads, replay semantics, and no physical durability.
- **okv-wal**: `[CODE-COMPLETE]` physical stable-storage and consensus metadata policy
  layered on `okv-log`. It owns `OKVR`/`OKVW` framing, checksums, synchronization,
  votes, committed identities, quorum evidence, and acknowledgement policy.
- **node-journal compaction**: `[CODE-COMPLETE]` a canonical rewrite of one
  voter's current vote, committed marker, purge marker, and retained Raft
  suffix through a synchronized same-directory replacement. The process path
  requires a checksummed durable state-machine snapshot to cover the purge
  target and reopens exact snapshot plus suffix state. Its local dirty-source
  composition remains `[EVALUATING]`.
- **application log**: `[FUTURE]` a transactionally emitted, partitioned record
  stream with retention and cursors independent of the recovery txLog.
- **object row base**: `[PROPOSED]` an immutable indexed row-oriented
  representation that reconstructs a range through a declared version.
- **manifested object LSM**: `[EVALUATING]` one immutable object closure whose
  runs preserve the same ordered MVCC algebra but may declare row, typed
  projection, or random-access columnar capabilities. The manifest plus the
  retained txLog suffix is authoritative, not any one file-format label.
- **row-object point-read pilot**: `[CODE-COMPLETE]` experimental `OKVB` data
  blocks plus a separately cacheable `OKVI` sparse index. It covers point
  values and tombstones with per-block checksums and one indexed data range GET;
  it is not yet the complete transactional segment format.
- **recovery suffix**: `[PROPOSED]` quorum-durable commit records newer than the
  object-durable version that are required to reconstruct acknowledged state.
- **row overlay**: `[PROPOSED]` queryable recent MVCC entries newer than a
  selected object row base, including tombstones.
- **serving image**: `[CODE-COMPLETE]` provider-neutral activation and exact
  point-read boundary for complete disposable range-local state. Apply,
  eviction, and partial admission remain `[PROPOSED]`. A serving image is never
  the only permanent database copy.
- **serving profile**: the selected hot-state implementation for a range.
  `ssd_resident` is `[CODE-COMPLETE]` as a bounded RocksDB image on disposable
  local media and `[VERIFIED]` for the topology-matched single-range named-NVMe
  read boundary; concurrent cache-pressure curves remain `[EVALUATING]`.
  `ram_resident` is `[PROPOSED]` as a bounded DRAM image with no data files or
  swap.
- **durability provider**: `[PROPOSED]` the implementation that makes a commit
  envelope and required consensus state recoverable before `COMMITTED`. The
  default is a regional stable-media txLog; synchronous object acknowledgement
  and an external durable journal are explicit alternatives.
- **transaction plane**: `[EVALUATING]` the distributed system that owns current
  MVCC, conflict resolution, commit, replication, placement, and
  active-main-branch serving. D56 reopens the objectKV-native RocksDB and
  OpenRaft plane as the primary bounded research lane. FoundationDB remains a
  correctness oracle, comparison control, and fallback transaction profile.
- **lifecycle adapter**: `[CODE-COMPLETE]` as provider-neutral types and a
  source-pinned preflight in `okv-plane`; `[EVALUATING]` as a live provider
  implementation. It owns retained changes, request outcomes, object frontier,
  restore, and generation mapping, while ordinary active-branch reads remain
  directly on the incumbent.
- **provider stamp**: `[CODE-COMPLETE]` the provider-local total-order commit
  identity. FoundationDB maps its ten-byte versionstamp to
  `(commit_version, batch_order)`. It is comparable only inside one objectKV
  generation.
- **object durable version**: the highest commit version reconstructable from
  authoritative object metadata and immutable objects without older txLog.
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
- **EvalProgram**: `[CODE-COMPLETE]` an ordered graph of requirement-linked
  gates. It records product admission state without producing a blended score.
- **GoldenPathScenario**: `[CODE-COMPLETE]` one frozen generator, seed set,
  architecture-surface registry, checkpoint DAG, and artifact handoff contract
  reused across an `EvalProgram`.
- **EvalSuite**: `[CODE-COMPLETE]` one comparable configuration contract for
  profiles, workloads, lanes, telemetry, and frozen inputs.
- **EvalProfile**: a machine, topology, durability, serving, cache-state, and
  budget selection for a run.
- **EvalWorkload**: one candidate, control, or poison operation executed by a
  suite.
- **EvalGate**: one requirement-linked claim and falsifier whose status is Code
  Complete, Verified, Evaluating, Proposed, or Future.
- **EvalReceipt**: one immutable, schema-valid run result whose evidence scope
  is limited to its exact revision, scenario, profile, backend, and artifacts.

## Current facts

Status claims in this section follow `docs/STATUS-TAXONOMY.md`. In particular,
`[VERIFIED]` names an executable receipt, while `[CODE-COMPLETE]` makes no
performance or operational claim.

- `[CODE-COMPLETE]` The repository contains a Rust workspace, an in-memory reference
  model, a pinned SlateDB adapter spike, a configurable eval runner, an OTel
  path, an exact seeded generation-fencing probe, an object-store conformance
  runner, local persisted-WAL contracts, a pinned OpenRaft per-node storage
  adapter, a deterministic three-node OpenRaft failover contract, and
  planning/RFC scaffolding.
- `[VERIFIED]` GP3.1 now compares the native version-bound snapshot with direct
  owned-value RocksDB inside the same recovered six-authority-process topology.
  Native retained 0.9089x and 0.9197x throughput in opposite process orders,
  while p99 was 0.9134x and 0.9132x control. All four runs passed 16 hard gates
  and emitted OTel logs, metrics, and traces. This admits the single-range read
  boundary, not replicated commit or a complete cell. D56 keeps the plane
  native-first; FoundationDB remains the semantic oracle and fallback profile.
- `[CODE-COMPLETE]` GP3.1.1 runs the same native and direct RocksDB boundaries
  with exact operation budgets across 1, 8, and 32 synchronized clients. A
  dirty 32-client local diagnostic produced 0.9835x and 1.0324x control
  throughput in opposite process orders. Native p99 was 0.8321x and 0.8717x
  control, with every runtime gate passing. Clean GCP R0 evaluation is pending;
  cache pressure is a separate gate.
- `[CODE-COMPLETE]` RFC-0041 and `okv-plane` freeze the incumbent adapter,
  provider stamp, generation, retained-change, object-frontier, and restore
  boundaries. The source-pinned preflight models FoundationDB 7.4.6 and TiKV
  8.5.7 against the same P1 through P10 gates and catches a false serializable
  label with an executed write-skew history. This is not a live provider
  receipt.
- `[VERIFIED]` The bounded R0 provider probes resolved the semantic branch.
  FoundationDB 7.4.6 rejected one write-skew transaction and aligned the exact
  commit, retained-change, and request-outcome versionstamps. TiKV 8.5.7
  committed both disjoint writers, matching its documented snapshot isolation
  and failing P1. This is single-machine semantic evidence, not HA.
- `[VERIFIED]` FoundationDB is the semantic and lifecycle control. GP2.5.2 rebuilt
  950 rows into an empty logical generation, matched the digest, fenced the old
  generation, and discarded three poisons. GP2.5.3 then deleted the source VM,
  boot disk, and provider SSD before reproducing the exact 950-record digest on
  a fresh cluster. Its formal positive passed 16 gates and its hidden-media
  control was discarded, with both run IDs in all three OTel signals. These
  receipts do not make FoundationDB the default plane. The native transaction
  plane remains `[EVALUATING]` through the subsequent replicated-commit gates.
- `[CODE-COMPLETE]` The unpublished `okv` crate exposes the first integrated
  `SingleRange` kernel API. It selects a generation-fenced publication root,
  verifies one immutable row-object base, catches up through the logical txLog
  with a `(commit_version, batch_order)` cursor, commits through the replicated
  authority, and serves exact point values, tombstones, and absence. Diagnostic
  run `74b29fe1` crossed six authority processes, one worker kill, an empty
  replacement, and a page bound that split a shared-version batch. Its 123.002
  ms first-correct-read result remains `[EVALUATING]` because the tree was dirty
  and every process shared one host.
- `[EVALUATING]` GCS run `6723ce8a` executes that public `SingleRange` path over
  the real `doss-objectkv-dev-okv-evals` bucket with required OTel. All 12 gates
  passed, including one killed worker, a distinct empty replacement, exact
  object-base plus txLog-tail state, one manifest GET, one index GET, one data
  range GET, and zero LIST operations. The 756.950 ms first-correct-read result
  is a dirty local-controller diagnostic, not a performance claim.
- `[CODE-COMPLETE]` `SingleRange` can activate the complete verified immutable
  base into a provider-neutral serving image before txLog catch-up. The first
  provider is RocksDB with its own WAL disabled, a declared local-byte ceiling,
  and generation plus coverage fencing. Dirty debug run `56535944` rebuilt an
  empty replacement into 86,667 local bytes, applied the suffix, returned exact
  state, and measured 100,000 public point reads at 824,252 reads/s and 1,583 ns
  p99 with zero object operations. The scratch volume maps to a named internal
  Apple SSD AP1024Z NVMe device. Performance remains `[EVALUATING]` until an
  optimized, repeated, clean-source ABBA run adds controlled-load evidence.
- `[CODE-COMPLETE]` The SlateDB spike can apply externally assigned versions and reject
  conflicting replay. It stores the complete logical latest-version record and
  rejects unsupported generations and range clears explicitly. It cannot yet
  expose an explicit public read version.
- `[CODE-COMPLETE]` The objectKV-owned row-object pilot partitions sorted
  versioned point values and tombstones into content-addressed data objects
  bounded below 4 MiB, one checksummed sparse index per object, and one
  checksummed manifest. Its release local-filesystem G4.1 scale diagnostic used
  one bounded data range GET per exact point read across 1, 8, and 64 MiB range
  images. The G4.2 pilot starts each sample with a new backend and reader,
  fetches one manifest, one selected index, and one data block, and keeps local
  first-read bytes nearly flat through the same scale sweep. G4.3 composes the
  same row base with a non-empty quorum-file txLog suffix, kills one real worker
  after recovery and before its first read, and starts a distinct empty-scratch
  replacement. That replacement returned exact base, update, delete, and
  tail-only insert reads using one manifest, one index, and one data range GET.
- `[CODE-COMPLETE]` The G4.11a.2 storage-layout runner generates one typed MVCC
  history and executes the indexed row-object, indexed Parquet, and hybrid
  columnar subjects against the same point, ordered-scan, compaction, media,
  and branch oracles. Its Parquet reader expands every requested range to
  independently checksummed 64 KiB blocks. The Vortex subject is not yet
  executable inside the Rust 1.88 workspace.
- `[EVALUATING]` A 1,024-key local debug-build preflight at seed 5701 kept exact
  semantics for all three subjects. Indexed Parquet improved projected scan
  rate 1.873x over the row control, but used 10 requests and 1,047,296 response
  bytes per point versus one request and 64,066 bytes. The hybrid used four
  requests, 697,344 bytes per point, and 1.925x the row control's stored/live
  amplification. This is a mechanism-selection diagnostic, not an admission
  receipt or cloud performance result.
- `[CODE-COMPLETE]` The storage-layout admission runner now alternates the
  indexed row control and split typed-run candidate across every seed and
  repeat. The split run stores the complete MVCC value once in a row sidecar
  and stores only declared analytical fields in its columnar projection. One
  manifest authenticates both representations. The same paired runner can now
  execute below a unique GCS run prefix without changing object identities or
  allowing candidate and control media to collide.
- `[EVALUATING]` Release-local run `f5dbba62` passed the frozen three-seed,
  three-repeat admission thresholds: 1.000x point requests, 1.000x point bytes,
  1.033x point p99, 9.124x projected scans, 1.030x storage amplification,
  1.035x compaction write amplification, and 1.137x resident index versus the
  row control. The dirty local result admits the split subject to clean GCS
  evaluation, not to the opaque KV default. The objectKV-dev GCS project and
  bucket now execute bounded canaries. The full alternating storage-layout
  suite remains unrun because its serial cloud request path exceeded the useful
  canary budget and requires bounded parallel scheduling plus OTel first.
  G4.4 replaces the local tail adapter with a linearizable retained transaction
  stream owned by three real OpenRaft data-authority processes. A killed worker
  and distinct empty-scratch replacement execute two frozen catch-up rounds
  around four concurrent commits and return exact point `Set`, point `Clear`,
  tail insertion, and `ClearRange` outcomes without physical journal access.
  G4.5 then rejected a monolithic authority snapshot at 9.172x projected growth.
  The `[CODE-COMPLETE]` RFC-0029 prototype splits serving, resolver, retry, and
  recovery state behind `S`, `R`, `Q(client)`, and projected `O` frontiers.
  Its G4.6 dirty diagnostic held complete projected state to 1.0029x while the
  object-only control grew 9.1694x and an incomplete poison was rejected. The
  run missed its 180-second budget at 545.726 seconds. The receipts remain
  `[EVALUATING]`. G4.7 then made `O` physical through an authenticated pending
  object frontier, complete closure validation, data-voter quorum certificate,
  and publication activation. It popped 16 records, persisted floor 18,
  recovered exact object state after both authority leader failovers and one
  data-voter restart, and rejected missing-pending, forged-coverage, and
  subquorum controls. Those dirty local receipts remain `[EVALUATING]`;
  internal OpenRaft log purge, client-floor cardinality, independent machines,
  remote object stores, scans, overload convergence, serving activation, and
  production latency remain open. G4.8 then tested bounded concurrent commit
  submission in a release build. It preserved exact retry, failover, voter
  restart, retained-stream, and final-value semantics, and improved median
  throughput from 38.772 to 153.708 transactions per second. It was discarded
  as the final group-commit mechanism because it missed the 200 transaction per
  second, 250 ms p99, and 4x paired gates. Followers grouped stable appends, but
  the leader still issued one append for each transaction. An explicit
  commit-proxy batch entry was the next performance falsifier. G4.9 implements
  that entry with a FoundationDB-style `(commit_version, batch_order)`
  versionstamp, per-item fingerprints and outcomes, deterministic in-batch
  conflict resolution, and a recovery cursor that can stop inside a batch. The
  release candidate reached 559.511 median durable transactions per second and
  34.016 ms maximum p99 versus 151.944 transactions per second for the same-
  durability one-entry control, a 3.682x gain. It produced 16 logical
  transactions per leader append, recovered exact state after failover and
  voter restart, and rejected duplicate-identity and early-ack controls. The
  dirty single-host receipts remain `[EVALUATING]`; sustained overload,
  independent stable media, OTel, concurrent objectification, clean-source
  reproducibility, and production latency remain open. G4.10 then begins with
  independent client requests. Its 16-item, 64-caller configuration reached
  581.791 median transactions per second but was discarded at 131.488 ms
  maximum p99 against a 100 ms ceiling. A separately frozen 32-item candidate
  reached 1,157.369 transactions per second, 76.101 ms maximum p99, and 32
  logical transactions per leader append. The same-durability one-entry control
  reached 182.093 transactions per second, a 6.356x paired gain. Sparse, byte,
  overload, and oversized controls passed their scoped gates. The 128 KiB byte
  control fit eight 8 KiB-value transactions in a 119,731 byte `OKVB2` entry.
  The mechanism is `[CODE-COMPLETE]`; all G4.10 performance receipts remain
  `[EVALUATING]` because the source was dirty and all voters shared one host.
  G4.10b composes that exact path with authenticated objectification while
  commits continue. Its 25% conflict candidate reached 1,075.343 median
  resolved outcomes per second, 104.274 ms maximum p99, 31.030 minimum outcomes
  per leader append, and 28.776x the same-durability one-entry control. Exact
  `ObjectState(O) + txLog(O,C]` recovery, conflict retry, both authority
  failovers, voter restart, fresh-controller replay, and both unsafe frontier
  controls passed. The mechanism is `[CODE-COMPLETE]`; dirty single-host local
  receipts remain `[EVALUATING]`. G4.11a adds checksummed durable state-machine
  snapshots, a fail-closed snapshot-coverage guard on purge, and canonical
  physical journal compaction. Across three fixed seeds it reduced at most
  6,391,575 journal bytes to 879 bytes and preserved exact full-quorum restart,
  retained-stream, retry, and new-suffix behavior. The purge-before-snapshot
  poison changed no physical state. The mechanism is `[CODE-COMPLETE]`, but the
  dirty single-host receipts remain `[EVALUATING]`. The unfrontiered snapshot
  shape reached 38.66082x physical amplification. G4.11a.1 then aligned `R`, a
  bounded 64-request `Q(client)` window, and authenticated `O` across four
  snapshot, purge, compaction, and full-quorum restart cycles. It preserved
  exact retry and object-plus-suffix reconstruction with zero anomalies.
  Snapshot growth passed at 1.091759x against 1.25x, but complete physical media
  failed at 19.692719x against 8x. The no-`Q` control reached 54.803467x and
  2.195933x. The frontier mechanism is `[CODE-COMPLETE]`; the current replicated
  snapshot representation is discarded. A manifested multi-layout LSM is now
  `[EVALUATING]` against the row-object control before independent-media work.
- `[VERIFIED]` The generation-aware reference model and `mvcc-semantics-v1` eval
  cover canonical replay, range clears, scans, retention errors,
  read-your-writes, exact seeded replay, and seven independently detectable
  negative subjects.
- `[VERIFIED]` The pure Cell v0 commit contract model freezes replayable conflict
  and mutation payloads, request identity, resolver and log-tag coverage,
  generation fencing, quorum acknowledgement, durable outcome reconstruction,
  and six negative controls. It is not a production WAL or consensus system.
- `[CODE-COMPLETE]` An independent strict-serializability oracle checks exact
  point and range snapshots, real-time order, conflict coverage, intervening
  writes, and atomic effects for deterministic Cell v0 histories. The same
  frozen schema now receives histories from both a four-range model and one
  three-process OpenRaft transaction authority. The process path covers lost
  reply, leader death, exact retry, and killed-replica replay on one machine.
  A clean immutable receipt and independent-machine run remain pending.
- `[VERIFIED]` The local persisted-WAL prototype writes opaque commit envelopes in
  checksummed versioned frames to three ordinary files, calls `sync_all` on each
  selected replica, and reconstructs only a contiguous matching two-file
  quorum after fresh opens. It rebuilds retained retry outcomes, ignores only
  incomplete final frames, rejects bad envelope chains, and fails closed when
  complete corruption leaves no quorum. The six negative subjects cover
  RAM-only deduplication, early acknowledgement, single-copy trust, torn-tail
  promotion, skipped chain checks, and ignored corruption.
- `[CODE-COMPLETE]` The `okv-log` crate implements the reusable ordered-record algebra
  below `okv-wal`. The per-node journal delegates append, truncate, and purge
  transitions to it while retaining validate-before-write ordering and frozen
  `OKVR` bytes. Raw accepted and rejected histories cover all five journal
  record kinds; application logs and object-retained log segments do not exist.
- `[VERIFIED]` Three OpenRaft nodes communicate through a length-framed JSON TCP
  protocol in Turmoil. Across three fixed seeds the cluster gate commits on a
  quorum, isolates the first leader, elects two successors, replaces an
  uncommitted stale suffix, survives a simulated node crash and bounce with the
  real per-node journal, and catches every node up to the same applied payloads.
  Three unsafe subjects are rejected.
- `[PROPOSED]` Real OS process crash, unsynced-disk loss, generation takeover,
  durable request-outcome integration, disk repair, and independent
  failure-domain placement remain ahead of this cluster contract.
- `[VERIFIED]` The pure ZebraDB HTAP contract model compares base-plus-tail output
  with a logical row oracle at one target version. It covers pushdown
  invalidation, schema and partition movement, analytical-tail retention,
  snapshot-lease GC, certificate races, unequal table watermarks, and five
  negative controls. It is not a DataFusion or Parquet implementation.
- `[VERIFIED]` The physical ZebraDB path pins DataFusion 54.0.0 and Arrow/Parquet
  58.3.0. Its first adapter reads a schema-v1 Parquet base and schema-v2 Arrow
  tail through a custom `TableProvider`. The admitted streaming candidate
  validates base and tail ordering across record-batch boundaries, scans from
  the minimum of independently lagging partition watermarks, reduces by logical
  identity, declares ordered incremental output, and binds continuation to one
  target version. The receipt covers bounded operator buffering, not manifests,
  leases, multiple execution ranges, full-query memory, or a performance curve.
- `[CODE-COMPLETE]` The C5 columnar RangeEngine now exposes its immutable
  projection stripes through a custom DataFusion `TableProvider` and
  incremental `ExecutionPlan`. Point lookup retains one approximately 7.8 KiB
  stripe fetch; scan execution coalesces adjacent stripes into bounded 256 KiB
  range reads, verifies every nested stripe checksum, and emits at most 128-row
  Arrow batches. A dirty release-local diagnostic produced exact SQL results,
  zero opaque-payload reads, 2.544M rows/s, and 54 projection requests versus
  1,761 for the one-stripe scan control. This remains `[EVALUATING]` until exact
  base-plus-live-tail execution, complete-query memory, clean OTel, and GCS
  curves exist.
- `[CODE-COMPLETE]` The `objectKV-dev` Terraform configuration validates locally.
- `[CODE-COMPLETE]` The objectKV workstream is registered as `OKV-BOOTSTRAP` in the
  local DOSSBOT project tracker and its dedicated playground port is documented.
- `[VERIFIED]` Two fresh `okv-sim` processes at seed 1103 emit byte-identical
  canonical traces across synced control state, crash/restart, network
  partition/repair, generation change, and stale-publication rejection. The
  deliberate stale-publication bug fails its oracle.
- `[VERIFIED]` Memory passes the `authority` object-store profile, filesystem
  passes only the `segment` profile, and pinned MinIO
  `RELEASE.2025-09-07T16-13-09Z` passes the local `authority` profile through
  Apache `object_store 0.14.1`. The GCS adapter now passes the public
  SingleRange recovery smoke; the standalone GCS authority conformance profile
  remains open.
- `[VERIFIED]` The physical publication adapter writes digest-addressed bytes
  through Apache `object_store` on local filesystem, reopens publication
  authority from a checksummed three-file synchronized quorum, resolves lost
  PUT, authority, and DELETE responses, walks exact manifests, and holds a
  durable per-object deletion reservation across unguarded delete. This is a
  same-machine adapter proof, not production authority consensus or cloud
  evidence.
- `[VERIFIED]` The first disposable publisher gate commits `Prepare` through
  three real OpenRaft authority processes, kills the dedicated publisher before
  its first object PUT, removes its scratch directory, and completes exact
  named-object verification plus atomic root publication from a replacement
  process with empty scratch.
- `[VERIFIED]` The next publisher gate injects a real first-object effect with a
  retryable-unknown response, kills that publisher, and starts a replacement
  with empty scratch. The replacement recovers the canonical job from the
  replicated intent, identifies the first immutable object as exact through a
  named read, completes and verifies the closure, and atomically publishes the
  root. A partial-closure publisher is rejected deterministically.
- `[VERIFIED]` The ambiguous-manifest gate retains the successful immutable
  manifest effect while replacing its response with retryable-unknown, kills
  that publisher, and starts a replacement with empty scratch. The replacement
  replays every data identity, verifies the existing manifest, and walks the
  complete named closure before root visibility. A manifest-only replacement
  that omits a child is rejected deterministically.
- `[VERIFIED]` The lost-`Publish`-response gate drops a successful reply after the
  replicated root transition applies, kills the publisher and accepting
  authority leader, and starts a replacement with empty scratch. The successor
  returns the retained outcome and exact retry causes no second authority or
  object effect. A convergence-only authority reaches the same root and closure
  but is rejected for losing the original outcome and applying `Publish` twice.
  Multipart residue, repeated unknown responses, abandoned intents, sweeper
  recovery, and generation-bound object-effect fencing are not admitted.
- `[VERIFIED]` The organization-owned `doss-objectkv-dev` project and regional,
  versioned `doss-objectkv-dev-okv-evals` bucket are reachable through current
  Google credentials. The R0 infrastructure receipt names their exact machine,
  GCS, OTel, and teardown evidence.
- `[CODE-COMPLETE]` The local Git origin is configured for `Doss-com/objectKV`.
- `[CODE-COMPLETE]` The `okv` package name is already occupied on crates.io.
- `[PROPOSED]` The GitHub repository is `Doss-com/objectKV`.
- `[PROPOSED]` Packages remain `publish = false` until crate naming and API
  boundaries are decided.
- `[PROPOSED]` Apache License 2.0 is the public license.
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

Database(C) = ObjectState(O) + txLog mutations in (O, C]
```

Every acknowledged mutation in `(O, C]` must remain in the replicated txLog.
txLog entries through `X` may be reclaimed only after object state is
reconstructable through `X`.

Object storage is the permanent tier. During `(O, C]`, the retained recovery
suffix is part of the authoritative state and its declared failure topology
determines the system's RPO.
