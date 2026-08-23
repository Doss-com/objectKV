# objectKV domain context

This file defines vocabulary and current facts. Behavioral policy lives in
`AGENTS.md`.

## Names

- **objectKV**: `[PROPOSED]` the open-source object-native transactional ordered
  KV kernel and public repository name.
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
- **serving worker**: `[FUTURE]` disposable compute that applies recent WAL,
  caches immutable objects, and serves versioned reads.
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
- `[EXISTS]` The SlateDB spike can apply externally assigned versions and reject
  conflicting replay. It stores the complete logical latest-version record and
  rejects unsupported generations and range clears explicitly. It cannot yet
  expose an explicit public read version.
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
- `[EXISTS]` The objectKV workstream is registered as `OKV-BOOTSTRAP` in the
  local DOSSBOT project tracker and its dedicated playground port is documented.
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
  process with empty scratch. Partial uploads, lost object and `Publish`
  replies, abandoned intents, sweeper recovery, and object-effect fencing are
  not admitted.
- `[ACTIVE-WORK]` The actual Google Cloud project and GCS bucket await interactive
  gcloud reauthentication and exact organization/billing verification.
- `[EXISTS]` `Doss-com/objectKV` did not exist when this scaffold was created.
- `[EXISTS]` The `okv` package name is already occupied on crates.io.
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

Database(C) = ObjectState(O) + WAL mutations in (O, C]
```

Every acknowledged mutation in `(O, C]` must remain in the replicated durability
log. WAL through `X` may be reclaimed only after object state is reconstructable
through `X`.

Object storage is the permanent tier. During `(O, C]`, the retained WAL suffix
is part of the authoritative state and its declared failure topology determines
the system's RPO.
