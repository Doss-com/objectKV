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
- **commit version**: a total-order identifier assigned to an accepted commit.
- **range**: `[FUTURE]` a logical keyspace ownership and routing unit, not a
  permanent data replica.
- **eval lane**: one research objective with one primary metric and frozen hard
  gates.

## Current facts

- `[EXISTS]` The repository contains a Rust workspace, an in-memory reference
  model, a pinned SlateDB adapter spike, a configurable eval runner, an OTel
  path, an exact seeded generation-fencing probe, an object-store conformance
  runner, and planning/RFC scaffolding.
- `[EXISTS]` The SlateDB spike can apply externally assigned versions and reject
  conflicting replay. It cannot yet expose an explicit public read version.
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

## Load-bearing invariant

For latest committed version `C` and object durable version `O`:

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
