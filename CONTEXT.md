# objectKV domain context

This file defines vocabulary and current facts. Behavioral policy lives in
`AGENTS.md`.

## Names

- **objectKV**: `[PROPOSED]` the open-source object-native transactional ordered
  KV kernel and public repository name.
- **okv**: the CLI, Rust package/module, configuration-prefix, and local
  shorthand namespace for objectKV.
- **ZebraDB**: `[FUTURE]` the DOSS database product intended to consume
  objectKV.
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
  model, a deterministic smoke eval, and planning/RFC scaffolding.
- `[EXISTS]` `Doss-com/objectKV` did not exist when this scaffold was created.
- `[EXISTS]` The `okv` package name is already occupied on crates.io.
- `[PROPOSED]` The GitHub repository is `Doss-com/objectKV`.
- `[PROPOSED]` Packages remain `publish = false` until crate naming and API
  boundaries are decided.
- `[PROPOSED]` Apache License 2.0 is the public license.

## Load-bearing invariant

For latest committed version `C` and object durable version `O`:

```text
O <= C

Database(C) = ObjectState(O) + WAL mutations in (O, C]
```

Every acknowledged mutation in `(O, C]` must remain in the replicated durability
log. WAL through `X` may be reclaimed only after object state is reconstructable
through `X`.
