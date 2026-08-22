# objectKV

The object-native transactional kernel for building databases.

Status: `[ACTIVE-WORK]` repository bootstrap. No durability, distribution, or
PostgreSQL compatibility claim exists yet.

objectKV is intended to become an ordered, versioned key-value kernel whose
permanent bytes live in object storage. A short-lived replicated log will make
commits fast. RAM and NVMe will be disposable serving caches. ZebraDB is the
first intended database built on the kernel, not part of the kernel itself. The
project and repository are named `objectKV`; CLI commands, Rust packages/modules,
configuration prefixes, and day-to-day shorthand use `okv`.

```text
PostgreSQL / ZebraDB / other database layers
                    |
          transactional ordered KV
                    |
       immutable versioned object segments
                    |
             S3 / GCS / Blob
```

## First proof

`[PROPOSED]` The first milestone is intentionally smaller than the end state:

1. Accept externally assigned commit versions.
2. Apply and read versioned mutations through a storage-engine boundary.
3. Persist immutable state through an object-store implementation.
4. Measure hot reads, cold reads, request amplification, compaction cost, and
   empty-cache reopen behavior.
5. Reject the architecture if the physical economics do not clear Gate 1.

The distributed WAL, disposable serving workers, ranges, OCC, PostgreSQL path,
and HTAP materialization follow only after the prior gate passes.

## Repository map

```text
crates/okv-model/   executable reference model and correctness oracle
crates/okv-eval/    eval entrypoint, currently a deterministic smoke suite
docs/               decisions, staged plan, eval design, PostgreSQL path
evals/              frozen suite definitions and result contract
experiments/        append-only research ledger conventions
rfcs/                architecture decisions before implementation hardens them
program.md          autonomous research operating loop
```

## Run what exists

```bash
cargo test --workspace
cargo run -p okv-eval -- smoke
```

The smoke command tests the versioned in-memory model. It is not a storage or
performance benchmark.

## Project principles

- Object storage is authoritative.
- Serving storage is disposable.
- The transaction layer is independent of physical storage.
- Published object bytes are immutable; transactional references are mutable.
- Object storage is not a coordination system.
- Correctness gates performance.
- OLTP and OLAP may use different physical layouts but share one logical history.
- objectKV is not ZebraDB.

Start with [the bootstrap plan](docs/BOOTSTRAP-PLAN.md), then choose one open RFC
or eval lane from [the contributor board](docs/CONTRIBUTOR-BOARD.md).

## License

`[PROPOSED]` Apache License 2.0. The repository is local and unpublished while the
initial project decisions are reviewed.
