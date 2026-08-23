# objectKV

The object-native transactional kernel for building databases.

Status: `[ACTIVE-WORK]` repository bootstrap. No durability, distribution, or
PostgreSQL compatibility claim exists yet.

objectKV is intended to become a FoundationDB-inspired ordered, transactional
key-value kernel whose permanent bytes live in object storage. A short-lived
replicated log will make commits fast. RAM and NVMe will be disposable serving
caches. The first pressure-test consumers are distributed Redis semantics,
distributed inverted search, and, most importantly, upstream PostgreSQL compute.
DataFusion over version-aligned analytical objects is the ZebraDB HTAP path.
These are consumers of the kernel, not protocol-specific kernel modes.

The project and repository are named `objectKV`; CLI commands, Rust packages and
modules, configuration prefixes, and day-to-day shorthand use `okv`.

```text
Redis / search / PostgreSQL / DataFusion
                  |
    FoundationDB-inspired ordered transactions
                  |
 transactional row segments + analytical artifacts
                  |
      S3-compatible object API / GCS / Blob
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
crates/okv-model/   executable MVCC and ZebraDB HTAP reference oracles
crates/okv-eval/    configurable eval runner and OTel instrumentation
crates/okv-htap/    Parquet, Arrow, and DataFusion snapshot overlay contracts
crates/okv-object/  named-object correctness boundary and conformance runner
crates/okv-sim/     exact seeded crash, network, and fencing replay probe
crates/okv-slate/   pinned SlateDB adaptation and external-version spike
crates/okv-wal/     checksummed local quorum frames and per-node stable journal
crates/okv-consensus/ pinned OpenRaft storage adapter and executable contracts
docs/               decisions, staged plan, eval design, PostgreSQL path
evals/              frozen suite definitions and result contract
infra/gcp/          guarded objectKV-dev project and GCS configuration
infra/minio/        digest-pinned local S3 protocol fixture
infra/otel/         pinned local OTel collector
experiments/        append-only research ledger conventions
rfcs/                architecture decisions before implementation hardens them
program.md          autonomous research operating loop
```

## Run what exists

```bash
cargo test --workspace
cargo run -p okv-eval -- smoke
cargo run -p okv-eval -- validate-suite evals/suites/phase0.toml
cargo run -p okv-object -- --backend memory --profile authority
cargo run -p okv-eval -- run evals/suites/object-store.toml \
  --profile memory-authority \
  --workload named-object-authority-contract \
  --backend memory
cargo run -p okv-eval -- run evals/suites/object-publication-adapter.toml \
  --profile local-fs \
  --workload object-publication-real-adapter \
  --backend object-store-local-fs+authority-quorum-fs
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-process.toml \
  --profile local-fs \
  --workload publisher-prepare-restart \
  --backend object-store-local-fs+process-openraft
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-put-recovery.toml \
  --profile local-fs \
  --workload publisher-first-put-unknown-restart \
  --backend object-store-local-fs+process-openraft
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-manifest-recovery.toml \
  --profile local-fs \
  --workload publisher-manifest-put-unknown-restart \
  --backend object-store-local-fs+process-openraft
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-publish-recovery.toml \
  --profile local-fs \
  --workload publisher-publish-unknown-restart \
  --backend object-store-local-fs+process-openraft
cargo run -p okv-eval -- run evals/suites/smoke.toml \
  --profile dev --workload model-smoke --backend model
cargo run -p okv-sim -- replay --seed 1103
cargo run -p okv-eval -- run evals/suites/fault-recovery.toml \
  --profile sim-dev --workload overlapping-generation-failures --backend turmoil
cargo run -p okv-eval -- run evals/suites/commit-contract.toml \
  --profile sim-dev --workload cell-commit-envelope --backend sim-model
cargo run -p okv-eval -- run evals/suites/persisted-wal.toml \
  --profile local-fs --workload persisted-wal-reopen --backend local-fs
cargo run -p okv-eval -- run evals/suites/raft-cluster.toml \
  --profile local-fs --workload openraft-three-node-failover \
  --backend turmoil-local-fs
cargo run -p okv-eval -- run evals/suites/raft-process.toml \
  --profile local-fs --workload openraft-process-lost-reply \
  --backend process-local-fs
cargo run -p okv-eval -- run evals/suites/htap-contract.toml \
  --profile model-dev --workload zebradb-base-plus-tail --backend model
cargo run -p okv-eval -- run evals/suites/htap-streaming.toml \
  --profile local-fs --workload zebradb-streaming-overlay \
  --backend datafusion-local-fs
```

The model smoke is not a storage or performance benchmark. The simulator probe
exercises one control-authority crash, restart, partition, repair, generation
change, and stale-publication oracle. It is not yet a replicated WAL simulator.
The object-store runner proves named-object semantics for one exact backend and
version. A passing `segment` profile is not evidence that the backend can host
mutable authority metadata.
The physical publication adapter writes immutable bytes through Apache
`object_store`, reopens publication authority from a synchronized three-file
local quorum, and serializes unguarded deletion against publication with a
durable per-object reservation. It is a single-process, single-machine recovery
proof. It is not a production distributed authority, independent-disk
durability result, cloud receipt, or throughput result.
The publisher-process gate starts three real OpenRaft authority processes,
commits an exact publication intent, kills a dedicated publisher before its
first object PUT, removes its scratch directory, and completes publication from
a replacement process with empty scratch. The ambiguous-PUT gate crosses the
next effect boundary: the first immutable PUT takes effect, its response becomes
retryable-unknown, the publisher is killed, and an empty-scratch replacement
verifies the existing named object before completing and publishing the exact
closure. The ambiguous-manifest gate then retains the complete manifest effect
while losing its response. Its empty-scratch replacement replays every data
identity, verifies the manifest, and walks the complete named closure before
root visibility. The lost-`Publish`-response gate then kills both the publisher
and accepting authority leader after the root transition applies but its reply
is dropped. The replacement recovers the retained outcome from the successor,
retries the exact identity without another transition, issues no object PUTs,
and walks the visible closure. Multipart residue, repeated unknowns,
abandoned-intent handling, sweeper recovery, and generation-bound effect
fencing remain ahead.
The commit-contract runner proves a deterministic envelope and failure oracle,
not production consensus. The persisted-WAL runner writes that envelope through
a checksummed frame to three local files, synchronizes each selected file,
reopens the topology, and reconstructs only matching quorum copies. It proves a
stable-storage seam on one machine, not Raft, replication transport, leader
election, independent failure domains, or a complete transaction cell.
The OpenRaft cluster runner adds deterministic three-node TCP replication,
explicit election, quorum failover, partition repair, stale-suffix replacement,
and journal replay after a simulated process bounce. It does not yet prove a
real OS process kill, unsynced-disk loss, generation takeover, durable request
deduplication, throughput, or a complete transaction cell.
The HTAP-contract runner proves exact model semantics. The streaming physical
runner reads a Parquet base, merges an Arrow tail incrementally across batch
boundaries, and proves exact output at one target version across two base
watermarks. Its memory receipt covers the overlay operator on a bounded fixture,
not complete-query memory, a `T - W_p` cost curve, manifests, leases, or Vortex.

## Project principles

- Object storage is the permanent tier; the retained WAL suffix is authoritative
  for committed versions not yet objectified.
- Serving storage is disposable.
- The transaction layer is independent of transactional segment encoding.
- Published object bytes are immutable; transactional references are mutable.
- Object storage is not a coordination system.
- Correctness gates performance.
- OLTP and OLAP may use different physical layouts but share one logical history.
- objectKV is not ZebraDB.

Start with [the system shape](docs/SYSTEM-SHAPE.md) and
[the bootstrap plan](docs/BOOTSTRAP-PLAN.md), then choose one open RFC or eval
lane from [the contributor board](docs/CONTRIBUTOR-BOARD.md).
Backend claims live in the versioned
[object-store capability matrix](docs/OBJECT-STORE-SUPPORT.md).
The [independent review synthesis](docs/research/EXPERT-REVIEW-SYNTHESIS.md)
tracks completed and pending adversarial reviews without implying consensus.
The active project slice is registered in the
[local project-tracking playground](docs/PROJECT-TRACKING.md).

## License

`[PROPOSED]` Apache License 2.0. The repository is local and unpublished while the
initial project decisions are reviewed.
