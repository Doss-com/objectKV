# objectKV

The object-native transactional kernel for building databases.

Status: `[EVALUATING]` repository bootstrap. Local and one-machine cloud
mechanisms have verified receipts. No production durability, independent-host
cell, real workload, or PostgreSQL compatibility claim is admitted yet.

objectKV is intended to become a FoundationDB-inspired ordered, transactional
key-value kernel whose permanent bytes live in object storage. A short-lived
replicated log will make commits fast. Assigned ranges use one of two bounded,
disposable hot-state profiles: RocksDB on NVMe or an in-memory serving image.
Both reconstruct from the same object base and durable tail. The first
pressure-test consumers are distributed Redis semantics,
distributed inverted search, and, most importantly, upstream PostgreSQL compute.
DataFusion over version-aligned analytical objects is the ZebraDB HTAP path.
These are consumers of the kernel, not protocol-specific kernel modes.

The project and repository are named `objectKV`; CLI commands, Rust packages and
modules, configuration prefixes, and day-to-day shorthand use `okv`.

```text
applications and data platforms
                  |
             okv-fabric
 KV | log | WAL | snapshots | branches | projections
                  |
            objectKV kernel
 ordered transactions | txLog | serving | publication
                  |
 transactional row segments + analytical artifacts
                  |
      S3-compatible object API / GCS / Blob
```

## First proof

`[EVALUATING]` The first milestone is intentionally smaller than the end state:

1. Accept externally assigned commit versions.
2. Apply and read versioned mutations through a storage-engine boundary.
3. Persist immutable state through an object-store implementation.
4. Measure hot reads, cold reads, request amplification, compaction cost, and
   empty-cache reopen behavior.
5. Reject or redesign the candidate mechanism if its physical economics do not
   clear Gate 1.

The first pinned SlateDB filesystem incumbent now executes deterministic ingest,
warm and cold point reads, ordered scans, and empty-cache reopen across three
seeds. It records per-API requests and bytes through the shared OTel path. This
is an incumbent measurement, not a Gate 1 pass: MinIO, GCS, forced compaction,
larger datasets, and named workload cost ceilings remain open.

The distributed WAL, disposable serving workers, ranges, OCC, PostgreSQL path,
and HTAP materialization follow only after the prior gate passes.

## Repository map

```text
crates/okv/         experimental integrated single-range kernel API
crates/okv-model/   executable MVCC and ZebraDB HTAP reference oracles
crates/okv-eval/    configurable eval runner and OTel instrumentation
crates/okv-htap/    Parquet, Arrow, DataFusion snapshot overlays, and the
                    bounded range-stripe table source
crates/okv-history-oracle/ independent strict-serializability history checker
crates/okv-log/     pure ordered opaque-record algebra used below the WAL
crates/okv-object/  named-object correctness boundary and conformance runner
                    plus the experimental indexed row-object point reader
crates/okv-sim/     exact seeded crash, network, and fencing replay probe
crates/okv-slate/   pinned SlateDB adaptation and external-version spike
crates/okv-wal/     checksummed local quorum frames and per-node stable journal
crates/okv-consensus/ pinned OpenRaft storage adapter and executable contracts
docs/               decisions, staged plan, eval design, PostgreSQL path
evals/              golden scenario, frozen suites, programs, metrics, and result contract
infra/gcp/          guarded objectKV-dev project and GCS configuration
infra/minio/        digest-pinned local S3 protocol fixture
infra/otel/         pinned local OTel collector
experiments/        append-only research ledger conventions
examples/okv-tetris/ interactive API-boundary example on okv-model + okv-log
formal/             executable TLA+ cell contract and finite model receipts
rfcs/                architecture decisions before implementation hardens them
program.md          autonomous research operating loop
```

## Run what exists

```bash
./experiments/run-okv-tetris-web.sh
OKV_ALLOW_DIRTY=1 ./experiments/run-cold-object-read.sh
OKV_ALLOW_DIRTY=1 ./experiments/run-empty-worker-recovery.sh
OKV_ALLOW_DIRTY=1 ./experiments/run-serving-recovery-process.sh
OKV_ALLOW_DIRTY=1 ./experiments/run-serving-recovery-openraft.sh
cargo test --workspace
cargo run -p okv-eval -- smoke
cargo run -p okv-eval -- validate-suite evals/suites/phase0.toml
cargo run -p okv-eval -- validate-program \
  evals/programs/objectkv-golden-path-v1.toml
cargo run -p okv-eval -- run evals/suites/single-range-kernel.toml \
  --profile local-fs \
  --workload integrated-single-range-versionstamp-recovery \
  --backend object-store-local-fs+authority-openraft+data-openraft
cargo run --release -p okv-eval --features resident-rocksdb -- run \
  evals/suites/single-range-ssd-smoke.toml \
  --profile local-process-rocksdb-smoke \
  --workload integrated-single-range-rocksdb-recovery \
  --backend object-store-local-fs+authority-openraft+data-openraft+rocksdb-local-fs
OKV_GCS_BUCKET=doss-objectkv-dev-okv-evals \
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:34318 \
  cargo run -p okv-eval -- run \
    evals/suites/single-range-kernel-gcs-smoke.toml \
    --profile local-controller-gcs-smoke \
    --workload integrated-single-range-gcs-versionstamp-recovery \
    --backend object-store-gcs+authority-openraft-local-process+data-openraft-local-process
cargo run -p okv-eval -- plan-program \
  evals/programs/objectkv-golden-path-v1.toml
cargo run -p okv-eval -- run evals/suites/phase0-slate-filesystem.toml \
  --profile local-fs \
  --workload slatedb-filesystem-baseline \
  --backend slatedb-local-fs
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
cargo run -p okv-eval -- run evals/suites/serializability.toml \
  --profile model-dev --workload strict-serializable-four-range-history \
  --backend cell-v0-resolver-model
cargo run -p okv-eval -- run evals/suites/serializability-process.toml \
  --profile local-fs --workload openraft-transaction-serializability \
  --backend process-openraft+transaction-authority
cargo run -p okv-eval -- run evals/suites/commit-proxy-batch32.toml \
  --profile local-process --workload commit-proxy-batch32-candidate \
  --backend data-openraft-local-process+stable-journal
# Requires an externally provisioned and provider-attested four-machine topology.
cargo run -p okv-eval -- run evals/suites/serializability-machines.toml \
  --profile gcp-three-zone \
  --workload openraft-machine-transaction-serializability \
  --backend gcp-three-zone-pd-ssd+openraft
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
cargo run --release -p okv-eval -- run \
  evals/suites/htap-columnar-range-source-coalesced.toml \
  --profile local-fs --workload c5-datafusion-coalesced-256k \
  --backend datafusion+local-fs-range-stripes
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
The G4.4 recovery runner adds a second three-process OpenRaft data authority and
a linearizable retained-transaction API. A killed worker and empty-scratch
replacement recover an object base, catch up to one frozen target, accept four
concurrent commits, catch up to a second target, and return exact point and
range-clear outcomes without physical Raft journal access. It still runs on one
host, retains commands without safe pop, and does not prove production latency,
durability, convergence, or economics.
The `single-range-kernel-v1` runner moves that composition behind the public
experimental `okv::SingleRange` API. Its first local diagnostic committed one
transaction through that API, paged a shared-version batch with the exact
`(commit_version, batch_order)` cursor, killed one range process, opened a
distinct replacement, and returned exact state from one manifest GET, one
index GET, one data range GET, and no LIST. Run `74b29fe1` passed all hard
gates with 123.002 ms first-correct-read latency. The receipt remains
`[EVALUATING]` because the source was dirty, OTel was disabled, and every
authority process shared one host.
Run `6723ce8a` then used the same public recovery and read path against real GCS
with required OTel. It passed all 12 hard gates in 7.254 seconds, reached first
correct read in 756.950 ms, and retained the one-manifest, one-index, one-range,
zero-LIST access bound. This remains `[EVALUATING]`: the source was dirty and
the six authority processes plus controller shared one Mac. See
`docs/artifacts/eval-receipts/single-range-kernel-gcs-2026-08-27/`.
The provider-neutral serving-image boundary now also has a public RocksDB
implementation. Dirty debug run `56535944` rebuilt a bounded 86,667-byte image
from objects, applied the txLog suffix, survived serving-process replacement,
and completed 100,000 exact `SingleRange` point reads at 824,252 reads/s and
1,583 ns p99 with zero post-activation object operations. This is
`[EVALUATING]`, not an NVMe performance claim. Its receipt is in
`docs/artifacts/eval-receipts/single-range-ssd-smoke-2026-08-27/`.
The G4.10 commit-proxy runner begins with independent client transactions and
closes quorum-durable batches on item count, encoded bytes, delay, or sender
shutdown. The retained 32-item local configuration reached 1,157.369 median
transactions per second, 76.101 ms maximum p99, and 6.356x the same-durability
one-entry control while sparse, byte, overload, oversized-request, replay,
failover, and restart gates passed. The receipts remain `[EVALUATING]` because
the source was dirty and all three voters shared one host and filesystem.
The G4.10b runner composes that path with a separate three-process publication
authority, a frozen immutable closure through `O`, controlled conflicts, and
physical txLog pop while suffix commits continue. The 25% conflict candidate
reached 1,075.343 median resolved outcomes per second, 104.274 ms maximum p99,
and 28.776x the same-durability one-entry control. Every seed reconstructed
exact final state from objects through `O` plus `(O,C]`; moving-frontier and
premature-pop controls failed before unsafe mutation. These receipts also
remain `[EVALUATING]` because both quorums and local object files share one host.
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

Start with [the layered architecture index](docs/architecture/README.md),
[the RangeEngine serving profiles](docs/architecture/RANGE-ENGINE.md),
[the architecture evidence map](docs/architecture/EVIDENCE.md),
[the living architecture tracker](docs/artifacts/objectkv-architecture/objectkv-architecture.html),
[the product specification](docs/PRODUCT-SPEC.md),
[the system shape](docs/SYSTEM-SHAPE.md),
[the compact architecture maps](docs/ARCHITECTURE-MAPS.md), and
[the bootstrap plan](docs/BOOTSTRAP-PLAN.md), then choose one open RFC or eval
lane from [the contributor board](docs/CONTRIBUTOR-BOARD.md).
Status claims follow the [proof-status contract](docs/STATUS-TAXONOMY.md).
Real-infrastructure percentages and failure receipts follow the
[paired benchmark program](docs/REAL-INFRA-EVALS.md).
Backend claims live in the versioned
[object-store capability matrix](docs/OBJECT-STORE-SUPPORT.md).
The [independent review synthesis](docs/research/EXPERT-REVIEW-SYNTHESIS.md)
tracks completed and pending adversarial reviews without implying consensus.
The active project slice is registered in the
[local project-tracking playground](docs/PROJECT-TRACKING.md).

## License

`[PROPOSED]` Apache License 2.0. The repository is local and unpublished while the
initial project decisions are reviewed.
