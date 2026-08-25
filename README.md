# objectKV

[![ci](https://github.com/Doss-com/objectKV/actions/workflows/ci.yml/badge.svg)](https://github.com/Doss-com/objectKV/actions/workflows/ci.yml)

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

`[EXISTS]` The current bounded transaction path now includes three commit
proxies under one predecessor chain, memory-only partitioned resolvers,
authenticated txLogs, full transaction-system generation replacement after
resolver or commit-proxy loss, and one online hot-range split through shadow
catch-up. This is local semantic evidence, not a throughput, multi-host, or
production-availability claim.

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

`[ACTIVE-WORK]` The first milestone is intentionally smaller than the end state:

1. Accept externally assigned commit versions.
2. Apply and read versioned mutations through a storage-engine boundary.
3. Persist immutable state through an object-store implementation.
4. Measure hot reads, cold reads, request amplification, compaction cost, and
   empty-cache reopen behavior.
5. Reject the architecture if the physical economics do not clear Gate 1.

The first pinned SlateDB filesystem incumbent now executes deterministic ingest,
warm and cold point reads, ordered scans, and empty-cache reopen across three
seeds. It records per-API requests and bytes through the shared OTel path. This
is an incumbent measurement, not a Gate 1 pass: MinIO, GCS, forced compaction,
larger datasets, and named workload cost ceilings remain open.

`[EXISTS]` A public KV Runtime resource model now accounts 1, 100, and 1,000
Range Engine assignments under one shared RAM and NVMe cache envelope. It
proves pressure semantics, not physical process density or production read
performance.

`[EXISTS]` The RFC-0057 physical-density gate selects one pinned SlateDB
database with many logical range prefixes as the KV Runtime default. All nine
correct 1, 100, and 1,000 topology workloads kept, and four controls discarded.
At 1,000 assignments the selected layout held one database, one decoded cache,
9 live tasks, and 9 object files. The shared database-per-range alternative
used 1,000 databases, 8,001 tasks, and 9,000 object files. This closes engine
cardinality, not production capacity or throughput.

`[EXISTS]` RFC-0058 gives the SlateDB adapter an objectKV-owned MVCC
key encoding. Exact `get(key, T)` and ordered `scan(begin, end, T)` retain point
history and tombstones, preserve arbitrary binary key order, survive reopen,
and refuse `T` above the applied frontier. The local history-depth suite now
executes at 1, 16, and 256 retained versions with real object-I/O receipts and
four independently discarding semantic controls. Near-latest cold point p99
reached 6.69 ms at depth 256, while physical amplification reached `283.47x`
and a cold scan read 74.0 MB. The encoding is accepted for the next prototype.
`[EXISTS]` RFC-0059 now supplies a monotonic read floor, typed
expired-snapshot refusal, a frozen per-job compaction filter, and a clean
depth-256 retained-window curve. Starting from 74.3 MB of history, windows 1,
16, and 64 converged to `1.225x`, `1.111x`, and `1.107x` retained logical
bytes; all five unsafe controls discarded. The curve is local filesystem
object-store evidence. `[ACTIVE-WORK]` RFC-0060 now has pure lease, read-floor,
collection-token, exact-publication, and root-aware delete transitions carried
by the existing OpenRaft state machine. Checksummed restart retains active
leases without changing the empty format-v1 snapshot. Candidate `5f62082` also
kept the three-seed process gate through 12 leader replacements and nine lost
committed replies; the missing-outcome control discarded. `[EXISTS]` Candidate
`fc30e59` now composes an authority-selected SlateDB base with signed txLog
quorums in disposable worker processes. Both M0 and M1 rebuild exact T=10 state
across authority and txLog node failures; six controls discard on every seed.
`[EXISTS]` Candidate `c79e099` also moves physical collection into a dedicated
child process and re-verifies both object closures after it exits. The
`2742400` follow-on pins the old M0 closure in the authority, blocks deletion
until release, reclaims it through exact permits, and keeps a fresh M1 worker
exact. The remaining unsafe subjects, remote-object curves, worker restart,
and concurrent serving remain open before this becomes a bounded long-running
path.

`[EXISTS]` Candidate `1ee9de4` adds the first release-build curve for that exact
authority base plus certified txLog view. Base-only open stayed between 0.60
and 0.73 ms from 1,024 through 65,536 keys. Certified-tail authentication and
overlay construction was linear at about 61 microseconds per record. The curve
also exposed the current performance boundary: every raw base point read still
issues one object-range request, and the former bounded-map scan let unrelated
tail keys increase base work. `[EXISTS]` The next two slices inject shared
RAM/NVMe cache ownership and stream the authority base plus resident tail. GCS
latency and production read-performance claims remain out of scope for these
local runs.

`[EXISTS]` Candidate `7071e33` now injects caller-owned shared decoded RAM and
bounded NVMe caches below the authority manifest-binding layer. On the clean
16K-key release pair, repeated point reads fell from 64 backend range GETs to
zero and the 1,024-row scan fell from 80 to one. With a 64-record tail, scan
GETs fell from 85 to one. View open costs about 1.6 ms more and the first miss
is slower. Resurrection controls, eviction under multiple ranges, and GCS
remain `[ACTIVE-WORK]`.

`[EXISTS]` Candidate `20899e7` replaces `limit + affected_tail_keys`
materialization with an ordered base cursor plus resident-tail merge. On the
clean 16K-key release points, the raw 1,024-record-tail scan now makes 80
backend range GETs, equal to the zero-tail raw scan, and runs at 186K rows/s.
The former path made 159 GETs and ran at 91K rows/s. The cached scan remains at
one backend GET. Tail authentication still costs about 61 microseconds per
record during view open, and the unobjectified tail remains resident memory.

`[EXISTS]` Candidate `79afb08` closes that ambiguity for the data path. It
populates the bounded local block cache, closes the view, discards decoded RAM,
rebuilds the cache object from the same directory, and opens a new authority
view. The clean zero-tail and 64-tail points serve first-point data and the
complete scan with zero backend bytes and zero successful backend range GETs.
This is not an offline reopen: view open still transfers 788 bytes of manifest
metadata from the backing store, and the first point performs one failed
metadata GET. Persistent NVMe accelerates data; object storage still
participates in worker bootstrap.

`[EXISTS]` Candidate `63c9531` adds the first persistent-cache fault control.
It corrupts every cached data part, reopens with fresh decoded RAM, and permits
only checksum-driven refusal or an exact value after backend range re-fetch.
The current implementation repairs from the backend and never exposes the
corrupt bytes as a value. Stale-root resurrection still needs a composed
authority control because cache contents alone cannot prove that a root remains
lease-protected and durable.

`[EXISTS]` Candidate `7eae670` adds that focused authority boundary.
Publication state now revalidates the exact active snapshot-lease token.
Historical raw or cache-backed opens bind that token to the outer published
Range Engine root, target version, and closure containing both that root and
the inner immutable-base manifest before any storage access. Release, expiry,
token drift, or root drift refuses with a typed error and zero cache or backend
requests.
Publication-authority generation is deliberately not equated with the
generation that produced the immutable base. A live process-authority read and
stale-snapshot control were still required before this could become a process
reopen guarantee.

`[EXISTS]` Candidate `e06a159` composes that boundary into the real handoff
suite. M0 first warms a persistent cache under an active lease. After M1
publication and M0 lease release, a fourth disposable worker reads live
authority and refuses the old root before storage access. The stale-authority
negative reuses a pre-release snapshot and reopens M0 in all three seeds, so it
discards. The suite compacts M0 into an independent M1 data closure, reclaims
three M0-only objects per seed, and proves M1 remains exact after collection.

`[EXISTS]` Candidate `52ca95e` adds a fifth worker with a bounded authority-read
deadline. When authority is unavailable it persists a fail-closed receipt and
does not open storage. The unsafe negative falls back to the pre-release
snapshot, reopens M0 in all three seeds, and discards.

`[EXISTS]` Candidate `505c997` adds a separate process-isolated persistent-cache
fault gate. Each seed prepares two real SlateDB caches, exits, overwrites every
part in one and truncates every part in the other, then reopens both through
fresh processes and decoded RAM. Clean run `83a36734` kept all 24 checks across
12 workers. It damaged 30 cache parts, repaired every exact value from object
storage, and returned zero wrong values. Four omitted-fault and
accepted-wrong-value controls discarded. Bounded eviction under many Range Engines and GCS
were the remaining local and remote gates.

`[EXISTS]` Candidate `5f7bf82` now forces eight logical Range Engines through
one 192 KiB persistent cache with a working set above 2 MiB. Clean run
`9375c874` kept exact first-pass and reverse-order rereads across three seeds.
The cache settled at no more than 131,292 bytes and rereads caused 130 backend
range refills, proving physical eviction. Disabling the bound retained about
2.1 MiB and caused zero refills; skip-reread and accepted-wrong controls also
discarded. The local cache hierarchy is admitted for this fixture. The frozen
GCS replay remains `[ACTIVE-WORK]`.

`[ACTIVE-WORK]` Candidate `f496e8d` makes that eviction suite backend-selectable
and adds a `gcs-dev` profile with isolated scratch prefixes and cleanup as a
hard gate. Clean local regression `2e1ce017` kept. Live GCS remains unrun: the
interactive gcloud credential is expired, and the active application-default
identity receives `PERMISSION_DENIED` for `doss-objectkv-dev`, so neither the
project nor bucket can be verified from this session.

`[EXISTS]` Candidate `be78904` also replaces the provider-bound range suite's
GCS discard stub with the real process path. Every remote immutable GET carries
the authority-selected GCS generation. Each child uses a guarded scratch
prefix and must remove all live objects; the controller repeats cleanup after
failure or timeout. The cloud profile requires OTel and a pinned request-price
snapshot. `[EXISTS]` Candidate `257fe2a` completed that frozen contract from an
ephemeral `us-central1-a` runner. Empty-cache first-point latency was 48.6 ms
median and 53.4 ms maximum across five seeds. Persistent-NVMe first-point
latency was 294.5 us median with zero serving-path GCS reads. All six identity
controls discarded, OTel exported all three signals, and live scratch cleanup
completed. The 128-point warmed working set is not a production hit ratio;
reuse-distance, cache-capacity, concurrency, and write economics remain
`[ACTIVE-WORK]`.

`[EXISTS]` The follow-on cache-economics gate now rejects passive demand
caching as the complete serving policy. With persistent NVMe bounded to 25
percent of logical bytes, Zipfian `0.99` missed 26.820 percent and a moving
10-percent hotset missed 14.535 percent, versus the 2.5-percent request-cost
ceiling. Candidate `d64a14f` then gave ideal placement the same capacity. Even
that oracle has 16.170-percent and 7.5-percent miss floors. objectKV therefore
needs explicit locally complete assigned-range images for hot-SLO reads, not a
claim that arbitrary cell bytes become fast through LRU. Objects remain the
authority and empty-disk rebuild source; local images remain disposable.

`[EXISTS]` Candidate `8fb20e5` moves the first PostgreSQL page reader through
the actual routed process path. Three encoded 8 KiB pages span two ranges; one
page advances through the authenticated txLog from objectKV version 1 to 2.
The client refreshes a stale route, preserves version 2, authenticates all
three pages, and separately enforces the PostgreSQL page-LSN frontier. Correct
run `977b368d` kept, while missing-page, corrupted-payload, changed-version,
and LSN-ahead controls discarded.

`[EXISTS]` Candidate `b04b128` wires one real PostgreSQL 18.6 heap relation to
that reader. A fresh PostgreSQL process read 148 actual heap pages through a
separate objectKV page service and returned the exact 2,000-row aggregate.
Thirteen callback requests covered blocks 0 through 147 with no `mdreadv`
fallback. Stopping the service caused connection refusal, and changing the
fixed page frontier caused a typed refusal. The cold debug scan took 233.045
ms; its immediate PostgreSQL shared-buffer repeat took 0.299 ms. This is a
synchronous read seam over an in-memory object store, not a page-write,
checkpoint, recovery, remote-object, or production AIO result.

`[EXISTS]` Candidate `c3c5df9` adds the first PostgreSQL write-side semantic
gate. A permanent page batch is admitted only when its exact objectKV view is
nonzero, it contains at most 128 pages, and PostgreSQL WAL is durable through
every page LSN. The admitted page mutations and request identity receive one
deterministic digest. Correct run `0bf18a75` kept six mutations across three
seeds; WAL-behind, zero-version, oversized-batch, and corrupted-digest subjects
discarded. This is an admission oracle, not a PostgreSQL callback, objectKV
commit, relation extent, checkpoint barrier, or durability result.

`[EXISTS]` Candidate `7de5c4e` binds admitted pages and authoritative
relation-fork extent into one real Cell transaction. Across three seeds, the
correct suite commits six pages and three extent values through 12 Cell process
starts, returns the exact response for duplicate retry, kills three leaders,
and finds the same pages plus `nblocks=2` on each successor. Missing extent,
changed retry identity, wrong receipt identity, and non-advancing commit-version
subjects discard. This is not yet the PostgreSQL write callback, a tagged txLog
quorum receipt, Range Engine reconstruction, or checkpoint-stable storage.

`[EXISTS]` Candidates `f89f8c1` and `402e0ae` add a mutable page service and
typed write-admission errors. The pinned PostgreSQL fork now routes one selected
main-fork relation's `smgr_readv`, `smgr_writev`, and `smgr_nblocks` through
objectKV. A real update checkpointed block 0 through a three-process Cell,
advanced version 5 to 9, rebuilt a fresh Range Engine, and returned the changed
row after PostgreSQL restart. The local 1,212,416-byte heap file kept SHA-256
`3770217f...d7e8`; stale-version and forced WAL-behind controls refused without
advancing state. The debug one-page checkpoint took 678 ms. This admits the
callback seam, not by itself service restart, object durability, stable
checkpoint, cross-backend version dissemination, or production performance.

`[EXISTS]` Selected callbacks now send expected version 0 to atomically select
the service's current immutable physical page-store view. A backend that began
at version 5 continued after its checkpointer advanced version 9, selected that
view in its next block-count operation, and returned the same-session result.
There is no version-discovery round trip or discovery-to-operation race.
Nonzero pinned versions still fail stale. This is not a SQL snapshot change.
PostgreSQL remains the MVCC authority; objectKV versions track subordinate
physical page flushes.

`[EXISTS]` Candidate `3bb2783` adds bounded local page-service recovery. The
service freezes the version-5 relation as one exact SlateDB manifest and
live-SST closure, then retains real Cell commit envelopes in two required
signed txLog sets, each with three local processes and quorum two. A complete
service restart used a nonexistent source-heap path, authenticated the object
closure and txLog suffix through version 10, returned the committed row, and
accepted a new checkpoint through version 11. A second restart recovered four
authenticated tail records through version 12 and returned both changes. A
missing txLog quorum and a missing live SST each refused startup. The debug
durable checkpoints took 465.720 ms and 561.758 ms. This is a local-filesystem,
empty-process-memory result, not replicated root publication, host-loss
recovery, stable PostgreSQL sync, remote objects, or a performance target.

`[EXISTS]` PostgreSQL's native sync-request queue now dispatches selected dirty
relations to an objectKV stable handler. The handler captures current physical
version `B`, derives the exact recoverable immutable-base plus certified-txLog
frontier, requires PostgreSQL WAL through its maximum page LSN, and waits for a
three-process publication authority to prepare, publish, and linearly read back
the content-addressed root. Version 13 reached authority term 3, index 4 before
the 829 ms debug checkpoint completed. A page-service restart reconciled the
same root without a source heap. With the authority removed, hot state reached
version 14 but `CHECKPOINT` failed and stable version 13 remained selected.

`[EXISTS]` The next bounded slice separates the Cell transaction authority from
disposable page compute. Stable sync reads the complete selected relation at
`B`, builds a versioned immutable SlateDB base, atomically selects its local
descriptor, publishes the exact visible-row and object-closure digest, and uses
the replicated authority's signed capability to pop both required txLog sets.
All six txLog nodes reached the same pop frontier.

A page-service restart with a nonexistent source heap recovered base 11 with
zero tail records, accepted later writes, published and popped through 13, and
survived a second zero-tail restart. When publication authority was removed,
hot base 14 was built but `CHECKPOINT` failed, stable stayed at 13, and txLog
pop stayed at 13.

`[EXISTS]` The follow-up now permits stable target `B` to name an older object
base `O` plus a complete certified txLog suffix `(O, B]`. txLog pop remains at
or below `O`. Complete relation objectification starts only from checkpoint
capture, uses immutable captured planning state outside the page-service mutex,
and becomes eligible for atomic activation by a later checkpoint. Page writes
do not schedule full relation rewrites.

A fresh three-page proof published `B=9/O=5`, then activated base 9 and
published `B=10/O=9`. Source-free restart recovered base 9 plus version 10.
Complete base materialization took 90 ms and 75 ms. During authority outage,
base 11 completed in 26 ms while the stable request waited 6.044 seconds to
fail; stable stayed 10 and pop stayed 9.

This remains a bounded same-host result. Objectification is asynchronous but
still rewrites the complete selected relation after each captured checkpoint.
Both authority harnesses are ephemeral, the root is single-relation and
local-filesystem, historical bases are not collected, and no remote empty-cache
or host-loss control has passed.

Authority recovery, incremental range or delta objectification, database-wide
root publication, concurrent view publication, PostgreSQL lifecycle,
remote-object recovery, and HTAP materialization follow only after their owning
gates pass.
The PostgreSQL read, existing-page write, bounded local sidecar recovery,
stable-sync, and root-pinned retention seams now exist as candidates, not
production claims.

## Repository map

```text
crates/okv-model/   executable MVCC and ZebraDB HTAP reference oracles
crates/okv-eval/    configurable eval runner and OTel instrumentation
crates/okv-htap/    Parquet, Arrow, and DataFusion snapshot overlay contracts
crates/okv-object/  named-object correctness boundary and conformance runner
crates/okv-postgres/ PostgreSQL page adapter, mutable service, and Cell commit gate
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
cargo run -p okv-eval -- run evals/suites/persisted-wal.toml \
  --profile local-fs --workload persisted-wal-reopen --backend local-fs
cargo run -p okv-eval -- run evals/suites/raft-cluster.toml \
  --profile local-fs --workload openraft-three-node-failover \
  --backend turmoil-local-fs
cargo run -p okv-eval -- run evals/suites/raft-process.toml \
  --profile local-fs --workload openraft-process-lost-reply \
  --backend process-local-fs
cargo run -p okv-eval -- run evals/suites/cell-process-snapshot.toml \
  --profile local-fs --workload cell-process-durable-snapshot-pop \
  --backend process-local-fs
cargo run -p okv-eval -- run \
  evals/suites/cell-routine-reconfiguration-process.toml \
  --profile local-fs --workload routine-reconfiguration-process-correct \
  --backend process-local-fs
cargo run -p okv-eval -- run evals/suites/cell-objectification.toml \
  --profile local-fs --workload cell-objectification-correct \
  --backend process-local-fs
cargo run -p okv-eval -- run evals/suites/htap-contract.toml \
  --profile model-dev --workload zebradb-base-plus-tail --backend model
cargo run -p okv-eval -- run evals/suites/htap-streaming.toml \
  --profile local-fs --workload zebradb-streaming-overlay \
  --backend datafusion-local-fs
cargo run -p okv-eval -- run \
  evals/suites/cell-partitioned-resolver-agreement.toml \
  --profile local-process \
  --workload cell-partitioned-resolvers-match-centralized-oracle \
  --backend transaction-openraft+partitioned-resolver-processes
cargo run -p okv-eval -- run \
  evals/suites/cell-transaction-system-recovery-curve.toml \
  --profile local-process \
  --workload recovery-tail-4096 \
  --backend local-process+replicated-authority+authenticated-tlog-inventory
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
The semantic process snapshot runner sends multi-key OCC transactions through
three real OpenRaft processes, persists the complete Cell v0 authority at one
applied position, removes the covered journal entries, restarts every voter,
replays an exact retained outcome, and continues with a new commit. It is a
bounded local process proof, not object-data durability, independent-disk
recovery, snapshot transfer, or a production WAL-pop admission.
The partitioned-resolver runner keeps the tenant transaction domain intact
while three ordered resolver processes decide clipped conflict ranges. Its
bounded history matches the centralized oracle across cross-range transactions
and resolver restart. It proves one fixed map with sequential prepared work,
not online split or merge, hot-range throughput, proxy batching, independent
hosts, or general strict serializability.
The transaction-system recovery curve excludes semantic-history construction
from its timed samples. It durably fences and admits through a live
three-process authority, authenticates binary tLog inventories, starts real
successor roles, and records five OTel phases. The admitted local curve is
linear and reads zero permanent database bytes, but 65,536 retained records per
tLog take 3.158 seconds because inventory scanning dominates. This is a local
optimization signal, not a production recovery SLO.
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
The architecture review readout is available as a
[review artifact](docs/research/architecture-review-readout-2026-08-23.html)
and [canonical Markdown](docs/research/architecture-review-readout-2026-08-23.md).
It freezes the current evidence, claim boundary, review decisions, stop
conditions, and recommended calibration order.
The current implementation, performance, verification, tradeoff, and vision
readout is available as an
[implementation deep-dive](docs/research/objectkv-implementation-deep-dive-2026-08-23.html)
and
[canonical Markdown](docs/research/objectkv-implementation-deep-dive-2026-08-23.md).
The diagram-led team artifact is published on
[Tapestry](https://tapestry.doss.com/a/f66ac680f7be46fe--objectkv-architecture-how-the-system-works-and-is-being-buil).
The focused runtime topology, SQL read-path, and finite-serving-envelope maps
are available in the [local diagram index](docs/diagrams/index.html) and the
[published map set](https://tapestry.doss.com/a/5a375c8fe804483d--objectkv-runtime-architecture-maps).

## License

`[EXISTS]` Apache License 2.0. The public repository is
[`Doss-com/objectKV`](https://github.com/Doss-com/objectKV). Hosted CI
verification is `[EXISTS]`.
