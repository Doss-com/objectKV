# objectKV bootstrap and implementation plan

## Program goal

Prove or falsify objectKV as an open-source, open-format storage substrate for
distributed applications and, if admitted by evidence, build its first
production-credible cell.

The target is a cell-scoped, strict-serializable ordered KV composed from
reusable `okv-log` and `okv-wal` primitives, a quorum-durable txLog hot path,
bounded disposable RAM or SSD serving state, and immutable S3-compatible
objects as permanent, branchable history. Prove that one version history can
support low-latency point and range OLTP, replay and branching, PostgreSQL
storage semantics, and DataFusion base-plus-tail OLAP without ETL.

Advance each layer from `[CODE-COMPLETE]` to `[VERIFIED]` only through frozen
correctness, crash-recovery, bounded-state, latency, throughput, and economics
gates. Run those gates with OTel telemetry and immutable receipts on real
infrastructure against same-durability RocksDB, TiKV, FoundationDB, or other
appropriate controls.

If native transaction authority cannot establish material object-native
leverage over an incumbent, pivot transaction authority to TiKV or FoundationDB
while retaining `okv-log`, `okv-wal`, object publication, branching, and the
version-aligned PostgreSQL plus DataFusion history. Material leverage means a
measured advantage in at least one load-bearing property such as independent
storage and compute scaling, branch and recovery cost, open-format access, or
total economics without an unacceptable correctness or latency regression.

The program succeeds with a documented go or pivot decision backed by the full
eval record. A go decision additionally requires contributor-ready
specifications, runnable examples, code, operational bounds, and immutable eval
receipts for the first production-credible cell. Until those receipts exist,
objectKV remains a research program rather than an admitted database product.

### Current golden-path frontier

`[VERIFIED]` The R0 mechanism reconstructs one public range from regional GCS
plus a retained txLog suffix into bounded local NVMe, survives worker loss, and
performs zero object operations during the measured hot-read window.

`[VERIFIED]` The topology-matched GP3.1 rerun admitted the single-range native
read boundary. Native retained 90.89 and 91.97 percent of owned-value direct
RocksDB throughput in opposite process orders. P99 was 0.913x control in both
orders. Exact replay, bounded native state, empty-worker reconstruction, zero
measured object operations, and all three OTel signals passed.

`[VERIFIED]` GP3.1.1 admits the resident read boundary at 8 and 32 concurrent
clients. Native retained 87.34 through 89.06 percent of matched direct RocksDB
throughput and kept p99 between 1.107x and 1.184x control in both orders.

`[EVALUATING]` The next native rungs are cache-pressure reads and three-node
replicated commit against a same-durability control. RAM, multi-range,
PostgreSQL, and HTAP remain blocked on those gates.
FoundationDB remains the semantic oracle and fallback profile. The immutable
concurrency evidence is under
`docs/artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/`.
Cache pressure follows as a separate gate with an explicit cache budget and
reusable larger-than-cache fixture.

### What the goal optimizes for

- One small, composable ordered transaction substrate for distributed
  applications.
- Fast resident reads and writes with permanent, open, branchable object state.
- Independent storage and compute scaling without putting object latency in the
  normal commit path.
- One exact version history usable by OLTP serving models, PostgreSQL, and
  DataFusion rather than an ETL-copied analytical truth.
- Claims backed by reproducible receipts, including negative controls and
  matched-durability alternatives.

### What the goal gives up

- Cross-cell transactions and a global synchronous version space.
- Object-only low-latency commits as the default durability profile.
- Immediate work on MultiRaft, PostgreSQL completeness, metaclusters, or HTAP
  optimization before the resident and cold-object performance curves pass.
- A commitment to owning transaction authority if TiKV or FoundationDB is the
  stronger measured foundation.

## Original intent

Launch objectKV as a contributor-ready open-source project, then use an eval-led
research program to progress from a versioned object engine to an object-native
transaction kernel, full PostgreSQL compute, and ZebraDB HTAP over one logical
history. Distributed Redis semantics and inverted search are earlier serving
models that pressure-test the kernel without replacing the PostgreSQL north star.

## Workstreams, Level 1

- W1. Physical object engine: prove object-store read, write, cache, compaction,
  and reopen economics.
- W2. Version and correctness model: define visibility, replay, deletion,
  snapshots, and storage-engine contracts.
- W3. Durability boundary: add a low-latency replicated WAL and object durable
  watermark without acknowledging impossible states.
- W4. Disposable serving: recover correct reads from metadata, recent WAL, and
  object storage with empty local state.
- W5. Logical distribution: route, split, move, fence, and compact logical
  ranges without copying their durable bytes.
- W6. Transaction semantics: global snapshots, OCC conflict ranges, retries,
  and strict serializability.
- W7. Eval and research system: freeze oracles, budgets, suites, result records,
  and keep/discard rules.
- W8. PostgreSQL and HTAP path: preserve PostgreSQL behavior, then connect one
  commit/version history to OLTP and DataFusion columnar execution.
- W9. OSS operations: establish naming, license, RFCs, governance, contributor
  tasks, release contracts, and public claims.

## Depth findings

### W1. Physical object engine

- Mechanism: Apache `object_store` for providers, a SlateDB adapter or extracted
  segment layer for the first implementation, RAM/NVMe cache, and immutable
  segment publication.
- First files: `crates/okv-segment`, `crates/okv-object`,
  `evals/suites/phase0.toml`.
- Finding: the pinned SlateDB revision publicly accepts externally assigned
  sequence numbers and custom WAL traits. Explicit public read-at-version and
  standalone segment-building seams remain upstream questions.
- Failure mode: the engine looks fast when warm but cold point reads or
  compaction generate uneconomic object requests and bytes.
- Rough scope: medium, 1 to 2 weeks for a comparable baseline, 2 to 4 weeks for
  a versioned adapter.

### W2. Version and correctness model

- Mechanism: a single-threaded executable specification, generated histories,
  differential checks, and an immutable-segment interface.
- First files: `crates/okv-model`, RFC-0002, RFC-0003, RFC-0004.
- `[VERIFIED]` The logical model fixes generation-aware versions, gaps, canonical
  exact replay, range tombstones, scans, read-your-writes, and an inclusive
  oldest-readable boundary. Large-value references remain open.
- Failure mode: implementation and oracle share the same bug or the version
  contract changes silently under benchmarks.
- Rough scope: small for point operations, large once range deletes, snapshots,
  and concurrent histories enter.

### W3. Durability boundary

- Mechanism: one three-node Raft log per cell, quorum fsync before acknowledge,
  asynchronous materialization, and conservative global watermarking.
- First files: future `crates/okv-wal-api`, `crates/okv-wal-raft`, RFC-0005,
  RFC-0007, RFC-0009.
- Entry gate: exact seeded simulation of generation recovery, virtual time, and
  injected log, network, clock, and object-store failures exists before WAL code.
- Open question: which persisted state surrounds `raft-rs`, and how are commit
  versions fenced across generation recovery?
- Failure mode: an acknowledged commit exists in neither reconstructable object
  state nor the retained WAL.
- Tigris-derived constraint: upload immutable bytes before publishing their
  authoritative pointer; commit data, index intent, and asynchronous task intent
  together; recover ambiguous uploads by identity; derive deletion safety from
  retained roots rather than incremental counters alone.
- Rough scope: large, 3 to 6 weeks after the versioned engine is credible.

### W4. Disposable serving

- Mechanism: serving workers subscribe to committed mutations, publish segments,
  maintain applied versions, and rebuild caches lazily.
- First files: future `crates/okv-storage-worker`, objectification evals, and
  empty-cache recovery scenarios.
- Open question: how much metadata must be eagerly loaded for bounded time to
  first correct read?
- Failure mode: logical readiness waits on copying or scanning the full dataset.
- Tigris-derived constraint: cache entries are version-addressed, bytes or
  blocks land before visible metadata, and every delayed populate is fenced by
  a newer invalidation or generation. Cache hits are part of the correctness
  history, not a performance-only layer.
- Rough scope: large, 3 to 6 weeks after the WAL path.

### W5. Logical distribution

- Mechanism: bounded complete cells, tenant database transaction domains,
  ordered key ranges, generation-fenced assignments, metadata-only split/move,
  shared historical segments, and background physical realignment. A future
  metacluster maps tenants to cells without cross-cell transactions.
- First files: future `crates/okv-router`, `crates/okv-control`, RFC-0006.
- Open question: how are segment references shared safely across child ranges
  while watermarking and GC remain correct?
- Failure mode: a stale owner publishes state or range movement triggers bulk
  durable-byte transfer.
- Rough scope: extra large, multi-month with fault testing.

### W6. Transaction semantics

- Mechanism: read versions, explicit read/write conflict ranges, partitionable
  resolver state, one ordered commit stream first, then measured scaling inside
  one cell. Transactions may span arbitrary in-cell ranges inside one tenant
  database.
- First files: future `crates/okv-txn`, `crates/okv-resolver`, RFC-0008.
- Open question: ordered versus hashed resolver domains, transaction lifetime,
  commit-unknown handling, and log partitioning threshold.
- Failure mode: a generated concurrent history observes a result the reference
  serializable model cannot produce.
- Rough scope: extra large, begins after ranges and global snapshots.

### W7. Eval and research system

- Mechanism: frozen correctness gates, lane-specific fixed-budget benchmarks,
  public and held-out seeds, machine profiles, noise measurement, and an
  append-only experiment ledger.
- First files: `program.md`, `docs/EVALS.md`, `evals/`, `experiments/`.
- Open question: which stable machine/backend profiles produce results worth
  comparing across contributors?
- Failure mode: an optimizer changes the test, buys speed with correctness, or
  overfits one visible workload.
- Rough scope: small for the chassis, ongoing for each system phase.

### W8. PostgreSQL and HTAP path

- Mechanism: use narrow Redis and inverted-index adapters as early pressure
  tests; first prototype PostgreSQL page/storage bridging over objectKV; later
  decide whether to deepen that fork or move logical relations/indexes directly
  onto objectKV. Materialize version-aligned Parquet for DataFusion before
  evaluating Vortex.
- First files: `docs/POSTGRES-PATH.md`, RFC-0010, a future isolated bridge crate
  or PostgreSQL fork repository.
- Open question: which PostgreSQL durability and MVCC responsibilities remain in
  PostgreSQL versus move into objectKV?
- Failure mode: two independent transaction/MVCC systems duplicate work and
  cannot agree on snapshot visibility.
- Rough scope: one to two weeks per thin bridge prototype, multi-quarter for full
  compatibility and HTAP.

### W9. OSS operations

- Mechanism: Apache-2.0, vendor-led RFC governance, issue templates, claim-safe
  README, public benchmark receipts, and one adopter-independent API.
- First files: `README.md`, `CONTRIBUTING.md`, `rfcs/`, `.github/`.
- Open question: whether and when public crates need a separate namespace from
  the `okv` shorthand.
- Failure mode: launch claims distribution or performance before evidence, or
  contributors implement conflicting invariants.
- Rough scope: small bootstrap, then continuous maintenance.

## Merged workstreams

- M1. Prove the object engine, covers W1 + early W2 + early W7. Produce a
  versioned engine and Gate 1 report.
- M2. Prove durable objectification, covers W3 + W4 + fault portions of W7.
  Produce low-latency commits and empty-cache recovery.
- M3. Prove the distributed transaction kernel, covers W5 + W6. Produce
  metadata-only range movement, global snapshots, and serializable transactions.
- M4. Prove PostgreSQL and HTAP consumption, covers W8. Produce real PostgreSQL
  regression evidence and version-aligned analytical snapshots.
- M5. Operate the OSS project, covers W9 + research governance from W7. Keep the
  public contract, RFC queue, and contribution lanes coherent through all phases.

## Sequence

### S0. Bootstrap the project

Status: `[EVALUATING]`.

Deliverables:

- public naming and OSS-boundary decisions;
- compiling Rust workspace;
- executable reference-model smoke eval;
- eval result contract and autonomous research program;
- RFC queue and contributor-ready tasks.

Exit: all local checks pass and one human can select a task without reconstructing
the architecture memo.

### S1. Establish the Phase 0 baseline

Status: `[EVALUATING]`. Object-store conformance and the repaired SlateDB
filesystem scale curve now execute; MinIO physical storage, GCS, compaction,
one bounded layout pass, and target-workload ceilings remain open.

Memory, filesystem, and pinned MinIO now have executable capability-profiled
conformance evidence. Filesystem is intentionally segment-only because the
shared Apache `object_store` API does not expose conditional update for its
local backend. A bounded real-GCS cache-admission canary now executes in
`doss-objectkv-dev`; Phase 0 SlateDB and object-authority conformance on GCS,
clean-source repetition, and required OTel remain open.

RFC-0021 and `phase0-slate-filesystem-v1` run 8 MiB per seed through pinned
SlateDB at revision `e0161973`. The original 129.9 ms reopen result included
closing the old instance and is superseded. Candidate `361a0fd` separates every
phase and gives each raw artifact a unique `run_id`. The repaired 1, 8, and
64 MiB runs kept exact logical results at 4.85, 6.19, and 424.13 ms for open
through first correct read. The 64 MiB open read 210,773,938 bytes, crossing
RFC-0022's stop threshold for the untuned incumbent. The repaired warm-instance
poison `402f095c` discarded on the fresh-cache gate.

Implement the same fixed dataset/workloads against:

1. SlateDB with filesystem storage;
2. SlateDB with local S3/MinIO;
3. SlateDB with one real cloud object-store profile;
4. the in-memory oracle for correctness only.

Gate 1 passes only if hot/cold latency, object request amplification, rewritten
bytes, compaction cost, and empty-cache reopen are measured and acceptable for a
named target workload. A blended average cannot pass the gate. One bounded
SlateDB layout and compaction pass may continue; another dataset-sized reopen
stops SlateDB as the incumbent without stopping objectKV.

### S2. Build the versioned object engine

Status: `[EVALUATING]` logical contract; storage-engine implementation remains
`[PROPOSED]`.

Introduce externally assigned versions, point reads, range reads, exact replay,
immutable segment publication, checksums, and manifest inspection behind a small
storage-engine contract. Differentially test every generated history against
`okv-model`.

The logical contract and generated differential gate now exist. The pinned
SlateDB adapter owns a full 16-byte latest-version metadata record, rejects
unsupported generations and range clears explicitly, and proves concurrent
lower versions cannot replace a higher one. Explicit historical reads and
atomic range clears remain adaptation seams.

Exit: Gate 1 re-runs against the objectKV adapter with no correctness failures.

### S3. Add the fast durability tier

Status: `[EVALUATING]` the simulator, local stable storage, three-node OpenRaft
replication, real-process retry recovery, and generation handoff gates exist;
the production WAL remains `[PROPOSED]`.

The first exact replay probe now preserves synced control authority across
crash/restart and rejects a stale generation after partition/repair. Extend that
harness to the coordinator, durable log, object store, and recovery state
machine. Then add one ordered replicated WAL, acknowledge after quorum
durability, consume it into immutable objects, and advance the conservative
object durable watermark. Ratekeep and eventually refuse commits at declared
`C - O` and retained-WAL bounds.

The persistence and consensus slices now frame the frozen commit envelope,
synchronize independent per-node journals, replicate through three OpenRaft
processes, recover a lost reply after real leader death, and fence a G1 to G2
generation handoff through signed recovery evidence. Remaining work includes
snapshot persistence and retained-outcome expiry, disk-full behavior, replica
repair, independent-disk loss, production timing and placement, object-root
reconciliation, and sustained throughput and latency curves.

Gate 2: every failing seed replays exactly; low-millisecond local-region commit
is demonstrated; brownout, kill/restart, disk-full, and lost-ack scenarios
preserve every acknowledged commit within the published RPO contract.

### S4. Make serving disposable

Status: `[EVALUATING]` for publication and disposable read serving. Complete
single-range materialization recovery is `[VERIFIED]` on the R0 mechanism; its
steady-state p99 is not admitted.

Separate read/materialization workers from permanent bytes. Start with an empty
cache and demonstrate bounded logical readiness independent of dataset size.

Four publisher gates now start replacement processes with empty scratch. The
first recovers after quorum-durable `Prepare` but before object effects. The
second recovers after the first immutable PUT takes effect and its response is
lost, using replicated intent plus exact named identity to finish the closure.
The third recovers after the immutable manifest PUT takes effect and its
response is lost, then replays and verifies every named child before root
visibility. The fourth recovers the exact retained `Publish` outcome after its
reply, publisher, and accepting authority leader are lost, then proves exact
retry causes no second authority or object effect. Multipart residue, repeated
unknowns, abandoned work, sweeper recovery, empty-disk read serving, and bounded
readiness remain open.

Gate 3: complete worker loss does not require durable dataset copy.

### S5. Add ranges, snapshots, and OCC

Status: `[PROPOSED]`.

Add logical ranges, assignment generations, metadata-only split/move, global
cell read versions, tenant transaction domains, conflict ranges, and resolver
checks in that order. The first cell centralizes throughput while keeping role
protocols explicit; resolver, proxy, and log partitioning follow measured
ceilings. Cross-cell transactions are out of scope.

Gates 4 and 5: range movement copies approximately zero durable database bytes;
generated histories remain strictly serializable at useful measured throughput.

### S6. Put full PostgreSQL compute on objectKV

Status: `[PROPOSED]`.

Begin the page/storage bridge spike once S2 establishes stable versioned storage.
Do not claim the PostgreSQL path until S3 provides a credible durability boundary.
Use PostgreSQL's own regression suite as the compatibility oracle.

Gate 6A: a real PostgreSQL server boots, creates a database, survives restart,
and passes a declared regression subset with objectKV as its durable backing.

### S7. Build the ZebraDB HTAP path

Status: `[FUTURE]`.

Map records and indexes to atomic objectKV transactions. Materialize Parquet from
the authoritative commit history with explicit per-partition coverage versions.
Query one exact version `T` through a DataFusion base-plus-tail overlay. The
durable analytical tail may outlive the recovery WAL. Predicate pushdown must
retain keys needed to invalidate stale base rows. Long queries use snapshot
leases; writes based on their results use transactional projections or later
dependency validation.

Gate 6B: a representative ZebraDB workload is simpler or materially better than
the current dual-system path enough to justify owning the substrate.

## Recalibration check

The core intent still holds, but the north star is sharper than the source memo:
full PostgreSQL compute is now an explicit consumer program. It does not change
the first gate. If Gate 1 fails, stop rather than compensating with distribution
or PostgreSQL work.
