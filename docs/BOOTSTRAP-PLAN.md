# objectKV bootstrap and implementation plan

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
- `[EXISTS]` The logical model fixes generation-aware versions, gaps, canonical
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

- Mechanism: one KV Runtime process hosts many Range Engine assignments under
  shared RAM and NVMe caches. Range Engines apply committed txLog mutations,
  maintain applied versions, publish segments, and rebuild disposable state
  lazily.
- First files: `crates/okv-object/src/kv_runtime.rs`, RFC-0056, the accounted
  resource-envelope suite, then a future physical KV Runtime crate plus
  objectification and empty-cache recovery scenarios.
- `[EXISTS]` The deterministic resource envelope passes 1, 100, and 1,000
  Range Engine assignment points with 4,608 fixed accounted RAM bytes per
  range and one process-wide cache request. Four pressure and topology faults
  discard.
- `[EXISTS]` RFC-0057 supplies and passes the physical follow-up. Fresh child
  processes compared one logical-range SlateDB against shared-cache and
  private-cache database-per-range layouts at 1, 100, and 1,000 assignments.
  All nine correct points kept and all four controls discarded. The result
  selects one database with logical range prefixes as the KV Runtime default,
  while leaving mixed workload, remote object storage, prefix-aware
  publication, and range movement open.
- `[EXISTS]` RFC-0058 implements the first exact-version serving seam on
  the selected layout. objectKV-owned MVCC keys now support exact point and
  ordered range reads at `T`, point tombstones, binary key order, applied
  frontier refusal, and reopen. The 1, 16, and 256 version-depth curve kept all
  correct points and discarded all controls. Depth 256 kept point reads viable
  but reached `283.47x` physical amplification and 74.0 MB per cold scan.
- `[EXISTS]` The serving slice now freezes a monotonic minimum-readable version
  per compaction job and preserves all newer versions plus one floor-visible
  value or tombstone per key. Candidate `3c9f008` kept depth-256 windows 1, 16,
  and 64 at `1.225x`, `1.111x`, and `1.107x` retained-byte amplification, with
  all five unsafe controls discarded. The local curve also exposes the pinned
  profile's eight-overlapping-SST per-key backpressure bound.
- `[EXISTS]` Pure lease admission, renewal, logical expiry, monotonic floors,
  frozen collection jobs, exact replacement publication, stale-epoch refusal,
  root-aware delete reservation, and checksummed snapshot restore now exist in
  the publication authority.
- `[EXISTS]` Candidate `5f62082` kept the correct three-process authority
  history through 12 leader replacements and nine lost committed replies. The
  missing-outcome control discarded on all three seeds.
- `[ACTIVE-WORK]` The remaining unsafe authority subjects, real worker death,
  OTel export, remote object storage, and concurrent serving remain acceptance
  gates. RFC-0060 freezes that remaining process history.
- Open question: how much metadata must be eagerly loaded for bounded time to
  first correct read?
- Failure mode: logical readiness waits on copying or scanning the full dataset.
- Failure mode: an unbounded snapshot floor retains every historical version,
  so object bytes, scan bytes, and compaction debt grow with history depth.
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
- `[EXISTS]` The pinned PostgreSQL 18.6 callback slice now reads, writes one
  existing main-fork page, and obtains authoritative block count through
  objectKV. One checkpoint advanced the Cell version and a PostgreSQL restart
  read the changed row through a fresh Range Engine without changing the local
  heap file.
- `[EXISTS]` Expected version 0 now binds a PostgreSQL callback atomically to
  the service's current immutable physical page-store view. Nonzero pinned
  versions still fail stale, and no discovery round trip remains.
- `[EXISTS]` Candidate `3bb2783` persists one exact immutable local object base
  plus two required signed txLog sets, recovers without reading the source
  heap, accepts a post-recovery write, and survives a second service restart.
  Missing-quorum and missing-object controls refuse startup.
- `[EXISTS]` PostgreSQL's checkpointer and `smgr_immedsync` now invoke one
  objectKV stable handler. The bounded proof published recoverable version 13
  through a three-process authority, reconciled it after page-service restart,
  and refused the next checkpoint after authority loss while hot state advanced
  to version 14.
- `[EXISTS]` Stable sync now builds a complete versioned relation base through
  `B`, atomically selects its local descriptor, publishes the exact closure,
  obtains a pinned authority capability, and pops both required txLog sets.
  A separate Cell transaction-authority harness stays live across disposable
  page-service restart; recovery at the popped base needs zero tail records and
  does not read the source heap.
- `[EXISTS]` Stable target `B` may now name object base `O <= B` plus the exact
  certified suffix `(O, B]`; pop is capped at `O`. Complete relation
  objectification is triggered only by checkpoint capture and runs from owned
  immutable inputs outside the bridge-state mutex. A later checkpoint can
  atomically activate the ready base and shorten the tail.
- `[ACTIVE-WORK]` The service still serializes write commit, view replacement,
  and stable publication under one lock. Objectification is asynchronous but
  remains a full relation rewrite per captured checkpoint. Both authority
  harnesses are ephemeral and same-host. Incremental objectification, authority
  recovery, a database-wide remote root, non-serial publication, and empty-cache
  restore precede any production durability claim.
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

Status: `[ACTIVE-WORK]`.

Deliverables:

- public naming and OSS-boundary decisions;
- compiling Rust workspace;
- executable reference-model smoke eval;
- eval result contract and autonomous research program;
- RFC queue and contributor-ready tasks.

Exit: all local checks pass and one human can select a task without reconstructing
the architecture memo.

### S1. Establish the Phase 0 baseline

Status: `[ACTIVE-WORK]`. Object-store conformance, the repaired SlateDB
filesystem scale curve, the bounded legacy serving-worker configuration pass, and a
local separate-role compaction contract now execute. Local overwrite
compaction also survives one real worker-process kill and reclaim. MinIO
physical storage, coordinator output adoption, concurrent-coordinator fencing,
one local active-output plus true-orphan GC boundary, and the explicit
checkpoint, clone, backup, analytical-lease, and tenant-move root graph now
execute. GCS, root expiry and abandonment, coordinator election and host
partitions, concurrent writers, and target-workload ceilings remain open.

Memory, filesystem, and pinned MinIO now have executable capability-profiled
conformance evidence. Filesystem is intentionally segment-only because the
shared Apache `object_store` API does not expose conditional update for its
local backend. Candidate `be78904` makes the provider-bound GCS cache-state
profile executable with exact generations, request-cost telemetry, guarded
scratch scope, and failure cleanup. Candidate `257fe2a` completes the first
in-region GCS matrix: empty-cache first point was 48.6 ms median, persistent
NVMe was 294.5 us median with zero serving-path GCS reads, and all six identity
controls discarded. Realistic cache hit rate, concurrency, and sustained-write
economics remain open.

RFC-0021 and `phase0-slate-filesystem-v1` run 8 MiB per seed through pinned
SlateDB at revision `e0161973`. The original 129.9 ms reopen result included
closing the old instance and is superseded. Candidate `361a0fd` separates every
phase and gives each raw artifact a unique `run_id`. The repaired 1, 8, and
64 MiB runs kept exact logical results at 4.85, 6.19, and 424.13 ms for open
through first correct read. The 64 MiB open read 210,773,938 bytes, crossing
RFC-0022's stop threshold for the untuned incumbent. The repaired warm-instance
poison `402f095c` discarded on the fresh-cache gate.

RFC-0024 freezes the only allowed local configuration pass. Candidate
`7567b99` removes embedded maintenance and the duplicate SlateDB object WAL
from serving workers, uses 64 KiB blocks, and enables Bloom filters for every
non-empty SST. Across seeds 1103, 2207, and 3301, fresh open read 402 bytes,
the first cold point used five requests and at most 210,439 bytes, and open
through the exact read took 3.81 to 4.12 ms. Total read bytes fell 89.3 percent
and written bytes fell 51.3 percent, while request count rose 31.9 percent.
This keeps a local candidate and moves the next falsifier to separate
compaction plus MinIO and GCS economics.

RFC-0025 closes the first local role-boundary question without closing the
maintenance design. Candidate `b240b38` compacts eight 1 MiB L0 SSTs to one
sorted run through a coordinator with no embedded worker and a separately
built 64 KiB worker. Runs `d6425f5e` and `5431c0fe` keep three seeds with exact
full scans, 1.027x maintenance write amplification, 538-byte fresh opens, and
first cold points at no more than 83,264 bytes. Missing-worker control
`af37279a` discards on the four intended maintenance gates. The next physical
falsifier is overwrite pressure plus worker death and reclaim, then MinIO and
GCS.

RFC-0026 closes that first local process-failure step. Candidate `803de76`
writes eight overlapping L0 snapshots, kills a worker only after its persisted
claim becomes `Running`, observes coordinator reclaim, and completes through a
fresh worker identity. Runs `238de077` and `882b1fcf` keep three seeds at zero
anomalies with 576 to 618 ms from kill to committed result. Every latest
overwrite remains exact. Missing-replacement control `af904d02` discards only
on identity and completion. Physical MinIO, coordinator death,
overlapping-coordinator fencing, and the bounded local root graph now pass. GCS,
root expiry and abandonment, independent host loss, and distributed sweeper
ownership are the next storage falsifiers.

Implement the same fixed dataset/workloads against:

1. SlateDB with filesystem storage;
2. SlateDB with local S3/MinIO;
3. SlateDB with one real cloud object-store profile;
4. the in-memory oracle for correctness only.

Gate 1 passes only if hot/cold latency, object request amplification, rewritten
bytes, compaction cost, and empty-cache reopen are measured and acceptable for a
named target workload. A blended average cannot pass the gate. The local
configuration pass is complete. A dataset-sized reopen under separate
compaction, MinIO, or GCS stops SlateDB as the incumbent without stopping
objectKV.

### S2. Build the versioned object engine

Status: `[ACTIVE-WORK]` logical contract; storage-engine implementation remains
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

Status: `[ACTIVE-WORK]` the simulator, local stable storage, three-node OpenRaft
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
generation handoff through signed recovery evidence. A bounded authority
checkpoint now restores the complete Cell v0 state after covered journal
removal. One bounded history now also checks actual values from linearizable
reads, actual read dependencies, and real-time order against commit sequence.
Candidate `27a86f1` composes memory-only partitioned conflict resolution,
authenticated tLog durability, and maximal-prefix generation recovery through
real processes. Candidate `674a443` then orders three commit proxies through one
replicated predecessor chain at every resolver and tLog, including explicit
progress for conflict-only batches. Remaining work includes proxy-failure gap
recovery, broader generated overlapping range histories, online resolver split
and merge, metadata propagation, ratekeeping on the partitioned path,
retained-outcome expiry, disk-full behavior, independent-disk loss, production
timing and placement, and sustained throughput and latency curves.

Gate 2: every failing seed replays exactly; low-millisecond local-region commit
is demonstrated; brownout, kill/restart, disk-full, and lost-ack scenarios
preserve every acknowledged commit within the published RPO contract.

### S4. Make serving disposable

Status: `[ACTIVE-WORK]` for publication workers and bounded local read recovery;
routed concurrent serving and full materialization recovery remain
`[PROPOSED]`.

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
retry causes no second authority or object effect. A fifth gate starts a fresh
serving process, resolves object state through `O=8`, applies a quorum-recovered
suffix, and reconstructs exact rows at `T=10`; its ignore-suffix control returns
stale rows at `8`. The suffix is copied into a bounded local WAL fixture.
A sixth gate removes that copied suffix: after transaction-leader death, the
fresh worker fetches committed envelopes directly from the live successor
authority and reconstructs exact `T=10` state. This establishes committed
envelopes, not raw transaction proposals, as the serving mutation boundary.
The seventh gate copies one committed envelope to three dedicated range-tagged
tLog processes with private synchronized roots and a hard retained-byte limit.
After one tLog dies, a fresh worker requires matching tag-`10` records from both
survivors and reaches exact `T=10`; omitting tag `10` leaves it stale at `8`.
Multipart residue, repeated unknowns, abandoned work, sweeper recovery,
commit acknowledgement integration, multi-record streaming, tLog repair and
partitioning, routed concurrency, lag-based ratekeeping, and readiness curves
remain open.

Gate 3: complete worker loss does not require durable dataset copy.

### S5. Add ranges, snapshots, and OCC

Status: `[ACTIVE-WORK]` for centralized snapshots, OCC, point reads, and one
empty-range phantom witness; logical range ownership and role partitioning
remain `[PROPOSED]`.

Add logical ranges, assignment generations, metadata-only split/move, global
cell read versions, tenant transaction domains, conflict ranges, and resolver
checks in that order. The first cell centralizes throughput while keeping role
protocols explicit; resolver, proxy, and log partitioning follow measured
ceilings. Cross-cell transactions are out of scope.

Gates 4 and 5: range movement copies approximately zero durable database bytes;
generated histories remain strictly serializable at useful measured throughput.

### S6. Put full PostgreSQL compute on objectKV

Status: `[EXISTS]` for one maintained-fork read, existing-page write, block
count, checkpoint, PostgreSQL process restart, complete single-relation
objectification, publication-authorized txLog pop, and bounded zero-tail local
page-service restart while an external transaction authority remains live.
Authority restart, incremental objectification, database-wide checkpoint roots,
lifecycle operations, remote recovery, and full recovery remain
`[ACTIVE-WORK]` or `[PROPOSED]`.

Begin the page/storage bridge spike once S2 establishes stable versioned storage.
Do not claim the PostgreSQL path until S3 provides a credible durability boundary.
Use PostgreSQL's own regression suite as the compatibility oracle.

Gate 6A: a real PostgreSQL server boots, creates a database, survives restart,
and passes a declared regression subset with objectKV as its durable backing.
The current callback proof now satisfies the narrow PostgreSQL, complete
single-relation objectification, root-pinned txLog pop, and zero-tail
page-service restart precursors. It does not satisfy Gate 6A because neither
authority recovers, the root is not database-wide or remote, only one selected
relation and existing-page writes are covered, and no declared regression
subset has run.

### S7. Build the ZebraDB HTAP path

Status: `[ACTIVE-WORK]` for the bounded DataFusion base-plus-tail operator;
durable analytical manifests, leases, and PostgreSQL integration remain
`[FUTURE]`.

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
