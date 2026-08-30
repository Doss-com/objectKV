# objectKV decision log

## D1. Public name and shorthand

Status: `[DECIDED]` 2026-08-22.

Decision: The project and GitHub repository are named `objectKV`. CLI commands,
Rust crates/modules, configuration prefixes, and local shorthand use `okv`.

Optimizes for: a legible public name without making every code identifier long.

Gives up: exact equality between the repository name and every package/binary.

## D2. OSS boundary

Status: `[PROPOSED]`.

Decision: objectKV contains only a general-purpose ordered transactional KV
kernel. ZebraDB, ZSL, ERP concepts, SQL semantics, and DOSS product behavior live
outside the kernel.

Optimizes for: independent adoption and a clean storage abstraction.

Gives up: product-specific shortcuts inside the kernel.

## D3. Initial public wedge

Status: `[PROPOSED]`.

Decision: The first usable artifact is a versioned object engine with an
embedded API and an executable economics report. Distribution is added only
after the object-engine gate passes.

Optimizes for: fast evidence on the novel physical thesis.

Gives up: an impressive distributed demo on day one.

## D4. Evaluation shape

Status: `[PROPOSED]`.

Decision: Correctness is a hard eligibility gate. Each performance/economics
lane has one primary metric, a fixed budget, and its own champion. There is no
single blended score across the whole system.

Optimizes for: experiments that cannot buy speed with incorrectness or hide one
bad dimension inside an average.

Gives up: one simple project-wide leaderboard number.

## D5. PostgreSQL meaning

Status: `[PROPOSED]`, needs prototype evidence before acceptance.

Decision: Interpret "full PostgreSQL on objectKV" first as an upstream-compatible
PostgreSQL compute process whose durable storage boundary is backed by objectKV.
Prototype the page/storage bridge before attempting a new PostgreSQL-compatible
SQL implementation.

Optimizes for: preserving the PostgreSQL parser, planner, executor, wire protocol,
extensions, and regression suite while changing the durable substrate.

Gives up: native use of objectKV transactions for every PostgreSQL internal in
the first bridge. The bridge may initially map pages or relations onto keys.

Revisit when: Gate 2 proves fast durability and the bridge spike identifies the
smallest PostgreSQL fork surface.

## D6. Autonomous research policy

Status: `[PROPOSED]`.

Decision: The agent may edit only the declared candidate surface for its lane.
The reference model, eval runner, suite definitions, held-out seeds, budgets, and
result schema are frozen. Every attempt is retained in an append-only ledger.

Optimizes for: reproducible learning and resistance to metric gaming.

Gives up: allowing an experiment to repair its own benchmark in the same change.

## D7. License

Status: `[PROPOSED]`.

Decision: Apache License 2.0.

Optimizes for: broad database-vendor adoption and alignment with the intended
dependency ecosystem.

Gives up: using source restriction as the commercial moat.

## D8. Package publication

Status: `[PROPOSED]`.

Decision: keep workspace packages unpublished during the research phases. The
`okv` crate name is already occupied, so public package names require a separate
naming decision after the API boundary stabilizes.

Optimizes for: avoiding a rushed namespace and compatibility promise.

Gives up: immediate `cargo add okv` onboarding.

## D9. SlateDB adaptation posture

Status: `[DECIDED]` for the first spike, 2026-08-22.

Decision: pin one exact upstream SlateDB revision behind an `okv-slate` adapter.
Use its public external sequence-number and custom-WAL seams. Seek small upstream
read-at-version and standalone-segment seams before considering a long-lived
fork.

Optimizes for: compile-backed learning with a narrow divergence surface.

Gives up: immediate control over SlateDB internals. A fork remains possible if
fencing, replay, or explicit-version reads cannot be made robust upstream.

## D10. Eval telemetry contract

Status: `[DECIDED]` for bootstrap, 2026-08-22.

Decision: suites and metrics are declarative TOML; compact results are validated
JSON; high-resolution logs, metrics, and traces use OTLP/HTTP through an OTel
Collector. Correctness remains a hard gate and each lane has one primary metric.

Optimizes for: configurable metrics, portable backends, bounded cardinality, and
automation that can trace a performance curve to its cause.

Gives up: a backend-specific query model until the shared telemetry store is
selected.

## D11. Development cloud boundary

Status: `[EVALUATING]`.

Decision: provision a DOSS-owned Google Cloud project with display name
`objectKV-dev`, one protected single-region GCS eval bucket, and a keyless runner
identity. The exact global project ID and billing attachment are accepted only
after interactive account verification.

Optimizes for: isolated and comparable cloud experiments with bounded blast
radius.

Gives up: multi-region evidence and shared-operator Terraform until remote state
and those specific eval lanes are ready.

## D12. Initial serving-model consumers

Status: `[PROPOSED]` for expert review.

Decision: use distributed Redis semantics and inverted search as early pressure
tests, upstream PostgreSQL as the compatibility-critical database consumer, and
DataFusion over version-aligned columnar objects as the ZebraDB HTAP path.

Optimizes for: exercising distinct latency, indexing, transaction, recovery, and
analytical access patterns against one kernel.

Gives up: an early claim that any one protocol surface is complete.

## D13. Physical format boundary

Status: `[PROPOSED]` for expert review.

Decision: use two physical contracts. Transactional segments encode the
kernel-owned MVCC algebra and optimize point/range access. Analytical artifacts
encode version-aligned, schema-aware Parquet or Vortex data for scan engines.
Their shared waist is a sorted versioned-entry stream plus fenced publication,
not one generic file-format interface.

Optimizes for: one logical history without leaking analytical schemas into the
kernel or pretending row and column formats have identical responsibilities.

Gives up: swapping Parquet or Vortex directly into the OLTP read path without a
workload-specific materialization and compatibility proof.

## D14. Deterministic simulation order

Status: `[EVALUATING]` after the first independent review.

Decision: build an exact, seeded, virtual-time simulation harness before the
replicated WAL. Every distributed component must run under it, and a failing
seed must replay exactly before that component can merge.

Optimizes for: reproducible generation recovery, fencing, retry, and watermark
failures before they become multi-process incidents.

Gives up: shipping the happy-path WAL first. This adds an earlier systems-test
investment but removes a larger late recovery retrofit.

Evidence: `okv-sim` pins Turmoil 0.7.2, fails closed without Tokio RNG seeding,
produces byte-identical local fresh-process traces, configures CI to repeat the
comparison, and detects a deliberate stale-generation publication bug. The
probe is not yet evidence for replicated-WAL recovery.

## D15. Acknowledgement and lag contract

Status: `[PROPOSED]`, required before Gate 2.

Decision: `COMMITTED` means quorum-fsynced in the declared WAL topology. Within
one cell, `C_cell` is the committed watermark and `O_cell` is the conservative
object-durable watermark. The contract
must publish regional RPO, `commit_unknown` behavior, a hard retained-WAL bound,
and the `C_cell - O_cell` thresholds for ratekeeping, commit refusal, and recovery.

Optimizes for: an honest durability claim and bounded behavior during object
store brownouts.

Gives up: describing object storage as authoritative for the unobjectified WAL
suffix or allowing unbounded commit progress during an object-store outage.

## D16. Control-plane bootstrap authority

Status: `[DECIDED]` for a bootstrap cell, 2026-08-22.

Decision: each bootstrap cell has a small statically configured coordinator
quorum outside the objectKV data keyspace. It owns cell identity, active
generation, transaction-system and WAL root, root control pointer, and completed
recovery identity. Bulk range state may move into a versioned system keyspace,
but the external root remains sufficient to locate and fence it. A future
metacluster has separate authority and is not required to recover an existing
cell.

Optimizes for: eliminating circular recovery and ensuring stale owners can be
fenced at manifest publication.

Gives up: a storage-only bootstrap and treating control metadata as an
implementation detail that can be placed inside the transaction system later.

Evidence required: RFC-0009 generation recovery, stale-publication simulation,
bounded root-open cost, and coordinator loss/recovery fixtures.

## D17. Object-store support is capability-profiled

Status: `[DECIDED]` for bootstrap, 2026-08-22.

Decision: publish two independent conformance results. The `segment` profile
proves named immutable create, identity reads, exact ranges, corruption
detection, unknown-outcome recovery, and LIST non-authority. The `authority`
profile additionally proves conditional root update, one-winner races, and
lost-update response recovery. A provider or API label is never a support row.

Optimizes for: preventing partial S3 compatibility or a segment-only filesystem
from being mistaken for a safe authority store.

Gives up: one binary supported/unsupported label. Operators must choose an
authority backend separately when a segment backend cannot pass conditional
update.

Evidence: `crates/okv-object`, `evals/suites/object-store.toml`, and
`docs/OBJECT-STORE-SUPPORT.md`.

## D18. Cells bound fleet topology, not intra-tenant transactions

Status: `[PROPOSED]` after architecture correction, 2026-08-22.

Decision: A cell is a complete distributed transaction, durability, storage,
control, and recovery system. A tenant database is the normal transaction
domain, so one bounded transaction may span arbitrary keys and ranges inside
that tenant. Cells have independent versions, generations, logs, and watermarks;
there is no cross-cell transaction. A metacluster owns tenant placement and
migration.

Optimizes for: FDB-like serializable semantics inside a bounded operating and
failure envelope.

Gives up: one global transaction domain and the simpler permanent design of one
sequencer, resolver, or log for every cell.

Evidence required: RFC-0011 review, Cell v0 multi-range serializability, declared
cell capacity/recovery envelopes, and a fenced snapshot-plus-tail tenant move.

## D19. Columnar lag changes cost, not snapshot freshness

Status: `[PROPOSED]` after architecture correction, 2026-08-22.

Decision: A ZebraDB analytical query chooses one target version `T`. Each
partition reads a columnar base through `W_p` and overlays the durable table
change tail `(W_p, T]`. The analytical tail has retention independent of the
short recovery WAL. A DataFusion source must preserve tail keys needed to
invalidate base rows before applying final predicates.

Optimizes for: exact current snapshots over one history while allowing columnar
materialization to lag.

Gives up: treating the analytical watermark as query freshness or pushing every
predicate below the overlay boundary.

Evidence required: RFC-0010 base-plus-tail oracle, predicate-invalidation
negative control, exact multi-table version alignment, and bounded overlay cost.

## D20. Analytical results do not create long OLTP transactions

Status: `[PROPOSED]` after architecture correction, 2026-08-22.

Decision: Invariant-critical aggregates are maintained as transactional
projections. Other analytical results that drive writes return a snapshot and
dependency certificate, then validate in a short transaction. Long planning
workflows produce proposals that revalidate or reserve resources before apply.

Optimizes for: serializable decisions without keeping a transaction open during
long scans or planning.

Gives up: free coordination for broad aggregates. Coarser dependency tokens are
simpler but cause more retries; finer tokens reduce false conflicts but enlarge
certificates and maintenance work.

## D21. Persist stable bytes before choosing a consensus library

Status: `[DECIDED]` for the first durability implementation, 2026-08-22.

Decision: place an opaque checksummed frame and local file persistence seam
under the frozen commit envelope before selecting OpenRaft or `raft-rs`.
Recovery groups identical frames by index and admits only a contiguous quorum.
Consensus metadata, election, replication transport, and generation activation
remain separate protocol layers.

Optimizes for: testing partial writes, file synchronization, quorum
reconstruction, envelope chains, and durable retry outcomes without binding the
kernel to a consensus library before the storage contract is executable.

Gives up: this prototype cannot prove distributed agreement or independent
failure-domain durability. A two-file match is only a local recovery rule until
the consensus and placement layers exist.

## D22. Pin OpenRaft for the bootstrap consensus spike

Status: `[DECIDED]` for the bootstrap spike, 2026-08-22.

Decision: pin OpenRaft `0.9.25` with its `storage-v2` and `serde` features
behind objectKV-owned request bytes, stable-journal framing, and network seams.
Do not use the `0.10` alpha line. The deterministic cluster harness will
disable automatic election and heartbeat timers and trigger those events from
the recorded schedule. The production timing policy remains a separate gate.

Optimizes for: exercising a maintained Raft state machine with pluggable log,
state-machine, network, and runtime boundaries while using OpenRaft's upstream
storage conformance suite as an independent contract check.

Gives up: the lower-level protocol and driver control offered by `raft-rs`.
The choice remains reversible because the `OKVC` commit envelope and `OKVR`
per-node journal are objectKV formats rather than OpenRaft compatibility
promises.

## D23. Reserve digest deletion in transactional authority

Status: `[DECIDED]` for the bootstrap fallback, 2026-08-23.

Decision: immediately before deleting an unreachable digest object, install a
durable per-object deletion reservation in the same serializable authority
transition that revalidates the mark epoch. Publication preparation for an
intersecting object is rejected until deletion is resolved and the reservation
is retired. Backends with native revision-guarded delete still use the exact
object identity; backends without it additionally require the reservation,
immutable digest key, quarantine, and named outcome recovery.

Optimizes for: closing the publication-versus-unguarded-delete TOCTOU window
without requiring every object API to expose conditional delete.

Gives up: a read-only revalidation fast path. Each fallback delete adds two
authority transitions and can temporarily block publication of the same digest.

Evidence: RFC-0007, RFC-0014, candidate `602b317`, and
`object-publication-adapter-v1`.

## D24. Replicate publication state in the cell authority

Status: `[DECIDED]` for the bootstrap cell, 2026-08-23.

Decision: keep publication intents, reader-visible roots, snapshot and query
pins, deletion reservations, request fingerprints, and durable outcomes as a
separate state domain inside the existing OpenRaft generation-authority state
machine. Publication does not receive a second consensus group until measured
authority throughput requires partitioning.

Optimizes for: atomic generation fencing, root compare-and-swap, exact retry
resolution, and one recovery snapshot without adding a cross-consensus commit
boundary.

Gives up: independently scaling publication control throughput during the
bootstrap phase. A homogeneous authority binary upgrade is required because
unknown objectKV command versions fail closed.

Evidence: RFC-0015, candidate `b530321`, clean run `550e5585`, ten discarded
negative subjects, and OTel run `8071bc8a`.

## D25. Recover publishers from replicated intent, not local scratch

Status: `[DECIDED]` for the first object-effect worker gate, 2026-08-23.

Decision: a publisher job and its transition request identities derive from
canonical immutable job bytes. `Prepare` must be quorum committed before the
first object PUT. A replacement publisher starts with empty scratch, replays
the exact authority outcome, verifies the named object closure, and publishes
the root through the same replicated cell authority.

Optimizes for: disposable publisher processes whose correctness state survives
worker loss without a local journal.

Gives up: treating scratch state, PID, wall clock, random identity, or object
existence as transaction-outcome authority. This first gate also defers partial
upload, lost object and `Publish` replies, abandoned-intent policy, and sweep.

Evidence: RFC-0017, candidate `ffc0c84`, clean run `3b5cb41f`, poisoned run
`26bde1fa`, and OTel run `ce7692da`.

## D26. Resolve ambiguous immutable PUTs by exact named identity

Status: `[DECIDED]` for the first partial-effect publisher gate, 2026-08-23.

Decision: a retryable-unknown PUT response proves neither success nor failure.
A replacement publisher reconstructs the same canonical job from the
quorum-durable intent, retries the same immutable name, and accepts an existing
object only after exact length and digest verification. It may request root
publication only after a complete named closure walk succeeds.

Optimizes for: disposable publishers and idempotent object effects without a
worker-local progress journal or object-store LIST authority.

Gives up: a write-only retry path. Ambiguous recovery adds exact named reads and
fails closed on conflicting identity. Lost manifest and authority replies,
multipart residue, abandonment, and sweeper effect fencing remain separate
gates.

Evidence: RFC-0018, candidate `a6dfeed`, clean run `a4a1aec5`, poisoned run
`fa9d729b`, and OTel run `b57f141f`.

## D27. Manifest identity does not prove object closure

Status: `[DECIDED]` for the ambiguous-manifest publisher gate, 2026-08-23.

Decision: an exact immutable manifest authenticates only the manifest bytes. A
replacement publisher recovering an ambiguous manifest PUT must replay and
verify every named data object, verify the manifest itself, and walk the
complete decoded closure before requesting root visibility.

Optimizes for: recovery that remains safe when a manifest exists but one of its
children is missing, corrupt, or was never created.

Gives up: treating one manifest identity read as a transitive proof of child
availability. Recovery performs a complete named walk before `Publish`.

Evidence: RFC-0019, candidate `57e28d4`, clean run `2660e09d`, poisoned run
`7ace2812`, and OTel run `5fd6240e`.

## D28. Current root convergence is not a durable command receipt

Status: `[DECIDED]` for the lost-`Publish`-response gate, 2026-08-23.

Decision: publication authority retains the exact result and command
fingerprint for every accepted `Publish` identity through the declared retry
window. A replacement resolves that outcome before treating the current root as
evidence about the original invocation. Exact retry must return the original
result and applied position without another authority transition.

Optimizes for: acknowledgement-aligned recovery after both the publisher and
accepting authority leader die, including cases where the same root remains
visible but does not identify which invocation produced it.

Gives up: accepting final-state convergence as sufficient recovery evidence.
Retained outcomes consume authority state and require an explicit future expiry
and snapshot-restoration contract.

Evidence: RFC-0020, candidate `72df70c`, clean run `a544deff`, convergence-only
run `82698bdb`, and OTel run `50ad5d86`.

## D29. Treat SlateDB Phase 0 as an incumbent, not a kernel verdict

Status: `[DECIDED]` for the first physical-economics receipt, 2026-08-23.

Decision: use pinned SlateDB over the local filesystem object-store backend to
establish deterministic ingest, read, scan, reopen, request, and byte evidence.
Do not treat this receipt as objectKV semantics or as a Gate 1 pass. Compaction,
remote S3 behavior, GCS, larger datasets, and named cost ceilings remain
separate falsifying experiments.

Optimizes for: obtaining an executable physical incumbent without coupling the
kernel contract to SlateDB internals or waiting for the distributed cell.

Gives up: drawing a product-feasibility conclusion from one local backend. Raw
request totals include expected not-found and conditional-operation probes, so
provider pricing requires classified request semantics rather than one blended
count.

Evidence: RFC-0021, candidate `12df9f8`, clean run `84410878`, warm-cache poison
`e53a01c4`, and OTel run `794c45da`.

## D30. Stop the untuned SlateDB incumbent at dataset-sized reopen

Status: `[DECIDED]` for the current physical incumbent, 2026-08-23.

Decision: supersede the original blended reopen timing with contract version 2,
which measures old-instance close, new-instance open, first correct read, cold
reads, and final close independently. Stop treating unmodified SlateDB defaults
as the objectKV physical incumbent after the repaired 64 MiB run read
210,773,938 bytes during open. Permit one bounded block, index, WAL, and
compaction configuration pass before replacing the incumbent.

Optimizes for: rejecting a recovery layout that scans durable data before the
first read while preserving the useful adaptation and measurement work.

Gives up: promoting SlateDB from the 8 MiB result or interpreting a correctness
`keep` verdict as a physical-economics pass.

Evidence: RFC-0022, candidate `361a0fd`, runs `8419d658`, `b7b18320`, and
`dd55baa9`, plus warm-cache poison `402f095c`.

## D31. Continue through vertical falsifiers, not more role scaffolding

Status: `[DECIDED]` for the next proof cycle, 2026-08-23.

Decision: connect one real transaction from `CommitEnvelope` and OCC through
the three-process Raft log, immutable objectification, `C/O/WAL` frontier
advance, WAL-pop proof, and exact empty-cache read before adding more
FoundationDB-like roles. Treat the PostgreSQL page bridge and ZebraDB HTAP
snapshot source as independent proofs until each has its own authority contract.

Optimizes for: finding a fatal composition or double-authority problem before
expanding the control plane.

Gives up: a broader distributed demo and a near-term claim that the existing
isolated mechanisms constitute a complete cell.

Evidence: `docs/research/overnight-strategy-audit-2026-08-22.md` and the three
pinned architecture, physical, and PostgreSQL/HTAP reviews.

## D32. Put ordered-log algebra below WAL policy without changing WAL bytes

Status: `[DECIDED]` for the first reusable log slice, 2026-08-25.

Decision: `okv-log` owns only deterministic partition-local ordered records,
explicit truncate-plus-append suffix replacement, prefix purge, replay, and
exact versus clamped reads. `okv-wal` retains physical framing, checksums,
filesystem synchronization, vote and committed metadata, quorum policy,
fencing, and acknowledgement. `NodeJournal` validates a cloned state before it
writes, then preserves the existing `OKVR` byte format.

Optimizes for: one reusable semantic waist for recovery logs and future
transactional streams, prefix-closed crash recovery, and byte-compatible WAL
refactoring.

Gives up: one abstraction that claims physical durability, consensus,
application retention, consumer coordination, and object-tier replay. Those
remain separate layers and future proofs.

Evidence: RFC-0024, `docs/LOG-ARCHITECTURE.md`, the 2026-08-25 Fable
cross-examination, frozen accepted and rejected `OKVR` histories, and the
`okv-log` plus `okv-wal` behavioral suites.

## D33. Separate SSD and RAM serving from durable acknowledgement

Status: `[PROPOSED]` after the memory and blob architecture review, 2026-08-25.

Decision: expose `ssd_resident` and `ram_resident` behind one `ServingImage`
contract. SSD uses a bounded disposable RocksDB image; RAM uses a bounded DRAM
image with no data files or swap. Select serving profile by range and keep it
orthogonal to the tenant transaction domain's durability profile. A volatile
memory quorum may report `BUFFERED`, but never `COMMITTED`; synchronous object
acknowledgement and an external durable journal remain explicit alternatives.

Optimizes for: a capacity-efficient SSD default, an optional lowest-latency RAM
profile, profile changes without permanent-byte movement, and a clean memory
versus blob architecture without misrepresenting replicated DRAM as durability.

Gives up: one undifferentiated hot tier. RAM pays more per resident byte and may
lose availability or latency during hydration; SSD spends local I/O maintaining
a disposable LSM. Removing stable media from the entire cell either weakens
acknowledgement, adds object latency, or introduces a separate durable journal.

Evidence required: RFC-0025, `hot-profile-point-v1`, matched RAM-backed and NVMe
RocksDB controls, bidirectional profile transition, pressure poisons, indexed
cold lookup, and a failure matrix that destroys or restarts the volatile quorum.

## D34. Track one golden scenario without optimizing one golden score

Status: `[PROPOSED]` after the cross-surface eval review, 2026-08-25.

Decision: define one `GoldenPathScenario` with a frozen generator, seeds,
architecture surfaces, checkpoint DAG, and artifact handoffs. Each checkpoint
is covered by one or more independent `EvalGate` entries. Each `EvalLane` keeps
one primary metric and hard gates. A verified component receipt does not verify
the golden path unless it carries the same scenario identity and required
artifact digests.

Optimizes for: finding where one logical history stops composing across kernel,
durability, object publication, serving, distribution, application logs, Redis,
search, PostgreSQL, HTAP, and economics while retaining honest per-lane metrics.

Gives up: one campaign score and the convenience of treating unrelated green
component tests as end-to-end evidence. The shared scenario adds artifact and
identity plumbing to every participating runner.

Evidence required: `objectkv-golden-path-v1`, validator poison tests, one
schema-valid receipt per checkpoint with a common scenario identity, and an
independent reconstruction of the artifact chain.

## D35. Use Tetris as the first executable application boundary

Status: `[CODE-COMPLETE]` for the developer playground, 2026-08-25.

Decision: freeze `objectkv-boundary-v0` around snapshot point and range reads,
one mutation transaction, stable request identity, a commit receipt, and one
associated application record. Run the example on the real `okv-model` MVCC
oracle and `okv-log` state machine. Keep the topology single-process and
in-memory until the example teaches us which client boundary should graduate
into the networked kernel.

Optimizes for: rapid application-driven iteration over transactions, MVCC,
ordered key layout, replay, and branching without hiding missing infrastructure
behind a demo service.

Gives up: durability, replication, conflict resolution, RPC, object
publication, and performance evidence. The example must label those omissions
and cannot be cited as a verified database-system result.

Evidence: `examples/okv-tetris/FROZEN-API-v0.md` and the scripted compile,
commit, snapshot, recovery, and branch smoke path. The local browser adapter
renders a minimal playfield and live kernel-proof panel over that same boundary
without a mock storage backend.

## D36. Reject monolithic transaction-authority state

Status: `[CODE-COMPLETE]` for the first Cell v0 split-state prototype,
2026-08-26. Real-process scale evidence remains `[EVALUATING]`.

Decision: do not implement object-frontier safe pop on the current
`StateMachineData` shape. Separate user values, OCC conflict history, retry
outcomes and fingerprints, and recovery commands behind four explicit
reclamation frontiers. The transaction path keeps the same strict-serializable
client contract, but serving state owns values, resolver state expires below the
minimum admitted read version, retry state expires below a declared retry floor,
and the txLog pops only through an authenticated object-durable frontier.

Optimizes for: bounded authority snapshots, independent scaling of serving and
conflict resolution, an honest retry window, and a safe relationship between
objectification and recovery-log reclamation.

Gives up: the implementation simplicity of one replicated state machine owning
the complete database, every conflict record, every retry result, and every
recovery command. The split introduces explicit frontier coordination and more
failure boundaries before the native transaction authority can be admitted.

Evidence: RFC-0028, suite hash `5b456689`, candidate run `93989e1c` at 9.172x,
no-pop control `e0eb9535` at 12.005x, and rejected retained-only poison
`3f64dcd0` at 1.000x with nine accounting anomalies.

## D37. Keep split-frontier state, reject current commit-path speed

Status: `[EVALUATING]` after the G4.6 local real-process diagnostic,
2026-08-26.

Decision: retain the RFC-0029 state split. The replicated frontier command
advances the resolver floor `R` and per-client retry floors `Q(client)` while
the object frontier `O` remains a non-mutating projection. Do not treat the
flat state curve as transaction-path admission. The current sequential,
sync-per-entry OpenRaft process path failed its 180-second execution budget and
requires a separately frozen commit-batching or group-commit experiment.

Optimizes for: state bounded by live keys, admitted transaction age, retry
window, and recovery lag instead of lifetime commits; stale reads and expired
retries fail closed; the next safe-pop protocol has one explicit object-only
frontier to authenticate.

Gives up: unlimited transaction age and retry age. The first split still places
all owners in one snapshot, leaves client-floor cardinality unbounded, and
offers no evidence that the current commit hot path is economically usable.

Evidence: RFC-0029; G4.6 candidate run `ed9c894b` at 1.0029x; object-only
control `3e68f55e` at 9.1694x; rejected serving-only poison `b8e1a531` at
1.0028x with nine anomalies. The candidate took 545.726 seconds, so the result
is not verified.

## D38. Authenticate the object frontier before physical txLog pop

Status: `[CODE-COMPLETE]` for the Cell v0 protocol and local six-process
implementation. Real-process receipts remain `[EVALUATING]`.

Decision: retain one exact immutable manifest as a pending publication root
before the data authority can advance object frontier `O`. The controller must
validate every named manifest, index, and data object before proposing the
physical pop. Publication activation then requires a distinct data-voter quorum
certificate over the exact frontier, generation, membership, and applied log
position.

Optimizes for: bounded recovery-stream state without putting object-store I/O
on the foreground commit path, plus a crash invariant that always retains an
object closure covering the persisted txLog floor.

Gives up: a single-authority transition and automatic cancellation of a stuck
pending frontier. Cell v0 temporarily retains both old and pending closures and
requires a signed cross-authority handshake.

Evidence: RFC-0030; candidate run `f314d6e1` physically popped 16 records,
persisted floor 18, recovered exact object state, and passed both authority
leader failovers plus data-voter restart. Missing-pending run `e3467af2` and
forged-coverage run `7bbf3d7e` rejected before pop. Subquorum run `5bfbd653`
left the pending frontier protected after pop. All four receipts share suite
hash `40b1d296` and remain diagnostic because the source tree was dirty.

## D39. Discard bounded concurrency as the final group-commit mechanism

Status: `[EVALUATING]` after the G4.8 dirty-source local release diagnostic,
2026-08-26. The implementation and instrumentation are `[CODE-COMPLETE]`.

Decision: retain bounded concurrent submission as a load-control and pipelining
primitive, but do not call it objectKV group commit. Advance to an explicit
commit-proxy batch entry that preserves independent request fingerprints,
per-transaction ordered outcomes, exact retries, and deterministic conflict
semantics inside one quorum-durable Raft entry.

Optimizes for: removing the measured leader one-sync-per-transaction bottleneck
without weakening quorum acknowledgement, strict serializability, or retry
recovery.

Gives up: a wire-format-free throughput fix. A batch entry adds a new command
and recovery contract, plus a bounded delay or count policy at the commit proxy.

Evidence: RFC-0031; candidate run `a27fd93c` reached 153.708 median durable
transactions per second and 264.887 ms maximum p99. Sequential control
`8e964ea9` reached 38.772 transactions per second, so the gain was 3.964x.
The candidate missed the frozen 200 transaction per second, 250 ms, and 4x
paired gates. Per-voter trace `candidate-trace-4801.json` recorded one entry per
leader append while both followers grouped 10.667 entries per append. Early-ack
run `69fa1b90` reported 3,400.389 apparent transactions per second but lost the
acknowledged transaction after quorum recovery and was correctly discarded.

## D40. Keep the explicit transaction-batch entry

Status: `[CODE-COMPLETE]` mechanism with `[EVALUATING]` dirty-source local
release receipts, 2026-08-26.

Decision: retain RFC-0032's bounded transaction batch as the Cell v0 commit-
proxy primitive. Transactions in one batch share a scalar snapshot commit
version and receive distinct 16-bit batch orders. Each retains its own request
fingerprint, conflict result, durable outcome, retry path, and recovery record.

Optimizes for: amortizing leader consensus and stable-log synchronization while
preserving independent short-transaction semantics and one exact database
snapshot boundary.

Gives up: one Raft entry per transaction and scalar-version uniqueness. The
ordered transaction versionstamp is now `(commit_version, batch_order)`, and
the retained-stream cursor must carry both components.

Evidence: G4.9 candidate `0f50aeae` reached 559.511 median durable transactions
per second, 34.016 ms maximum p99, and 16 logical transactions per leader
append. Same-durability one-entry control `0a891a4a` reached 151.944
transactions per second, a 3.682x paired gain. Duplicate-identity control
`e12401cb` rejected before mutation. Early-ack poison `13ab1d24` appeared to
reach 13,179.572 transactions per second but lost every acknowledged outcome
after recovered-quorum election and was discarded. All receipts share suite
hash `c34b47eb` and remain diagnostic because the tree was dirty and all voters
shared one host.

## D41. Retain a 32-item bounded commit proxy for the next stress gate

Status: `[CODE-COMPLETE]` mechanism with `[EVALUATING]` dirty-source local
release receipts, 2026-08-26.

Decision: discard the 16-item, 64-caller G4.10a configuration because its
131.488 ms maximum p99 missed the frozen 100 ms ceiling. Retain the distinct
32-item, 64-caller G4.10a.1 configuration as the local Cell v0 candidate for
conflict and object-frontier stress. Thirty-two items is an experiment
envelope, not a stable public limit or an adaptive production policy.

Optimizes for: starting from independent client requests, bounding queue, byte,
and delay growth, amortizing leader synchronization, and returning explicit
backpressure before replication.

Gives up: zero queueing delay and a smaller maximum entry. A fixed 32-item
policy may be wrong under skewed values, conflicts, tenants, or remote media;
those curves remain falsifiers rather than tuning follow-ups.

Evidence: the discarded G4.10a run `1002a622` reached 581.791 median
transactions per second but 131.488 ms maximum p99. G4.10a.1 run `be37cc6b`
reached 1,157.369 transactions per second, 76.101 ms maximum p99, and 32
transactions per leader append. Same-durability one-entry run `cbe29754`
reached 182.093 transactions per second, a 6.356x paired gain. Sparse, byte,
overload, and oversized-request controls passed their scoped gates. All
receipts remain diagnostic because the tree was dirty and all voters shared
one host.

## D42. Emit compact v2 transaction wire while retaining v1 reads

Status: `[CODE-COMPLETE]` format mechanism with `[EVALUATING]` local byte
receipt, 2026-08-26.

Decision: encode opaque key, value, range, and nested payload bytes as unpadded
base64 in `OKVT2`, `OKVQ2`, and `OKVB2`. Continue to decode the corresponding
v1 integer-array formats and derive retry identity from transaction semantics,
not serialized generation bytes.

Optimizes for: removing bootstrap JSON integer-array amplification without
making an unreviewed binary codec the persistent compatibility boundary.

Gives up: zero-copy decode and final wire efficiency. Base64 still adds roughly
one third to opaque bytes and requires a later versioned binary codec if entry
bytes or CPU become the measured bottleneck.

Evidence: v1 and v2 fixtures plus malformed-base64, semantic retry, batch,
recovery, failover, and restart tests. The 128 KiB byte control moved from one
8 KiB-value transaction in an 89,097 byte v1 entry to eight transactions in a
119,731 byte v2 entry without crossing the cap.

## D43. Retain concurrent commit plus authenticated objectification

Status: `[CODE-COMPLETE]` mechanism with `[EVALUATING]` dirty-source local
release receipts, 2026-08-26.

Decision: retain the 32-item native transaction-authority composition after
G4.10b. Freeze object coverage at `O`, keep admitting independent transactions,
and recover final `C` from `ObjectState(O) + txLog(O,C]`. Do not move the
foreground acknowledgement boundary to object storage.

Optimizes for: quorum-latency commits, portable permanent objects, exact
conflict outcomes, and disposal or replacement of serving compute without a
second database truth.

Gives up: immediate production or durability claims. The retained composition
still depends on a fixed local batch policy, one host, local object files, and
an internal OpenRaft journal whose physical byte reclamation is not yet proven.

Evidence: candidate run `2c89ebe1` reached 1,075.343 median resolved outcomes
per second, 104.274 ms maximum p99, 31.030 minimum logical outcomes per leader
append, and 95.673 ms maximum object-frontier time. Same-durability one-entry
run `30471b68` reached 37.369 outcomes per second, a 28.776x paired gain.
No-conflict run `95b5d388` reached 1,093.306 outcomes per second. The 75%
conflict curve and both unsafe controls remained exact. All receipts are local
and inconclusive because the tree was dirty, OTel was disabled, and both
quorums shared one host.

## D44. Require durable state snapshots before physical Raft-log reclamation

Status: `[CODE-COMPLETE]` crash-safe state snapshot, guarded purge, and
canonical node-journal compaction; `[EVALUATING]` local process composition,
2026-08-26.

Decision: freeze RFC-0036 as the next Cell v0 admission gate. A voter may
compact its append-only node journal only after a checksummed state-machine
snapshot durably covers the requested purge position and has reopened exactly.
The first independent-media topology uses three hosts, each collocating one
data voter and one publication voter on separate persistent roots. One host
loss must leave both quorums available. GCS remains asynchronous permanent
state and is not part of foreground acknowledgement.

Optimizes for: proving that repeated object-frontier advancement actually
bounds local recovery media while preserving exact host-loss recovery.

Gives up: treating application-level txLog pop as sufficient reclamation. It
also adds snapshot write amplification and an explicit maintenance protocol
that must be rate-limited against foreground commits.

Evidence: `NodeJournal::compact` constructs one canonical vote, committed
marker, purge marker, and retained suffix; synchronizes a same-directory
replacement; atomically renames it; synchronizes the parent directory; and
ignores a pre-rename stale replacement after authoritative replay. The state
machine writes a checksummed `OKVS` snapshot through synchronized atomic
replacement, validates snapshot metadata against encoded state, and fails
closed on corruption. Process purge rejects any target not covered by the
local durable snapshot.

G4.11a candidate run `7eeaa179` stopped and reopened all three voters with
exact state, retained stream, retry, and new suffix behavior. It reduced at
most 6,391,575 journal bytes to 879 bytes. Poison run `8fb8a75a` rejected purge
before snapshot without moving bytes or markers. Both local receipts remain
`[EVALUATING]` because the source was dirty, OTel was disabled, and the voters
shared one host.

## D45. Reject unfrontiered snapshots as the bounded Cell v0 state shape

Status: `[CODE-COMPLETE]` frontiered process composition with `[EVALUATING]`
dirty-source local receipts; current snapshot encoding discarded, 2026-08-26.

Decision: retain the G4.11a snapshot and journal maintenance protocol, but do
not carry its unfrontiered snapshot shape into G4.11b. Before independent
media, run four real process cycles that align resolver floor `R`, a bounded
64-request retry window `Q(client)`, and authenticated object frontier `O`,
then snapshot and purge through the resulting applied position.

Optimizes for: discovering whether permanent object advancement actually
bounds every local state owner, not only the physical Raft journal.

Gives up: moving directly to three GCP machines after the first successful
restart. It adds one local falsifier because remote topology cannot repair a
lifetime-sized snapshot.

Evidence: G4.11a's journals collapsed to 879 bytes, but the three snapshots
totaled at most 5,066,472 bytes for 131,072 logical workload bytes. The
`storage.amplification` maximum was 38.66082x. The frozen G4.11a.1 suite caps
snapshot plus retained journal amplification at 8x, caps cycle-four snapshot
bytes at 1.25x cycle one, and requires exact expired and retained retry
semantics across a full-quorum restart. Failure requires snapshot-state or
codec redesign before independent-media execution.

G4.11a.1 candidate `be53d36c` aligned `R`, a 64-request `Q(client)` window,
and authenticated `O` across four complete snapshot, purge, compaction, and
full-quorum restart cycles. It preserved exact retry and object-plus-suffix
reconstruction with zero correctness anomalies. Its 1.091759x maximum snapshot
growth passed the 1.25x gate, but 19.692719x maximum complete physical media
missed the 8x gate. No-retry-frontier control `9b236c46` grew to 54.803467x and
2.195933x, proving that `Q(client)` is necessary. Accounting poison `829e35c4`
reported 0.05365x while independent accounting found 19.692719x and rejected
the omission. The aligned frontier mechanism remains; the replicated snapshot
representation is not admitted.

## D46. Evaluate one manifested multi-layout LSM before G4.11b

Status: `[EVALUATING]` architecture fork, 2026-08-26.

Decision: do not redefine objectKV as a Parquet database and do not make
consumers invent the primary point-read path. Before independent-media
execution, compare the current row-object control with a manifested
multi-layout LSM: row-oriented L0 deltas, random-access columnar L1 and lower
runs, one primary-key access path, and one authenticated object closure.

The logical permanent state remains:

```text
ManifestedObjectState(O) + txLog(O, C]
```

The physical encoding may vary by level and typed namespace. Opaque KV ranges
remain eligible for a row format. A typed PostgreSQL or table namespace may use
a columnar compacted base only if its point-read and update curves pass the
same-contract controls.

Optimizes for: one commit history, one object frontier, one branchable object
closure, and a chance to serve both exact primary-key reads and DataFusion
scans without maintaining a separate analytical base.

Gives up: treating one file format as a universal abstraction. It introduces
format-aware compaction, a primary row-address or covering-index lifecycle,
and a larger read-path surface. If point reads require excessive object
requests, bytes, or index RAM, the row transactional base remains the admitted
shape and columnar files stay derived.

Evidence: G4.11a.1 bounded lifetime growth but failed complete-media economics.
Apache Paimon demonstrates primary-key LSM tables over columnar objects, while
its PFile proposal documents the scaling cost of converting columnar files for
KV lookup. RisingWave retains row-based Hummock for point and update workloads.
Lance and Vortex provide random-access columnar mechanisms worth measuring.
The owning research note is
`docs/research/columnar-lsm-source-of-truth-2026-08-26.md`.

The first `[EVALUATING]` 1,024-key local preflight kept exact semantics across
the indexed row, indexed Parquet, and hybrid subjects. Parquet reached 1.873x
the row control's projected scan rate, but its full-row point path used 10
requests and 16.35x the response bytes per operation. The hybrid used four
requests and 1.925x the row control's stored/live amplification. This rejects
plain Parquet as the generic point path, but does not yet decide Vortex,
coalesced range reads, a typed sidecar design, the frozen full profile, or GCS.

## D47. Admit the split typed run to GCS evaluation, not to the kernel default

Status: `[EVALUATING]` mechanism admitted locally, 2026-08-26.

Decision: keep the indexed row object as the default representation for opaque
KV ranges. Advance one typed-run subject to clean-source GCS evaluation. That
subject stores the complete MVCC value once in an indexed row sidecar and
stores only declared analytical fields in a columnar projection. One active
manifest authenticates both access paths as one object closure.

Optimizes for: row-control point requests and bytes, typed projected scans,
complete media accounting, and one branchable history without an external ETL
copy.

Gives up: the claim that one purely columnar file is sufficient for every
access pattern. Typed fields are duplicated, compaction must publish both
representations atomically, and the nested closure increases index and
manifest state.

Evidence: release-local run `f5dbba62-0f47-46af-8bb7-d1f7efa6a353`
alternated the candidate and row control across three seeds and three repeats.
It returned a 1.000x point-request ratio, 1.000x point-byte ratio, 1.033x median
point-p99 ratio, 9.124x projected-scan throughput, 1.030x storage-amplification
ratio, 1.035x compaction-write ratio, and 1.137x resident-index ratio. Every
frozen local gate passed. The source was dirty and the backend was local, so
the result remains `[EVALUATING]`.

Next decision boundary: admit the typed run only after clean-source GCS cold
and warm curves, exact split-closure recovery, and DataFusion base-plus-tail
exactness. Namespaced GCS execution and its frozen suite are `[CODE-COMPLETE]`;
the objectKV-dev project and bucket execute bounded canaries, but the full
alternating storage-layout suite remains `[EVALUATING]` until bounded parallel
scheduling and OTel replace the serial request path. A GCS failure retains the
row base and demotes the projection to a derived analytical artifact.

## D48. Publish lane-specific comparisons, not a stack headline

Status: `[CODE-COMPLETE]` comparison contract, 2026-08-26.

Decision: every performance or economics claim names one program gate, paired
control, primary metric, direction, practical threshold, and comparison scope.
Reject a comparison when hardware, build, seed, metric, hard-gate, or sample
identity does not match. Direct RocksDB is a serving-mechanism control. It is
not a durability-equivalent replacement for a TiKV solution control.

Optimizes for: percentages that retain their semantic meaning and can be
reproduced by another contributor.

Gives up: one early score that claims objectKV is globally faster or cheaper
than an incumbent. The project will carry several curves until a complete
matched solution stack is runnable.

Evidence boundary: `okv-eval compare-results` emits the frozen
`comparison.schema.json` receipt and requires at least five samples for a
performance verdict. No cross-stack result is `[VERIFIED]` yet.

## D49. Prove the single-runner object and serving curves before R1

Status: `[CODE-COMPLETE]` infrastructure contract with `[EVALUATING]` live
execution, 2026-08-26.

Decision: begin real-infrastructure work with one private, fixed-shape GCP
runner and a separate OTel collector. Run candidate and control sequentially on
the same machine. Add the three-zone transaction topology only after the R0
curves reproduce.

Optimizes for: finding object request, read amplification, cache, recovery, and
columnar-layout failures at the lowest cost and smallest operational surface.

Gives up: R0 cannot prove quorum latency, voter-failure availability, or
independent failure domains. Those remain R1 gates, not inferred properties.

The owning runbook and failure matrix are `docs/REAL-INFRA-EVALS.md`.

## D50. Put disposable serving images behind the public range

Status: `[CODE-COMPLETE]` contract with `[EVALUATING]` performance, 2026-08-27.

Decision: put one provider-neutral `ServingImage` activation and point-read
boundary below `SingleRange`. Integrate RocksDB on bounded disposable local
media first, then implement the RAM profile against the same contract. Keep
durability profile, serving profile, and immutable object layout as separate
configuration axes.

Optimizes for: measuring the actual public kernel without making RocksDB part of
the permanent format or forcing the RAM profile through an SSD abstraction.

Gives up: partial admission, range iteration, incremental tail application, and
profile handoff in the first interface. These remain separate gates rather than
speculative methods.

Evidence required: RFC-0039, a public `SingleRange` candidate and direct RocksDB
control in optimized separate processes, zero object operations after complete
activation, bounded local bytes, exact reads, required poisons, and a provider
device receipt before using the word NVMe in a performance claim.

Current evidence: dirty debug run `56535944` passed exact reconstruction,
worker replacement, bounded activation, 100,000 public point reads, and zero
post-activation object operations. It reached 824,252 reads/s and 1,583 ns p99
on a local arm64 filesystem. This is `[EVALUATING]`; it has no optimized build,
ABBA sample set, OTel export, or isolated provider-runner receipt. The local
scratch volume is backed by a named Apple SSD AP1024Z NVMe device.

## D51. Move resident correctness to transitions, not every point lookup

Status: `[VERIFIED]` bounded experiment completed, 2026-08-27.

Decision: stop incremental optimization of the current `SingleRange` resident
read wrapper. Materialize the authoritative object base plus visible txLog
suffix into a native resident engine. Verify generation, coverage, closure, and
frontier at activation or transition boundaries, then let the engine own the
steady-state point lookup. Reuse `okv-log`, publication, reconstruction,
branching, and historical views across resident engines.

Optimizes for: direct-engine p99, one materialized resident state, and a clean
separation between lifecycle correctness and the steady-state read data plane.

Gives up: a provider-neutral external overlay on every point read. Each
resident engine needs an explicit MVCC encoding, suffix-application, frontier,
and crash-recovery contract. A direct engine path is admitted only after it
returns the same exact versions and survives empty-worker reconstruction.

Evidence: optimization run 1 moved complete-image access ahead of manifest
location. Candidate throughput improved 11.18 percent from the prior clean
run. AB retained 80.68 percent of direct RocksDB throughput and BA retained
80.02 percent, both inside the frozen 20 percent envelope. P99 remained 1.353x
and 1.300x control, failing the executable limit in both orders. All mechanism,
identity, and OTel gates passed. See
`docs/artifacts/eval-receipts/single-range-ssd-gcp-r1-2026-08-27/README.md`.

Result: the native engine passed exact replay, snapshot, generation, local-byte,
and zero-object-read checks. It retained 84.11 and 82.68 percent of owned-value
direct RocksDB throughput, but p99 was 1.210x and 1.272x control. Both process
orders failed the frozen 1.20x p99 ceiling. See
`docs/artifacts/eval-receipts/single-range-native-resident-gcp-r2-2026-08-27/README.md`.

## D52. Stop owning the resident transaction plane

Status: `[VERIFIED]` decision trigger with `[EVALUATING]` provider selection,
2026-08-27.

Decision: stop expanding the custom RocksDB resident engine into a distributed
transaction system. Preserve it as an executable correctness prototype. Put an
incumbent TiKV or FoundationDB plane below objectKV's retained log and object
lifecycle, then select between them with a bounded adapter and matched-infra
evaluation. Do not advance GP3.2 RAM, MultiRaft, PostgreSQL, or HTAP performance
before that selection.

```text
PostgreSQL | Redis | search | virtual filesystem | DataFusion
                            |
            objectKV version and lifecycle contract
       okv-log | okv-wal | publication | branch | rebuild
                            |
              TiKV or FoundationDB resident plane
                            |
                     RAM/NVMe serving state
```

Optimizes for: reaching the object-native product thesis without rebuilding
Raft storage, MVCC, range scheduling, compaction, backup, and production failure
handling before objectKV has demonstrated lifecycle leverage.

Gives up: a fully objectKV-owned transaction kernel and the ability to tune its
steady-state local read path below the incumbent API. The retained object layer
must prove value through open history, cheap branches, empty-worker recovery,
independent compute, exact DataFusion snapshots, or economics.

Evidence: the original wrapper appeared to carry a 30 to 35 percent p99 tax
against a pinned-slice control. Correcting the control to owned 1 KiB values
reduced that diagnostic ratio to 1.092x, showing that ownership semantics were
a material benchmark confounder. The separate native implementation still
failed the corrected p99 gate in both final process orders. Its throughput
passed, correctness anomalies were zero, and all four final run IDs appear in
OTel logs, metrics, and traces.

Next decision boundary: freeze the minimal plane interface, implement the same
single-range objectification and rebuild adapter against TiKV and FoundationDB,
and compare operational fit before selecting one. This is a provider choice,
not authorization to rebuild either system inside objectKV.

## D53. Advance FoundationDB alone through the lifecycle gates

Status: `[VERIFIED]` semantic elimination and logical lifecycle with
`[EVALUATING]` FoundationDB admission, 2026-08-27.

Decision: remove TiKV from the objectKV lifecycle implementation branch. TiKV
remains a useful alternative-stack reference, but objectKV will not add a
resolver, predicate-lock service, or certifier above TiKV to turn snapshot
isolation into the strict-serializable P1 contract. Advance FoundationDB alone
through logical reconstruction, provider-media-loss recovery, and matched
hot-path overhead. Do not call FoundationDB selected before those gates pass.

```text
TiKV live write skew: both commit -> reject for P1
FoundationDB live write skew: one conflicts -> advance
  -> logical object lifecycle
  -> provider-media-loss reconstruction
  -> provider-incarnation authority
  -> retained-write overhead vs direct FoundationDB
  -> provider admission or stop
```

Optimizes for: preserving the strict-serializable kernel contract without
rebuilding distributed transaction validation inside objectKV.

Gives up: TiKV's direct RocksDB and MultiRaft data path as the implementation
base. A future TiKV integration would require an explicitly weaker transaction
product or a separately justified serializable layer.

Evidence: FoundationDB 7.4.6 passed five live semantic gates with zero
anomalies. TiKV 8.5.7 committed both writers in the frozen write-skew history.
The frozen FoundationDB plus GCS logical-lifecycle batch reconstructed 950 rows
into an empty generation, replayed five chunks idempotently, matched the state
digest, fenced the old generation, and discarded all three poisons. Candidate
`ca9195186c4bd85573dddfe2d63a376693a031e9` and its private GCP machine receipt
produced complete logs, metrics, and traces. GP2.5.2 is `[VERIFIED]` for this
logical scope. GP2.5.3 then reconstructed the same logical state on a fresh
provider after the source VM and both source disks were observed absent.
GP2.5.4 incarnation authority and GP3.1 overhead still gate provider admission.

## D54. Separate provider-media loss from provider-incarnation fencing

Status: `[VERIFIED]` GP2.5.3 provider-media-loss mechanism and GP2.5.4 local
compound-fence processes; `[EVALUATING]` GP2.5.4 real-provider composition,
2026-08-27.

Decision: GP2.5.3 uses two distinct FoundationDB cluster and disk identities.
An external controller observes the source instance, boot disk, and provider
data disk absent before the first write to the fresh destination cluster. A
same-cluster restore while the source media remains reachable is the executed
poison. This gate proves object-closure sufficiency after media loss only.

Add GP2.5.4 for provider-incarnation authority. A source process being deleted
cannot prove that a later-resurrected source identity is fenced. The current
logical generation key resides inside FoundationDB and therefore cannot govern
two separate clusters after one cluster is lost.

```text
GP2.5.3: source disk gone -> exact fresh-cluster restore
GP2.5.4: old cluster returns -> cannot acknowledge, route, or publish
```

Optimizes for: one falsifiable correctness claim per gate and no promotion of a
namespace-reset result into a distributed-fencing result.

Gives up: treating physical deletion as complete HA evidence. An external cell
incarnation authority remains required by RFC-0009 before provider admission.

Evidence: candidate `50c72159781e14d3db06d792beac34838572fc91`
reconstructed 950 exact records on a fresh FoundationDB cluster after the
source VM, boot disk, and provider SSD were observed absent. All 16 formal
positive gates passed, the same-cluster poison was discarded, and both run IDs
occur in OTel logs, metrics, and traces. Contract and receipts:
`docs/research/provider-media-loss-gp2.5.3.md` and
`docs/artifacts/eval-receipts/provider-media-loss-r0-2026-08-27/README.md`.

## D55. Compose external incarnation authority with a provider-local fence

Status: `[VERIFIED]` local process mechanism; `[CODE-COMPLETE]` dual-provider
GCP harness; `[EVALUATING]` real FoundationDB resurrection, 2026-08-27.

Decision: GP2.5.4 uses a compound fence. The external OpenRaft authority owns
the active incarnation, routing, and reader-visible object frontier. Before a
destination may activate, the source FoundationDB provider receives a
transactionally visible fence that every objectKV commit reads. The R0 policy
does not put an external coordinator call on every FoundationDB commit.

```text
external Prepare(G2)
  -> source FoundationDB fence transaction
  -> exact destination reconstruction and ready digest
  -> external Activate(G2), bound to source-fence receipt digest
  -> destination activation
  -> restart source with the same disks
  -> source resurrection consumes activation and restart receipt digests
  -> reject source commit, route, and publication
```

Optimizes for: keeping the incumbent transaction hot path local while moving
cross-provider identity, routing, and publication authority outside both
provider clusters.

Gives up: automatic protection from a source disk image rolled back to before
the provider-local fence. That case requires a current route lease or
per-commit external authorization. The latter must clear GP3.1 before adoption
because it changes the reason to use FoundationDB.

Evidence: clean candidate `b415d502665eff9b6df4c095e33480b628348db2`
received `keep` with zero anomalies and exact fresh-process replay. Its
stale-source control received `discard` with exactly three anomalies across
commit, route, and publication. Both run IDs occur in captured OTel logs,
metrics, and traces. The simultaneous source and destination Terraform shape,
phase-separated FoundationDB probe, strict receipt schema, and controller are
code complete. Cross-provider order uses hash-bound receipt dependencies rather
than source and destination wall-clock comparisons. Real GCP execution remains
required. Contract and local receipt:
`docs/research/provider-incarnation-gp2.5.4.md` and
`docs/artifacts/eval-receipts/provider-incarnation-local-r0-2026-08-27/README.md`.

## D56. Keep objectKV native-first and expose applications through okv-fabric

Status: `[VERIFIED]` single-range native read boundary through 32 clients;
`[EVALUATING]` native transaction-plane admission, 2026-08-27.

Decision: objectKV is the fixed product program. Evaluations select mechanisms,
serving profiles, and transaction-plane implementations; they do not decide
whether the project continues. Reopen the objectKV-native RocksDB and OpenRaft
transaction plane as the primary bounded research lane. Keep FoundationDB as a
strict-serializability oracle, matched comparison, and fallback transaction
profile while the native lane earns admission through explicit gates.

Applications and data platforms integrate through `okv-fabric`, the unified
API above the value-native kernel:

```text
PostgreSQL | Redis | search | filesystem | DataFusion | applications
                              |
                         okv-fabric
       transactions | KV | log/WAL | snapshots | branches | projections
                              |
                        objectKV kernel
       native transaction plane | serving images | object publication
                              |
                  immutable S3-compatible state
```

The topology-matched GP3.1 rerun admits the single-range native snapshot
boundary, but it does not show that the distributed system is nearly complete.
Native retained 0.9089x and 0.9197x direct RocksDB throughput in opposite
process orders. Its p99 was 0.9134x and 0.9132x control. Both frozen constraints
passed twice. GP3.1.1 then retained 0.8734x through 0.8906x direct RocksDB
throughput and kept p99 between 1.1072x and 1.1842x control at 8 and 32 clients
in both process orders. The next native sequence separates three claims:

1. measure CPU-per-read and cache-pressure curves without weakening the
   admitted single-range semantics;
2. measure one three-node replicated commit path against a same-durability
   control;
3. prove strict serializability, failover, object-frontier safety, and empty
   recovery before adding range splitting or cross-range commit.

Optimizes for: full control of the hot path, a Rust-native kernel, portable
object state, and one application fabric that can expose transactional,
log-oriented, branch, and analytical abstractions without an incumbent database
becoming the product boundary.

Gives up: the shorter route to production maturity offered by making
FoundationDB the default plane. Consensus operations, MVCC, resolver scaling,
range placement, repair, and production failure handling remain objectKV work
and must be admitted one bounded claim at a time.

Evidence: RFC-0040 and the topology-matched GP3.1 AB/BA receipt establish the
current native performance boundary. RFC-0041 and the FoundationDB receipts
remain the semantic and lifecycle controls. Neither set of receipts verifies a
complete native distributed cell. See
`docs/artifacts/eval-receipts/single-range-native-matched-gcp-r0-2026-08-27/README.md`
and
`docs/artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/README.md`.

## D57. Separate concurrent-read admission from cache-pressure admission

Status: `[VERIFIED]` concurrent runner and GCP R0 receipt, 2026-08-27.

Decision: GP3.1.1 changes only concurrent point-read clients at the admitted
single-range native snapshot boundary. It runs native and matched direct
RocksDB at 1, 8, and 32 clients. The operation budget is total across clients,
clients enter one synchronized window, and receipt percentiles merge every
measured operation.

Cache budget, larger-than-cache fixtures, CPU time, and RocksDB read
amplification remain a separate next gate. They do not enter the first
concurrency receipt.

The clean GCP R0 receipt executed 24,000,000 measured reads across 120 samples.
At 8 clients, native retained 0.8798x and 0.8734x control throughput; p99 was
1.1842x and 1.1220x control. At 32 clients, throughput was 0.8803x and 0.8906x
control; p99 was 1.1072x and 1.1478x control. Every comparison constraint
passed in both process orders. All 128 workload gates passed, all hot-read
windows issued zero object operations, and every run ID appears in OTel logs,
metrics, and traces.

Optimizes for: attributing a regression to parallel reader contention before
changing cache policy or working-set size.

Gives up: one combined concurrency and eviction result. The verified result is
limited to one 4 MiB resident working set; it cannot be generalized to cache
pressure.

Evidence: RFC-0042 and
`docs/artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/README.md`.

## D58. Sequence performance admission from storage geometry upward

Status: `[PROPOSED]`, 2026-08-27.

Decision: light workload metrics in dependency order. After the verified
resident read curve, run cache pressure, GCS cold-point and physical-layout
geometry, independent-media replicated commit, objectification and recovery
bounds, metadata branching and lazy reopen, multi-range scale, the optional RAM
profile, `okv-fabric` workloads, PostgreSQL OLTP, exact DataFusion HTAP, and
complete-stack economics. Every workload keeps its own primary metric,
specialist control, correctness gates, and receipt.

```text
resident read
  -> cache and object geometry
  -> replicated commit and bounded recovery
  -> branch and multi-range cell
  -> optional RAM
  -> application surfaces
  -> PostgreSQL and HTAP
  -> comparative production envelope
```

Optimizes for: exposing storage and transaction bottlenecks before consumer
work hides them, preserving lane-specific comparisons, and generating the most
architectural learning before expensive distributed or compatibility work.

Gives up: building the most visible application surface next. Redis,
PostgreSQL, and HTAP performance work waits for the kernel boundary it is meant
to measure. RAM remains an optional optimization and does not delay the
SSD-backed cell.

A missed curve causes a mechanism or provider-profile redesign and a new
receipt. It does not stop the objectKV program. The canonical matrix and task
sequence are `docs/BOOTSTRAP-PLAN.md` and T27 through T37 in
`docs/CONTRIBUTOR-BOARD.md`.

## D59. Hold T27 at the native CPU boundary before expanding the curve

Status: `[VERIFIED]` calibration execution and negative result, 2026-08-28.

Decision: do not start the 1 GiB cache-pressure admission or move the serving
claim upward after the first 64 MiB calibration. Profile and optimize the
current native version-bound point-read path, persist one content-addressed
fixture across all four subjects, make CPU and physical-byte comparisons
executable, and rerun the unchanged AB and BA calibration first.

The clean GCP R0 run executed 60 million measured reads. All 84 workload hard
gates passed, each of the four run IDs appeared in OTel logs, metrics, and
traces, and cleanup completed. Native retained 0.5968x and 0.5659x direct
RocksDB throughput; p99 was 1.3312x and 1.5567x control. Both formal
comparisons returned `worse`. Native CPU time was 1.6685x and 1.7460x control,
while peak RSS was effectively equal, native cache hit ratio was slightly
higher, and Linux reported zero physical read bytes for every subject.

This rejects the current read composition at the calibration point. It does
not reject object-backed serving or reverse D56, because no measured read
reached an object API or physical NVMe. The evidence localizes the immediate
cost above physical media. It also shows that a 2x fixture with Zipf 1.4 is not
an isolated NVMe curve on this host, because block-cache hits exceeded 99.4
percent and the operating-system page cache satisfied the remaining physical
path.

Optimizes for: removing a measured CPU tax before larger fixtures make each
iteration expensive, preserving the frozen comparison, and separating native
software overhead from local-media behavior.

Gives up: immediate progress into GCS refill, replicated commit, and the 1 GiB
admission. Those layers remain sequenced behind a passing local serving
boundary.

Evidence: RFC-0043 and
`docs/artifacts/eval-receipts/native-resident-cache-pressure-gcp-r0-2026-08-28/README.md`.

## D60. Admit the corrected calibration and retain the broader T27 gate

Status: `[VERIFIED]` decision, 2026-08-28.

Decision: mark GP3.1.2 `[VERIFIED]` after the corrected 64 MiB calibration
cleared its throughput, p99, CPU/read, semantic, telemetry, and zero
physical-read constraints in both process orders. Keep T27 and master-matrix
row 1 `[EVALUATING]` until the frozen cache-coverage and skew sweep executes
with an explicit operating-system page-cache treatment.

The owning defect was a forced flush after every disposable tail advance.
Activation created a base SST, the advance created a second SST, and untouched
latest reads probed both. Keeping recent tail state mutable reduced the worst
CPU/read ratio from 1.7460x to 1.0586x control and raised the minimum throughput
ratio from 0.5659x to 0.9432x. The corrected A/B and B/A run executed 60 million
reads; all 84 workload gates and eight explicit comparison constraints passed.
Every run ID occurred in OTel logs, metrics, and traces.

The top-level comparator verdict is `inconclusive` because its primary verdict
asks whether either subject is at least 20 percent faster. That does not fail
this non-inferiority gate. The gate's four explicit constraints define
admission and passed in both orders. A later evaluator change should represent
improvement and non-inferiority objectives separately instead of requiring a
manual interpretation.

Optimizes for: retaining the native version-bound point-read path only after it
stays close to the owned-value RocksDB control and preserving a strict status
boundary between one passing calibration and a complete cache-pressure curve.

Gives up: claiming a RocksDB speedup or an isolated NVMe result. Linux reported
zero physical read bytes for every subject, so the next T27 slice must control
the operating-system page cache and reuse one fixture across all subjects
before the 1 GiB sweep.

Evidence:
`docs/artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/README.md`.

## D61. Use direct table reads as an evaluator treatment, not the portable default

Status: `[VERIFIED]` mechanism decision, 2026-08-28.

Decision: retain buffered RocksDB reads as the portable product default. Use
matched direct table reads when an experiment must isolate physical NVMe work
from the operating-system page cache. Candidate and control must receive and
report the same setting, and a profile mismatch invalidates the result.

One clean GCP R0 smoke used 16 MiB of values, a 4 MiB block cache, Zipf 0.8,
eight clients, and 100,000 reads per subject. Native and control both passed 22
of 22 hard gates. Linux reported 2,960.75 and 2,966.00 physical bytes per read,
a 0.9982x ratio, where the earlier buffered calibration reported zero. This
verifies that the mechanism exposes physical media and preserves matched
configuration. One sample does not admit throughput, latency, or amplification.

Optimizes for: attributable device work and a direct answer to whether native
serving performs extra physical I/O relative to owned-value RocksDB.

Gives up: use of the operating-system page cache, identical behavior across all
provider filesystems, and the right to treat this smoke result as the product's
default read profile.

Evidence:
`docs/artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/README.md`.

## D62. Bootstrap evaluation authorities at the object frontier

Status: `[EVALUATING]`, with phases 0 through 5 and the 64 MiB preflight
`[VERIFIED]`, 2026-08-29.

Decision: build one content-addressed logical object fixture, establish its
covered-through version with one canonical empty transaction on each fresh
evaluation authority, and put only the suffix `(O, C]` through the transaction
plane. Native and control derive separate resident-image identities from the
same fixture because their physical codecs differ. They do not share a mutable
RocksDB directory.

The authority truthfully retains one empty anchor record until a separately
authenticated object-frontier pop. It must retain zero base-value records and
zero base mutation bytes. Candidate and control must also consume one exact
retained-tail digest; a logically similar suffix at different commit versions
is not a matched fixture.

The first 64 MiB T27 run replicated every base value before publishing the
same values to objects. Authority scratch reached about 1.2 GiB, roughly 19
times logical value bytes, and setup took 35 to 40 minutes per subject. That is
an ingest workload, not a resident read fixture. Ingest and objectification
economics remain load-bearing in T30.

Optimizes for: an honest object-base plus txLog-suffix boundary, practical
1 GiB cache experiments, and one logical identity across candidate, control,
and both process orders.

Gives up: treating T27 fixture load as write-path evidence. The empty anchor is
evaluator-only and does not authorize production import or restore from an
arbitrary object closure.

Evidence: RFC-0044,
`docs/research/reviews/fable-object-frontier-fixture-review-2026-08-28.md`, and
the 64 MiB setup measurements in
`docs/artifacts/eval-receipts/native-resident-cache-pressure-gcp-r0-2026-08-28/README.md`.
The phase-0 candidate established `O=2` across 20 fresh authorities and the
changed-identity bypass poison was detected. Its clean GCP receipt is in
`docs/artifacts/eval-receipts/object-fixture-anchor-gcp-r0-2026-08-28/README.md`.
Phase 1 reconstructed a 4 MiB logical base from 11 content-addressed objects,
kept all base values out of txLog, bound one exact seven-record suffix, and
proved distinct semantic native/control image identities over one equal
complete logical image. The candidate and four poisons passed from clean
source. Its receipt is in
`docs/artifacts/eval-receipts/object-fixture-contract-gcp-r0-2026-08-28/README.md`.
Phase 2 started independent empty native and direct-control processes from that
fixture and tail. Their actual physical image IDs differ, their complete
logical digest is equal, and both formal receipts passed, including the
regenerated-control poison. Phases 4 and 5 then verified persisted GCS reuse,
generation-pinned cross-invocation locators, standalone direct construction,
read-only consumption, and independent fixture and trace seeds. The 64 MiB
fresh-process preflight passed both order comparisons with direct NVMe reads
and collector-side telemetry. The exact preflight plan and position evidence
then passed the five required negative controls. The 1 GiB performance curve
remains open. Evidence is in
`docs/artifacts/eval-receipts/object-fixture-resident-process-gcp-r0-2026-08-28/README.md`.

## D63. Admit the 64 MiB fresh-process preflight without promoting T27

Status: `[VERIFIED]` decision, 2026-08-29.

Decision: accept the immutable 64 MiB direct-NVMe ABBA result as the T27
preflight. Keep T27 and master-matrix row 1 `[EVALUATING]` until the remaining
capability and schedule poisons pass and the frozen 1 GiB coverage plus skew
sweep executes.

Native retained 0.8652x and 0.9739x matched direct RocksDB throughput in the
two process orders. P99 was 1.0048x and 0.9882x; CPU/read was 1.0718x and
0.9797x; physical bytes/read were 1.0647x and 1.0638x; read amplification was
1.0000x. Every position used a fresh process, an empty NVMe scratch directory,
one explicit RocksDB cache, one exact generation-pinned fixture, and
object-viewer credentials. All comparison and cache-pressure gates passed.
The sealed run records successful flush and shutdown for logs, metrics, and
traces, and the collector independently contains the run ID in all three.

The first fixture used base version 1 and reached the independent oracle before
failing its required version-2 anchor. The plan boundary now rejects that
locator before oracle construction. A valid write attempt under the measured
object-viewer principal also failed with permission denied and created no
objects. The rejected fixture was removed after preserving its locator and
failure.

Optimizes for: proving the complete measurement chain on a bounded dataset
before paying for 540 one-GiB positions, while preserving fresh-process,
credential, machine, NVMe, and telemetry identities.

Gives up: treating four short 1,024-read windows as a stable performance curve.
This result does not admit cache coverage, skew, sustained tail latency, or
the broader RangeEngine claim.

Evidence:
`docs/artifacts/eval-receipts/t27-fresh-process-preflight-gcp-r0-2026-08-29/README.md`.

## D64. Seal plan poisons as portable decoder receipts

Status: `[VERIFIED]` mechanism and exact preflight replay, 2026-08-29.

Decision: negative controls for AABB ordering, missing positions, effective
option drift, and hidden direct-position providers are first-class T27
artifacts. `t27-plan-poison-check`
authenticates a valid source plan, applies exactly one controlled corruption,
recomputes the poisoned plan digest, and invokes the same production decoder
used by the controller. It seals both plan digests, the exact poisoned file
digest, the expected rejection, the observed rejection, and its own receipt
digest under a JSON schema. `t27-position-poison-check` does the same for one
authenticated direct-position receipt while changing only its hidden-provider
inventory field.

Optimizes for: replayable negative evidence that crosses machines and evaluator
invocations, while proving the rejection is structural rather than a stale or
obviously invalid digest.

Gives up: treating unit tests as sufficient evidence. The commands passed
against the frozen GCP plan and one real direct-position receipt at source
`9ca447d`. The missing-locator process produced no output plan and left the
versioned fixture manifest unchanged. This admits the negative-control
mechanism, not T27's 1 GiB performance curve.

Evidence:
`docs/artifacts/eval-receipts/t27-preflight-poisons-r0-2026-08-29/README.md`.

## D65. Evaluate a staged quorum txLog before changing transaction agreement

Status: `[PROPOSED]`, 2026-08-30.

Decision: preserve `okv-log` as pure ordered-record algebra and evaluate a
single-writer, client-driven quorum service inside `okv-wal`. The candidate
stages a bounded tail in RAM, persists `COMMITTED` appends to local NVMe on a
declared quorum, and publishes complete immutable segments to object storage
asynchronously. `quorum_ram` may return only `BUFFERED`.

Do not insert this service beside the current OpenRaft transaction log and call
the double write an architecture. First prove the standalone protocol and
performance curve. Then use T29 to choose whether it replaces only per-node
stable storage or supports a later FoundationDB-shaped commit-proxy, resolver,
and txLog plane.

Optimizes for: a reusable cloud WAL primitive, one writer-to-quorum network
round trip, bounded fast media, open immutable history, and an explicit path
from `okv-log` to `okv-wal` without putting object latency on every commit.

Gives up: treating physical presence as transaction commit, multi-writer order
inside one log stream, or assuming the BtrLog prototype validates objectKV's
transaction plane. Writer fencing, unknown-outcome recovery, cross-stream
ordering, and transaction integration remain separate gates.

Evidence contract: RFC-0045 and `evals/suites/staged-txlog.toml`. `[VERIFIED]`
L0 deterministic protocol semantics now cover quorum acknowledgement, writer
epochs, exact retries, suffix repair, committed segment visibility,
manifest-only reads, and bounded queues. `[VERIFIED]` L1 runs three real
log-node processes over TCP and proves synchronized local journals, exact
retries, restart and torn-tail recovery, stale-writer fencing, and deterministic
segment bytes across three seeds and three process poisons. Independent media,
object publication, transaction integration, and performance rungs remain
`[PROPOSED]`.
