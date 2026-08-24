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

Status: `[ACTIVE-WORK]`.

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

Status: `[ACTIVE-WORK]` after the first independent review.

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

## D32. Centralize the first complete transaction authority

Status: `[DECIDED]` for the Cell v0 vertical proof, 2026-08-23.

Decision: implement the first complete transaction path as one deterministic
semantic state machine behind the existing three-process OpenRaft group. It
orders requests, checks OCC conflict ranges, assigns commit versions from the
applied Raft position, applies atomic multi-key mutations, emits the canonical
commit envelope, and retains exact outcomes. Keep the read-version, proxy, and
resolver interfaces explicit, but do not partition them before the complete
objectification and empty-cache recovery path works.

Optimizes for: proving one serializable, recoverable, end-to-end Cell v0 before
introducing distributed conflict-resolution and log-routing protocols.

Gives up: horizontal transaction throughput in the first cell. Duplicate and
rejected requests may consume Raft entries until the commit proxy gains a safe
pre-append retained-outcome check.

Evidence: `experiments/semantic-raft-prototype/NOTES.md`; seeds `1103`, `2207`,
and `3301` each passed 11 checks with zero anomalies and converged all three
processes at applied index 8.

## D33. Durable authority snapshot precedes WAL pop

Status: `[DECIDED]` for the next recovery gate, 2026-08-23.

Decision: never advance the reclaimable Raft-log position from object-data
publication alone. WAL pop requires both a verified immutable data closure
through `O_cell` and a durable state-machine snapshot that binds its exact
applied log position, generation, membership, tenant state, conflict history,
publication state, fingerprints, and retained retry outcomes. A restarted
voter must restore that snapshot plus the retained suffix before the pop is
admitted.

Optimizes for: exact recovery of both user data and transaction/control
authority after log reclamation.

Gives up: early WAL reclamation while process recovery still depends on replay
from the first retained entry. Snapshot size, outcome retention, and install
latency become explicit costs.

Evidence: the `purge_without_durable_snapshot` process control fails closed
after all three journals are purged; at the decision point,
`ProcessNodeConfig` used `SnapshotPolicy::Never` and state-machine snapshots
were not persisted.

## D34. Persist complete Cell v0 authority before reclaiming its log

Status: `[DECIDED]` for the bounded process prototype, 2026-08-23.

Decision: encode the complete centralized state machine in a versioned `OKVS`
snapshot frame, synchronize a temporary file, atomically publish it, and
synchronize its parent directory before exposing the snapshot position. Restore
and validate the applied log ID and membership before starting OpenRaft. Reject
truncation, checksum disagreement, unsupported versions, duplicate request or
row identities, and metadata-to-state disagreement.

Optimizes for: making user state, OCC history, commit envelopes, transaction and
publication authority, membership, fingerprints, and retained outcomes one
recoverable authority boundary.

Gives up: small checkpoints and cheap unbounded retry retention. Snapshot bytes,
snapshot latency, retry-window expiry, and post-snapshot suffix replay now need
independent limits and curves.

Evidence: the version-1 fixture at
`crates/okv-consensus/fixtures/state-machine-snapshot-v1.hex`; focused seed
`1103` restored all three voters after journals were purged through index `8`,
returned the exact retained outcome, and committed the next transaction at
index `10`. Configured run `3ec077dd-4f2e-44a4-add1-880dbe1c250c` kept
candidate `09e9344` with 42 checks and zero anomalies across three seeds; the
no-snapshot run `4fcd329e-fc9e-496d-895d-b2fd19637491` discarded with four
anomalies per seed.

## D35. Object visibility and authority recovery advance separate frontiers

Status: `[DECIDED]` for the bounded objectification prototype, 2026-08-23.

Decision: advance `O_cell` only after the publisher verifies every immutable
child named by the range manifest and the publication authority installs that
exact manifest root. Compute the bootstrap log-pop frontier as
`min(O_cell, S_authority)`, where `S_authority` is the durable transaction
state-machine snapshot position. Root visibility alone does not advance
`O_cell`, and object reconstruction alone does not recover retained outcomes or
OCC authority above `S_authority`.

Optimizes for: independently testable data durability and transaction-authority
recovery, including a fresh worker that begins with only the replicated root.

Gives up: reclaiming the complete objectified prefix while authority snapshots
lag. Transaction and publication remain separate three-voter process roles in
this prototype, joined by one generation fence rather than presented as a
single fused quorum.

Evidence: focused seed `1103` passed 16 checks with `C_cell=10`, `O_cell=10`,
`S_authority=8`, safe pop `8`, and exact reconstruction of `a=80, z=240` from
one content-addressed segment. Omitting the segment produced three anomalies
beginning at `closure_complete_before_publish`; using `O_cell` alone for pop
produced `safe_pop_is_dual_frontier_minimum`.
Configured run `acdbd621-6aba-4bd6-b533-3efca57be0ed` kept candidate
`4fdf4a0` across three seeds with zero anomalies. Missing-closure run
`b4bf435c-2104-476a-84c6-e27d6e81789f` and object-only-pop run
`bc4d5d2f-a9bf-4b10-a029-c5120b6ce606` both discarded with exact replay.

## D36. Separate Raft membership identity from storage incarnation

Status: `[DECIDED]` for replacement-voter repair, 2026-08-23.

Decision: never reinstall destroyed storage under a Raft node identity while a
surviving leader may retain replication progress for that identity. Admit the
replacement as a fresh node and storage incarnation, catch it up as a learner
from a durable authority snapshot plus the retained Raft suffix, then perform a
generation-authorized membership swap. An alternative incarnation-epoch
protocol remains possible, but it must explicitly invalidate volatile leader
progress and pass the same failure gate.

Optimizes for: preventing a leader from treating an erased or replaced disk as
already caught up because its membership ID is unchanged.

Gives up: transparent same-ID disk replacement and file-copy-only repair. The
repair path now requires learner admission, snapshot transfer, suffix catchup,
generation authorization, and membership removal.

Evidence: a discarded focused probe killed node `1`, erased its root, installed
a valid authority snapshot at `S_authority=8`, seeded the exact purged-log
boundary, and restarted the same node ID while the live cell had committed
through `C_cell=10`. The replacement remained at snapshot `8` because the
leader retained replication progress through `10` for node ID `1`. Forcing a
leader reset did not safely repair the node and exposed endpoint instability.
No code or eval lane from this failed probe was promoted.

Follow-up evidence: candidate `693cf26` started blank node `4` as a learner,
installed the durable authority snapshot at `8` through OpenRaft, replayed the
retained suffix through transaction position `10` and learner-membership
position `11`, preserved the exact outcomes for requests `4` and `5`, and
reopened the learner exactly after process death. Configured run
`799804f8-9fe7-4db8-aa13-3ca89dabcc34` kept 60 checks with zero anomalies. The
log-only control `671b6db8-f377-4ed5-ad55-22c83800b41a` reconstructed the rows
but discarded with two anomalies per seed because no authority snapshot was
installed. Generation-authorized voter replacement remains a separate gate.

## D37. Keep PostgreSQL commit authority singular in the page bridge

Status: `[DECIDED]` for the first PostgreSQL implementation phase, 2026-08-23.

Decision: pin the first literal PostgreSQL path to upstream 18.6 commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7` and implement objectKV behind a
maintained PostgreSQL storage-manager fork. PostgreSQL WAL, LSN, tuple MVCC,
transaction status, checkpoint, and recovery remain the sole commit authority.
objectKV stores relation pages and forks under PostgreSQL ordering and stable
barriers. It does not independently commit the same SQL transaction.

Optimizes for: running actual PostgreSQL heap, index, catalog, extension, and
recovery behavior while testing the object-native physical-storage thesis.

Gives up: using objectKV's native serializable transaction protocol for
PostgreSQL page writes in the first phase. It also accepts a maintained upstream
fork and leaves WAL, control files, SLRUs, prepared transactions, and replication
slots outside the relation bridge until a later system-state recovery phase.

Evidence: `docs/research/postgres-18-6-storage-bridge.md` traces the pinned
`f_smgr`, buffer WAL-before-data rule, commit flush, checkpoint, AIO, table AM,
bootstrap, and non-relation state paths. `evals/suites/postgres-page-bridge.toml`
defines the first positive and poison controls.

## D38. Admit the PostgreSQL fork seam separately from the storage thesis

Status: `[DECIDED]` for experiment admission, 2026-08-23.

Decision: count compile, `initdb`, boot, relation lifecycle, checkpoint, and
restart through a second `f_smgr` slot as evidence that the maintained-fork seam
is viable. Do not count the result as objectKV or object-storage evidence while
that slot delegates to PostgreSQL's `md` manager.

Optimizes for: learning whether the required PostgreSQL hook can exist and load
early enough before spending effort on a remote backend.

Gives up: no durability, performance, asynchronous I/O, or empty-cache claim can
be made from this probe. Those claims require an objectKV-backed callback family
and the configured positive and poison controls.

Evidence: `experiments/postgres-smgr-probe/` records the exact upstream commit,
test-only patch, commands, passing SQL outcomes, restart result, and admission
boundary.

## D39. Separate routine voter repair from generation recovery

Status: `[DECIDED]` for the next replacement-voter gate, 2026-08-23.

Decision: when a healthy transaction-system quorum replaces one failed voter,
preserve the cell generation and ordered commit history. Advance a separately
authorized membership epoch, admit a fresh node and storage incarnation as a
learner, install the durable authority snapshot plus retained suffix, certify
its exact position, commit the Raft membership change, and retire the old voter.
Allocate a new generation only when transaction-system continuity is lost and
RFC-0009 recovery is required.

Optimizes for: routine repair without aborting the transaction system, changing
the version generation, or reusing unsafe replication progress.

Gives up: the existing full-generation recovery endpoint cannot be reused as
the routine repair API. The authority needs a membership epoch, pending
reconfiguration record, learner-admission credential, and exact-position
certificate purpose.

Evidence: same-ID disk repair failed, while fresh learner node `4` recovered
snapshot `8`, replayed through position `11`, retained exact outcomes, and
reopened. RFC-0023 specifies the promotion and retirement contract. Candidate
`22f2b09` implements its pure state-machine model. Run `486e5799` kept 95
checks at zero anomalies, preserved generation `7`, advanced membership epoch
`4` to `5` exactly once, and discarded eight bounded unsafe controls. The
replicated authority and real OpenRaft follow-up is candidate `76116dc`. Run
`bfcfe002` kept 57 checks at zero anomalies across three seeds, verified exact
snapshot-plus-suffix readiness and two purpose-bound certificate quorums,
advanced membership epoch once, preserved generation, fenced the removed
voter, committed after failover, and restarted the replacement. Independent
failure domains, remote transfer economics, production signer custody, and
control-authority lease behavior remain `[ACTIVE-WORK]`.

## D40. Separate object serving from maintenance

Status: `[DECIDED]` for the local SlateDB Phase 0 candidate, 2026-08-23.

Decision: keep SlateDB as a candidate segment implementation only under the
`objectkv-serving-v1` profile. A serving worker uses 64 KiB SST blocks, Bloom
filters on every non-empty SST, no duplicate SlateDB object WAL, and no
embedded compactor or garbage collector. The replicated objectKV transaction
log owns the recent durable tail. Compaction and garbage collection are
separate object workers with their own future correctness and cost gates.

Optimizes for: metadata-bounded empty-worker open, bounded cold-point bytes,
singular durability authority, and disposable serving processes.

Gives up: standalone SlateDB durability and maintenance inside the serving
process. Total requests increased 31.9 percent, so cloud request pricing and
range-scan behavior remain open even though total read and written bytes fell.

Evidence: RFC-0024, candidate `7567b99`, baseline run `0eec937b`, configured
run `07dad330`, confirmation run `5a9846fc`, and warm-cache poison `c0affb91`.
Fresh-open reads fell from 210,773,938 bytes to 402 bytes. First-point reads
used five requests and at most 210,439 bytes across three seeds. Every logical
gate passed and OTel recorded zero anomalies for the correct runs.

## D41. Preserve segment format across maintenance roles

Status: `[DECIDED]` for local evaluation, 2026-08-23.

Decision: run SlateDB compaction outside the serving database handle with a
coordinator that has no embedded worker and a separately built worker that
uses the same 64 KiB SST blocks and whole-SST Bloom filter threshold as
`objectkv-serving-v1`. Do not use the current Admin convenience worker path,
because it does not expose the block-size override and would silently restore
default 4 KiB output blocks.

Optimizes for: role isolation, physical-format parity, explicit maintenance
I/O, and bounded fresh serving reads after compaction.

Gives up: a shorter convenience API. This local same-runtime proof does not
establish process isolation, crash reclaim, concurrent serving, or cloud
economics.

Evidence: RFC-0025, candidate `b240b38`, runs `d6425f5e` and `5431c0fe`, and
missing-worker control `af37279a`. Eight L0 SSTs became one sorted run across
three seeds, every row remained exact, maintenance wrote 1.027x logical bytes,
fresh open read 538 bytes, and the first cold point fetched at most 83,264 bytes.

## D42. Reclaim maintenance by job identity, not process identity

Status: `[DECIDED]` for the local process-failure candidate, 2026-08-23.

Decision: persist compaction job state independently from worker processes. A
worker must claim a scheduled job with a fresh identity and heartbeat it. After
the heartbeat timeout, the coordinator resets the silent claim to unowned
`Scheduled`; a different worker may then resume or repeat the immutable-object
work and the coordinator alone commits the result to the manifest.

Optimizes for: disposable maintenance processes, explicit failure detection,
idempotent reclaim, and a single manifest-commit authority.

Gives up: immediate completion during the heartbeat timeout and reliance on
process-local telemetry. The force-killed worker cannot flush its OTel counters,
so child I/O requires a later durable telemetry path.

Evidence: RFC-0026, candidate `803de76`, runs `238de077` and `882b1fcf`, and
missing-replacement control `af904d02`. Three claimed worker processes were
killed, reclaimed, and replaced. Completion took 576 to 618 ms from kill, every
latest overwrite remained exact, and bounded fresh reads held.

## D43. Admit the physical segment path through local S3-compatible storage

Status: `[DECIDED]` for local MinIO evaluation, 2026-08-23.

Decision: retain the `objectkv-serving-v1` SlateDB segment candidate after the
same serving, separate-coordinator, separate-worker, compaction, fresh-reopen,
and exact-read contract passes through pinned MinIO. Treat request count and
bytes as the portable observations. Treat local wall-clock latency only as a
development baseline.

Optimizes for: one physical contract across filesystem and S3-compatible
implementations, explicit conditional metadata behavior, and attributable
remote-store request economics.

Gives up: claiming public-cloud latency, provider durability, throttling,
multi-region behavior, coordinator recovery, or garbage collection from a
single-host MinIO receipt.

Evidence: RFC-0027, candidate `abb2c64`, runs `229bfced` and `6f0e194b`, and
missing-worker control `d1125f50`. Eight L0 SSTs became one sorted run across
three seeds, every row remained exact, maintenance wrote 1.027x logical bytes,
fresh open read 538 bytes, and the first cold point used five requests and at
most 83,264 bytes. The control discarded on exactly four maintenance gates.

## D44. Persist the worker-to-coordinator handoff before manifest publication

Status: `[DECIDED]` for local process evaluation, 2026-08-23.

Decision: treat a `Compacted` job, including its immutable output SST
identities, as the durable handoff between maintenance execution and manifest
authority. Only a coordinator publishes those outputs into the serving
manifest. A replacement coordinator must first adopt any valid `Compacted`
jobs before scheduling new work.

Optimizes for: disposable coordinators, reuse of completed immutable work, a
single manifest authority, and an exact recovery decision after process death.

Gives up: deleting unreferenced outputs immediately. Durable active-job state
must remain a garbage-collection root until the job is committed or proven
terminal. Output absent from both the manifest and all active jobs needs a
separate aged-orphan collection contract.

Evidence: RFC-0028, candidate `851decb`, runs `ab8b22d4` and `e73b3458`, and
missing-restart control `b2045e82`. Three worker results survived real
coordinator process death and were committed by distinct replacement processes
without rerunning a worker. Kill through manifest commit took 29.4 to 30.5 ms,
every latest overwrite remained exact, and bounded fresh reads held.

## D45. Admit bounded concurrent Cell v0 histories before role partitioning

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: require the centralized Cell v0 authority to pass a 1,000-transaction
concurrent history before partitioning proxies, resolvers, or transaction logs.
The gate combines one-winner read/write conflicts, disjoint two-key atomic
writes, ordered blind writes, lost-reply recovery after leader death, exact
retry, and restarted-node convergence. Conflict declarations remain part of
the client contract and are not inferred by the kernel.

Optimizes for: finding a fatal transaction-composition error before adding role
distribution, and keeping the expected outcome independent of task scheduling.

Gives up: treating this bounded workload as a general strict-serializability
proof. Range phantoms, multi-proxy read-version causality, partitioned resolver
agreement, and arbitrary generated histories remain separate gates.

Evidence: RFC-0029, candidate `1e01b08`, runs `9616bf69` and `f66bb379`, and
omitted-read-conflict control `c837f980`. Each correct run evaluated 3,000
logical transactions across three seeds with 2,100 commits, 900 conflicts,
three real leader kills, exact replay, and zero anomalies. The control committed
all 3,000 transactions, produced no conflicts, and discarded with two anomalies
per seed.

## D46. Fence compaction coordinators with a durable monotonic epoch

Status: `[DECIDED]` for local process evaluation, 2026-08-23.

Decision: a coordinator must advance the shared compactor epoch before changing
compaction state or the serving manifest. A newer live coordinator fences every
older epoch. The stale process must discover that fact and exit; controller
termination is not accepted as proof of single authority.

Optimizes for: one manifest authority during overlapping coordinator lifetimes,
explicit stale-owner rejection, and a fail-closed maintenance path.

Gives up: built-in coordinator election or availability. Epoch fencing prevents
two authorities but does not choose when a replacement should start, and this
local proof does not cover host partitions or public-cloud conditional writes.

Evidence: RFC-0030, candidate `2c6a854`, runs `aaaecbb6` and `85672759`, and
external-kill control `2899bb28`. Across three seeds, coordinator epochs
advanced 0 -> 1 -> 2, each epoch-1 process self-fenced in 13.56 to 21.61 ms,
each epoch-2 process remained live through compaction, and exact serving reads
held. The control reached the same data but failed the self-fencing gate.

## D47. Root unpublished compaction output before collecting true orphans

Status: `[DECIDED]` for local process evaluation, 2026-08-23.

Decision: completed worker output remains reachable while any active compaction
record names it, even before serving-manifest publication. An aged immutable SST
may be deleted only when no serving manifest or active compaction record names
it. Uncertain root inventory fails closed and retains the object.

Optimizes for: no premature deletion across the worker-output to manifest
handoff, plus executable proof that truly unreachable objects do not leak
forever.

Gives up: aggressive deletion and any claim about the full future root graph.
Checkpoints, clones, backups, analytical leases, public-cloud LIST behavior, and
cross-tenant roots remain separate obligations.

Evidence: RFC-0031, candidate `dea0b20`, runs `8d606761` and `26b19dfb`, and
dry-run control `161eac32`. Across three seeds, GC preserved every completed but
unpublished compaction output, replacement coordinators committed the exact
preserved objects, and a second GC deleted each aged unreferenced SST in 1.88 to
1.92 ms. Exact fresh serving reads held. The control failed only the intended
orphan-deletion gate.

## D48. Judge serializability from actual reads, not declared conflicts

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: every semantic history receipt records the values returned by a
linearizable read, its read version, durable transaction outcomes, commit
sequences, and non-overlap order. The witness independently replays committed
writes and checks actual read dependencies. Client-declared conflict ranges are
inputs to the kernel, not evidence accepted by the oracle.

Optimizes for: detecting a missing dependency declaration that an
outcome-count or final-state test can overlook, while using objectKV's commit
sequence as one explicit serialization witness.

Gives up: exhaustive history search. Range reads, phantoms, arbitrary operation
generation, multiple read-version proxies, and partitioned resolver agreement
remain separate gates.

Evidence: RFC-0032, candidate `a93041f`, correct run `56a132c6`, and
omitted-conflict control `aa460aa8`. The correct subject checked 1,200 read
values, 300 committed actual-read dependencies, and 727,650 real-time edges
across 3,000 transactions with zero anomalies. The control committed every
transaction but failed the actual-read-dependency class as intended.

## D49. Empty range reads conflict with later insertions

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: a range read creates a dependency over the complete half-open key
interval, including keys that do not exist at the read version. The oracle
constructs the resulting dependency graph from actual range and point
observations, then rejects any committed cycle independently of final rows.

Optimizes for: preventing insertion phantoms through the real centralized Cell
v0 authority, including a leader death between the two dependent submissions.

Gives up: a general range-history proof. Range clears, overlapping intervals,
arbitrary generated histories, multiple read-version proxies, and partitioned
resolver agreement remain separate gates.

Evidence: RFC-0033, candidate `5d4427d`, correct run `04b84730`, and
omitted-range control `f4678cd8`. The correct subject committed 300 insertions,
durably rejected 300 dependent range writes, checked 600 dependency edges, and
converged after three leader kills with zero cycles. The control committed all
600 transactions and exposed 300 two-edge dependency cycles while its rows and
process convergence still appeared valid.

## D50. Carry a causal floor across read-version proxies

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: a tenant session advances `min_known_version` after every acknowledged
commit or exact read. Any read-version proxy handling that session must obtain a
linearizable authority version at or above that floor, or return retryable
unavailability. A locally valid older cache is not an acceptable latest read.

Optimizes for: read-your-writes and real-time order when a session changes proxy
instances or the transaction authority changes leader.

Gives up: serving from a lagging proxy. This proof also gives up throughput and
availability claims because it does not cover batching, proxy generation
rollover, or a bounded lag policy.

Evidence: RFC-0034, candidate `d910d10`, correct run `eec5ca77`, and
ignore-minimum control `d280df19`. The correct subject started six independent
proxy processes across exact replay, honored 300 causal floors, and observed all
300 acknowledged writes through three authority leader kills. The control
processes returned a valid pre-commit cache on every handoff, causing 300
minimum-version violations and 300 stale observations while the authority data
itself still converged exactly.

## D51. Every retention owner is a durable publication root

Status: `[DECIDED]` for bounded local evaluation, 2026-08-23.

Decision: checkpoints, clones, backups, analytical leases, and tenant moves all
register durable manifest pins in the publication authority. Mark walks one
complete root snapshot. Sweep reserves deletion only if the root-intent epoch is
unchanged, so a root pinned after mark invalidates the stale delete plan.

Optimizes for: one auditable liveness graph and fail-closed reclamation when a
retention owner races collection.

Gives up: automatic reclamation when a root owner fails to unpin. Lease expiry,
abandoned tenant moves, cross-tenant scale, public-cloud inventory behavior,
independent host loss, and distributed sweeper ownership remain separate gates.

Evidence: RFC-0035, candidate `d1ce1ec`, correct run `885dfdb4`, and
omitted-analytical-lease control `6e8ce843`. The correct subject preserved all
15 root instances across three seeds, reclaimed only six clone-unique objects,
deferred all three stale delete plans, and replayed exactly. The control omitted
three lease roots, reclaimed their otherwise live closures, produced 12
anomalies, and discarded.

## D52. A serving worker reaches `T` from object state at `O` plus retained WAL

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: a disposable serving worker may label a read as version `T` only
after it resolves the authoritative object root, verifies and applies object
state through `O`, and quorum-recovers and applies every retained mutation in
`(O, T]`. A base-only answer at `O < T` is stale even when the base is valid.

Optimizes for: testing the load-bearing recovery equation through a fresh OS
process before adding range routing or cache policy.

Gives up: claiming the copied local WAL fixture is the production transaction
log. Original OpenRaft log consumption, live tailing, arbitrary historical
versions, independent hosts, and object-store brownouts remain separate gates.

Evidence: RFC-0036, candidate `9e733e2`, correct run `ed0cdfe8`, and
ignore-retained-suffix control `690e0844`. The correct subject passed 45 checks
across three seeds and reconstructed exact rows at `T=10` from `O=8` plus one
suffix record. The control opened the same valid closure but stopped at `8`,
returned stale rows, produced nine anomalies, and discarded.

## D53. Serving workers consume committed envelopes, not transaction proposals

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: the transaction authority exposes committed `CommitEnvelope` bytes
as the storage mutation boundary. Serving workers never replay raw OpenRaft
transaction proposals. Cell v0 may serve the suffix through a linearizable
authority read; the distributed design must retain and route the same envelope
bytes through dedicated tagged transaction logs.

Optimizes for: keeping conflict resolution and durable rejection inside the
transaction system while giving disposable storage workers one unambiguous
mutation stream.

Gives up: treating the current OpenRaft journal as the final tLog. The journal
also contains duplicate retries, rejected transactions, blank entries, and
membership changes. The admitted pull feed is correct but central and does not
establish streaming throughput, range tags, backpressure, or partitioned log
recovery.

Evidence: RFC-0037, candidate `e1c2437`, correct run `bf79522d`, and
dropped-final-envelope control `3db9c604`. The correct subject rebuilt exact
`T=10` state after three transaction-leader kills with no copied WAL directory.
The control stopped at `O=8`, returned stale rows, produced nine anomalies, and
discarded.

## D54. Tagged tLogs retain committed envelopes behind a hard byte bound

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: a dedicated tLog record contains the exact committed envelope and
every envelope-required range tag. Each process owns private synchronized
storage and rejects the next append before its hard retained-byte limit is
crossed. A serving worker accepts a range record only when the configured
quorum returns matching record bytes for its assigned tag.

Optimizes for: separating transaction proposals from the serving mutation
stream while making range routing, quorum reconstruction, and bounded retained
storage explicit.

Gives up: claiming the bounded bridge is the final commit-proxy protocol. The
current controller copies one committed envelope from the transaction authority
to one three-process tLog set. Transaction acknowledgement integration,
multi-record streaming, lag-based ratekeeping, repair, partitioned log sets,
and independent hosts remain open.

Evidence: RFC-0038, candidate `beec908`, correct run `851d0654`, and
missing-tag control `136b2523`. The correct subject passed 69 checks across
three seeds after one tLog death per seed. The control contacted both survivors
but reconstructed no tagged suffix, returned stale rows, produced 12 anomalies,
and discarded.

## D55. Visibility waits for every required tagged-log quorum

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: ordering and conflict resolution create a replicated staged outcome,
not a visible commit. The readable frontier, user rows, and client
acknowledgement advance only after the authority records a quorum receipt for
every tagged log set required by the exact commit envelope. A restarted proxy
resumes the same transaction identity, version, and envelope, then publishes
the outcome once.

Optimizes for: preventing acknowledged or visible commits whose serving log is
incomplete, while keeping permanent bulk state outside the transaction
authority.

Gives up: a single-step commit path. The bounded receipt is process-derived but
not cryptographically authenticated, and an undurable staged head blocks later
publication. Signed receipts, abort and timeout policy, generation takeover,
multi-record lag and backpressure, repair, and partitioned routing remain open.

Evidence: RFC-0039, candidate `c549587`, correct run `5a2e5a7f`, and
acknowledge-after-one-set control `0da1a0c1`. The correct subject passed 84
checks across three seeds, survived six proxy deaths, retained eighteen exact
records across two three-node log sets, published once at `T=11`, returned the
retained retry outcome, and reconstructed exact state in fresh workers. The
control left log set `20` empty, acknowledged at visible frontier `10`,
produced 51 anomalies, and discarded.

## D56. Commit visibility requires policy-bound tLog certificates

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: the replicated transaction authority installs monotonic signer
policy for each tagged-log set independently of transaction input. It records a
durability certificate only after distinct policy members provide valid
Ed25519 attestations over the exact staged statement and configured quorum is
met. Policy installation consumes an authority Raft position, not a database
commit version. The immutable commit envelope retains its actual Raft log index
while the staged commit sequence advances only for database transactions.

Optimizes for: making durable log participation independently verifiable after
proxy or authority recovery and preserving a dense logical database version
space across control-plane transitions.

Gives up: trusting authenticated transport or proxy-supplied node lists as the
durability boundary. Production key custody, policy rotation, signer process
incarnation, generation takeover, and safe staged-head abort remain explicit
control-plane work.

Evidence: RFC-0040, candidate `6a81821`, correct run `f5e3720a`, and five
controls `f4425295`, `83fbcf79`, `26433766`, `1235b238`, and `52044094`. The
correct subject passed 96 checks across three seeds, collected 45 process
attestations after 18 synchronized tLog appends, survived six proxy deaths,
published once at `T=11`, and reconstructed exact state. Each forged or stale
certificate control produced 51 anomalies and discarded.

## D57. An active successor publishes the exact certified staged head

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: a transaction-system generation handoff retains the completed
recovery identity. After the old data-log generation is fenced and the
successor voter set is recovered and activated, the successor may publish one
fully certified old-generation staged head through a distinct replicated
takeover action. The action binds the recovery identity, old generation,
transaction identity, logical commit sequence, envelope digest, and every prior
log-set certificate. It applies the original immutable envelope once and
changes the domain generation atomically.

Optimizes for: forward progress without creating a second transaction history
or trusting a successor to reconstruct transaction intent.

Gives up: automatically aborting an incompletely certified head or recovering
an arbitrary staged prefix. A missing certificate remains safely blocking until
the old log generation is fenced strongly enough to prove that no late quorum
can surface.

Evidence: RFC-0041, candidate `f350a12`, correct run `959a2211`, and controls
`81bef774`, `e086ad66`, `fd11f355`, `e6061870`, and `59dffe26`. The correct
subject passed 105 checks across three seeds, survived three authority leader
deaths and three voter-set changes, published transaction 11 once, replayed a
lost reply, fenced old publication, and committed successor transaction 12.
The controls produced 6, 3, 3, 30, and 27 anomalies and discarded.

## D58. An incomplete staged head requires durable fence and absence quorums

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: an active successor may abort one incompletely certified
old-generation staged head only after every required tagged-log set has
durably fenced the old generation under the same recovery identity and at
least one incomplete set supplies a write-quorum of signed local-absence
observations. The abort consumes its logical sequence, leaves rows and the
visible frontier unchanged, and makes the next transaction chain from the last
committed envelope.

Optimizes for: making forward progress after an incomplete durability attempt
without inferring absence from timeout or permitting a late old-generation
quorum to create a second history.

Gives up: availability when any required fence quorum is unreachable, dense
visible version sequences, and automatic recovery authorization. Production
authorization for a destructive tLog fence, signer custody, multi-record
prefix classification, lag, repair, and partitioning remain open.

Evidence: RFC-0042, candidate `341beb9`, correct run `338ef8b4`, and controls
`6a9f4002`, `86eda531`, `6b7f30a8`, `af6cc5a5`, `10988118`, and `125b71cc`.
The correct subject passed 132 checks across three seeds, restarted fenced tLog
processes, rejected six late old-generation appends, replayed three lost abort
replies, and committed successor transaction 12. The controls produced 3, 9,
6, 12, 6, and 6 anomalies and discarded.

## D59. Recover only the longest quorum-present staged prefix

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: after every required old-generation tLog set durably fences one
exact bounded staged window, the active successor publishes only its longest
leading run of records present at write quorum in every set. The first record
absent at write quorum in any required set and every dependent later record are
terminally aborted. Unknown inventory blocks. Every sequence is consumed, and
the next successor transaction chains from the last recovered envelope.

Optimizes for: retaining durably replicated ordered work after proxy failure
without rewriting envelope history or treating timeout as absence.

Gives up: later suffix work after the first proven absence, dense visible
version sequences, and availability when any required inventory remains
unknown. The admitted window is capped at four records and 16 KiB. Production
limits, recovery authorization, signer custody, moving log sets, lag,
backpressure, repair, and partitioning remain open.

Evidence: RFC-0043, candidate `900b646`, correct run `ea3fb589`, and controls
`fc9dda4e`, `c76e5159`, `fa35669d`, `49665db4`, `1800d15f`, and `12f27160`.
The correct subject passed 168 checks across three seeds, recovered six
records, aborted six records, restarted three fenced tLog processes, rejected
six late appends, replayed three lost replies, and committed successor
transaction 15. The controls produced 24, 21, 18, 27, 12, and 3 anomalies and
discarded.

## D60. Ratekeep exact bytes before allocation and pop only with publication capability

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: a transaction reserves exact projected frame bytes from fresh signed
capacity quorums in every required tLog set before sequence allocation. During
objectification lag, insufficient soft-limit capacity returns one stable retry
outcome without staging or appending. Pop requires a quorum-signed replicated
publication root. Every tLog verifies the pinned authority membership, exact
root reference, manifest bytes, and embedded snapshot frontier before durable
local deletion. Admission resumes only after durable pop quorums.

Optimizes for: bounded fast-tier durability during object-store stalls and a
deletion authority that does not trust the commit proxy or local object
inventory.

Gives up: write availability when any required set lacks safe headroom and a
single-round commit path. The current reservation authority is centralized,
keys are deterministic evaluation material, topology is fixed, and failed-log
repair, moving log sets, partitioned proxies, independent hosts, and cloud
retention curves remain open.

Evidence: RFC-0044, candidate `868c3de`, correct run `d510af28`, and controls
`e8e5595e`, `181de920`, `af7f4cc4`, `a9b0376d`, `9668e699`, and `f7d0114c`.
The correct subject passed 180 checks across three seeds, denied nine attempts
before allocation, collected 180 capacity attestations and 18 pop attestations,
restarted three tLogs, and recovered exact transaction 16. The controls
produced 4, 5, 5, 7, 5, and 3 anomalies per seed and discarded.

## D61. Repair tLogs as certified non-voting learners before policy movement

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: replace a failed tLog with a distinct empty process and storage
incarnation outside the active policy. An active write quorum must certify the
exact retained snapshot identity before install. After install and restart, an
active quorum must separately certify the learner's exact retained root and
frontier before it is repair-ready. The learner contributes no capacity,
durability, pop, or serving evidence until a later replicated policy transition
promotes it.

Optimizes for: restoring a verified copy without letting one survivor or one
stale learner redefine durable truth.

Gives up: immediate redundancy restoration and one-step replacement. The
admitted correct path transfers one full four-record suffix with no concurrent
append. Chunking, live-tail catch-up, external machine identity, independent
hosts, and production key custody remain open. Failure is an observed policy
condition, not proof that the old process is dead.

Evidence: RFC-0045, candidate `670ef0a`, correct run `a3c3356a`, and controls
`6d99de75`, `b97e5c23`, `c90d7af6`, `2deb8392`, `4fe75506`, and `5c00a9ae`.
The correct subject passed 69 checks across three seeds, collected six repair
and six readiness attestations, installed 12 records, restarted three learners,
and recovered transaction `14` while counting only active nodes `2` and `3`.
The controls produced 2, 2, 2, 1, 1, and 2 anomalies per seed and discarded.
Candidate `8ef5c87` was discarded before scoring because its duration metric
violated the telemetry schema.

## D62. Activate repaired tLogs only through a replicated policy transition

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: move a repair-ready learner into an active log set through three
separate durable proofs. The transaction authority first prepares the exact
one-member replacement from policy `P` to `P+1` and binds the RFC-0045
readiness certificate. A quorum of `P+1` stages that exact transition. The
authority then commits `P+1`, and a distinct authority quorum certifies the
committed activation before any successor process may append, attest, report
capacity, pop, or serve. Prepare and commit occupy the cell version order;
tagged-log positions remain independent.

Optimizes for: one unambiguous active policy at every transaction boundary and
activation that does not trust controller configuration.

Gives up: writes during the bounded prepare-to-activate interval and a one-step
repair operation. The admitted path moves one member in one log set on one
host. Concurrent live-tail catch-up, chunked and remote repair, joint-policy
writes, zone replacement, production key custody, removed-root destruction,
and concurrent policy movement remain open.

Evidence: RFC-0046, candidate `b69714c`, OTel run `8b8d9705`, and controls
`c45557f7`, `aa1166c8`, `6fad03b0`, `9363aad0`, `b89ce548`, `1fbd0e47`, and
`d92439f4`. The correct subject passed 90 checks across three seeds, collected
six repair, six readiness, nine stage, and six activation attestations,
committed three policy transitions, fenced every removed root, and recovered
exact transaction `17` from the active E2 members. The controls produced 1, 1,
3, 1, 1, 5, and 1 anomalies per seed and discarded.

## D63. Repair a live tLog through durable base chunks plus an ordered tail

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: bind every repair transfer to one immutable descriptor and active
policy quorum certificate. A learner durably acknowledges fixed chunks, resumes
exact retries after restart, completes one certified base, and then installs
only the consecutive records appended while repair was in flight. Current
readiness is a second quorum proof over the combined root. The learner remains
outside capacity, durability, pop, and serving quorums until a separate
RFC-0046 policy transition.

Optimizes for: bounded restart work, tail-only catch-up bandwidth, and continued
commit progress while one failed member is repaired.

Gives up: streaming new appends into an incomplete base, unbounded catch-up,
remote and multi-repair scheduling, transfer garbage collection, and immediate
promotion. The admitted path is one same-host learner with two concurrent
appends and one short readiness barrier.

Evidence: RFC-0047, candidate `254cf421`, OTel run `28dfe9f4`, and controls
`97893c13`, `30ae3394`, `1198e1c0`, `d5f85770`, `25ee028b`, `528f1eec`, and
`0190688e`. The correct subject passed 51 checks across three seeds, survived
six learner restarts, retried six durable chunks exactly, transferred 11,499
base bytes plus 5,751 tail bytes, and advanced learners and fresh workers to
transaction `16`. The controls produced 1, 1, 1, 1, 4, 1, and 1 anomalies per
seed and discarded.

## D64. Partition conflict resolution by ordered ranges without shrinking the transaction domain

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision: assign non-overlapping ordered conflict ranges to resolver processes,
route every clipped read and write conflict to every overlap, and require one
distinct, map-bound, durable signed decision from every touched partition
before the replicated transaction authority records a global disposition.
Candidate versions are globally ordered and consumed by conflict decisions.
Committed writes finalize on every touched resolver; conflicted writes finalize
on none. Resolver partitions are throughput roles inside one tenant transaction
domain, not atomicity boundaries.

Optimizes for: horizontal conflict checking while preserving arbitrary
cross-range transactions inside one cell and retaining one recovery authority
for ambiguous finalization.

Gives up: unconstrained concurrency in the first proof. One unresolved touching
decision blocks newer work on that partition. The admitted map is fixed to
three same-host processes and does not cover split, merge, hot-range balancing,
proxy batching, independent failure domains, or production key custody.

Evidence: RFC-0048, candidate `65664bf`, OTel run `8be62401`, and controls
`0cddd6e2`, `abfbe8cd`, `b7db369a`, `a4891e60`, `92c60192`, `85389fdd`, and
`4f5912ca`. The correct subject matched the centralized Cell v0 oracle across
1,800 attempts, 1,200 commits, 600 conflicts, 3,003 signed decisions, 3,000
finalizations, three restart replays, exact rows, and exact envelope chains.
Every control produced one anomaly per seed and discarded.

## D65. Recover resolver loss by replacing the transaction-system generation

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision under test: keep resolver conflict history in generation-scoped memory.
Do not synchronize prepares or finalizations on resolver storage. If a resolver
fails, fence the complete transaction-system generation, derive the durable
commit boundary from the replicated authority, and activate empty successor
resolvers with a read-version floor at that boundary. A partial resolver admission may create
a bounded false conflict but may not create a false commit.

Optimizes for: FoundationDB-style batching and a small resolver commit path that
uses the existing generation-recovery mechanism.

Gives up: isolated resolver restart inside one generation, exact centralized
commit rates under cross-partition contention, and survival of old read
transactions across recovery.

Evidence: RFC-0049, candidate `b69b245`, OTel run `e334c857`, and controls
`d2dde4c1`, `e9551019`, `1fa4c4a9`, `ed58133e`, `0ea78ab3`, and `0cc71d81`.
Across three seeds, 1,800 attempts in 228 ordered batches produced 699 commits,
1,098 conflicts, three safe false conflicts, 2,706 decisions, three resolver
losses, three replicated generation fences, exact rows, and exact envelope
chains. The successor resolver scratch was empty, the recovery floor included
the durable head, and old-generation traffic failed closed. The correct path
used zero resolver durable synchronizations and zero finalization RPCs. Every
control produced one anomaly per seed and discarded.

## D66. Derive stateless-resolver recovery from authenticated tLog inventories

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision under test: complete signed resolver agreement permits the transaction
authority to stage one exact envelope but does not make it visible. Visibility
requires a policy-authenticated quorum certificate for that envelope in every
required tLog set. After resolver loss, a successor derives its recovery floor
from durable signed inventories of every required old-generation tLog set and
publishes only the maximal contiguous quorum-present prefix.

Optimizes for: one exact durability boundary across memory-only conflict
resolution, proxy loss, tagged-log persistence, and generation recovery.

Gives up: moving to multiple commit proxies or online resolver-map movement
before the single-proxy composition is falsified. RFC-0050 retains same-host
processes, fixed ranges, one bounded staged window, and evaluation-only key
custody.

Evidence: RFC-0050, candidate `27a86f1`, OTel run `0411bfa5`, and controls
`48afad06`, `41e0faf2`, `8265bc49`, `4f1bea9a`, `2f1fe28a`, `a2a75e60`, and
`415d9372`. Across three exact-replay histories, the subject checked 21 signed
resolver decisions, performed 51 real tLog appends, recovered six
quorum-present records, aborted six quorum-absent suffix records, and published
the successor at version `15`. It used zero resolver synchronizations and zero
finalization RPCs. The correct run had zero anomalies; every control produced
one anomaly per seed and discarded.

## D67. Order every commit-proxy batch through one predecessor chain

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision under test: let three commit proxies batch independently, but require a
replicated sequencer ticket that binds every batch to one previous and current
version, one pinned proxy incarnation, and one exact batch digest. Resolvers and
tLogs may buffer different arrival orders, but may process only the contiguous
ticket chain. Every batch, including a conflict-only batch, advances each active
tLog through an exact progress frame.

Optimizes for: removing one permanent commit-proxy throughput role without adding
durable consensus to the memory-only resolver path or weakening the RFC-0050 tLog
publication boundary.

Gives up: claiming throughput improvement from the semantic gate. The proposed
proof retains one sequencer, fixed resolver ranges, same-host processes, bounded
pending batches, evaluation key custody, and no online metadata propagation or
proxy-failure recovery.

Evidence: RFC-0051, candidate `674a443`, OTel run `2c1c8544`, and controls
`e7a65678`, `00016074`, `662ecca2`, `b21dfae1`, `d2e7e3fc`, `1d791160`,
`7313a74c`, and `e5e7d3ce`. Across three exact-replay histories, three commit
proxies per seed processed 288 transactions through 72 replicated sequencer
tickets, 348 resolver decisions, and 72 ordered tLog progress frames. The path
committed 180 transactions, rejected 108 conflicts, bounded pending work to four
batches, and acknowledged all 72 batches only after both required tLog sets.
The correct run had zero anomalies. Every frozen unsafe subject discarded.

## D68. Split resolver ownership through shadow catch-up and one ordered cutover

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision under test: replace one hot resolver range with two fresh child
processes inside the active transaction-system generation. Copy the source's
bounded recent conflict history at one frontier, dual-stream touching batches
until both children catch up, then commit one map metadata mutation through the
global proxy and tLog predecessor chain. Each ticket uses one map epoch, and the
retired source fails closed after cutover.

Optimizes for: local hotspot relief without pausing unrelated resolver ranges,
copying durable database bytes, or replacing the full transaction-system
generation.

Gives up: immediate cutover and one-copy conflict traffic during movement. The
proposal retains one split at a time, one same-host controller, bounded history,
fixed tLog placement, and evaluation-only key custody.

Evidence: RFC-0052, candidate `04738b5`, OTel run `30297004`, and controls
`c7feb034`, `e85bd186`, `8cc3a129`, `ba474e4d`, `1e62903c`, `f888a1fc`,
`e5394179`, and `f12997be`. Across three exact-replay histories, 360
transactions passed through 90 sequencer tickets. The source exported 180
history entries across the histories, two children installed 96 clipped
snapshot entries, and the five resolver workers made 732 decisions. The path
committed 261 transactions, rejected 87 conflicts, abandoned and retried 12
old-map requests, and durably advanced 90 tLog progress frames. The correct run
had zero anomalies. Every frozen unsafe subject discarded.

## D69. Recover commit-proxy loss by fencing the transaction-system generation

Status: `[DECIDED]` for bounded local process evaluation, 2026-08-23.

Decision under test: if any active commit proxy dies, fence the complete
transaction-system generation. Authenticate every required old tLog-set
inventory, publish only the maximal contiguous quorum-present prefix, abandon
the first incomplete ticket and its dependent suffix, and start fresh proxies
and resolvers above every version issued in the old generation. Never replace a
missing nonempty ticket with a no-op. Resolve a fully durable lost-reply commit
through its stable request identity.

Optimizes for: one conservative recovery boundary supported by FoundationDB's
generation model and objectKV's admitted authenticated tLog inventory and
retained-outcome contracts.

Gives up: uninterrupted commits during isolated proxy loss. The semantic gate
records recovery duration but does not set a latency objective. Exact-batch
within-generation takeover remains an optimization only if measured recovery
curves require it.

Evidence: RFC-0053, candidate `bf72639`, OTel run `1c55dad7`, and controls
`64afbe20`, `7435350c`, `cbdb06b2`, `c65209b1`, `7aeaba80`, `e43de6df`,
`df3c08bd`, `4a2ed758`, and `5a3f433f`. Across three exact-replay histories,
the path attempted 432 transactions through 108 tickets, four generations per
seed, nine proxy deaths, 510 durable tLog writes, and nine recovery fences. It
committed 336 transactions, abandoned and retried 24 batches, and recovered
three exact lost-reply outcomes. The correct path had zero anomalies. Every
frozen unsafe subject discarded.

## D70. Isolate recovery work before adding proxy-takeover complexity

Status: `[DECIDED]` for bounded local process calibration, 2026-08-23.

Decision under test: retain full transaction-system generation recovery only
if its local work remains linear in retained authenticated tLog inventory,
pending tickets, and successor role count, and reads zero permanent database
bytes. Measure failure observation, generation fence, inventory reduction, role
recruitment, and successor admission independently.

Optimizes for: deciding from a measured recovery curve whether the simpler
FoundationDB-style generation boundary is operationally credible.

Gives up: a production recovery SLO from the local-process gate. Independent
hosts, network partitions, authority quorum loss, and cloud control-plane
latency remain separate feasibility work.

Evidence: RFC-0054, candidate `90c1526`, evaluated suite hash `06717ac6`, and
210 correct recovery samples. The tail curve kept at 0.292, 0.465, and 3.158
seconds for 256, 4,096, and 65,536 records per tLog. Pending 8 and 512 stayed
flat at 0.468 and 0.459 seconds. Topology points kept at 0.390, 0.627, and 1.313
seconds. The 1 GiB and 1 PiB logical database points used identical work, read
zero database bytes, and kept at 0.460 and 0.474 seconds. All four controls
discarded; workspace tests and warning-free Clippy passed.

## D71. Require a measured hotspot benefit from online resolver splitting

Status: `[PROPOSED]`, eval frozen before implementation, 2026-08-23.

Decision under test: retain RFC-0052 online resolver splitting as the local
hotspot mechanism only if two long-lived child resolver processes beat the
one-source incumbent on a balanced splittable workload under one fixed machine,
binary, workload digest, concurrency, and ordered batch stream. Preserve exact
cross-range outcomes and measure the curve when the boundary misses the hot key
or transactions require both children.

Optimizes for: deciding from paired evidence whether the split machinery
creates useful resolver-service capacity instead of assuming that semantic
partitioning implies throughput.

Gives up: an end-to-end cell throughput claim. The first curve excludes
sequencer, proxy, tLog, object-store, and independent-host costs from timing.

Evidence required: RFC-0055, 21 paired source and split samples at every frozen
point, five discarding controls, exact oracle outcomes, fixed benchmark
identity, a reported conservative balanced ratio, and workspace validation.

## D72. Name the serving process KV Runtime and share its caches

Status: `[DECIDED]` for taxonomy and accounted pressure semantics,
`[ACTIVE-WORK]` for physical implementation, 2026-08-24.

Decision: call the disposable RAM and NVMe process a **KV Runtime**. A KV
Runtime hosts many logical **Range Engine** assignments. Use **txLog** in public
architecture prose; preserve existing `tlog` Rust symbols, eval IDs, and
historical receipts until a deliberate compatibility rename. Apply one
process-wide RAM cache, NVMe cache, and pressure controller rather than
reserving a default cache per Range Engine.

Pressure order is cache eviction, objectification, range movement, rate limit,
then commit refusal at a hard bound. A Range Engine owns assignment state and a
recent MVCC overlay, not permanent durable bytes or an OS process.

Optimizes for: making disposable compute finite while keeping range placement
as the unit of scaling and recovery.

Gives up: private-cache isolation and the convenience of treating one embedded
database with default settings as one range. The physical runtime must prove
shared-cache behavior or reduce its supported range count.

Evidence: RFC-0056 and suite hash `491c5602`. The 1, 100, and 1,000 accounted
Range Engine points pass every hard gate at 4,608 fixed bytes per range, while
four topology and pressure faults discard. The evidence is intentionally not a
physical RSS, local-engine density, or read-performance claim.

## D73. Select embedded-engine cardinality from physical density

Status: `[DECIDED]` for the next serving prototype, 2026-08-24.

Decision: default to one pinned SlateDB database with logical range prefixes
inside each KV Runtime. Keep database-per-range as a deliberate isolation tier,
not as the local serving default.

Every subject must write and explicitly flush real values, report physical RSS,
tasks, threads, descriptors, database and cache instances, object and NVMe
files, object I/O, and phase durations, then close and prove exact reads after
an empty-RAM and empty-NVMe reopen. Preserve each raw receipt with the exact
executable hash. Accounted memory is not a substitute for a physical sample.

Optimizes for: making the Range Engine to embedded-engine relationship an
observed cost decision before routed serving code depends on it.

Gives up: choosing database-per-range for implementation convenience before
measuring its manifest, task, file, lifecycle, and memory multiplication. A
multi-database isolation tier remains possible even if it loses as the default.

Evidence: RFC-0057, suite hash `69e079a5`, all nine correct topology points
kept, exact semantic replay passed, and four controls independently discarded.
At 1,000 assignments the selected layout used one database, one decoded cache,
9 live tasks, 9 object files, 30.5 MiB median peak RSS, and 115 ms empty-cache
reopen. The shared-cache database-per-range layout used 1,000 databases, 8,001
tasks, 9,000 object files, 141.5 MiB peak RSS, and 3.98 s reopen. Private caches
increased peak RSS to 190.2 MiB and reopen to 4.24 s. This selects physical
engine cardinality only. Mixed workload, remote object storage, compaction,
prefix-aware publication, and range movement remain required gates.

## D74. Own the MVCC read key above SlateDB

Status: `[DECIDED]` for the next bounded serving prototype, 2026-08-24.

Decision under test: encode one physical entry per user key and commit version
inside the single SlateDB database selected by D73. Escape raw binary user keys
so their lexical order is preserved, terminate the key, then append the
complemented 128-bit version so newest versions sort first. Store point clears
as explicit value envelopes. Do not encode transient Range Engine identity in
the durable key.

The serving adapter executes exact `get(key, T)` and ordered
`scan(begin, end, T)`, retains old values and point tombstones, survives close
and reopen, and returns `snapshot_unavailable` when `T` exceeds its applied
frontier. RFC-0058 measured latest, near-latest, and oldest-retained reads
across 1, 16, and 256 versions per hot key with physical object request and
byte receipts.

Optimizes for: keeping the stable objectKV MVCC and transactional-segment
contract independent of a non-public SlateDB `snapshot_at(sequence)` method.

Gives up: direct latest-key lookup as the complete read path. objectKV must own
history collection, range tombstones, and the amplification created by retained
versions. If near-latest point reads scan complete history, reopen the small
upstream snapshot seam or an objectKV-owned segment reader.

Evidence: candidate `fe2906d`, suite hash `e3bc8644`, correct runs `6f8a776b`,
`5b997825`, and `38845ff1`, plus four independently discarding controls. At
depth `256`, near-latest empty-cache point p99 was 6.69 ms and the 32-point
batch used 130 range GETs. The 1,024-row near-latest cold scan read 74,032,200
bytes through 1,128 range GETs in about 0.92 seconds. Physical bytes per live
byte grew from `1.20x` at depth `1` to `283.47x` at depth `256`. Keep the key
encoding. Reject unbounded retained history and require snapshot leases plus
version-GC before admitting a long-running serving system.

## D75. Separate snapshot admission from physical history collection

Status: `[DECIDED]` local collection mechanism and replicated authority
boundary, `[ACTIVE-WORK]` physical composition, 2026-08-24.

Decision under test: the replicated Cell authority owns durable leases and one
monotonic minimum-readable version `F`. KV Runtimes reject `T < F` before
reading user state. Each compaction job freezes one `F_job`, keeps every MVCC
version newer than it, keeps the first value or point tombstone at or below it,
and drops only older entries. Successful manifest publication advances a
separate physically-collected frontier `G <= F`.

Optimizes for: bounding long-running object bytes and scan cost without making
SlateDB the authority for objectKV snapshot validity.

Gives up: arbitrary unleased time travel and immediate reclamation after the
logical floor advances. The first implementation uses one cell-wide floor and
may conservatively retain an extra anchor when a compaction input begins in the
middle of one logical key.

Current evidence: candidate `3c9f008`, suite hash `c288cd4d`, kept depth-256
retained windows 1, 16, and 64 while all five controls discarded. Starting
from 74.3 MB, post-GC state converged to `1.225x`, `1.111x`, and `1.107x`
retained logical bytes. Cold scan bytes scaled from 0.32 to 4.66 to 18.57 MB
with the retained window, while local cold point p99 stayed between 0.155 and
0.181 ms. Exact floor and latest reads, a floor tombstone, bounds, publication,
and reopen passed. The local filter is accepted for the next prototype.

Operational constraint: the pinned serving profile stalls at eight overlapping
L0 SSTs per key without active compaction. Production must compact continuously
or rate-limit before the bound.

RFC-0059 now has durable lease authority, a dedicated collector process, and a
process-composed authority base plus certified txLog handoff with old-root
reclamation. It still requires the remaining authority crash subjects,
concurrent writes, range tombstones, and remote-object curves before the
complete decision.

## D76. Co-locate snapshot leases with publication roots

Status: `[ACTIVE-WORK]`, correct three-process contract admitted 2026-08-24.

Decision under test: extend the replicated publication authority instead of
creating an independent lease consensus group. One authority history must
atomically admit a lease, pin its exact manifest closure, update the root-intent
epoch, derive the monotonic read floor, prepare a frozen collection job, publish
its replacement manifest, and advance the collected frontier.

The transaction system continues to own the latest committed version `C`.
The first shared-state-machine implementation may mirror `C` directly. Any
later process split requires an authenticated gap-free frontier feed.

Optimizes for: one serializable answer to read admission, object reachability,
and reclamation. This removes the unsafe gap between registering a query lease
and pinning the objects it needs.

Gives up: independently scaling a lease service and publication authority.
Long analytical reads depend on publication-authority availability. Expiry uses
a replicated logical clock before objectKV claims a production wall-clock
policy.

Evidence required: RFC-0060's three-process crash matrix, exact lost-response
replay, authority snapshot restore, renewal versus expiry ordering, frozen job
receipts, publication exactly once, stale root and epoch rejection, and all
negative subjects discarding.

Current evidence: the pure authority rejects snapshots outside `F <= T <= C`,
expires leases only through a replicated logical tick, changes closure roots
and the root-intent epoch atomically, freezes collection inputs and floors, and
advances cell-wide `G` only with an exact replacement receipt for the configured
top-level transactional manifest root. The existing checksummed state-machine
snapshot restores active leases while the empty format-v1 fixture remains
byte-stable.

Process evidence: candidate `5f62082`, run `78df81e3`, kept 42 checks across
three seeds, 12 leader replacements, and nine deliberately dropped committed
replies. Acquire, renew, and publish recovered exact durable outcomes; a frozen
job remained at 200 while `F` advanced to 224; replacement publication moved
`G` to 200 once; stale delete marks failed after lease release; and the exact
delete permit survived restart. Disabling durable outcomes discarded in run
`bd9b73b9`.

Candidate `87794a6`, run `1dc3440f`, kept the same positive history after adding
five authority faults and process controls. Together with retained-outcome
loss, all six controls discarded on every seed: backdated admission
`578e62e8`, omitted lease-root epoch `a5cde72d`, stale range epoch `63ff010e`,
premature `G` advancement `df749dac`, stale input root `95c369e4`, and missing
outcome `90104df9`. This admits co-location and the exact token compares as the
next prototype boundary.

Candidate `3c8a52e`, run `a9d1b1f8`, then kept the real local physical
composition across three seeds. The worker discovers the exact input manifest
and live SSTs before obtaining its token, compacts real SlateDB MVCC history,
re-reads the replacement closure, replaces the authority leader, and publishes
through the successor. The omit-SST, semantic-digest-as-manifest, and
skip-failover controls discarded in runs `0f0232da`, `15ecd6ac`, and
`ad93d32a`.

Candidate `b228bd3`, run `49d4d445`, adds an exact read-only authority-rooted
manifest view and keeps both M0 and M1 MVCC reads exact after internal latest
moves. It does not yet admit the full base plus txLog serving handoff, a
separate collector process, worker-local expiry behavior, incomplete snapshot
restore, stale generation, remote storage, or production clock custody.

## D77. Make the physical closure binder part of the trusted boundary

Status: `[DECIDED]` for the local SlateDB collector and read-only immutable-base
adapter, `[ACTIVE-WORK]` for base plus txLog handoff, 2026-08-24.

Decision: an engine-specific collector binder must re-read the exact physical
manifest and every live child object after compaction, then submit that complete
immutable closure against the frozen `CollectionJobToken`. A semantic receipt
or manifest identity alone is insufficient. The replicated publication
authority validates the capability and exact submitted receipt, but does not
parse SlateDB-specific manifest bytes to discover omitted SSTs.

The serving path must resolve the manifest selected by the objectKV authority
root. It may not blindly open SlateDB's internal latest manifest. If the pinned
engine cannot open an explicit historical manifest, collection must write into
an isolated staging namespace and promote the complete root only after the
authority accepts it.

Optimizes for: one exact physical recovery closure and a serving view that
changes at the same replicated transition as `G`.

Gives up: treating the embedded engine's internal manifest publication as the
database commit point. The first process composition still reserves the broad
`kv-runtime/` namespace, so it does not yet permit concurrent per-range
collection jobs.

Evidence: candidate `3c8a52e`, suite hash `aee84768`, correct run `a9d1b1f8`,
and controls `0f0232da`, `15ecd6ac`, and `ad93d32a`. The authority accepted the
control receipt that omitted a live SST, while the eval binder detected the
closure mismatch. That result makes binder correctness a trusted safety
requirement rather than an observability concern.

Follow-on evidence: candidate `b228bd3`, suite hash `86eacf38`, run
`49d4d445`. The adapter verifies the exact manifest path, byte length, and
SHA-256 before open, filters manifest enumeration at that root, and disables
WAL replay. The M0 reader stays on M0 after M1 exists; independent M0 and M1
views both preserve exact retained MVCC points and scans.

Candidate `fc30e59`, suite hash `9bf20342`, run `da53cee9`, composes that
adapter with the replicated publication authority, two independent signed
txLog sets, and disposable Range Engine workers. Both M0 and M1 workers verify
the manifest plus every live SST before opening. The remaining collection work
is root-walk deletion and remote object-store validation.

Candidate `c79e099`, suite hash `2fb2eb53`, run `3a0e5bfb`, moves physical
collection into a dedicated child process. The child obtains its token over the
three-node authority network and exits before the controller re-hashes both
input and output closures. Omitted SST `d9baa91e`, semantic-only root
`d188aa0a`, and skipped failover `4cadcddd` still discard.

## D78. Bind the immutable base and txLog chain in one Range Engine root

Status: `[DECIDED]` for the bounded process-composed serving handoff,
`[ACTIVE-WORK]` for concurrent operation and reclamation, 2026-08-24.

Decision: a Range Engine serving root is more than a SlateDB manifest identity.
It must bind the manifest to the cell and tenant, transaction generation,
covered-through version, minimum-readable version, and commit-chain digest at
that frontier. A serving view may overlay only the gap-free commit-envelope
chain above the base. Commit versions must increase and reach the exact target,
but may skip numeric positions used by non-commit replicated-log entries. Every
suffix envelope must link to the prior digest and carry one valid quorum
certificate for every required txLog set. A root change builds the
replacement completely, then uses an exact-current-root compare before changing
the process-local pointer. Existing readers retain the old immutable view.

Optimizes for: exact reconstruction during asynchronous objectification and
history collection. The same transaction can be served before publication from
the old base plus tail and after publication from the new base plus a shorter
tail, without treating SlateDB internal latest as authority.

Gives up: a manifest-only publication payload and immediate object deletion at
root replacement. Root metadata and certificate verification are on the open
path, and old roots remain live until both replicated reachability and local
reader references release them.

Candidate evidence: `27d32aa` introduced the view; `f46d632` corrected its
version rule and proves numeric gaps. Together they add the public serving-side
txLog certificate verifier, `AuthorityRangeRoot`,
`AuthorityBoundRangeView`, and
`RangeServingState`. The focused physical test materializes M0 through version
2 and M1 through version 5. It proves exact version 8 from M0 plus certified
commits 5 and 8 and from M1 plus certified commit 8, retains an exact M0 reader
across the local compare-and-swap, and rejects a tampered certificate.

Frozen evidence: candidate `fc30e59`, suite hash `9bf20342`, correct run
`da53cee9`. Across seeds `1103`, `2207`, and `3301`, each subject starts seven
transaction processes, a three-node publication authority, two three-node
signed txLog sets, and two disposable Range Engine workers. It kills the
publication leader and one member of each txLog set. M0 at version 3 plus
certified commits 5 and 10, and M1 at version 5 plus certified commit 10, both
match the transaction oracle. The positive lane kept 36 of 36 checks with
exact replay.

Six same-topology controls discarded on every seed: premature M1 publication
`68d2bc66`, omitted intermediate tail `5f7441dd`, tampered signature
`de75ed8e`, stale policy epoch `ee85fb34`, wrong prior-root compare
`7f04dbd8`, and skipped authority failover `2797bff1`.

Follow-on candidate `2742400`, suite hash `fd5b52a6`, correct run `7805dd6d`,
adds authority-owned old-root reclamation. Each M0 reader lease names its exact
root closure. Every unique M0 delete reservation is rejected while the lease
is live. Release advances the root-intent epoch; a fresh mark obtains exact
permits, physically deletes two unique M0 objects per seed, retires every
permit, and starts a third M1 worker that still matches T=10. The positive lane
kept 57 of 57 checks.

Three new controls discard on every seed: bypass the pinned closure
`83a7544a`, reuse the pre-release mark epoch `257069b4`, and retire permits
before physical deletion `206a22e2`. The original six handoff controls also
continue to discard with exact replay.

This admits the serving handoff and reclamation semantics, not their
performance. The fixture is small, local-filesystem backed, sequentially
orchestrated, and exporter-free.
It does not prove concurrent tail application, worker crash recovery,
remote request economics, or mixed OLTP throughput. Collector isolation is
proven in the adjacent physical-composition suite, not yet folded into this
same history. Those are the next decisions, not hidden claims in this one.

## D79. Make cache ownership and streaming merge part of the Range Engine read contract

Status: `[DECIDED]` for the next implementation slice, 2026-08-24.

Decision: do not treat the raw authority-bound SlateDB reader as the final
foreground read path. A KV Runtime must inject one shared decoded RAM cache and
one bounded shared NVMe block cache into every authority-bound Range Engine
view. Point reads check the certified recent overlay first, then RAM, then
NVMe, then immutable object storage. Cache identity must derive from immutable
object and block identity, never transient Range Engine assignment identity.
Cache loss changes latency, not correctness.

Replace the current bounded range implementation, which reads `limit + all
affected tail keys`, with two ordered iterators and a streaming primary-key
merge. The merge suppresses a base row when the tail has a later value or
tombstone, emits tail inserts in order, and stops at the logical row limit.

Optimizes for: making object storage the durable rebuilding base rather than a
mandatory remote RTT on every foreground read, while keeping compute
disposable and cache budgets process-wide.

Gives up: the simplicity of opening an authority manifest over a raw object
store and materializing a bounded map. Cache population, eviction, corruption,
and stale-entry prevention become explicit correctness and resource concerns.

Evidence: candidate `1ee9de4` release runs kept all six RFC-0061 curve points.
Base-only view open remained 0.60 to 0.73 ms across 1K to 64K keys. Tail work
was linear at 4.07 ms for 64 records and 62.06 ms for 1,024. Every base point
still issued one object `get_range`. A 1,024-record tail reduced scan throughput
from about 196K to 91K rows/s and raised range GETs from 80 to 159. These are
local OS-warm numbers, not GCS latency evidence.

Next evidence required: matched raw, RAM-warm, NVMe-warm, and fully cold cache
profiles; content-addressed cache resurrection controls; bounded eviction under
multiple Range Engines; then the same matrix on `objectKV-dev` GCS.

Follow-on evidence: candidate `7071e33`, suite hash `bc176108`, keeps matched
raw and combined shared-cache release points at 16K keys. Repeating the same 64
points makes 64 backend range GETs on the raw reader and zero through the
shared cache. A 1,024-row scan falls from 80 to one backend GET with no tail and
from 85 to one with a 64-record tail. Median scan throughput rises from 196K to
248K rows/s and from 178K to 233K rows/s respectively. The cache adds about 1.6
ms to view open and roughly 200 microseconds to the first miss.

This admits combined cache injection, not every cache tier. The measured warm
pass may hit decoded RAM. A fresh decoded cache over the same local block cache
is still required to isolate NVMe. Corruption, stale-entry resurrection,
eviction under multiple Range Engines, and remote object behavior remain open.

Streaming follow-on evidence: candidate `20899e7`, suite hash `268beac9`,
keeps four clean release points with an authority-bound base cursor and ordered
resident-tail merge. The zero-tail and 1,024-record-tail raw scans both make 80
backend range GETs. Long-tail scan throughput improves from 91K to 186K rows/s,
while cached scans remain at one GET. Raw scans now discard above 96 requests
and cached scans discard above four. This admits the merge shape. Tail
certificate verification and overlay construction remain linear at about 61
microseconds per record, and the unobjectified tail remains resident memory.

NVMe-reopen follow-on evidence: candidate `79afb08`, suite hash `c31143ca`,
keeps zero-tail and 64-tail clean release points after closing the first view,
discarding decoded RAM, reconstructing the local cache object from its existing
directory, and opening a fresh authority view. First-point data and the ordered
scan transfer zero backend bytes and make zero successful backend range GETs.
The scan makes zero backend requests.

This does not admit offline reopen. View open still transfers 788 bytes of
manifest metadata and performs two successful GETs, one list, and two failed
metadata GETs. The first point performs one additional failed metadata GET.
Decision: keep persistent NVMe as a data acceleration tier, retain object
storage as a worker-bootstrap dependency, and do not add an authority-bound
metadata cache until remote measurements show that bootstrap is a material
availability or latency bound. Process-isolated corruption, resurrection, and
multi-range eviction were the required follow-on controls.

Corruption follow-on evidence: candidate `63c9531` overwrites every persisted
cache data part, reconstructs the cache with fresh decoded RAM, and rejects any
non-exact value. The focused fixture currently detects the corrupt bytes and
re-fetches exact range data from the backing store. This satisfies the first
corruption rule, but it is not yet a process-isolated receipt or torn-write
matrix. Candidate `505c997` below closes that byte-fault gap. Resurrection is
closed by the historical-authority and process controls. Multi-range eviction
remains open.

Historical-authority follow-on evidence: candidate `7eae670` validates that a
snapshot-lease token is still the exact active value in current publication
state, then binds historical Range Engine opens to the outer published root,
closure membership for both outer root and inner immutable-base manifest, and
target version. Release, expiry, token drift, or root drift refuses before
storage access. The focused released and wrong-root controls make zero storage
requests.

Decision: an old-root cache hit is never proof of authorization. Historical
reopen must begin with a current replicated-authority read. Do not compare the
publication-authority generation to the generation that produced the base;
those can differ after recovery.

Process follow-on evidence: candidate `e06a159` moves the rule into the
four-worker handoff suite. Correct release run `2b1bdc6a` kept 60 checks and
refused all three post-release M0 reopen attempts after reading live authority.
It then reclaimed 9 of 9 M0-only objects, including compacted data, and kept M1
exact. Negative run `93773b96` supplied the pre-release authority snapshot and
reopened M0 in all three seeds, producing a discard. Decision: authority
freshness is a required worker-bootstrap dependency, not a cache policy. The
remaining admission subjects at that point were authority unavailability,
torn cache writes, and bounded eviction across many Range Engines.

Availability follow-on evidence: candidate `52ca95e` adds a fifth worker with
a bounded authority-read deadline. Correct run `805cc0cf` persisted three
unavailable-authority refusals and opened zero historical views. Negative run
`1c769733` fell back to the pre-release snapshot and reopened M0 in every seed.

Decision: authority unavailability fails historical worker bootstrap. Do not
serve from cached bytes or last-known authority state. This optimizes for
read correctness and reclamation safety. It gives up historical-read
availability during an authority outage. The production retry deadline and
client-facing error policy require a later operating curve.

Cache-byte fault follow-on evidence: candidate `505c997` adds a dedicated
four-process-per-seed gate outside the authority handoff. Prepare workers create
and populate real persistent cache parts, then exit. The controller either
overwrites every part without changing length or truncates every part to half
length and fsyncs it. Fresh workers reconstruct the cache and decoded RAM.
Correct run `83a36734` kept all 24 checks across three seeds, damaged 30 parts,
repaired all reads exactly through the backend, and returned zero wrong values.
Four omitted-fault and accepted-wrong-value controls discarded.

Decision: admit persistent NVMe only as a disposable, self-validating data
tier. On byte damage, exact backend repair and typed refusal are both correct;
a non-exact value is never correct. Keep physical fault exercise as a hard gate
so a passing run cannot silently skip injection. This optimizes for automatic
cache recovery without treating local media as durable state. It gives up
availability if neither a validated cache block nor its authoritative backing
object can be read. Bounded multi-range eviction is the remaining local cache
admission subject before the GCS matrix.

Multi-range eviction follow-on evidence: candidate `5f7bf82` creates eight
logical range assignments in one 2 MiB immutable base, serves all of them
through one 192 KiB persistent cache, discards decoded RAM, and rereads the
ranges in reverse order. Correct run `9375c874` kept exact results across three
seeds, settled at no more than 131,292 cache bytes, and made 130 backend range
GETs on reread. Disable-bound run `77e7adea` retained 2,105,380 bytes and made
zero reread range GETs. Skip-reread and accepted-wrong controls also discarded.

Decision: the KV Runtime cache budget is one physical shared bound, not a
per-Range Engine entitlement. Eviction may remove every local block for a
range, and the next read must refill exact bytes from the authority-selected
immutable base. Do not pin range working sets by default. This optimizes for
dense range assignment and bounded NVMe. It gives up per-range residency and
can expose noisy-neighbor miss costs. Concurrent fairness and quota policy need
a separate workload; the immediate next gate is the same cache-state matrix on
GCS.

GCS harness follow-on evidence: candidate `f496e8d` makes the bounded eviction
worker select either local filesystem or GCS without changing the semantic
contract. GCS processes receive unique scratch prefixes and must delete every
object before their cleanup gate passes. Clean local regression `2e1ce017`
kept after the backend split.

Decision: remote profiles own scratch isolation and cleanup as correctness
gates, not operator conventions. This optimizes for repeatable cloud experiments
and bounded cost. It gives up retaining failed-run objects for ad hoc debugging;
high-resolution receipts belong in telemetry instead. No GCS result is admitted
yet. The interactive credential is expired, and the current
application-default identity lacks permission to verify `doss-objectkv-dev`.

## D80. Publish immutable Range Engine generations with a full view token

Status: `[DECIDED]` for the process-local publication contract and coordinated
process correctness gate, `[ACTIVE-WORK]` for sustained load, memory, and
telemetry evidence, 2026-08-24.

Decision: build and authenticate a replacement Range Engine view off the read
path, then replace one process-local immutable `Arc` only if the current full
view token matches. The token binds the complete authority root, target
version, and final authenticated txLog-chain digest. Readers clone the current
view under a short lock and perform all storage I/O after releasing that lock.
Readers already holding the old view finish against it; later readers obtain
the new view.

A manifest key is not an adequate compare token. Objectification can leave the
base at `K` while successive authenticated tails advance from `T1` to `T2`.
Both views then name the same manifest. A stale controller comparing only that
key could overwrite `T2` with `T1`, which is a same-manifest ABA error.

Optimizes for: exact nonblocking reads during tail advancement and
objectification, with replacement work outside the foreground lock. It also
gives the future routed read service one explicit snapshot generation per
request.

Gives up: in-place mutation of one shared tail map. Each publication builds a
new authenticated overlay and retains old view memory until its last reader
releases it. Publication cadence and retained-generation memory therefore need
backpressure and measurement.

Initial evidence: candidate `e0f1b12` adds public `RangeServingViewToken` and changes
`RangeServingState::install_if_current` from manifest-key comparison to full
token comparison. The focused test starts 16 reader tasks, makes every task
retain the `T=5` generation, atomically installs `T=8` over the same base, and
proves each retained read returns exactly `T=5` while each later read returns
exactly `T=8`. A stale `T=5` publisher with the same manifest is refused and
does not disturb `T=8`. Both Range Engine view tests pass; owning-package
strict Clippy passes. This is not yet a process-isolated throughput result.

Process follow-on evidence: candidate `e3866b2`, suite hash `48d82a11`, profile
hash `489d83c0`, and release executable SHA-256 `cf469a79` add
`cell-range-serving-concurrency-process-v1`. Correct run `0aa7c992` kept all
21 hard checks across three child processes. Six same-base publications per
seed retained eight prior-generation readers, yielding 144 exact old-view and
144 exact new-view reads with zero mixed results. Semantic replay was exact.
The pointer-swap section measured 250 ns median and 625 ns p99 across 18 local
samples. That excludes replacement construction and authentication.

Four controls discarded: accepted stale rollback `fb61c181` ended at `T=3`
instead of `T=9`; skipped overlap `20699bf5` retained no old readers and
reported 144 mixed/absent old results; accepted mixed receipt `f6c3a2d8`
reported one anomaly per seed; skipped stale probe `6778d9f7` failed its
exercise gate.

Next evidence required: sustained point and range load during publication,
replacement build plus certificate-authentication latency, retained-generation
bytes, read p50/p99, slow-reader pressure, worker failure, partial-tail
controls, and required OTel export. Then repeat the relevant cache and rebuild
curves on `objectKV-dev` GCS.

## D81. Route direct reads at the KV Runtime process boundary

Status: `[DECIDED]` for the protocol shape, first independent-process gate, and
fixed-version client refresh algorithm. `[ACTIVE-WORK]` for authoritative
RangeMap publication, replacement routing, security, sustained load, and
remote performance admission, 2026-08-24.

Decision: one KV Runtime network service routes all of its local Range Engine
assignments. Do not create one server or one private concurrency pool per
range. A client request names its cell, tenant, expected range ID, routing
epoch, and exact read version. The runtime validates those identities and the
half-open range before it captures one immutable serving view. A scan that
crosses the assignment returns the split boundary; the client fans out at the
same snapshot version.

Optimizes for: direct OLTP reads without a storage proxy, process-wide
backpressure, explicit stale-route repair, and a narrow waist reusable by
PostgreSQL, Redis, and search layers.

Gives up: transparent server-side cross-range reads. The client must preserve
`T`, refresh stale routing, fan out, and merge. The prototype also uses one
length-prefixed JSON request per TCP connection, which favors falsifiable
semantics over wire efficiency.

Evidence: candidate `6d0cf63` adds `KvReadRouter` and the bounded TCP protocol.
Follow-on candidate `6361695` expands the real-TCP focused regression to two
non-overlapping assignments over an authority-bound immutable view. It returns
exact point and range answers, reports the routing epoch and applied frontier,
routes an empty point through the second assignment, fences a stale epoch,
returns a split boundary for a crossing scan, and refuses `T` above the
applied frontier. Owning-package strict Clippy passes.

Security boundary: tenant indexing and authority-root identity checks prevent
accidental local cross-tenant routing, but do not authenticate a caller. A
public client requires an authenticated tenant capability, TLS, quotas, and
audit identity. Do not describe the current protocol as internet-safe.

Next evidence required: independent server and client processes, several
ranges and two tenants, route refresh, saturation, oversized frames, worker
death, read p50/p99, cache state, backend bytes, and OTel completeness. Only
then should the PostgreSQL bridge treat this as its stable read transport.

Process follow-on evidence: candidate `bd9d959`, suite hash `64236864`, profile
hash `acef836f`, and release executable SHA-256 `b1bf79ed` start one independent
KV Runtime per seed. Correct run `740e7111` keeps 192 exact point reads and 48
exact scans, exercises every typed routing and snapshot refusal, kills every
worker, and observes connection refusal after death. Local process-warm point
latency is 112 microseconds p50 and 152 microseconds p99; scan latency is 133
microseconds p50 and 231 microseconds p99. Four stale-route, crossing-scan,
wrong-value, and skipped-kill controls discard.

This closes the first independent-process item above, not the remaining list.
The fixture uses one tenant, sequential requests, an in-memory object store,
fresh TCP plus JSON per request, and no OTel exporter. Worker death proves
unavailability, not range-map refresh or reroute to a replacement.

Route-refresh follow-on evidence: candidate `b068256`, suite hash `d74f6e19`,
profile hash `8dc66b3f`, and release executable SHA-256 `d30fb2fe` start three
independent workers. Correct run `7636b6fc` refreshes a stale unsplit map once
per seed, restarts at the original `T=8`, fans out across a real split at `k5`,
and returns 21 of 21 expected rows. The stale-map, changed-version, and missing
second-range controls discard.

This closes the client refresh algorithm item, not production routing. The
refresh source is an in-process test authority, and both post-refresh ranges
terminate at one KV Runtime endpoint. A replicated RangeMap, concurrent
publication, replacement endpoint, worker-death reroute, and tenant capability
remain required before PostgreSQL may depend on transparent recovery.

## D82. Bind PostgreSQL page reads to two independent frontiers

Status: `[DECIDED]` for the read-only adapter and first process gate,
`[ACTIVE-WORK]` for PostgreSQL callback integration and every write, barrier,
and recovery contract, 2026-08-24.

Decision: a physical PostgreSQL page read carries an exact objectKV version and
a maximum admitted PostgreSQL page LSN. These are independent clocks. The
objectKV version selects the immutable physical view; the LSN frontier rejects
any page that is newer than the PostgreSQL bridge root permits. Never infer one
clock from the other.

Physical identity is the ordered tuple of cluster, tablespace, database,
relation, temporary backend, fork, and block. A value contains format version,
page LSN, PostgreSQL checksum metadata, exact 8 KiB payload length, payload
SHA-256, and bytes. Consecutive reads require every expected block key and
restart at unchanged objectKV version after a stale route.

Optimizes for: preserving upstream PostgreSQL heap, index, buffer, and MVCC
semantics while objectKV supplies a verifiable physical page substrate.

Gives up: treating objectKV transactions as PostgreSQL commit authority in the
page-bridge phase. PostgreSQL WAL remains authoritative, and the bridge root
must bind both clocks explicitly.

Evidence: candidate `8fb20e5`, suite hash `857c3b12`, profile hash `659609ee`,
and executable SHA-256 `1a458df1`. Correct run `977b368d` keeps nine of nine
pages across three independent workers, three route refreshes, two ranges, and
objectKV version 2 under page-LSN frontier 800. Missing-page `7256f045`,
payload-corruption `d8d0a2a5`, changed-version `3332607a`, and LSN-ahead
`7dd9189d` controls discard.

Limits: the object store is in-memory, both ranges share one KV Runtime
endpoint, requests use fresh TCP plus JSON, and the PostgreSQL fork does not
call this reader. No page write, WAL-before-page proof, sync, checkpoint, AIO,
restart, or remote-object result exists.

Read-callback status update: D83 closes only the literal synchronous callback
item above. Every write, barrier, recovery, remote-object, and production AIO
limit remains open.

## D83. Complete PostgreSQL non-file reads through its AIO callback chain

Status: `[DECIDED]` for the first synchronous read seam, `[ACTIVE-WORK]` for
production asynchronous submission, page writes, stable barriers, and recovery,
2026-08-24.

Decision: the selected PostgreSQL 18.6 relation uses an objectKV
`smgr_startreadv` callback with no `mdreadv` or `mdstartreadv` fallback. A
separate page-service process binds the exact physical relation, block range,
objectKV version, and maximum PostgreSQL page LSN, then reads authenticated
pages through the routed KV Runtime and Range Engine.

PostgreSQL enters `smgr_startreadv` even when `io_method=sync`. The probe fork
therefore adds a narrow immediate-completion helper for a non-file storage
manager that has already filled the requested buffers. The helper runs the
existing upper AIO callback chain. It is not an asynchronous objectKV
implementation.

Optimizes for: exercising the actual PostgreSQL buffer and executor path now,
preserving its page verification, and making missing service or changed
frontier fail closed.

Gives up: claiming the current TCP sidecar is a production read path. The probe
uses a Rust debug binary, fresh connections, an in-memory object store, and an
immutable import of a relation originally written by `md`.

Evidence: candidate `b04b128`, PostgreSQL commit `724edf9`, patch SHA-256
`910aef1e`, and page-service executable SHA-256 `4e7cd0cd`. A fresh PostgreSQL
process reads 148 heap pages through 13 objectKV callbacks and returns 2,000
rows with `sum(id)=2001000`. Service unavailable and changed-frontier controls
refuse. The cold debug scan takes 233.045 ms; the immediate 148-buffer hit takes
0.299 ms.

Next evidence required: freeze this external-fork lifecycle in `okv-eval`, then
implement objectKV page write and block count behind a WAL-before-page gate.
Checkpoint, empty-cache restart, remote object storage, cancellation, security,
connection pooling, and OTel completeness remain open.

## D84. Enforce PostgreSQL WAL-before-page at the objectKV effect boundary

Status: `[DECIDED]` for permanent-page admission, `[EXISTS]` for subordinate
commit, relation extent, and literal callbacks, `[ACTIVE-WORK]` for stable
storage publication, 2026-08-24.

Decision: every permanent PostgreSQL page batch carries the exact objectKV view
against which it was prepared and PostgreSQL's observed durable WAL frontier.
The bridge refuses the complete batch before producing any objectKV mutation if
the view is zero, the batch is empty or above 128 pages, its block range
overflows, or any page LSN is above the WAL frontier. Successful admission
produces ordered page mutations and one domain-separated SHA-256 over version,
WAL frontier, request identity, keys, and values.

PostgreSQL WAL remains the SQL commit and recovery authority. The admitted
mutation batch is subordinate work. It has no objectKV commit version and does
not let PostgreSQL advance a checkpoint. Buffered acceptance, transaction-log
durability, objectification, and checkpoint-stable publication require distinct
receipts.

Optimizes for: a fail-closed, replayable WAL-before-page effect boundary that
can be exercised before the maintained fork mutates data.

Gives up: treating one admission result as proof of durability. D85 through D87
add commit retry, relation extent, literal callback, and bounded local service
restart. Stable checkpoint behavior remains separate.

Evidence: candidate `c3c5df9`, suite hash `0fff8f62`, profile hash `1594da41`,
and executable SHA-256 `5a5e946f`. Correct run `0bf18a75` passes 15 checks,
admits three batches and six mutations, and replays exactly. WAL-behind
`118ba54b`, zero-version `ee71a5b4`, oversized-batch `b14da383`, and
wrong-digest `c74e05ad` discard.

## D85. Make relation extent atomic with subordinate page commit

Status: `[DECIDED]` for the local Cell transaction shape, `[EXISTS]` for literal
callbacks, signed txLog durability, and serving reconstruction, `[ACTIVE-WORK]`
for stable publication, 2026-08-24.

Decision: one relation fork has one versioned authoritative extent key. An
existing page write keeps the extent unchanged and cannot reach beyond it. An
extend begins exactly at the prior extent and writes consecutive pages plus the
resulting block count in one Cell transaction. The transaction reads and writes
the extent conflict range, writes every page conflict range, and derives its
Cell retry identity from the stable PostgreSQL request identity.

The bridge receipt verifies the same identity and generation, a commit version
strictly above the transaction read version, and the committed envelope. The
commit version need not equal read version plus one because it is a Cell Raft
log position. Duplicate submission of the same command must return the exact
original outcome without advancing the visible version.

Optimizes for: one snapshot in which pages and `smgr_nblocks` agree, plus exact
lost-reply recovery across leader handoff.

Gives up: treating the Cell v0 envelope as the final page-durability or
checkpoint receipt. Separate tagged txLog, Range Engine, objectification, and
stable-publication evidence remains mandatory.

Evidence: candidate `7de5c4e`, suite hash `0f3a3a8b`, profile hash `7e8a0b61`,
and executable SHA-256 `9bdb2235`. Correct run `bb7e18fa` passes 24 checks over
12 process starts and three leader handoffs, commits six pages plus three
extent values, and resolves three duplicate retries exactly. Missing extent
`5816809e`, changed retry identity `247a6cdb`, wrong receipt identity
`68282231`, and non-advancing version `71d18d48` discard.

## D86. Bind PostgreSQL callbacks to an atomic physical page-store view

Status: `[DECIDED]` for the mutable callback probe, `[ACTIVE-WORK]` for
concurrent generation publication, 2026-08-24.

Decision: expected objectKV version 0 means select the service's current
immutable physical page-store view while beginning the requested read, block
count, or existing-page write. The selected reader, version, and page-LSN
frontier come from one state generation. A nonzero expected version remains an
exact pin and is refused when it is not current.

Do not use a separate current-version discovery operation. It adds a network
round trip and permits the view to advance between discovery and use. Also do
not interpret a newly selected physical version as a PostgreSQL logical
snapshot change. PostgreSQL remains the tuple-MVCC and visibility authority.

The bounded service retains one mutex through write admission, Cell commit,
fresh Range Engine construction, and publication. This proves atomic selection
by serializing writers. It is not the production concurrency design.

Optimizes for: one race-free callback contract that live PostgreSQL backends can
use without restart and without weakening exact pinned-version refusal.

Gives up: concurrent write preparation in the current probe. The next design
must use immutable generation pointers, prepare against one generation, and
publish through a short compare-and-swap or authority-validated critical
section with bounded retry.

Evidence: the pinned PostgreSQL 18.6 fork began at physical version 5, updated
row 6 to `objectkv-atomic-current-v2`, checkpointed one native heap page through
the Cell to version 9, and returned the same row plus authoritative
`nblocks=148` in the same session. The checkpoint took 688 ms in a debug build.
The local heap file retained SHA-256
`3770217fa7ca29da2d79580fa5fd68616a9257d6460801f0a1ade6cfc078d7e8`.
An explicit version 5 request refused at current version 9 without changing
state. The superseded discovery operation was removed; protocol operation 4 is
now reserved for the distinct stable-sync contract in D88.

Limits: write publication is serial, and no replicated stable-root receipt,
remote object store, production transport, or OTel result is claimed. D87
records the bounded local service-recovery decision that followed this one.

## D87. Recover the PostgreSQL page service from an exact object base plus signed txLogs

Status: `[DECIDED]` for bounded local-process recovery, `[ACTIVE-WORK]` for
replicated stable-root publication and production generation recovery,
2026-08-24.

Decision: freeze one closed SlateDB base at the PostgreSQL bridge's bootstrap
objectKV version. Persist a descriptor that binds the Cell and tenant identity,
transaction-system generation, PostgreSQL relation fork, base version, base
page-LSN frontier, exact manifest, and complete live-object closure. Retain each
later committed Cell envelope in every required signed txLog set. A serving
process may open only after every base object verifies and every required log
set yields one unique quorum history with valid attestations through the target.

When a durable descriptor exists, the service must not read the source heap.
For this bounded prototype it recreates the same deterministic Cell baseline and
replays the retained transactions using their original identities, conflicts,
resolver set, log tags, mutations, and read versions. Each replay must return
the same commit sequence and byte-exact envelope before the service accepts a
new write.

An append retry may count an already-retained byte-exact record as a durable
acknowledgement. Different bytes at the same position, missing required-log
quorum, incomplete certificate coverage, broken commit chain, wrong identity,
or any missing object in the frozen closure must refuse availability.

Optimizes for: proving that the PostgreSQL process and page-service process are
disposable while object bytes plus the authenticated transaction suffix remain,
without treating a local heap file or service memory as hidden durability.

Gives up: claiming that the local descriptor is a publication-authority receipt
or that deterministic baseline replay is production Cell recovery. It also
keeps synchronous local txLog appends, serial view publication, local failure
domains, and an unbounded tail until later objectification and pop work.

Evidence: candidate `3bb2783`. The first durable checkpoint changed row 7,
advanced the physical page-store from base version 5 through version 9, and took
465.720 ms. PostgreSQL shutdown retained version 10. A complete service restart
used a nonexistent source-heap path, authenticated two retained records across
two required 3-node, quorum-2 signed txLog sets, returned row 7, then accepted a
post-recovery checkpoint through version 11 in 561.758 ms. After shutdown
retained version 12, a second service restart authenticated four tail records
and returned both row 7 and row 8. The selected 1,212,416-byte local heap kept
SHA-256 `3770217fa7ca29da2d79580fa5fd68616a9257d6460801f0a1ade6cfc078d7e8`.
A disposable root with only one historical node in txLog set 10 refused with
`no unique txLog quorum`; a separate root missing its named live SST refused
during physical object verification.

Next decision: make the D88 root self-contained across authority restart,
objectification, retention pop, and empty-cache remote recovery before treating
it as a production WAL-recycling boundary.

## D88. Complete PostgreSQL checkpoint sync only after replicated stable-root selection

Status: `[DECIDED]` and `[EXISTS]` for the bounded local proof,
`[ACTIVE-WORK]` for production retention and recovery, 2026-08-24.

Decision: keep hot page acceptance and PostgreSQL stable sync as different
receipts. A successful selected-relation `smgr_writev` appends the exact Cell
envelope to every required signed txLog set, reconstructs a fresh Range Engine,
and then registers one deduplicated objectKV relation tag in PostgreSQL's native
sync-request queue. It does not complete the checkpoint barrier.

When PostgreSQL processes that tag, operation 4 captures the page service's
atomic current version `B`. The sidecar derives a recoverable frontier binding
the relation, immutable base descriptor and complete object closure, maximum
page LSN, certified tail digest, final Cell chain digest, and each required
txLog set's durable position and envelope digest. PostgreSQL WAL must be flushed
through that maximum page LSN.

The sidecar persists the frontier as one content-addressed manifest, prepares
and publishes it through the generation-fenced three-process publication
authority, then performs a linearizable read-back. PostgreSQL sync returns only
if the observed destination root equals the exact manifest. Explicit
`smgr_immedsync` uses the same handler. If the authority is unavailable, hot
txLog state may advance but the checkpoint must fail and the prior stable root
must remain selected.

Optimizes for: using PostgreSQL's existing checkpointer ordering and request
deduplication, preserving fast hot acceptance, and making the hot-versus-stable
frontier visible and fail closed.

Gives up: production checkpoint safety today. The authority harness is
ephemeral, all failure domains are local, txLogs are retained without pop, the
root does not objectify through `B`, and no empty-cache remote restore is
performed. The page service also holds its state lock across authority I/O.

Evidence: the literal PostgreSQL 18.6 checkpointer wrote row 9 through objectKV
version 13, then spent 160 ms in sync and completed only after authority term 3,
index 4 selected manifest
`193e84d3aec75b94a7098de8c20520197597552f6d629f482ad137ba8cecf070`.
The whole debug checkpoint took 829 ms. A new page-service process used a
nonexistent source heap, recovered version 13, reconciled the exact root, and
served rows 7, 8, and 9 to the still-running PostgreSQL process. After the
authority stopped, the next dirty-page flush reached hot version 14, but
PostgreSQL returned `checkpoint request failed`; stable version 13, term 3,
index 4, and the manifest digest remained unchanged. The local heap SHA-256
remained
`3770217fa7ca29da2d79580fa5fd68616a9257d6460801f0a1ade6cfc078d7e8`.

Next decision: D89 must objectify through `B` and bind txLog deletion to the
published root. One database checkpoint root must still aggregate all relation
forks and recover the publication authority itself before the remote
empty-cache control can admit PostgreSQL WAL recycling.

## D89. Separate transaction authority from disposable page compute and pop txLogs only from a self-contained published base

Status: `[DECIDED]` and `[EXISTS]` for the bounded local single-relation proof,
`[ACTIVE-WORK]` for incremental objectification and production recovery,
2026-08-24.

Decision: a page service configured for stable sync must use an external Cell
transaction authority. The authority process set owns the live transaction
version and outlives disposable PostgreSQL page-serving compute. The page
service may own RAM, local object-cache state, Range Engine views, and the
relation-specific txLog processes used by this prototype, but it may not reset
transaction history during a post-pop restart.

At stable version `B`, the page service reads every selected relation page at
one unchanged objectKV version, adds the authoritative relation extent, and
materializes a complete immutable SlateDB base. The base has a versioned
database path, `minimum_readable_version = B`, the final Cell chain digest, and
a digest of the complete visible relation rows. A versioned base descriptor is
written first. One atomic `postgres-root.json` replacement then selects it
locally, so a crash cannot pair a new PostgreSQL descriptor with an old base
descriptor.

The stable manifest serializes the same relation-domain snapshot as a
`CellStateSnapshot` plus PostgreSQL WAL and object-closure fields. This lets the
generic txLog protocol verify the exact published manifest, Cell identity,
generation, and frontier. After replicated prepare, publish, and linearizable
read-back, a quorum of pinned publication-authority signers issues one pop
capability. Every required txLog process durably advances `popped_through` only
after validating that capability and returns an Ed25519 attestation. The page
service accepts the cleanup receipt only when every required log-set
certificate verifies. Publication success defines PostgreSQL stable state;
cleanup failure is recorded separately and never weakens the selected root.

An older published root remains valid after the local base advances. Startup
authenticates that archived root as a self-contained immutable closure instead
of asking the newer active base to reconstruct an older frontier. This was a
required correction found by the first post-pop write and checkpoint run.

Optimizes for: an explicit compute/storage split, finite txLog retention,
atomic local root selection, and one cryptographic chain from PostgreSQL WAL
coverage to object closure to deletion permission.

Gives up: efficient checkpoint cost in this version. Every stable sync scans
and rewrites the complete selected relation while holding the page-service
state lock. Historical object bases are not collected. The transaction and
publication authority harnesses remain same-host, ephemeral controllers; the
publication authority cannot restart from this proof. txLog processes are
still page-service children, and only one existing main-fork relation is
covered.

Evidence: a three-page PostgreSQL 18.6 relation advanced from version 5 to 11,
published manifest
`1c99d2ca34b56f9b671de363ad810b00eade6e05580e571dacb191d889a9ca5b`,
and durably popped both three-node txLog sets through 11. All six nodes recorded
the same frontier. With the source heap path absent, a new page-service process
recovered base 11 with zero authenticated tail records and returned the exact
row. A post-restart checkpoint advanced through versions 12 and 13; the second
service restart recovered base 13 with zero tail records and returned
`objectified-stable-v1` and `post-pop-restart-v3`.

The first full three-page debug checkpoint took 1.980 seconds, including 703 ms
in sync. A no-new-page checkpoint that rebuilt and published base 12 took 440
ms, including 435 ms in sync. One dirty-page checkpoint through base 13 took
810 ms, including 448 ms in sync. These numbers expose the full-rewrite cost;
they are not a production performance claim.

After the publication authority stopped, a committed PostgreSQL update flushed
hot version 14 and built a local base 14, but checkpoint returned an error after
the five-second prototype timeout. Stable remained 13 at authority index 8,
and all six txLog nodes remained popped only through 13 while retaining the
version-14 suffix. Local objectification alone therefore did not authorize
deletion or falsely advance PostgreSQL stable state.

Next decision: replace complete-relation checkpoint rewrites with incremental
objectification outside the foreground lock, persist and recover both
authorities across independent hosts, aggregate all relation forks in one
database checkpoint root, and prove empty-cache remote restore before enabling
PostgreSQL WAL recycling.

## D90. Publish stable PostgreSQL roots from a lagging object base and objectify only captured checkpoints

Status: `[DECIDED]` and `[EXISTS]` for the bounded local single-relation proof,
`[ACTIVE-WORK]` for incremental objectification and production scheduling,
2026-08-24.

Decision: stable target `B` does not require a newly materialized base at `B`.
The authority-selected root may name immutable base frontier `O <= B` plus the
complete certified txLog suffix `(O, B]`. PostgreSQL checkpoint sync may finish
after that root is replicated and read back. txLog pop may advance only through
`O`, never through `B`, until a later stable root selects a newer base.

Checkpoint capture owns the complete-relation objectification trigger in this
prototype. It captures an immutable reader and all durable planning inputs at
one version, then releases the bridge-state mutex. Relation scanning and object
materialization run in one single-flight worker with at most one newest pending
capture. Page-write acknowledgement does not schedule objectification. A later
checkpoint may atomically activate the newest ready base `R`, provided the
current base is no newer than `R` and `R` is no newer than the hot target.

This supersedes only D89's synchronous scheduling requirement. D89's external
transaction authority, root identity, object-closure verification, atomic base
selection, and publication-authorized deletion rules remain in force.

Optimizes for: keeping complete relation scans and immutable object writes off
the checkpoint publication lock, bounding full-base creation to checkpoint
captures instead of page writes, and allowing stable progress while
objectification lags.

Gives up: zero-tail stable roots on every checkpoint. Until the newer base is
selected, restart replays the certified suffix and txLog storage remains pinned
above `O`. The prototype still rewrites the complete relation once per captured
checkpoint, retains historical bases, and serializes stable authority I/O under
the page-service mutex. This is not incremental objectification.

Evidence: candidate `54d2510` first proved that stable `B=10` could name base
`O=9` plus one certified record, pop only through 9, and restart without the
source heap. It was retained as a safety result but rejected as a performance
shape because every page write scheduled a full base and publication-authority
timeout inflated objectification from about 400 ms to 6.4 s.

Candidate `171b14c` moved planning inputs into the checkpoint capture and
removed the page-write trigger. In a fresh three-page PostgreSQL 18.6 run, the
first stable root published `B=9/O=5`; the next activated base 9 and published
`B=10/O=9`. Complete base materialization took 90 ms and 75 ms. A replacement
page service recovered `B=10` from base 9 plus one certified record while the
source heap path did not exist. With the publication authority unavailable,
hot state advanced to 11, stable and pop stayed at 10 and 9, and the captured
base 11 completed in 26 ms while stable sync waited 6.044 seconds to fail. A
separate three-write checkpoint that never reached stable capture created no
new base, confirming that page writes no longer schedule full rewrites.

Next decision: replace the complete checkpoint-captured relation rewrite with
range or delta objectification, move publication I/O outside the state lock,
garbage-collect unreachable historical bases, then measure lag and retained
txLog bytes under concurrent multi-relation checkpoints.

## D91. Keep immutable object deltas, discard the v1 payload as a production format

Status: `[DECIDED]` and `[EXISTS]` for the bounded local object-delta contract,
`[ACTIVE-WORK]` for compact encoding, layer compaction, and remote proof,
2026-08-24.

Decision: a checkpoint may advance object frontier `O` by selecting one ordered,
content-addressed immutable delta above complete base frontier `F`. The durable
root names the complete base and every delta descriptor. Restart authenticates
that lineage before serving, and publication-authorized txLog pop remains capped
at `O`.

Keep this architecture and its identity, chain, closure, restart, and deletion
rules. Do not keep v1's nested JSON payload, full commit envelopes, and repeated
per-record certificates as the target storage format.

Optimizes for: bytes proportional to changed history instead of untouched
relation size, immutable publication, exact source-free reconstruction, and a
clean separation between flush and later compaction.

Gives up: an immediate production encoding. The v1 correctness format has high
changed-byte amplification, one layer of restart work per capture, and no
compaction or garbage-collection policy.

Evidence: candidate `fc88122` ran five deterministic high-entropy seeds over a
128-page, 1 MiB relation with one changed 8 KiB page. Correct append and restart
kept. Missing object, corrupt object, broken chain, omitted closure, pop ahead,
and replacement full-base controls all discarded with exact replay.

The optimized local median delta was 11.106x the changed page but only 8.31
percent of replacement full-base bytes. Delta materialization took 11.08 ms,
activation took 12.05 ms, and source-free reopen took 9.77 ms. The full-base
reference materialized in 20.31 ms. Delta materialization plus activation took
23.13 ms, so the 1 MiB case has a byte advantage but no end-to-end latency
advantage yet.

Next decision: measure the relation-size crossover, changed-page batching knee,
and restart cost across layer count. A compact binary v2 must reach at most 2x
changed bytes without weakening any v1 corruption or restart gate.

## D92. Keep base plus delta after the relation-size crossover

Status: `[DECIDED]` and `[EXISTS]` for the local five-seed curve,
`[ACTIVE-WORK]` for worker-ready latency, compact encoding, remote objects, and
layer growth, 2026-08-24.

Decision: keep the immutable full base plus selected delta lineage as the
PostgreSQL objectification shape. Do not require a full-base rewrite during a
checkpoint or before a worker can serve one point.

Optimizes for: changed-history write cost independent of untouched relation
size, immutable publication, exact source-free reconstruction, and a measured
path to outperforming replacement bases as relations grow.

Gives up: treating current activation and restart as production-ready. At 512
MiB, delta materialization plus activation took 1.138 seconds and source-free
full-snapshot verification took 4.549 seconds. The complete eval process used
3.26 GB maximum RSS. Those costs must be split into readiness, first-read, and
offline verification phases.

Evidence: candidate `efa9d54`, five deterministic high-entropy seeds, one
fixed 8 KiB changed page, and relation sizes 2, 128, 4,096, and 65,536 pages.
The delta remained byte-identical within each seed. Its delta-to-rewrite time
ratios were 1.788x, 1.134x, 0.3359x, and 0.2502x. Byte ratios were 336.07
percent, 8.308 percent, 0.2622 percent, and 0.01639 percent. Every correct hard
gate passed, and the full-base-in-candidate-root control discarded.

The JSON v1 payload remains rejected because it writes 11.106x the logical
changed page. This decision admits the architecture, not the encoding.

Next decision: separate worker-ready, first-point, first-range, and full-oracle
scan timing. Then run the same cache-state matrix on GCS before claiming fast
replacement-worker recovery.

## D93. Keep lazy manifest-bound readiness as the performance shape, retain eager production audit

Status: `[DECIDED]` and `[EXISTS]` for the local OS-warm phase curve,
`[ACTIVE-WORK]` for provider-bound integrity and GCS cache-state proof,
2026-08-24.

Decision: keep replacement-worker readiness, first read, full-snapshot oracle,
and complete physical-closure audit as separate phases. Keep the experimental
manifest-bound open seam. Do not change the production helper, which still
audits every live SST before returning.

Optimizes for: relation-size-independent compute recruitment, exact first-read
measurement, bounded worker memory, and putting complete byte validation on the
correct performance curve.

Gives up: claiming that current local lazy serving is production-safe. Before a
worker returns remote rows, the selected root must bind provider generation and
checksum identity, touched blocks must authenticate cryptographically, or the
local copy must already be authenticated against that root.

Evidence: candidate `e2c9dd5` ran five process-isolated release samples at 128,
4,096, and 65,536 PostgreSQL pages. Physical closure grew from 1.09 MB to
555.04 MB. View readiness grew only 2.04x, from 2.33 ms to 4.75 ms. First base
point reads stayed between 0.142 and 0.181 ms, first eight-page ranges stayed
between 0.570 and 0.621 ms, and median worker RSS stayed below 68 MiB. The
complete audit grew from 2.12 ms to 1.046 seconds and the full oracle from 8.88
ms to 4.493 seconds, as expected for whole-relation work. Changed-manifest,
changed-delta, and skipped-audit controls all discarded.

Next decision: bind GCS generation and checksum metadata into publication
authority, then replay metadata-warm, persistent-cache-warm, and cold-cache
first-read curves. Do not claim economical cloud operation until GET count,
transferred bytes, cache hit rate, and dollar cost are measured.

## D94. Bind lazy immutable reads to provider revision plus application digest

Status: `[DECIDED]` and `[EXISTS]` for local identity controls,
`[ACTIVE-WORK]` for GCS cache-state and cost curves, 2026-08-24.

Decision: admit a version-2 provider-bound range root that selects the provider
namespace, exact object revision, length, and publication-time SHA-256 for the
manifest and every live SST. A lazy Range Engine may use that root only through
a read-only facade that applies the selected revision to every touched GET and
refuses objects outside the closure. Version-1 roots remain eager-audit-only.

For GCS, use immutable generation as the provider revision and retain
objectKV's application SHA-256. Do not claim provider CRC32C because Apache
`object_store` 0.14.1 does not expose it in the GCS read result.

Optimizes for: relation-size-independent worker readiness, exact same-key
overwrite fencing, portable application identity, and keeping whole-closure
verification off first-read latency.

Gives up: a storage-Byzantine threat model and cryptographic authentication of
an individual range response. A provider that can mutate bytes within one
generation requires authenticated blocks. Provider-bound reads also retain a
remote dependency on every cache miss.

Evidence: RFC 0066 freezes the cache-state, request, byte, cost, OTel, and six
negative-control contracts. Candidate `35ef183` adds byte-exact v1 and v2
fixtures, publication-time full-byte binding, canonical provider-closure
digests, a read-only exact-revision object facade, and the lazy open seam. Four
focused tests pass. They prove exact range success, refusal after a same-bytes
overwrite changes revision, changed-byte refusal, provider-scope refusal, and
old/new reader behavior.

Current economics: GCS Standard single-region flat-namespace Class B
operations list at $0.0004 per 1,000. One remote GET per read is therefore
$0.40 per million reads, while the frozen $0.01 per million warmed-read target
requires 97.5 percent cache hits under a one-GET miss model. Class A operations
list at $0.005 per 1,000, so one object PUT per transaction is rejected as an
architecture regardless of local latency.

Next decision: run metadata-warm, persistent-NVMe-warm, and empty-object-cache
curves on GCS. Continue if a cold 8 KiB point remains at or below eight GETs,
512 KiB, and 100 ms in-region p99, and if the workload's measured cache hit rate
fits the declared cost target.

## D95. Keep the provider-bound cached-read shape after the local executable curve

Status: `[DECIDED]` and `[EXISTS]` for the five-seed local versioned-store
curve and six controls, `[ACTIVE-WORK]` for GCS and realistic cache-hit curves,
2026-08-24.

Decision: keep exact provider-revision reads behind RAM and persistent NVMe.
Do not put object-store reads on every OLTP operation, and do not make a cloud
performance or economics claim from the local result.

Optimizes for: sub-millisecond cached point reads, bounded cold-start object
work, exact same-key overwrite fencing, and disposable compute whose durable
rebuild state remains on objects.

Gives up: offline serving and a zero-metadata remote dependency. Persistent
NVMe still required two metadata GETs during reopen. A cache miss still pays
one or more object requests, and the observed 100 percent post-warmup hit rate
came from repeating a 128-point working set.

Evidence: candidate `ae515ec`, frozen suite
`provider-bound-range-read-v0`, five 32 MiB process-isolated release seeds, and
one replay per state. Persistent-NVMe-warm first point was 83.8 us p50;
metadata-warm was 0.362 ms; empty-cache was 0.339 ms. Warm p99 medians were 84
to 86 us. Empty-cache activation through first point used exactly eight
revision-checked GETs and 380,519 bytes. The working-set fill used about 38 to
40 GETs and 2.35 MB, after which 1,000 measured reads issued zero provider
GETs. Changed generation, same bytes at a new generation, missing revision,
changed bytes, changed namespace, and skipped revision enforcement all
discarded.

Next decision: authenticate to `objectKV-dev`, run the same matrix on GCS with
OTel and scratch cleanup, then measure cache capacity and reuse distance. Stop
or redesign if in-region empty-cache p99 exceeds 100 ms, activation plus first
point exceeds eight GETs or 512 KiB, or realistic hit rate misses the declared
cost envelope.

## D96. Admit the real GCS runner, retain the cloud-claim gate

Status: `[DECIDED]` and `[EXISTS]` for the executable GCS path,
`[ACTIVE-WORK]` for the first authenticated cloud receipt, 2026-08-24.

Decision: replace the provider-bound suite's GCS discard stub with the same
process-isolated Range Engine path used by the accepted local curve. Each
worker receives one validated scratch prefix, writes its SlateDB base through
Apache `object_store`, binds every immutable manifest and SST to its GCS
generation, serves only exact-generation GETs, and deletes every live scratch
object before returning. The controller repeats cleanup after a worker error
or timeout.

Optimizes for: one comparable local and cloud contract, fail-closed generation
identity, bounded leaked live objects after process failure, OTel-required cloud
runs, and request cost computed from the pinned GCS price snapshot.

Gives up: claiming that live-name cleanup immediately removes retained bytes.
Bucket versioning and soft delete may retain noncurrent or deleted generations
after the scratch prefix lists empty. The first cloud receipt must report this
policy as retained-storage cost, or the eval bucket must adopt a separate
scratch retention policy. It also gives up a cloud performance claim until the
project, bucket, compute region, and telemetry collector are verified.

Evidence: candidate `be78904` maps
`gcs-generation-bound-process` into the provider-bound worker, adds guarded
per-process prefixes, exact GCS namespace and generation binding, worker and
controller cleanup, a zero-live-object cleanup gate, and the pinned regional
Standard Class B request price of $0.0004 per 1,000 operations. Six focused
worker tests pass, including all six provider controls and refusal of an
unguarded GCS scope before network I/O. The frozen suite validates with nine
workloads and requires metrics, traces, and logs for `gcs-dev`. A local dirty
diagnostic preserved every hard gate but is not a comparable performance
result.

The cloud run remains unexecuted. The selected gcloud operator session cannot
refresh its token noninteractively, so project and bucket existence are still
unverified. The GitHub repository is also not published: the local Git
repository has no remote, and `Doss-com/objectKV` does not exist.

Next decision: reauthenticate, verify or create the guarded `objectKV-dev`
project and bucket, run the three correct cache states plus all six controls
from `us-central1`, and record live-object cleanup plus retained-generation
storage policy. If the cold curve passes, freeze cache reuse-distance and
worker-churn experiments. If it fails, redesign activation metadata before
further PostgreSQL work.

## D97. Publish a clean objectKV source snapshot, retain research history locally

Status: `[DECIDED]` and `[EXISTS]`, 2026-08-24.

Decision: use candidate `48f0a3b` as the source boundary for the first public
`Doss-com/objectKV` `main` branch. Publish one clean root snapshot rather than
the 297-commit local research history. Exclude the internal DOSSBOT tracker and
user-specific local paths from that snapshot. Preserve the complete research
branches and their exact experiment commits in the local repository.

Optimizes for: a reviewable public provenance boundary, a small contributor
clone, no accidental publication of internal workflow metadata, and one
default branch whose checked-in format, strict clippy, tests, and eval smoke
contracts pass at the selected source.

Gives up: public commit-by-commit provenance for the pre-launch research phase.
The accepted receipts still name their candidate commits, but those objects
remain in the private local research history unless a later decision publishes
an explicit history archive.

Evidence: the selected source contains an Apache-2.0 license, contribution and
security policies, issue and pull-request templates, 346 tracked files totaling
7.1 MB, and no tracked credential or private-key filenames. Candidate
`48f0a3b` passes `cargo fmt --all -- --check`, strict workspace all-target
clippy, the complete workspace test suite, and `okv-eval smoke`. GitHub reports
that the authenticated owner can create repositories in `Doss-com`, and the
`Doss-com/objectKV` name is available.

Completion gate: create the clean public snapshot, rerun its exact checks,
create the public repository, push only that snapshot to `main`, and verify the
first hosted CI run before opening contributor issues.

Launch receipt: `Doss-com/objectKV` is public. Candidate `a1ada58` passes the
complete hosted Linux CI job. Linux exposed two launch-only harness defects:
one macOS-specific process import lacked a target guard, and the publication
fault harness required survivor 102 to lead even when survivor 103 had already
won a valid election. Both are repaired without changing a frozen eval or
protocol rule. Secret scanning, push protection, Dependabot security fixes,
private vulnerability reporting, issues, and discussions are enabled.

Next decision: route the first bounded contributor issues through the existing
RFC and eval contracts, then run the provider-bound cache-state matrix on the
authenticated `objectKV-dev` GCS playground.

## D98. Admit the in-region GCS read shape only behind RAM and NVMe

Status: `[DECIDED]` and `[EXISTS]` for the frozen five-seed cache matrix and six
identity controls, `[ACTIVE-WORK]` for production hit-rate and throughput
curves, 2026-08-24.

Decision: continue the object-native architecture with GCS as authoritative
rebuild storage and persistent NVMe plus RAM as the serving tiers. Do not place
a GCS data request on the steady-state OLTP path. Keep worker recruitment,
authenticated view open, first data miss, and cache-hit latency as separate
curves.

Optimizes for: sub-millisecond cached reads, disposable compute, exact
generation-bound recovery, and capacity economics that can move cold bytes out
of replicated local storage.

Gives up: treating object storage as a direct substitute for RocksDB latency.
An in-region data miss still costs 29.4 to 53.4 ms in this fixture, one logical
8 KiB point reads a 64 KiB cache part, and full empty-cache worker-to-row
latency is 431.1 ms median. The experiment repeats a 128-point working set, so
it does not prove the frozen 97.5 percent production hit-rate target.

Evidence: candidate `257fe2a`, suite `provider-bound-range-read-v0`, GCS bucket
`doss-objectkv-dev-okv-evals`, and one ephemeral `n2-standard-8` runner in
`us-central1-a`. Empty-cache first point was 48.6 ms median with 4.8 ms median
absolute deviation and 53.4 ms maximum. Metadata-warm but data-cold was 40.8 ms
median. Persistent-NVMe first point was 294.5 us median and its eight-point
range was 1.504 ms median, both with zero serving-path provider reads. Warm p99
medians were 245 to 284 us. Every exact, replay, request, byte, memory, and
cleanup gate passed. Changed generation, same bytes at a new generation,
missing revision, changed bytes, changed namespace, and skipped revision
enforcement all discarded. OTel exported metrics, traces, and logs.

The run deleted every live scratch object, but bucket versioning and soft
delete retained 218 generations totaling 1,464,840,385 bytes. Frequent evals
need a separately governed short-retention scratch policy. This storage result
does not alter the frozen read suite.

Next decision: freeze a cache-capacity and reuse-distance matrix with realistic
skew, concurrency, worker churn, and larger datasets. In parallel, measure
sustained objectification and compaction. Continue only if a practical RAM and
NVMe budget holds the required hit ratio and if request, byte, compute, and
storage costs beat the replicated-local incumbent for a named workload.

## D99. Freeze cache economics before changing cache policy

Status: `[DECIDED]` and `[EXISTS]` for the RFC and eval contract,
`[ACTIVE-WORK]` for implementation and first local curve, 2026-08-24.

Decision: freeze RFC 0067 and suite `provider-bound-cache-economics-v0` before
changing cache admission, eviction, part size, or prefetch. Measure uniform,
Zipfian `0.99`, and moving-hotset traces against persistent-NVMe capacities of
1, 5, 10, and 25 percent of logical data. Keep decoded RAM fixed separately.
Use provider miss ratio as the sole primary metric.

Optimizes for: exposing whether the admitted GCS shape is economical under a
fixed physical budget, preserving exact provider identity, and producing a
curve that cache-policy candidates can improve without changing workload or
hardware.

Gives up: claiming that one synthetic trace predicts every PostgreSQL, Redis,
or search workload. The first churn point reopens decoded views in one process
while retaining persistent cache; it does not prove replacement-process or
cross-host cache reuse.

The frozen request-cost threshold is a 2.5 percent provider miss ratio. This is
equivalent to $0.01 per million logical reads when every miss costs one Class B
GET under snapshot `gcs-us-central1-standard-2026-08-24`. The separate latency
goal is stricter because a 40 to 50 ms miss must occur in fewer than one percent
of reads to stay outside a sub-millisecond p99.

Four unsafe controls disable the persistent-cache bound, skip the exact-result
oracle, skip provider-revision enforcement, or perturb replay. A semantically
correct workload that exceeds the economic threshold is still discarded and
recorded. Do not increase cache capacity, runtime, or hardware to turn that
result green.

Next decision: implement the frozen process worker without changing the suite,
run the local curve and controls, then select only representative boundary
points for GCS. If 25 percent persistent capacity cannot approach the target on
the named skewed or moving-hotset trace, revisit workload scope before cache
optimization.
