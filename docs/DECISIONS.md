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
