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

Status: `[PROPOSED]` after the first independent review.

Decision: build an exact, seeded, virtual-time simulation harness before the
replicated WAL. Every distributed component must run under it, and a failing
seed must replay exactly before that component can merge.

Optimizes for: reproducible generation recovery, fencing, retry, and watermark
failures before they become multi-process incidents.

Gives up: shipping the happy-path WAL first. This adds an earlier systems-test
investment but removes a larger late recovery retrofit.

## D15. Acknowledgement and lag contract

Status: `[PROPOSED]`, required before Gate 2.

Decision: `COMMITTED` means quorum-fsynced in the declared WAL topology. `C` is
the committed watermark and `O` is the object-durable watermark. The contract
must publish regional RPO, `commit_unknown` behavior, a hard retained-WAL bound,
and the `C - O` thresholds for ratekeeping, commit refusal, and recovery.

Optimizes for: an honest durability claim and bounded behavior during object
store brownouts.

Gives up: describing object storage as authoritative for the unobjectified WAL
suffix or allowing unbounded commit progress during an object-store outage.

## D16. Control-plane bootstrap authority

Status: `[PROPOSED]`, unresolved.

Decision: choose and specify one bootstrap authority for range maps, epochs,
generations, and durable watermarks before range distribution work. Candidate
shapes are a small dedicated consensus service or a system keyspace with an
explicit coordinator and recovery bootstrap.

Optimizes for: eliminating circular recovery and ensuring stale owners can be
fenced at manifest publication.

Gives up: treating control metadata as an implementation detail that can be
placed inside the transaction system later.
