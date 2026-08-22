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
