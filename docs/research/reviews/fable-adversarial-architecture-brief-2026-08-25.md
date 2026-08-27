# Fable adversarial architecture review brief

Work read-only in `/Users/wileyjones/Documents/doss/repos/okv`. Do not edit
files, change Git state, or run mutating commands. Inspect the current working
tree, including uncommitted files, because the review target is the live
architecture candidate rather than only `HEAD`.

The target at invocation time is branch
`research/object-publication/process-recovery-v1`, `HEAD`
`a56442ad800deedd72a404a0886e88831eb308a0`, with a materially dirty working
tree. Distinguish committed, code-complete, verified, evaluating, proposed, and
future claims. Follow `AGENTS.md` and `docs/STATUS-TAXONOMY.md`. Do not use
`[EXISTS]` or `[ACTIVE-WORK]` in new conclusions.

## Required source inspection

Read primary repository sources before forming the verdict:

- `README.md`, `CONTEXT.md`, `program.md`, and `Cargo.toml`;
- `docs/PRODUCT-SPEC.md`, `docs/PRODUCT-SPEC-SHEET.md`,
  `docs/SYSTEM-SHAPE.md`, `docs/ARCHITECTURE-MAPS.md`,
  `docs/BOOTSTRAP-PLAN.md`, `docs/BIDEC-EVAL-PROGRAM.md`,
  `docs/DECISIONS.md`, `docs/EVALS.md`, `docs/LOG-ARCHITECTURE.md`,
  `docs/POSTGRES-PATH.md`, and `docs/OBJECT-STORE-SUPPORT.md`;
- `docs/PLAYGROUND-GOLDEN-PATH.md`, `docs/PROJECT-TRACKING.md`,
  `docs/research/playground-g0-g6-architecture-review-2026-08-25.md`,
  `docs/research/playground-golden-path-2026-08-25.md`,
  `docs/research/resident-hot-path-g3.1.md`, and
  `docs/research/resident-hot-path-prototype.md`;
- RFC-0002 through RFC-0011, RFC-0014 through RFC-0020, and RFC-0023 through
  RFC-0025. Read other RFCs when a cited contract depends on them;
- `evals/programs/objectkv-product-thesis-v1.toml`,
  `evals/programs/objectkv-golden-path-v1.toml`, both playground scenarios,
  relevant suites, `evals/metrics.toml`, result schemas, and
  `experiments/ledger.jsonl` receipts cited as evidence;
- current implementation in `crates/okv-model`, `okv-log`, `okv-wal`,
  `okv-consensus`, `okv-object`, `okv-publication`, `okv-app-history`,
  `okv-eval`, `okv-htap`, `okv-sim`, and the playground composition code.

Use the August 22 and August 25 Fable reviews only as prior hypotheses. Do not
repeat them without checking whether the current tree resolved, narrowed, or
invalidated each claim.

## Adversarial question

Should DOSS continue building objectKV as its own object-native transactional
kernel, or stop and build the product on a simpler substrate such as TiKV,
FoundationDB, RocksDB plus an object tier, or PostgreSQL plus disaggregated
storage?

Attack the design at the level where a correct local proof can still compose
into an impossible or economically losing production system. Cover all of the
following:

1. Semantic and correctness impossibilities, including version assignment,
   strict serializability, range and predicate conflicts, retry identity,
   generations, retention, branches, and snapshot claims.
2. Consensus and durability gaps, including quorum acknowledgement, stable
   media, generation takeover, txLog truncation, objectification debt,
   multi-range commit, independent failure domains, regional loss, and
   `commit_unknown` behavior.
3. Object publication and garbage collection, including recursive closure,
   ambiguous effects, fencing, pins, reservations, branch roots, LIST
   semantics, compaction, backup, restore, and leak or premature-delete modes.
4. Resident RAM and SSD serving, including admission, eviction, object misses,
   hydration, profile handoff, hot-key behavior, RPC and concurrency costs,
   rebuild debt, memory economics, and whether one contract can honestly cover
   both profiles.
5. Point-read and HTAP performance cliffs, including manifest and index work,
   cold GET count, small values, skew, range scans, page-native PostgreSQL,
   exact base-plus-tail overlays, tail growth, invalidation, materialization,
   and bounded memory.
6. Multi-tenancy and cell boundaries, including noisy neighbors, transaction
   domains, metadata authority, tenant deletion, encryption, migration,
   cross-cell behavior, global services, and the path to MultiRaft.
7. Operational complexity, including upgrades, mixed versions, observability,
   capacity limits, overload, cloud failures, repair, disaster recovery,
   supportability, and the number of independently correct control loops.
8. API and taxonomy conflicts across docs, RFCs, code, and evals. Identify
   places where two names imply one concept, one name hides two concepts, or a
   status or gate overstates what the receipt proves.
9. Whether the current technology tree is ordered by decisive falsifiers. Ask
   whether G0 through G6, the playground GP-G0 through GP-G6 ladder, and the
   product golden path prove the risky composition early enough, or allow the
   team to spend months proving components before the product thesis can fail.

## Required output

Return one structured Markdown review. It must contain:

1. A one-sentence verdict with a confidence level and the single decisive
   reason.
2. A source ledger that separates observed repository facts from inference,
   with file and line citations for load-bearing claims.
3. The strongest build case. Explain why owning objectKV could be rational,
   what advantage incumbents cannot cheaply copy, and the minimum architecture
   that preserves that upside.
4. The strongest do-not-build case. Name the simpler alternative, what it gives
   up, and why the trade is still better.
5. A ranked list of existential risks. For each risk, give mechanism, current
   evidence, missing evidence, earliest decisive test, and stop or pivot gate.
6. A cross-layer contradiction matrix for the nine attack areas above. Prefer
   concrete failure traces over generic concerns.
7. A G0 through G6 and GP-G0 through GP-G6 evidence audit. State exactly what
   each verified receipt proves, what it does not compose with, and whether the
   status taxonomy is honest.
8. A technology-tree critique. Identify gates that are too early, too late,
   redundant, non-decisive, or missing. Then propose a reordered tree optimized
   for maximum product-thesis learning per engineering week.
9. A ranked experiment plan with hard numeric or binary gates. Each experiment
   needs invariant, workload and fault model, candidate and control, primary
   metric, correctness gates, cost accounting, stop condition, and the decision
   it resolves. Include an integrated three-host plus object-backend slice,
   admitted SSD versus RAM, cold indexed point reads, outage debt, branch and GC
   under failure, multi-range coordination, PostgreSQL authority mapping, and
   exact HTAP tail scaling.
10. Concrete technology-tree changes. Name exact docs, RFCs, eval programs,
    gates, and code boundaries to add, delete, merge, split, or relabel. Do not
    implement them.
11. A final call among `BUILD NARROWLY`, `PIVOT SUBSTRATE`, or `STOP`, with the
    next three gates and the evidence that would reverse the call.

Be adversarial without being theatrical. Do not reward the quantity of local
proofs. Treat a verified component result as insufficient until its composition
boundary is explicit. Also do not dismiss the design merely because it is hard.
Steelman both sides before making the call.
