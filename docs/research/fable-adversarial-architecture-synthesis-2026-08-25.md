# objectKV adversarial architecture synthesis

Review date: 2026-08-25. Recommendation: `[EVALUATING]`.

## Clarity

Question: Should DOSS keep building objectKV as its own transactional kernel?

Punchline: Continue only as a narrow falsification program; make strict
serializability, real-host plus cloud composition, and resident-serving
economics the next three gates.

Counter: If those gates pass, owning the kernel preserves a version-integrated
object base that FoundationDB, TiKV, and PostgreSQL cannot reproduce cheaply.

Next: Stop adding leaf publication proofs and execute the external
serializability-oracle gate before more physical or distributed scope.

Confidence: Medium. Fable returned `BUILD NARROWLY` at 0.6 confidence, and the
current tree explicitly says its verified playground rungs do not compose into
one production cell.

## Review receipt

- Raw review:
  `docs/research/reviews/fable-adversarial-architecture-review-2026-08-25.md`.
- Brief:
  `docs/research/reviews/fable-adversarial-architecture-brief-2026-08-25.md`.
- Tool: OpenCode CLI 1.18.20.
- Model: `anthropic/claude-fable-5`.
- Agent: read-only `plan` agent.
- Target: `/Users/wileyjones/Documents/doss/repos/okv`, branch
  `research/object-publication/process-recovery-v1`, `HEAD`
  `a56442ad800deedd72a404a0886e88831eb308a0`, live dirty tree.
- Session: `ses_fc342493effe55dc07M0a8iGPT`, exit 0.

Exact successful command:

```text
opencode run --print-logs --model anthropic/claude-fable-5 --agent plan --format default --title "objectKV adversarial architecture review 2026-08-25" "Execute the attached review brief. Inspect the repository files directly. Return the full structured Markdown review in your response. Do not edit files." --file docs/research/reviews/fable-adversarial-architecture-brief-2026-08-25.md
```

The runtime log explicitly recorded `providerID=anthropic`,
`modelID=claude-fable-5`, `agent=plan`, and `mode=primary` before inference.

## Evidence-backed readout

1. The semantic center remains unresolved. RFC-0008 is still draft and leaves
   conflict representation, read-version authority, commit-unknown behavior,
   and multi-resolver recovery open (`rfcs/0008-transaction-isolation.md:3` and
   `:35-45`). The current commit envelope serializes conflict bytes but does not
   evaluate them (`crates/okv-sim/src/commit.rs:84-85`).
2. Consensus evidence is real but bounded. The current process harness disables
   autonomous ticks, heartbeats, elections, and snapshots
   (`crates/okv-consensus/src/process_node.rs:190-193`). It does not constrain
   multi-host latency or availability under real failure detection.
3. The GP-G0 through GP-G6 ladder is honest at the leaf. It explicitly states
   that no rung inherits proof and the receipts do not prove one continuously
   integrated production cell (`docs/PLAYGROUND-GOLDEN-PATH.md:49-51`).
4. The product thesis remains exposed at G3 through G6. Resident serving is
   evaluating; cold reads, branches, and multi-range cell behavior remain
   proposed (`evals/programs/objectkv-product-thesis-v1.toml:149`, `:196`,
   `:228`, and `:250`).
5. The repository already names the right stop condition: pivot to TiKV or
   RocksDB if one focused cycle cannot satisfy G3, G4, and one leverage gate
   (`docs/BIDEC-EVAL-PROGRAM.md:303`). The missing move is sequencing the
   cheapest semantic falsifier before more publication depth.

## Steelman cases

| Case | Best argument | Tradeoff |
|---|---|---|
| Build narrowly | One version authority can join `ObjectState(O)`, `txLog(O, C]`, disposable serving, branches, GC, and exact HTAP. That integrated invariant is the defensible advantage. | DOSS owns distributed transaction correctness, recovery, membership, and a large operational surface. |
| Pivot substrate | Put the publication, branch, and HTAP layer over FoundationDB or TiKV. The hardest strict-serializable machinery arrives battle-tested while most object-native leverage survives. | Two retention and authority systems must be reconciled; version mapping and transaction limits become permanent adapter constraints. |

The do-not-build case wins immediately if the conflict design requires a global
throughput bottleneck or the real-host and cloud curves miss their controls.
The build case wins only after those results exist.

## Ranked existential risks and hard gates

| Rank | Risk | Decisive gate | Failure decision |
|---:|---|---|---|
| 1 | Strict serializability is assumed by downstream RFCs but not designed end to end. | 1,000 concurrent multi-range histories across frozen seeds, checked by an independent serializability oracle, zero anomalies. | `PIVOT SUBSTRATE` if conflict representation needs cell-wide coarse tokens for product-shaped work. |
| 2 | Current quorum evidence is one-host and controller-driven. | Three real hosts and independent media versus TiKV at the same durability; commit p99 no worse than 1.25x after one optimization cycle. | `PIVOT SUBSTRATE` if the curve fails or acknowledged state is lost. |
| 3 | Cold point reads and reopen work may grow with database size. | 10 GiB and 10 M keys on GCS and S3; at most one data-range GET after index warmup; metadata bytes remain bounded. | `PIVOT SUBSTRATE` if GET count or reopen bytes scale with total data. |
| 4 | RAM may not earn a product role and the SSD wrapper may hide structural overhead. | SSD within 20% of direct RocksDB under RPC, concurrency, and writes; RAM improves one named end-to-end metric by at least 20%. | Remove RAM if its gate fails; pivot the serving base if SSD fails. |
| 5 | Outage debt, branch roots, and GC are proven separately but not under combined failure. | Bounded txLog debt during a 30-minute object brownout plus 100 branches and GC during publisher, sweeper, and generation failures. | Stop on any premature delete, unbounded debt, or full-dataset recovery. |

## Technology-tree changes

Sequence for maximum product-thesis learning per engineering week:

1. Promote RFC-0008 and the external serializability oracle to the first gate.
2. Run existing conformance and scale paths on real GCS and S3, including the
   10 GiB cold indexed-read curve.
3. Pull GP-G7 forward as one three-host integrated slice with autonomous
   elections, real disks, a real object backend, and a TiKV control.
4. Run the admitted SSD versus RAM matrix under RPC, concurrency, writes,
   object misses, hydration, and profile handoff.
5. Only after those pass, run worker and sweeper depth, branch plus GC failure,
   HTAP tail scaling, and the PostgreSQL authority-mapping probe.

Park until then: more publisher micro-boundaries, MultiRaft, metacluster,
Redis, inverted search, and PostgreSQL compatibility implementation.

## Decisions

- D1. Continue narrowly. Optimize for decisive falsification, not the number of
  verified component receipts. Give up parallel feature breadth.
- D2. Keep publication, branch, and HTAP work as the preserved asset if the
  transaction kernel fails. Give up the assumption that a substrate pivot
  discards the whole program.
- D3. Treat GP-G0 through GP-G6 as bounded component evidence. Give up using the
  count of verified rungs as a proxy for production-cell progress.

No product code or canonical architecture contract was changed by this review.
