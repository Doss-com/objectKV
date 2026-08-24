# objectKV architecture review readout

Status: `[ACTIVE-WORK]` review packet, 2026-08-23. Implementation work is
paused at commit `99df834b7bb62451ce6717f0cc657fd6b65e40e1`. The continuing
overnight strategy audit is read-only and pinned to an earlier clean commit.

## Answer

objectKV is ready for a bounded Architecture Review 1 focused on invariants,
claim boundaries, stop conditions, and the next falsifying experiments. It is
not ready for a production architecture freeze, a public FoundationDB
replacement claim, or a ZebraDB implementation commitment.

The current strategy is directionally coherent:

```text
bounded cell transaction authority
        +
replicated retained WAL suffix
        +
immutable object-native bulk state
        +
disposable serving and materialization workers
        +
exact analytical base-plus-tail reads at version T
```

The main risk is no longer whether these components can be described. It is
whether the composed system preserves the semantics while meeting recovery,
request-amplification, tail-cost, and hotspot-throughput envelopes. The review
should keep the thesis alive only as a sequence of falsifiable proofs.

## Strategy status

| Area | Status | Evidence | Current boundary |
|---|---|---|---|
| Cell transaction semantics | `[EXISTS]` local evidence | Actual-read conflict witnesses, range-phantom checks, causal read versions, partitioned resolvers, three ordered commit proxies, and online range split contracts | No independent-host or end-to-end cell capacity envelope |
| Durability and recovery | `[EXISTS]` local evidence | Authenticated tLog inventories, full generation replacement, staged takeover, absence-quorum abort, retained-outcome replay, objectification, and fresh serving from object base plus retained tLogs | Full-generation recovery remains the fallback; recovery topology and inventory work are not yet production-bounded |
| Object-native physical tier | `[EXISTS]` local evidence | Immutable publication, fenced root authority, fresh-instance reopen, SlateDB serving profile, separate compaction work, and pinned MinIO protocol fixture | GCS latency, request economics, throttling, brownout, and multi-host durability remain unmeasured |
| Resolver hotspot scaling | `[ACTIVE-WORK]` | The frozen RFC-0055 balanced diagnostic passed semantic and integrity gates and measured a 3.814 median split/source throughput ratio | The run used a dirty source and is inconclusive until a clean rerun, the full skew/crossing curve, and all five negative controls complete |
| Exact HTAP overlay | `[EXISTS]` bounded operator | DataFusion merges a Parquet base and ordered Arrow tail to one exact target version without unsafe predicate pushdown | Durable manifests, leases, interval reads, schema evolution, and a `T - W_p` cost curve remain open |
| PostgreSQL bridge | `[ACTIVE-WORK]` | A PostgreSQL 18.6 fork with a second `smgr` slot compiles, boots, and restarts | Real objectKV page callbacks, WAL/checkpoint barriers, truncate incarnation, crash recovery, and I/O economics remain open |
| Cell fleet and metacluster | `[PROPOSED]` | The topology separates tenant transaction domains, ranges, objects, cells, and fleet routing | No cross-cell transaction is intended; tenant placement and migration are future work |
| OSS launch | `[ACTIVE-WORK]` local only | Repository, RFCs, eval framework, OTel path, research loop, contributor board, and local project tracker exist | No remote is configured, Apache 2.0 is proposed, and the public repository has not launched |

## Early indicators

1. `[EXISTS]` The architecture is earning semantic confidence before throughput
   claims. Unsafe controls are first-class eval subjects, not informal test
   cases.
2. `[EXISTS]` The admitted transaction-system recovery curve is independent of
   permanent database size in the bounded local model. The 1 GiB and 1 PiB
   profiles recovered in 0.460 and 0.474 seconds while reading zero database
   bytes.
3. `[EXISTS]` Recovery work currently scales with retained authenticated tLog
   inventory. At 65,536 retained records per tLog, recovery took 3.158 seconds,
   with inventory scanning accounting for 2.870 seconds. That is the first
   measured recovery bottleneck.
4. `[ACTIVE-WORK]` Resolver splitting has a strong best-case diagnostic signal.
   The balanced run used 21 paired samples and equal logical work, with 172,032
   source and 172,032 split decisions. The split examined 694,177,554 history
   entries versus 1,391,853,330 for the source. The result is not admitted
   because it has not been repeated from a clean candidate or tested across the
   missed-boundary and crossing curve.
5. `[EXISTS]` Tigris supports the layering thesis, specifically transactional
   metadata around immutable object bytes, version-addressed caches, and atomic
   work intent. It does not support the replacement thesis because Tigris still
   delegates transactional authority to FoundationDB.
6. `[EXISTS]` Exact HTAP base-plus-tail semantics work as an operator. The open
   question is whether durable tail retention and snapshot acquisition remain
   bounded enough to be an operational database path.
7. `[ACTIVE-WORK]` The PostgreSQL storage seam is mechanically reachable. The
   authority mapping is still unresolved in code: PostgreSQL WAL must remain the
   only commit authority for the first page bridge.

These are positive continuation signals, not product-readiness signals.

## Architecture Review 1 scope

The review should decide six items.

### D1. Transaction and fleet boundaries

Proposed decision: a tenant database is the normal transaction domain inside
one bounded cell; a transaction may span its ranges, but may not cross cells.

Tradeoff: this preserves useful FDB-like transactions while bounding recovery
and failure radius. It gives up global atomic transactions and requires explicit
tenant migration.

### D2. Durable authority split

Proposed decision: replicated transaction logs and generation authority own the
unobjectified committed suffix; object storage owns permanent bulk state after
fenced publication. Object storage is never the coordination system.

Tradeoff: commits avoid synchronous object-store publication. The system must
bound retained WAL, objectification lag, and recovery work.

### D3. Recovery posture

Proposed decision: keep full transaction-system generation replacement as the
correct fallback. First optimize authenticated inventory summaries, checkpoint
cadence, parallel recruitment, and independent-host recovery rather than adding
partial-role recovery semantics.

Tradeoff: the fallback remains simple and auditable, but role loss causes a
larger coordinated recovery event.

### D4. PostgreSQL authority

Proposed decision: PostgreSQL WAL, LSN, and tuple MVCC remain authoritative in
the first bridge. objectKV begins as subordinate page materialization and object
durability, with a separate objectKV storage version.

Tradeoff: this protects PostgreSQL compatibility and recovery semantics. It does
not immediately prove PostgreSQL running directly on objectKV transactions.

### D5. ZebraDB analytical truth

Proposed decision: one commit history and one snapshot version remain the only
logical truth. Columnar layouts are derived bases; every exact query overlays a
durable live tail through `T` or returns `snapshot_unavailable`.

Tradeoff: freshness is exact, but materialization lag becomes query cost and
retention pressure.

### D6. Public claim boundary

Proposed decision: launch objectKV as a research kernel after repository,
license, contribution, and evidence hygiene are complete. Do not claim a
FoundationDB replacement, distributed PostgreSQL, or production HTAP database
until the corresponding gates pass.

Tradeoff: the narrower claim improves credibility and makes expert contribution
useful earlier. It gives up a larger launch narrative.

## Stop or narrow conditions

Stop the full architecture, or narrow the responsible layer, if any of these
conditions appears:

- any acknowledged loss, serializability violation, stale-generation commit,
  premature WAL pop, or incorrect empty-cache reconstruction;
- recovery work depends on total permanent database bytes rather than retained
  authority state and the unobjectified suffix;
- the clean RFC-0055 balanced curve misses its frozen conservative `1.10`
  split/source threshold, or any unsafe benchmark control passes;
- GCS request amplification, tail latency, throttling, or cost misses the first
  declared ceilings after one bounded layout-tuning pass;
- the PostgreSQL bridge requires a second commit decision, permits a page ahead
  of durable PostgreSQL WAL, or cannot preserve checkpoint and truncate rules;
- exact HTAP requires a second source of truth, silently mixes manifests, or
  needs unbounded analytical-tail retention;
- safe operation requires synchronous object publication on every commit.

## Work to stop during calibration

- Stop adding distributed roles, metacluster machinery, or broad consumer
  features until the current curves decide the existing design.
- Stop treating Redis, search, PostgreSQL, and HTAP as parallel launch tracks.
  They are pressure tests, with PostgreSQL and exact HTAP carrying the main
  architectural burden.
- Stop broad performance optimization before the owning metric, negative
  controls, and comparison envelope are frozen.
- Stop expanding public claims ahead of GCS, independent-host, PostgreSQL crash,
  and durable HTAP-source receipts.

## Recommended calibration order

When work resumes:

1. Complete the clean RFC-0055 resolver hotspot curve and all five controls.
2. Run the GCS physical, request-economics, throttling, and brownout gate in
   `objectKV-dev` once billing and credentials are available.
3. Route real PostgreSQL heap and index page callbacks through the bridge and
   execute the WAL, checkpoint, restart, and truncate crash matrix.
4. Put the exact HTAP operator behind durable manifests, snapshot leases, and
   interval-bounded analytical-tail reads; measure the `T - W_p` curve.
5. Repeat transaction-system recovery and objectification across independent
   hosts and failure domains.
6. Test compact authenticated tLog summaries, checkpoint cadence, parallel
   successor recruitment, and only then consider more transaction-system role
   partitioning.

This sequence optimizes for falsifying the object-native thesis before spending
more on consumer breadth. It gives up short-term feature velocity.

## Review format and output

Use one 90-minute review:

1. Ten minutes: claim boundary and current system map.
2. Twenty minutes: D1 through D5 invariant review.
3. Twenty-five minutes: evidence, missing links, and stop conditions.
4. Twenty minutes: rank the next three falsifying experiments.
5. Fifteen minutes: record decisions, owners, and claim changes.

The review is complete only when it records:

- accept, revise, or reject for D1 through D6;
- three funded experiments with primary metrics and hard gates;
- explicit stop or narrowing conditions;
- what work is deferred to keep those experiments on the critical path;
- the next review trigger, based on evidence rather than a date.

## Pause checkpoint

- Goal state: paused.
- Candidate: `99df834b7bb62451ce6717f0cc657fd6b65e40e1`.
- Worktree: clean at the time this packet was drafted.
- RFC-0055: implementation preserved; clean admission run and controls pending.
- Overnight semantic audit: running read-only, pinned to
  `22e4728e2a80e29399b45010f53c69ca3fc7de26`; cycle 1 reported 20 expected
  results and zero unexpected results.
- External reviews: Fable and two internal focused reviews complete; Kimi 3 is
  blocked before inference on OpenRouter authentication.
- Repository: local and unpublished, with no configured Git remote.
- Cloud playground: infrastructure definition exists; GCP project creation is
  blocked on account, billing, and credential availability.

## Review inputs

- `README.md`
- `docs/SYSTEM-SHAPE.md`
- `docs/research/overnight-strategy-audit-2026-08-22.md`
- `docs/research/EXPERT-REVIEW-SYNTHESIS.md`
- `docs/research/tigris-codebase-study.md`
- `docs/research/postgres-18-6-storage-bridge.md`
- `rfcs/0048-cell-transaction-system.md` through
  `rfcs/0055-resolver-hotspot-throughput-curve.md`
- `evals/suites/` and `experiments/ledger.jsonl`
