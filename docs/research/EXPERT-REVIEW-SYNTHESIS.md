# objectKV independent review synthesis

Status: `[ACTIVE-WORK]`. The Fable review is complete. The Kimi 3 review is
blocked before inference on local OpenRouter authentication, so the findings
below are one independent review plus maintainer synthesis, not model consensus.

## Review ledger

| Reviewer path | State | Receipt |
|---|---|---|
| Claude Code, Fable | `[EXISTS]` complete on 2026-08-22 | `docs/research/reviews/fable-2026-08-22.md` |
| OpenCode, OpenRouter `moonshotai/kimi-k3` | `[ACTIVE-WORK]` blocked before inference with `User not found` | rerun after `opencode auth login --provider OpenRouter` |

The exact shared prompt is in `docs/research/multi-review-brief.md`. Credentials
must stay outside the repository and review artifacts.

## Preliminary synthesis

1. Generation recovery is more dangerous than the happy-path commit pipeline.
   Exact seeded replay must be a merge requirement before replicated WAL work,
   not a later robustness project.
2. The durability statement needs two named watermarks. `C` is quorum-WAL
   committed and `O` is object durable. The interval `(O, C]` remains dependent
   on the WAL topology, so regional RPO, WAL growth, throttling, refusal, and
   `commit_unknown` behavior must be stated before Gate 2.
3. The physical boundary needs two interfaces. OLTP segments own the kernel's
   MVCC algebra and point/range access. Analytical artifacts own schema-aware
   Parquet or Vortex projection and pruning. Their shared waist is a sorted
   versioned-entry stream plus fenced publication, not a generic file-format
   plugin.
4. Modern first-party object stores provide strong named-object visibility. The
   operational threats are conditional-operation differences, tail latency,
   request pricing, throttling, no multi-object atomicity, and the semantic
   spread hidden by the phrase S3-compatible.
5. Redis and search are envelope probes, not launch claims. Redis exposes the
   commit-latency and hot-key ceiling. Search should publish immutable index
   segments and use okv for versioned catalog and cursor authority rather than
   update one posting-list key per term.
6. PostgreSQL should begin with one explicit authority mapping. The leading
   hypothesis is a Neon-shaped path where PostgreSQL WAL and LSN remain the
   commit authority while objectKV proves the versioned storage substrate. It
   remains a hypothesis until the bridge spike and crash matrix decide it.
7. Control-plane bootstrap is unresolved. Range maps, generations, and durable
   watermarks cannot be vaguely placed inside a transaction system that depends
   on them to start. A small consensus authority or a bootstrapped system
   keyspace needs a worked recovery protocol.

## Changes applied to the plan

- `[PROPOSED]` D13 now separates transactional segments from analytical
  artifacts.
- `[PROPOSED]` D14 makes exact deterministic simulation a prerequisite for WAL.
- `[PROPOSED]` D15 makes acknowledgement, RPO, and lag backpressure one contract.
- `[PROPOSED]` D16 forces a bootstrap authority decision before distribution.
- `[PROPOSED]` `evals/suites/fault-recovery.toml` turns brownout, takeover,
  generation recovery, GC, and range movement into configurable eval lanes.
- `[EXISTS]` The first `okv-sim` generation-fencing probe executes crash,
  restart, partition, repair, generation activation, stale publication, exact
  fresh-process replay, and a failing negative control. Replicated WAL and
  objectification faults remain proposed.

## What can falsify this synthesis

- Kimi or human reviewers produce a simpler recovery design that preserves exact
  replay and acknowledged-commit safety without simulation-first sequencing.
- A compile-backed format prototype shows one interface can preserve the full
  MVCC algebra and analytical pushdown without leaking schemas or format-specific
  compaction into the kernel.
- A PostgreSQL bridge crash matrix demonstrates that objectKV can be the sole
  commit authority without duplicating PostgreSQL WAL or weakening compatibility.

Until those receipts exist, the repository should optimize for falsifying these
risks and should give up an early distributed-database product claim.
