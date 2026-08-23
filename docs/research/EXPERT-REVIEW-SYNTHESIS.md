# objectKV independent review synthesis

Status: `[ACTIVE-WORK]`. The Fable review and two focused internal Codex
multi-agent reviews are complete. The Kimi 3 review is blocked before inference
on local OpenRouter authentication, so the findings below are one independent
model review, two internal reviews, and maintainer synthesis, not model or
expert consensus.

## Review ledger

| Reviewer path | State | Receipt |
|---|---|---|
| Claude Code, Fable | `[EXISTS]` complete on 2026-08-22 | `docs/research/reviews/fable-2026-08-22.md` |
| OpenCode, OpenRouter `moonshotai/kimi-k3` | `[ACTIVE-WORK]` blocked before inference with `User not found` | rerun after `opencode auth login --provider OpenRouter` |
| Codex multi-agent, cell topology | `[EXISTS]` internal review on 2026-08-22 | `docs/research/reviews/codex-cell-topology-2026-08-22.md` |
| Codex multi-agent, exact HTAP overlay | `[EXISTS]` internal review on 2026-08-22 | `docs/research/reviews/codex-htap-overlay-2026-08-22.md` |

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
8. A follow-up architecture correction separates physical granularity,
   transaction topology, and fleet topology. A cell should be a bounded complete
   FDB-like database cluster; a tenant database is its normal transaction domain;
   ranges and objects remain below that boundary. This is now RFC-0011, not an
   implemented claim.
9. A lagging analytical base does not require a stale query. RFC-0010 now
   proposes exact base plus durable table-tail overlay through one target version
   `T`. The watermark bounds cost. Predicate pushdown must not remove tail keys
   required to invalidate matching base rows.
10. A correct topology diagram is not yet a safe WAL contract. Cell authority,
    retained deduplication, causal read versions, all-resolver aggregation,
    tagged-log frontiers, and bounded recovery roots are pre-WAL freeze gates.
11. Exact HTAP needs a second coverage watermark `A_p`, atomic and complete
    change capture, schema-at-`T` normalization, two-effect partition movement,
    exact-or-error leases, and phantom-safe certificate validation.
12. Tigris validates the value of an FDB-like transaction substrate, immutable
    bytes behind transactional metadata, version-addressed caches, and atomic
    work intent. It does not validate objectKV's replacement of FoundationDB.
    Its published failures add cache resurrection, short-transaction
    continuation, and ground-truth GC as explicit eval obligations.

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
- `[EXISTS]` The first `okv-wal` slice persists opaque commit envelopes in
  versioned checksummed frames across three local files and reconstructs only a
  matching two-copy prefix after fresh opens. Six negative subjects detect
  unsafe acknowledgement, recovery, torn-tail, chain, corruption, and retry
  interpretations. This is a stable-storage proof, not consensus evidence.
- `[PROPOSED]` RFC-0011 defines cell, tenant database, range, segment, and
  metacluster as separate topology layers, with no cross-cell transaction.
- `[PROPOSED]` RFC-0010 defines exact base-plus-tail snapshot semantics,
  analytical-tail retention, snapshot leases, and later write validation.
- `[DECIDED]` for bootstrap, one coordinator quorum per cell owns its generation
  and root control pointer. The future metacluster remains separate.
- `[PROPOSED]` The WAL envelope is gated on cell/tenant identity, canonical
  commit identity, resolver aggregation, mutation tags, checksums, and durable
  deduplication semantics.
- `[PROPOSED]` The HTAP eval uses exact canonical row equality as a hard gate;
  tail rows, bytes, memory, spill, and latency measure cost separately.
- `[PROPOSED]` Block-before-pointer publication, transactional task intent,
  cache resurrection, and ground-truth GC now have separate contributor tasks.
  See `docs/research/tigris-codebase-study.md`.

## SlateDB inference follow-up

The pinned SlateDB source resolves one Fable inference and confirms the other.
Its single-writer batch loop checks an externally supplied sequence against the
current oracle and rejects `seqnum <= current` before advancing it, so the
adapter's preflight check does not permit a lower sequence to land after a
higher one. The adapter now has a concurrent 20-versus-10 regression fixture and
maps the losing lower apply to a non-monotonic result. See the pinned
[SlateDB write path](https://github.com/slatedb/slatedb/blob/e0161973d8d7ffdede7c44725729838811674e99/slatedb/src/batch_write.rs#L189-L220).

The second inference was correct: a SlateDB snapshot sequence is not a stable
objectKV logical version. `okv-slate` now stores objectKV's complete 16-byte
version in private metadata and rejects nonzero generations until the underlying
external sequence seam can represent or map them safely.

## Primary-source checks for the topology correction

- FoundationDB defines shards as continuous key ranges and routes sequenced,
  resolver-checked transactions through tagged logs inside one cluster:
  [HA write path](https://apple.github.io/foundationdb/ha-write-path.html).
- FoundationDB tenants are named transaction domains confined to a keyspace;
  the documented feature remains experimental:
  [tenants](https://apple.github.io/foundationdb/tenants.html).
- FoundationDB publishes bounded tested cluster and data-size envelopes, which
  motivates declared cell limits without predicting objectKV's limits:
  [known limitations](https://apple.github.io/foundationdb/known-limitations.html).
- DataFusion exposes the custom source, plan, ordering, partitioning, and filter
  declarations needed for an exact overlay operator:
  [custom source guide](https://datafusion.apache.org/library-user-guide/custom-table-providers.html).

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
