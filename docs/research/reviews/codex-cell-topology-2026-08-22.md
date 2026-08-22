# Internal adversarial review: cell topology

Status: `[EXISTS]` read-only Codex multi-agent review of commit `07a449a` on
2026-08-22. This is an internal architecture review, not external expert
consensus.

## Verdict

The bounded complete-cell topology is coherent, but the transaction and recovery
protocol is not safe to turn into a WAL envelope until authority, deduplication,
causal read versions, durable frontiers, and bounded recovery roots are frozen.

## System map

```text
metacluster directory [FUTURE]
  -> TenantId to CellId + RoutingEpoch

tenant client
  -> ReadVersionService
  -> RangeMap and ServingWorker
  -> CommitProxy
       -> VersionAuthority
       -> Resolver set
       -> tagged DurableLog set
       -> Materializer
       -> ManifestAuthority and object store
       -> DurableFrontier, WAL pop, and Ratekeeper

GenerationAuthority
  -> recruits and fences every cell role
  -> owns the recoverable control root
```

## Blocking conflicts

1. RFC-0009 already chose a static coordinator quorum for the bootstrap cell,
   while D16 and RFC-0011 still described bootstrap authority as unresolved.
   Freeze one quorum per cell and keep future metacluster authority separate.
2. Retained client-identity deduplication is stronger than FoundationDB's
   general unknown-result contract. If retained, it must be atomically durable,
   replayable, generation-safe, and governed by explicit expiry.
3. The old Cell v0 and Cell v1 milestones both claimed ownership of direct
   distributed reads and range routing. Distinguish the pre-cell substrate from
   the first complete Cell v0, then make v1 dynamic distribution.
4. One global `O` is a safe bootstrap pop frontier, but partitioned tagged logs
   require `O_cell` derived from per-range and per-consumer durable frontiers.
5. Strict serializability needs a causal `min_known_version` and stale
   read-version-proxy rejection before multiple proxies are admitted.
6. Recovery cannot validate every historical range manifest before any range
   serves while also claiming bounded empty-cache startup. Use a bounded root
   and index, verify active ranges before service, and open other ranges lazily.
7. Resolver aggregation must say no commit exists unless all required resolvers
   accept. A rejected resolver may retain conservative conflict state, so
   identical internal state after rejection is not required.
8. FoundationDB tenant documentation supports transaction-domain vocabulary,
   not objectKV security, quotas, encryption, or noisy-neighbor claims.

## Contracts to freeze before WAL

- F1: per-cell generation and root authority;
- F2: commit outcomes and retained deduplication semantics;
- F3: version, log index, read causality, and generation timeline;
- F4: WAL envelope with cell, tenant, generation, version, identity,
  fingerprint, checksums, conflict domains, tags, and acknowledgement evidence;
- F5: all-resolver aggregation and rejection semantics;
- F6: `O_cell`, per-consumer frontiers, WAL pop, GC, and lag backpressure;
- F7: tenant namespace, routing epoch, and isolation boundary.

## Minimal negative controls

1. Cross-range strict-serializable histories with stale routes and generations;
   omit one conflict and return one stale read version as separate subjects.
2. Crash at every commit acknowledgement and deduplication boundary; include
   leader-only fsync and RAM-only deduplication subjects.
3. Race objectification, safe frontier advancement, and WAL pop.
4. Compare one resolver/log role with partitioned roles; include partial resolver
   aggregation and missing mutation-tag subjects.
5. Move one tenant while writes continue; prove one writable routing epoch and
   no cross-tenant key or quota escape.

## Disposition

RFC-0005, RFC-0008, RFC-0009, RFC-0011, D16, the contributor board, and the
cell-scale eval plan own these findings.
