# RFC-0048: Partitioned resolver agreement

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0008, RFC-0011, RFC-0029, RFC-0032, RFC-0033, RFC-0034

## Decision

`[EXISTS]` Partition conflict resolution by non-overlapping ordered key ranges
under one immutable resolver-map epoch. A transaction is resolved by every
partition overlapped by any canonical read or write conflict range. The
replicated transaction authority may commit only one retained global disposition
after every required resolver process returns a map-bound signed decision.

The first bounded contract uses three resolver processes and one replicated
three-node Cell v0 transaction authority. It compares the exact commit and
conflict history against the admitted centralized resolver oracle. It tests
point and overlapping-range conflicts, empty-range phantoms, transactions that
cross one or both partition boundaries, and disjoint work. Resolver partitions
do not define transaction domains. One tenant transaction may touch all three.

## Resolver map

The evaluation key generator emits one-byte keys below `0xf0`. Resolver-map
epoch `1` assigns the complete bounded test domain:

```text
resolver 1: [0x00, 0x50)
resolver 2: [0x50, 0xa0)
resolver 3: [0xa0, 0xf0)
```

The authority pins the ordered ranges, resolver IDs, process incarnations,
public keys, and map digest. A conflict range crossing a boundary is routed to
every overlapped partition. The resolver sees the intersection with its owned
range. Empty intersections are not sent.

## Resolution protocol

For transaction identity `X`, read version `R`, candidate version `V`, map
epoch `E`, and required resolver set `Q`:

```text
prepare(X, R, V, E):
    authority durably records X, canonical conflict ranges, V, E, and Q

resolve_partition(p):
    require p in Q
    require exact map E and pinned process incarnation
    require every older decision touching p to have a global disposition
    compare local read-conflict intersections against committed writes after R
    persist ACCEPT or CONFLICT before signing
    bind X, R, V, E, map digest, p, conflict digest, and decision

decide(X):
    verify one pinned decision from every p in Q
    any CONFLICT -> durable global CONFLICT
    all ACCEPT -> atomically apply mutations and durable global COMMIT
    missing, duplicate, stale, or mixed-map evidence -> remain prepared

finalize_partition(p):
    replay the authority's retained global disposition
    only a committed transaction adds its clipped write ranges
    persist finalized-through V before resolving newer work touching p
```

Candidate versions are consumed regardless of disposition. The replicated
authority is the recovery source for an ambiguous partition finalization. A
lost finalize reply is retried exactly after resolver restart. This first
contract allows only one unresolved candidate per touched partition. Disjoint
partitions may make progress independently.

## Correct bounded history

For each seed, generate 100 rounds with a fixed mix of:

1. a point read and write inside one partition;
2. an empty range read followed by an insertion in that range;
3. a read range crossing one resolver boundary and writes on both sides;
4. a transaction whose conflicts and mutations touch all three partitions;
5. two disjoint transactions on resolver `1` and resolver `3`;
6. a stale transaction that must conflict with a committed cross-boundary write.

The centralized oracle and partitioned path receive the same invocation order,
read versions, conflict ranges, mutations, candidate versions, and injected
authority or resolver restart boundaries. They must produce the same durable
status, visible rows, commit-envelope chain, and final conflict index.

## Negative subjects

The frozen suite independently attempts to:

1. route a crossing range only by its start key;
2. commit after only a strict subset of required partitions accepts;
3. count the same resolver identity twice;
4. combine decisions from two resolver-map epochs;
5. acknowledge a partition decision before its journal is durable, then restart;
6. resolve newer touching work before applying the prior global disposition;
7. activate a split map while an old-map transaction remains prepared.

Every subject must replay exactly, diverge from the centralized oracle or block
where the unsafe subject commits, export OTel, and discard.

## Eval plan

Freeze `cell-partitioned-resolver-agreement-v0` with seeds `1103`, `2207`, and
`3301`. Each seed starts three transaction-authority processes and three
resolver processes with separate synchronized roots. The event budget is 2,400
across 600 transaction attempts per seed plus restart and agreement checks.

The primary metric is correctness anomalies. Secondary receipts include
transactions committed, conflicts, serializability constraints, resolver
decisions, cross-partition attempts, map epochs, process restarts, durable
finalizations, and operation duration. OTel carries correctness, availability,
transaction commits, transaction conflicts, constraint count, frontier, and
duration with exact candidate, suite, profile, run, workload, and backend
labels.

## Evaluation outcome

Candidate `65664bf` kept OTel-enabled run `8be62401` at 1,800 of 2,400
allowed events with zero anomalies and exact replay across seeds `1103`,
`2207`, and `3301`. The correct subjects committed 1,200 transactions,
rejected 600 centralized-oracle conflicts, persisted 3,003 signed resolver
decisions, and applied 3,000 global finalizations. Three resolver restarts
replayed the exact prepared decision. Visible rows and commit-envelope chains
matched the centralized Cell v0 oracle after all 600 attempts per seed.

Seven clean controls replayed exactly and discarded:

| Subject | Run | Anomalies per seed |
|---|---|---:|
| route a crossing range by its start key | `0cddd6e2` | 1 |
| commit after partial acceptance | `abfbe8cd` | 1 |
| count one resolver identity twice | `b7db369a` | 1 |
| combine mixed map epochs | `a4891e60` | 1 |
| lose an acknowledged volatile decision | `92c60192` | 1 |
| resolve before prior global finalization | `85389fdd` | 1 |
| activate a split over prepared old-map work | `4f5912ca` | 1 |

Prometheus observed availability `1`, correctness anomalies `0`, 1,200
commits, 600 conflicts, 3,003 checked partition decisions, and frontier `600`
under exact candidate, suite, profile, run, workload, and backend labels. The
frozen source suite hash is `193c8fa3`; the evaluated suite hash is `86a4f947`;
the profile hash is `124ff6fa`.

This admits one fixed three-partition map, sequential global candidate order,
one unresolved touching decision per partition, same-host processes, and a
bounded deterministic history. It does not admit online split or merge,
concurrent in-flight transactions on one partition, proxy batching, hot-range
throughput, resolver failure during a global authority failover, independent
hosts or zones, production key custody, or general strict-serializability
verification.

## Alternatives

### Hash conflict domains

Hashing point keys balances uniform workloads but makes ordered range conflicts
touch many or all domains. Ordered partitions align conflict routing with range
reads, range movement, and serving ownership. Hot ranges may still require
splitting or a separate aggregation strategy.

### Trust the commit proxy to name accepted resolvers

The current prototype carries a static `accepted_resolvers` list in the command.
That is scaffolding, not proof. This contract requires process-derived signed
decisions checked against the authority-pinned map.

### Let every resolver see every transaction

This preserves centralized semantics while multiplying work. It is a useful
control oracle, not the intended scaling boundary.

## Unresolved questions

1. How are resolver partitions replicated without turning one slow replica into
   the commit bottleneck?
2. Can accepted-but-globally-aborted prepares be reclaimed without a per-key
   distributed transaction?
3. What batching protocol preserves one version order across multiple commit
   proxies?
4. How are read conflict ranges compacted and garbage-collected below the oldest
   admitted read version?
5. How are hot ranges split without globally pausing commit?
6. Which fault model and key custody protect resolver attestations in production?

## Tradeoff

This contract optimizes for proving that ordered partitioning preserves the
centralized isolation result before chasing throughput. It gives up pipelined
multi-proxy commit, replicated resolver partitions, dynamic hot-range splitting,
and high availability during one resolver-process failure.
