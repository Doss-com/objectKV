# RFC-0051: Global batch ordering across multiple commit proxies

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0008, RFC-0009, RFC-0011, RFC-0034, RFC-0039,
  RFC-0040, RFC-0048, RFC-0049, RFC-0050

## Decision under test

`[PROPOSED]` Run several independent commit proxies inside one transaction-system
generation while preserving one cell-wide commit-batch order. A replicated
sequencer gives each proxy batch a unique chain link containing the previous
batch version, current batch version, proxy identity and incarnation, generation,
transaction interval, and exact batch digest. Every resolver and required tLog
set may receive batches in a different network arrival order, but may process a
batch only after its declared predecessor.

Resolver acceptance still stages rather than publishes. A client acknowledgement
requires the exact ordered batch frame to be quorum durable in every required tLog
set. Conflict-only or empty-mutation batches carry progress frames so every
resolver and tLog advances across the same version chain without inventing a
mutation.

## Why this gate is next

RFC-0050 closes the single-proxy durability boundary across memory-only resolvers,
authenticated tLogs, and generation recovery. It still submits one already
ordered candidate stream. Adding proxies without a predecessor contract permits
two resolvers to observe different conflict orders and permits tLogs to expose a
later proxy batch before an earlier batch is durable.

FoundationDB assigns each proxy batch a current and previous commit version. Its
resolvers and transaction logs wait for the previous version before processing the
current version. The primary write-path description makes the global version chain,
not proxy arrival order, the concurrency-control and log order.

Sources:

- [FoundationDB read and write path](https://apple.github.io/foundationdb/read-write-path.html)
- [FoundationDB HA write path](https://apple.github.io/foundationdb/ha-write-path.html)
- [FoundationDB recovery internals](https://github.com/apple/foundationdb/blob/main/design/recovery-internals.md)

This gate tests that ordering rule. It does not claim throughput improvement.

## Frozen bounded model

Use one cell and tenant, one transaction-system generation, one replicated
three-node sequencing authority, three commit-proxy processes, three memory-only
resolver processes under map epoch `1`, and two three-process authenticated tLog
sets with quorum two. Each seed contains 24 commit batches, eight assigned to each
proxy, with four transaction attempts per batch. At least four batches are
conflict-only after resolution. The maximum pending out-of-order window is eight
batches.

For each seed:

1. establish one visible prefix and one read-version floor;
2. admit three pinned proxy identities and incarnations for the active generation;
3. enqueue client work independently at all three proxies;
4. obtain one sequencer ticket per batch, where every ticket uniquely binds
   `(previous_version, version]`, proxy identity, transaction interval, and batch
   digest;
5. deliver the 24 ticketed batches to each resolver in a different deterministic
   permutation, including successor-before-predecessor arrival;
6. buffer at most eight pending batches per resolver and process only the
   contiguous ticket chain;
7. preserve transaction order inside each batch and resolve every overlapping
   range at every required resolver;
8. compare every global disposition with the centralized Cell v0 oracle in the
   sequencer order;
9. emit one exact tLog progress frame per batch, including conflict-only batches;
10. deliver frames to every tLog process in different deterministic permutations
    and make each process durably advance only through a contiguous predecessor
    chain;
11. acknowledge a batch only after its exact frame is quorum durable in both
    required tLog sets, which also proves every predecessor frame is durable;
12. publish visible envelopes in batch and transaction order, then compare rows,
    envelope bytes, chain links, conflict outcomes, resolver orders, tLog orders,
    and client acknowledgements with the frozen oracle.

The correct subject uses zero resolver file synchronizations and zero resolver
finalization RPCs. The sequencing authority and tLogs retain the durable ordering
facts. Commit proxies and resolvers remain generation-scoped processes.

## Negative subjects

The frozen suite independently attempts to:

1. issue the same current batch version to two different proxies;
2. accept a ticket whose previous version skips one issued batch;
3. let one resolver process batches in network arrival order;
4. let one tLog process durably advance in network arrival order;
5. reuse a valid ticket with different transaction bytes;
6. acknowledge a batch after only one required tLog set reaches quorum;
7. accept a batch from a stale or unpinned proxy incarnation;
8. omit the progress frame for a conflict-only batch and process its successor.

Every negative subject must replay exactly, expose at least one contract anomaly,
export OTel, and discard.

## Eval plan

Freeze `cell-multi-commit-proxy-ordering-v0` with seeds `1103`, `2207`, and
`3301`. Each subject uses the same 24 batches and 96 transaction attempts per
seed. The event budget is 1,024 per seed.

The primary metric is correctness anomalies. Secondary receipts include sequencer
tickets, proxy process starts, out-of-order deliveries, maximum pending batches,
resolver decisions, conflict-only progress frames, tLog appends and attestations,
batch acknowledgements, commits, conflicts, and resolver synchronization or
finalization operations.

## Passing contract

A pass requires:

- three independent commit-proxy processes with pinned generation identities;
- one unique gap-free sequencer ticket chain across every proxy;
- exact ticket and batch digest verification before resolver or tLog processing;
- bounded buffering for successor-before-predecessor delivery;
- the same batch and transaction order at every resolver;
- exact centralized-oracle dispositions in sequencer order;
- every crossing conflict routed to every overlapping resolver;
- one exact tLog progress frame for every sequenced batch;
- the same batch order at every active tLog process;
- quorum durability in every required tLog set before acknowledgement;
- a later batch cannot acknowledge or publish across a missing predecessor;
- stale proxy generations and incarnations fail closed;
- exact rows, visible envelope bytes, envelope chain, conflict outcomes, and
  acknowledgement set;
- zero resolver durable synchronizations, zero resolver finalization RPCs, zero
  telemetry drops, valid schema, exact replay, and budget hold.

## Alternatives

### Serialize all client work through one commit proxy

This retains the RFC-0050 proof and avoids distributed arrival order. It also
leaves one proxy as the permanent transaction-system throughput ceiling.

### Let resolvers assign the global order

Resolvers could run consensus or coordinate a common order themselves. That adds
a second sequencer to the conflict path and makes resolver recovery responsible for
durable order. The proposed contract keeps resolvers memory-only.

### Trust arrival order because commit versions are unique

Unique versions do not force two independent processes to observe the same order.
An explicit predecessor link and bounded pending rule are required at both
resolvers and tLogs.

### Omit conflict-only tLog frames

This saves empty writes but prevents a tLog from distinguishing an intentionally
empty batch from a missing predecessor. The bounded gate optimizes for one exact
recoverable version chain and gives up that optimization.

## Tradeoff

This contract optimizes for deterministic cross-proxy ordering with memory-only
resolvers and the existing authenticated tLog boundary. It gives up throughput
claims, sequencer partitioning, metadata mutation propagation, proxy-failure
recovery, online resolver-map movement, independent hosts, and production key
custody.

## Unresolved questions

1. What batching and ticket-allocation window produces useful parallelism without
   making the pending window or tail latency unbounded?
2. Can conflict-only progress be represented without writing one frame to every
   tLog while retaining an exact recovery chain?
3. Does proxy failure after ticket allocation require immediate generation
   recovery, or can a separately authenticated no-op close the gap safely?
4. How are transaction-state metadata mutations replayed at every proxy before a
   later ticket becomes executable?
5. How does an online resolver split join the predecessor chain without pausing
   unrelated ranges?

## Evaluation outcome

Candidate `674a443` kept OTel run `2c1c8544` with zero anomalies and exact
replay across seeds `1103`, `2207`, and `3301`. The three histories issued 72
replicated sequencer tickets across nine commit-proxy process instances and
processed 288 transactions. They committed 180, rejected 108 conflicts, checked
348 resolver decisions, and durably advanced 72 tLog progress frames. Four
conflict-only frames per seed advanced the same chain without publishing a
mutation. All 72 batches acknowledged only after quorum durability in both
required tLog sets.

Every resolver and all six tLog workers per seed received a different bounded
arrival permutation, reconstructed the same 24-batch predecessor chain, and
used at most four pending batches. Visible rows, evaluation envelope bytes,
envelope links, conflict outcomes, and acknowledgement sets matched the frozen
sequencer-order oracle. Resolver durable synchronization and finalization RPC
counts remained zero.

Eight clean controls replayed exactly and discarded:

| Subject | Run | Anomalies per seed |
|---|---|---:|
| duplicate current version | `e7a65678` | 11 |
| skipped previous version | `00016074` | 10 |
| resolver arrival-order execution | `662ecca2` | 1 |
| tLog arrival-order durability | `b21dfae1` | 2 |
| mutated ticketed batch | `d2e7e3fc` | 1 |
| acknowledge before every tLog set | `1d791160` | 2 |
| stale proxy incarnation | `7313a74c` | 1 |
| omitted conflict-only progress frame | `e5e7d3ce` | 3 |

Prometheus observed availability `1` and correctness anomalies `0` for the
correct run. Every control exported availability `0` and its exact anomaly
count with candidate, suite, profile, run, workload, and backend labels. The
evaluated suite hash is `0a2640ab`; the profile hash is `b7e077dd`. The correct
path used 585 of 1,024 budgeted events. Workspace tests and warning-free Clippy
passed.

This admits one replicated sequencer, three same-host one-shot proxy processes,
fixed resolver ranges, bounded pending batches, two local authenticated tLog
sets, and evaluation-only key custody. It does not admit throughput improvement,
long-lived proxy services, proxy-failure recovery after ticket allocation,
sequencer partitioning, metadata propagation, online resolver-map movement,
independent hosts, or production identity and key custody.
