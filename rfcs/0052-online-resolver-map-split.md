# RFC-0052: Online resolver-map split through shadow catch-up

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0006, RFC-0008, RFC-0009, RFC-0011, RFC-0048,
  RFC-0049, RFC-0050, RFC-0051

## Decision under test

`[PROPOSED]` Split one hot resolver range inside an active transaction-system
generation without changing the tenant transaction domain. Prepare two shadow
child resolvers, copy the source resolver's bounded recent conflict history at
one exact batch frontier, and dual-stream later touching batches to the source
and children. The old map remains authoritative until both children attest to
the same caught-up predecessor frontier.

Commit the map cutover as one replicated metadata mutation in the RFC-0051
global batch chain. Every commit proxy and required tLog must durably process
that cutover before any later ticket may use the new map. One transaction uses
exactly one map epoch. Old-map work not globally disposed before the cutover
retries under the new map. The retired source resolver may not decide new-map
work or contribute a delayed reply.

## Context and invariant

Map epoch `1` assigns `[0x50, 0xa0)` to resolver `2`. The bounded split replaces
that source with two new process identities:

```text
map epoch 1:
  resolver 1 [0x00, 0x50)
  resolver 2 [0x50, 0xa0)
  resolver 3 [0xa0, 0xf0)

map epoch 2:
  resolver 1 [0x00, 0x50)
  resolver 4 [0x50, 0x78)
  resolver 5 [0x78, 0xa0)
  resolver 3 [0xa0, 0xf0)
```

The invariant is one conflict history and one authoritative owner set at every
global batch version. A transaction crossing `0x78` after cutover must receive
both child decisions. A transaction crossing the larger source boundary must
still span every overlap. Resolver partitions remain internal roles and do not
shrink transaction atomicity.

FoundationDB's transaction-state-store design requires metadata mutations to
reach every commit proxy in commit order, because divergent range maps can
corrupt routing. Its data-distribution design serializes range-map changes as
ordinary transactions. objectKV applies that total-order principle to its
resolver map and adds a shadow conflict-history transfer. This shadow split is
an objectKV design inference, not a claim about FoundationDB's resolver
implementation.

Sources:

- [FoundationDB transaction state store](https://github.com/apple/foundationdb/blob/main/design/transaction-state-store.md)
- [FoundationDB data distributor internals](https://github.com/apple/foundationdb/blob/main/design/data-distributor-internals.md)
- [FoundationDB architecture](https://apple.github.io/foundationdb/architecture.html)

## Frozen bounded model

Use one cell and tenant, one transaction-system generation, one replicated
three-node sequencing and metadata authority, three commit-proxy processes,
source resolver map epoch `1`, two shadow child resolver processes, and two
three-process authenticated tLog sets with quorum two. The history contains 30
batches of four transactions, with a maximum of eight pending out-of-order
batches.

For each seed:

1. process batches `1` through `8` through map epoch `1`, including point,
   crossing-source, crossing-future-split, and all-range transactions;
2. replicate one split descriptor naming the source map digest, destination map
   digest, split boundary, source and child incarnations, copy frontier, and
   maximum conflict-history entries;
3. export resolver `2` conflict history through the exact batch-8 frontier,
   clip it to `[0x50, 0x78)` and `[0x78, 0xa0)`, and install it into empty
   shadow resolvers `4` and `5`;
4. process batches `9` through `15` in the RFC-0051 predecessor order, use map
   epoch `1` for authoritative decisions, and dual-stream each touching clipped
   request to the shadow children;
5. retain at least one cross-range batch at each shadow catch-up boundary while
   unrelated ranges continue through the same global order;
6. require source and shadow attestations that all three processed through
   batch `15` with matching clipped-history roots;
7. resolve every old-map transaction through batch `15`; unresolved old-map
   client work is abandoned and must retry;
8. commit one map-cutover metadata mutation as batch `16`, copy it to every
   proxy, and make its exact tLog progress frame quorum durable in both required
   sets;
9. activate map epoch `2` only after that durability boundary;
10. process batches `17` through `30` through resolvers `1`, `4`, `5`, and `3`,
    including transactions crossing the new `0x78` boundary and transactions
    spanning every partition;
11. reject delayed map-1 requests and replies from resolver `2`;
12. compare global dispositions, rows, evaluation envelope bytes, proxy map
    views, resolver conflict roots, tLog progress roots, and acknowledgements
    with a centralized oracle that applies the same cutover version.

The split copies only bounded generation-local conflict metadata. It copies zero
durable database bytes and changes no object-segment ownership. Physical serving
range movement remains a separate RFC-0006 gate.

## Negative subjects

The frozen suite independently attempts to:

1. cut over before one shadow child reaches the barrier frontier;
2. omit one source conflict entry from a child's initial snapshot;
3. combine map-1 and map-2 resolver replies in one transaction;
4. count a delayed source-resolver reply after map-2 activation;
5. route a range crossing `0x78` to only one child;
6. let one commit proxy process a post-cutover batch with map epoch `1`;
7. activate map epoch `2` before the cutover progress frame is quorum durable in
   every required tLog set;
8. change the split boundary or destination map digest after shadow catch-up.

Every negative subject must replay exactly, expose at least one contract anomaly,
export OTel, and discard.

## Eval plan

Freeze `cell-online-resolver-map-split-v0` with seeds `1103`, `2207`, and
`3301`. Each subject uses the same 30 batches and 120 transaction attempts per
seed. The event budget is 1,536 per seed.

The primary metric is correctness anomalies. Secondary receipts include
sequencer tickets, proxy process starts, old-map and new-map transactions,
source history entries, child snapshot entries, shadow catch-up batches,
resolver decisions, cutover metadata applications, tLog progress frames,
abandoned old-map work, retries, copied durable database bytes, and maximum
pending batches.

## Passing contract

A pass requires:

- one immutable split descriptor bound to old and new map digests;
- one exact source conflict-history snapshot through the declared frontier;
- every source entry appears in exactly one clipped child snapshot;
- shadow children start empty and do not influence pre-cutover dispositions;
- every touching catch-up batch reaches the source and correct shadow child;
- source and children attest to the same cutover predecessor frontier;
- no unresolved old-map transaction crosses the cutover;
- every proxy applies the cutover metadata in the global batch order;
- every required tLog set durably records the cutover progress frame;
- no new-map ticket executes before the cutover durability boundary;
- every transaction uses exactly one resolver-map epoch;
- crossing ranges route to every child overlap after cutover;
- retired source requests and replies fail closed;
- abandoned old-map client work retries with a new identity and map epoch;
- centralized-oracle dispositions, rows, evaluation envelopes, acknowledgement
  set, proxy map views, resolver roots, and tLog roots are exact;
- zero durable database bytes copied, zero resolver durable synchronizations,
  zero resolver finalization RPCs, zero telemetry drops, valid schema, exact
  replay, and budget hold.

## Failure model

- different proxy, resolver, and tLog network arrival orders;
- delayed old-map requests and replies;
- one shadow child lagging the source barrier;
- missing or altered conflict-history transfer entries;
- stale proxy map view after cutover;
- incomplete tLog durability for the cutover frame;
- retries at the map boundary;
- controller loss is not admitted in this first online split proof.

## Alternatives

### Replace the full transaction-system generation

RFC-0049 already makes this safe and simpler. It pauses every resolver and
abandons all old reads for a local hotspot split. Keep it as the fallback and
recovery path.

### Reuse resolver `2` as one child

Shrinking the live process in place saves one process but makes its pre-cutover
and post-cutover identity ambiguous. Two fresh children give the cutover a clear
fence and rollback target.

### Activate children immediately after snapshot copy

This loses source writes accepted after the snapshot frontier. Dual streaming
through one caught-up predecessor barrier is required.

### Dual-authorize source and children during cutover

Letting either owner decide creates overlapping authority and map-dependent
outcomes. Shadows may observe and attest, but only one map epoch is authoritative
for each ticket.

## Tradeoff

This contract optimizes for a bounded hot-range split without full transaction-
system recovery or durable database-byte movement. It gives up immediate
cutover, doubles conflict traffic for the moving range during catch-up, and
requires an ordered metadata frame at every proxy and tLog. Merge, split
controller recovery, several concurrent movements, serving-range movement,
hotspot throughput curves, independent hosts, and production key custody remain
open.

## Compatibility and migration

The split descriptor and cutover frame use explicit format version `1`. Map
epoch `1` remains the rollback path until epoch `2` is quorum durable and every
proxy reports it applied. An implementation that does not understand epoch `2`
must fail closed before accepting a post-cutover ticket.

## Unresolved questions

1. How large may the recent conflict-history snapshot become before a full
   generation replacement is cheaper than an online split?
2. Can several disjoint ranges move concurrently without one cell-wide metadata
   bottleneck?
3. How are split controller loss and abandoned shadow state recovered or
   collected?
4. When should the source process be terminated versus retained as a bounded
   rollback shadow?
5. How does resolver-map movement coordinate with serving-range and tLog-tag
   movement without creating one giant cutover transaction?

## Evaluation outcome

Candidate `04738b5` kept OTel run `30297004` with zero anomalies and exact
replay across seeds `1103`, `2207`, and `3301`. The three histories processed
360 transaction attempts through 90 replicated sequencer tickets, 9 commit-
proxy process instances, 9 source-map resolver process instances, 6 shadow
resolver process instances, and 18 durable tLog process instances. They
committed 261 transactions, rejected 87 conflicts, and abandoned 12 old-map
requests at the cutover before retrying the same work under new identities and
map epoch `2`.

Resolver `2` exported 180 conflict-history entries across the histories. The
two empty children installed 96 exact clipped snapshot entries through batch
8, shadowed 42 catch-up batches through batch 15, and matched the source's
clipped roots before activation. All three proxies applied the immutable split
descriptor in global order. Both required tLog sets made the batch-16 cutover
frame quorum durable before batch 17 used the new map. The correct path copied
zero durable database bytes, synchronized no resolver storage, issued no
resolver finalization RPCs, and used at most four pending batches.

Eight clean controls replayed exactly and discarded:

| Subject | Run | Anomalies per seed |
|---|---|---:|
| cutover before shadow catch-up | `c7feb034` | 9 |
| omitted source history entry | `e85bd186` | 4 |
| mixed resolver-map epochs | `8cc3a129` | 1 |
| accepted retired source reply | `ba474e4d` | 1 |
| routed crossing range to one child | `1e62903c` | 1 |
| stale commit-proxy map | `f888a1fc` | 1 |
| activation before every tLog quorum | `e5394179` | 5 |
| mutated split descriptor | `f12997be` | 3 |

Prometheus observed availability `1` and correctness anomalies `0` for the
correct run. Every control exported availability `0` and its exact anomaly
count with candidate, suite, profile, run, workload, and backend labels. The
evaluated suite hash is `40c231b4`; the profile hash is `884d66bb`. The correct
path used 1,332 of 1,536 budgeted events. Workspace tests and warning-free
Clippy passed.

This admits one same-host, one-shot resolver split with bounded conflict
history and one controller. It does not admit throughput improvement,
split-controller recovery, merge, concurrent movements, serving-range or
tLog-tag movement, independent hosts, or production identity and key custody.
