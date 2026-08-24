# RFC-0050: Stateless resolver recovery from authenticated tLog inventories

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0008, RFC-0009, RFC-0011, RFC-0039, RFC-0040,
  RFC-0041, RFC-0043, RFC-0048, RFC-0049

## Decision under test

`[PROPOSED]` Compose the RFC-0049 memory-only resolver path with the existing
authenticated tagged-log durability and generation-recovery path. A complete
set of valid resolver attestations permits the transaction authority to stage
one exact commit envelope. It does not make mutations visible. Visibility
requires a policy-authenticated quorum certificate for that exact envelope in
every required tLog set.

After resolver loss, fence every required old-generation tLog set, collect a
signed inventory of the exact unresolved staged window, and recover the
longest contiguous prefix that is quorum-present in every set. The successor
resolver generation starts empty with a minimum read version equal to that
authenticated recovered frontier. A replicated authority marker is not a
durability proof and may not define the recovery floor by itself.

## Why this gate is next

RFC-0049 admitted memory-only resolvers, ordered batches, fail-closed old
generation traffic, and empty successor state. Its recovery fence is ordinary
application data and its floor comes from the replicated authority. RFC-0043
separately admitted real signed tLog inventories and maximal-prefix recovery.
Neither result proves that the same resolver-accepted transaction crosses both
boundaries in the intended order.

This gate closes that composition gap before work on multiple commit proxies or
online range movement. Those features multiply concurrency and are not useful
until the single-proxy durability boundary is exact.

## Frozen bounded model

Use one cell and tenant, one commit-proxy controller, three memory-only resolver
processes per transaction-system generation, one three-node external generation
authority, three old transaction-authority voters, three successor voters, and
two three-process authenticated tagged-log sets with quorum two. Resolver map
epoch `1` has the three fixed ordered ranges from RFC-0048. The staged recovery
window contains four consecutive envelopes and is bounded to 16 KiB.

For each seed:

1. commit a ten-transaction visible baseline;
2. install authenticated membership and quorum policy for tLog sets `10` and
   `20`;
3. resolve and stage transaction `11` through every touched memory-only
   resolver, append its exact envelope to quorum in both tLog sets, record both
   certificates, but leave it invisible;
4. resolve and stage transaction `12`, append its exact envelope to quorum in
   both tLog sets, but lose the proxy before either certificate is recorded;
5. resolve and stage transaction `13`, append to quorum only in set `10`, and
   retain only a minority in set `20`;
6. stage dependent transaction `14` with no durability quorum;
7. begin a crossing-range candidate `15`, retain only a strict subset of its
   resolver replies, kill resolver `2`, and prove the unresolved candidate was
   never staged;
8. stop the complete old resolver generation;
9. durably prefix-fence every member of both old-generation tLog sets under one
   recovery identity and collect exact signed inventories for transactions
   `11` through `14`;
10. recover and publish transactions `11` and `12`, the maximal prefix that is
    quorum-present in both required sets;
11. abort transactions `13` and `14`, consume their ordered versions, and
    prove neither can become visible;
12. activate the successor authority and three empty successor resolver
    processes with a minimum read version of `12`;
13. reject delayed old-generation resolver requests, resolver replies, and tLog
    appends;
14. retry candidate `15` under a new successor identity and read version, then
    resolve, durably certify, and publish it at version `15`;
15. compare visible rows, original envelope bytes, chain links, terminal
    outcomes, and resolver decisions with the centralized oracle and frozen
    tLog inventories.

The correct subject performs zero resolver file synchronizations and zero
resolver finalization RPCs. Resolver state is disposable. tLog records,
signatures, durable fences, and the replicated transaction authority are the
durable evidence.

## Negative subjects

The frozen suite independently attempts to:

1. publish a resolver-accepted transaction before quorum durability exists in
   every required tLog set;
2. recover from a replicated authority marker without every required signed
   tLog inventory;
3. activate successor resolvers before the old tLog prefix fences are durable;
4. count a delayed old-generation resolver reply in a successor decision;
5. admit a successor read below the authenticated recovered frontier;
6. abort a transaction that is quorum-present in every required tLog set;
7. publish beyond the first record with quorum absence in one required tLog
   set.

Every negative subject must replay exactly, expose at least one contract
anomaly, export OTel, and discard.

## Eval plan

Freeze `cell-stateless-resolver-authenticated-tlog-recovery-v0` with seeds
`1103`, `2207`, and `3301`. The event budget is 512 per seed. Each subject runs
the same real-process topology and exact staged window.

The primary metric is correctness anomalies. Secondary receipts include
resolver decisions, resolver losses, staged records, authenticated tLog
attestations, prefix-fence observations, recovered and aborted records,
successor retries, resolver durable synchronizations, and resolver finalization
RPCs.

## Passing contract

A pass requires:

- complete and valid resolver evidence before an envelope is staged;
- no visibility from resolver acceptance alone;
- exact envelope bytes appended to each required tLog set;
- visibility only after authenticated quorum durability in every required set;
- an unresolved partial-resolver candidate is never staged or visible;
- every old-generation tLog set is durably fenced before successor activation;
- signed inventories cover the same exact bounded staged window;
- the recovered frontier is the maximal contiguous prefix quorum-present in
  every required tLog set;
- a quorum-present but uncertified envelope is recovered;
- the first quorum-absent envelope and its dependent suffix are aborted;
- successor resolver scratch starts empty at the authenticated frontier;
- successor reads are never below that frontier;
- old resolver requests, replies, and tLog appends fail closed;
- abandoned work retries with a new identity and consumes the sequence after
  the complete old staged window;
- exact rows, envelope bytes, envelope chain, terminal outcomes, and canonical
  replay;
- zero resolver durable synchronizations, zero resolver finalization RPCs,
  zero telemetry drops, valid schema, and budget hold.

## Alternatives

### Treat the replicated authority as the durability boundary

This is the RFC-0049 bounded proof. It is simpler but contradicts the intended
objectKV architecture, where transaction logs survive transaction-system loss
and determine recovery. It also makes the authority marker an unauthenticated
claim about bytes held by another subsystem.

### Require certificates to be recorded before recovery can publish

This would safely retain transaction `11` but incorrectly discard transaction
`12`, whose exact bytes reached quorum before the proxy failed. Signed tLog
inventories are required to recover acknowledged durability independently of
proxy progress.

### Persist resolver prepares

This retains isolated resolver restart and exact outcomes but restores the
storage synchronization and finalization path that RFC-0049 removed. Keep
RFC-0048 as an oracle and fallback, not as the target commit path.

## Tradeoff

This contract optimizes for one exact durability boundary and one recovery
protocol across memory-only conflict resolution and authenticated transaction
logs. It gives up resolver availability within a generation and delays
multi-proxy throughput, online resolver-map movement, independent hosts,
recovery-time curves, and production key custody.

## Unresolved questions

1. How do several commit proxies impose the same candidate order at every
   resolver and tLog set?
2. What bounds the unresolved staged window under sustained proxy loss and
   tLog lag?
3. How does online resolver split or merge join a transaction-system generation
   fence without a second recovery protocol?
4. What recovery-time target and false-conflict rate are acceptable for a cell?
5. Which identities and keys are owned by the cell control plane in production?

## Evaluation outcome

Candidate `27a86f1` kept OTel-enabled run `0411bfa5` with zero anomalies and
exact replay across seeds `1103`, `2207`, and `3301`. The three correct
histories staged 12 resolver-accepted records, checked 21 signed resolver
decisions, performed 51 real tLog appends, collected 18 prefix-fence
attestations and 72 exact inventory observations, recovered six records, and
aborted six records. Every history recovered frontier `12`, consumed the
aborted versions `13` and `14`, and published a new crossing-range transaction
at version `15`.

Transaction `12` was quorum-present in both required tLog sets but had no
certificate recorded in the replicated authority. Recovery published it from
the authenticated inventories. Transaction `13` was quorum-absent in set `20`,
so it and dependent transaction `14` were aborted. A partial resolver candidate
was never staged or visible. Each successor started three empty memory-only
resolver processes at floor `12`, rejected old-generation requests and replies,
and used real G2 tLog certificates before publishing the retry. The path used
zero resolver durable synchronizations and zero resolver finalization RPCs.

Seven clean controls replayed exactly and discarded with one anomaly per seed:

| Subject | Run |
|---|---|
| publish before tLog quorum | `48afad06` |
| recover from an authority marker without every tLog inventory | `41e0faf2` |
| activate successor resolvers before the tLog prefix fence | `8265bc49` |
| count an old-generation resolver reply | `4f1bea9a` |
| admit a read below the authenticated frontier | `2f1fe28a` |
| abort a quorum-present record | `a2a75e60` |
| publish beyond the quorum-absence boundary | `415d9372` |

Prometheus observed the correct run with availability `1` and correctness
anomalies `0`. Each control exported availability `0` and three total anomalies
with the exact candidate, suite, profile, run, workload, and backend labels.
The suite hash is `1ed74325`; the profile hash is `cf633bfc`. The correct path
used 210 of 512 budgeted events. Workspace tests and warning-free Clippy passed.

This admits the single-proxy composition, fixed resolver ranges, one bounded
staged window, same-host real processes, and evaluation-only signer custody. It
does not admit multiple commit proxies, online resolver-map movement,
independent hosts, recovery-time objectives, sustained lag, ratekeeping on the
partitioned resolver path, or production identity and key custody.
