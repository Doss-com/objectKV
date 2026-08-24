# RFC-0053: Commit-proxy loss through transaction-system generation recovery

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0008, RFC-0009, RFC-0011, RFC-0039,
  RFC-0040, RFC-0041, RFC-0042, RFC-0050, RFC-0051, RFC-0052

## Decision under test

`[PROPOSED]` Treat loss of any active commit proxy as loss of the complete
transaction-system generation. Fence the old sequencer, proxies, resolvers, and
tLogs before accepting new work. Derive the old generation's visible boundary
from authenticated inventories of every required tLog set, publish only the
maximal contiguous quorum-present prefix, and abandon the first incompletely
durable ticket plus its dependent suffix.

Activate fresh commit proxies and resolvers in a higher generation whose first
version is greater than every version issued in the old generation. A ticket
allocated before proxy death is never replaced by a no-op and is never replayed
under a second old-generation ticket. Client work without a retained durable
outcome retries in the successor generation with the same request identity.
Client work that became durable before its reply was lost resolves to the exact
retained outcome without a second mutation.

## Why generation recovery is the first contract

RFC-0051 proves one global predecessor chain across three live commit proxies.
It leaves one dangerous gap: a proxy can die after receiving a sequencer ticket
but before every resolver and required tLog sees the ticketed batch. Later
proxies can buffer successors, but they cannot determine whether replacing the
missing nonempty batch with a no-op would discard client work or conflict
evidence.

FoundationDB resolves transaction-system role loss by replacing the complete
transaction-system generation. Its recovery process locks the old coordinated
state and tLogs, derives a known committed version, recruits new proxies,
resolvers, and tLogs, and exposes the successor only after recovery state is
installed. It permits `commit_result_unknown` when recovery overlaps a commit.
This RFC adopts that recovery boundary and combines it with objectKV's stronger
stable-request-identity and authenticated multi-tLog-set contracts.

Sources:

- [FoundationDB recovery internals](https://github.com/apple/foundationdb/blob/main/design/recovery-internals.md)
- [FoundationDB technical overview](https://github.com/apple/foundationdb/wiki/Technical-Overview-of-the-Database)
- [FoundationDB HA write path](https://apple.github.io/foundationdb/ha-write-path.html)

An isolated within-generation proxy takeover may be evaluated later if recovery
duration is unacceptable. It is not required for correctness in this gate.

## Frozen bounded model

Use one cell and tenant, four sequential transaction-system generations, one
replicated three-node sequencing and recovery authority, three commit-proxy
processes per active generation, three memory-only resolver processes under a
fixed map, and two three-process authenticated tLog sets with quorum two. Each
seed has 36 ticketed batches of four transaction attempts and three proxy-loss
episodes. At most eight successor batches may be pending behind a missing
predecessor.

For each seed:

1. start generation `1`, pin every role incarnation, and make batches `1`
   through `6` visible in predecessor order;
2. allocate ticket `7` to proxy `1`, durably record the issued-version high
   watermark and ticket digest, then kill the proxy before resolver delivery;
3. let other proxies allocate tickets `8` through `10`, but require resolvers
   and tLogs to buffer them behind ticket `7` without disposition,
   acknowledgement, or publication;
4. fence generation `1`, authenticate both old tLog-set inventories, recover
   visible boundary `6`, abandon tickets `7` through `10`, and activate
   generation `2` above the old issued-version high watermark;
5. process generation-2 batches `11` through `16`, then kill proxy `2` for
   ticket `17` after all resolver decisions and one required tLog set reach
   quorum but before the second required set receives the frame;
6. buffer tickets `18` through `20`, fence generation `2`, recover visible
   boundary `16`, and abandon ticket `17` plus tickets `18` through `20` even
   though one tLog set contains partial durable evidence;
7. activate generation `3` above every generation-2 issued version and process
   batches `21` through `26`;
8. kill proxy `3` for ticket `27` after its exact frame reaches quorum in both
   required tLog sets but before the client receives its acknowledgement;
9. fence generation `3`, derive visible boundary `27`, preserve ticket `27`
   exactly once, return `commit_unknown` for the dropped reply, and activate
   generation `4` above every generation-3 issued version;
10. retry every abandoned logical request with its original request identity in
    a successor generation, resolve the ticket-27 identity to its retained
    outcome without another mutation, and process fresh batches through `36`;
11. reject every delayed request, resolver reply, tLog append, and client
    acknowledgement from a fenced generation;
12. compare dispositions, retained outcomes, rows, evaluation envelope bytes,
    version uniqueness, generation fences, tLog roots, and acknowledgements
    with a centralized oracle using the same crash boundaries.

The old generation may have versions that were issued but never committed. The
successor does not fill those holes. Its authenticated generation fence closes
the old chain, and its version floor exceeds every old issued version so no
version can be reused.

## Negative subjects

The frozen suite independently attempts to:

1. continue the same transaction-system generation after one commit proxy dies;
2. replace the missing nonempty ticket `7` with a no-op and execute successors;
3. publish ticket `17` from one required tLog set's quorum evidence;
4. omit fully durable ticket `27` from the recovered visible prefix;
5. execute or acknowledge a successor across a missing predecessor before the
   old generation is fenced;
6. derive the recovery boundary from an unauthenticated or incomplete tLog-set
   inventory;
7. reuse an old issued version in the successor generation;
8. accept a delayed old-generation role response after successor activation;
9. replay the lost ticket-27 acknowledgement as a second mutation instead of
   returning its retained request outcome.

Every negative subject must replay exactly, expose at least one contract
anomaly, export OTel, and discard.

## Eval plan

Freeze `cell-commit-proxy-generation-recovery-v0` with seeds `1103`, `2207`,
and `3301`. Each subject uses the same 36 batches, 144 transaction attempts,
and three crash stages per seed. The event budget is 2,048 per seed.

The primary metric is correctness anomalies. Secondary receipts include issued
ticket high watermarks, proxy process starts and deaths, pending successors,
generation fences, authenticated tLog inventory records, recovered visible
boundaries, abandoned tickets, preserved unknown-result commits, stable-identity
retries, rejected old-generation traffic, recovery logical steps, recovery wall
duration, commits, conflicts, and acknowledgements.

The first run establishes semantic recovery and records duration without a
latency threshold. A later curve gate will vary pending-window size, tLog count,
and retained-tail length and will set the availability objective from measured
evidence rather than inventing one here.

## Passing contract

A pass requires:

- three real commit-proxy processes in every active generation;
- proxy loss triggers one complete transaction-system generation fence;
- the old sequencer, proxies, resolvers, and tLogs reject post-fence work;
- recovery reads authenticated inventories from every required old tLog set;
- the recovered boundary is the maximal contiguous quorum-present prefix;
- partially durable ticket `17` and every dependent successor are abandoned;
- fully durable ticket `27` is preserved exactly once despite the lost reply;
- repeating ticket `27`'s request identity returns the retained outcome;
- abandoned client work retries with the same request identity in a successor
  generation and cannot recover a false committed result;
- no missing nonempty ticket is substituted with a no-op;
- no batch executes or acknowledges across a missing predecessor;
- the successor generation starts above every old issued version;
- versions are globally unique across all four generations;
- every transaction uses one generation and one resolver map epoch;
- centralized-oracle dispositions, rows, evaluation envelopes, tLog roots,
  acknowledgement set, and retained outcomes are exact;
- zero resolver durable synchronizations, zero resolver finalization RPCs, zero
  telemetry drops, valid schema, exact replay, and budget hold.

## Failure model

- proxy death after ticket allocation and before resolver delivery;
- proxy death after resolver agreement and one required tLog-set quorum;
- proxy death after every required tLog-set quorum and before client reply;
- successor tickets arriving before a missing predecessor;
- incomplete, altered, or unauthenticated old tLog inventories;
- delayed old-generation role traffic;
- lost successful client reply and stable-identity retry;
- recovery-authority leadership loss is outside this first composition because
  RFC-0041 and RFC-0042 evaluate replicated takeover separately.

## Alternatives

### Replay the exact batch inside the same generation

The sequencer could retain every full canonical batch until it becomes durable,
then allow a replacement proxy to resume it. This can reduce outage time. It
also turns the sequencer into a second transaction payload log and requires
safe takeover at every partial resolver and tLog state. Defer this complexity
until measured generation-recovery duration proves it necessary.

### Close the ticket with an authenticated no-op

A no-op is safe only if no client mutation or conflict decision associated with
the ticket can still commit. Proving that fact after partial delivery requires
the same global fence and inventory work as generation recovery. Substituting a
no-op before that proof can silently discard client work.

### Stall only the failed proxy's assigned tickets

All proxies share one predecessor chain. A missing ticket prevents every
resolver and tLog from safely processing later tickets, regardless of which
proxy owns them. Per-proxy chains would weaken global serial order.

## Tradeoff

This contract optimizes for one conservative and source-backed recovery rule
across proxy, resolver, and tLog failure. It gives up uninterrupted commits
during proxy loss and does not claim a recovery-latency target. If the measured
outage curve is operationally unacceptable, exact-batch successor takeover
becomes a justified optimization with this full-generation path as fallback.

## Compatibility and migration

The bounded proof adds no public API or object format. Generation fences,
issued-version high watermarks, retained request outcomes, and tLog inventory
certificates are internal versioned records. Unknown record versions fail
closed. Existing single-proxy and live multi-proxy gates remain valid as
pre-failure components.

## Unresolved questions

1. What recovery-duration objective is required for a cell before isolated
   within-generation proxy takeover is worth the added replicated payload state?
2. Should a successor fast-forward by a fixed version interval or use a
   generation-prefixed version space?
3. How much successor work may be accepted and buffered before the missing
   predecessor triggers ratekeeping?
4. Can a recovery controller reconstruct the same boundary while one tLog set
   is repairing or changing policy?
5. How does resolver-map split-controller recovery compose with a simultaneous
   commit-proxy loss?

## Evaluation outcome

Candidate `bf72639` kept OTel run `1c55dad7` with zero anomalies and exact
replay across seeds `1103`, `2207`, and `3301`. The three histories issued 108
sequencer tickets and attempted 432 transactions through four sequential
transaction-system generations per seed. They injected nine commit-proxy
deaths, committed 336 transactions, abandoned 24 ticketed batches, retried
those 24 logical batches through stable request identities, and resolved three
fully durable lost-reply outcomes without a second mutation.

The real-process path started 36 commit proxies, 36 memory-only resolvers, and
72 tLogs across the histories. The tLogs performed 510 synchronized record
writes. Eighteen authenticated inventory receipts per seed recovered the exact
common prefix after each failure. The correct subject used 1,830 of 2,048
budgeted events. Prometheus observed availability `1` and correctness anomalies
`0`. Workspace tests and warning-free Clippy passed.

Nine controls replayed exactly and discarded:

| Subject | Run | Anomalies per seed |
|---|---|---:|
| continue same generation | `64afbe20` | 5 |
| replace missing nonempty ticket with no-op | `7435350c` | 6 |
| publish from one required tLog set | `cbdb06b2` | 5 |
| omit fully durable lost-reply ticket | `c65209b1` | 8 |
| execute across missing predecessor | `7aeaba80` | 5 |
| trust incomplete tLog inventory | `e43de6df` | 7 |
| reuse old issued version | `df3c08bd` | 3 |
| accept fenced-generation reply | `4a2ed758` | 2 |
| duplicate unknown-result mutation | `5a3f433f` | 5 |

The evaluated suite hash is `9f8dc2a4`; the profile hash is `74034ac6`.
This admits a conservative same-host generation-recovery rule. It does not
establish an outage objective, independent failure domains, controller failure
during recovery, concurrent resolver-map movement, production authorization,
or signer custody. Exact-batch within-generation takeover remains unneeded
unless the next recovery-duration curve fails its operational target.
