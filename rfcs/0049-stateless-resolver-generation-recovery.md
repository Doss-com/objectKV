# RFC-0049: Stateless resolver generation recovery

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0008, RFC-0011, RFC-0029, RFC-0032, RFC-0033, RFC-0034,
  RFC-0041, RFC-0043, RFC-0048

## Decision under test

`[PROPOSED]` Keep resolver conflict history only in generation-scoped memory.
Do not synchronize resolver prepares or finalizations to local storage. If a
resolver fails, stop the complete transaction-system generation, fence its
commit path, recover the durable replicated-authority prefix, and start a successor
generation with empty resolver state and a read-version floor equal to the
recovered committed boundary.

An accepted decision at only a subset of resolvers may remain in memory and
cause bounded false conflicts. It must never cause a false commit. Work that
has not crossed the durable replicated-authority visibility boundary when recovery
starts is not committed and clients must retry it in the successor generation.

## Why this follows RFC-0048

RFC-0048 proves exact centralized-oracle agreement with durable per-partition
prepare and finalize. That protocol imposes at least two resolver storage
synchronizations and two resolver RPC rounds on committed work. FoundationDB's
published architecture instead makes resolvers stateless, accepts conservative
partial-admission false positives, and recovers role failure by replacing the
transaction-system generation.

RFC-0049 tests whether objectKV can safely remove the durable resolver journal
while preserving its existing generation and tagged-log durability contracts.

## Frozen bounded model

Use one tenant, three ordered resolver ranges from RFC-0048, one three-node
replicated transaction authority, and three resolver OS processes with private
empty scratch directories. One commit-proxy controller submits ordered batches
of eight candidate transactions. Each resolver processes its clipped batch in
candidate-version order and immediately adds accepted write conflicts to its
in-memory recent history.

For each seed:

1. commit a prefix containing point, empty-range, crossing-range, all-range,
   disjoint, and stale-read shapes;
2. create one batch in which an early candidate is accepted by one resolver
   and rejected by another, then observe at least one safe false conflict;
3. start another batch, obtain a strict subset of resolver replies, and kill
   resolver `2` before the global disposition exists;
4. fence generation `G1` and classify its durable tagged-log prefix;
5. discard the unresolved batch and start generation `G2` with new resolver
   incarnations and empty conflict histories;
6. set the `G2` read-version floor to the last recovered `G1` commit;
7. reject a delayed `G1` request and reply;
8. retry abandoned logical work with a new `G2` identity and read version;
9. commit a second ordered batch and prove that every partitioned commit is
   admitted by the centralized oracle, every centralized conflict is rejected,
   and safe false conflicts never become visible;
10. compare rows, commit envelopes, consumed candidate versions, and the
    recovery boundary with the authoritative outcome history.

The correct subject performs no resolver file write, file synchronization, or
finalize RPC. The replicated authority is the only durable commit boundary in
this gate. Composition with the separately admitted tagged-log durability and
generation-fence protocols remains outside this falsifier.

## Negative subjects

The frozen suite independently attempts to:

1. continue generation `G1` after one resolver is lost;
2. activate `G2` before the old replicated-authority prefix is fenced;
3. count a delayed `G1` resolver reply toward a `G2` decision;
4. admit a `G2` transaction whose read version is below the recovery floor;
5. publish the unresolved partial-reply `G1` transaction during recovery;
6. omit a durably committed `G1` head from the recovery floor.

Every subject must replay exactly, expose at least one safety or availability
contract anomaly, export OTel, and discard.

## Eval plan

Freeze `cell-stateless-resolver-generation-recovery-v0` with seeds `1103`,
`2207`, and `3301`. Each seed evaluates 600 attempts across ordered batches of
eight and one generation recovery. The event budget is 2,400 per seed.

The primary metric is correctness anomalies. Secondary receipts include
commits, conflicts, safe false conflicts, resolver decisions, ordered batches,
process starts, generation fences, abandoned candidates, recovery duration,
resolver durable synchronizations, and resolver finalization RPCs.

## Passing contract

A pass requires:

- exact visible rows and commit-envelope chain against the authoritative
  partitioned outcome history;
- every partitioned commit is admitted by the centralized oracle;
- every centralized conflict is rejected by the partitioned path;
- every commit has complete resolver agreement and replicated-authority
  durability;
- safe false conflicts are counted separately and never become false commits;
- resolver failure prevents further work in the old generation;
- successor activation follows a replicated old-generation fence marker;
- the recovery floor includes every acknowledged old-generation commit;
- successor resolver scratch starts empty;
- every successor read version is at or above the recovery floor;
- old-generation requests and replies fail closed;
- unresolved old-generation work is absent from visible state;
- abandoned logical work retries with a new identity and read version;
- zero resolver durable synchronizations and zero resolver finalize RPCs;
- exact canonical replay, zero telemetry drops, valid schema, and budget hold.

## Alternatives

### Keep RFC-0048 as the production protocol

This permits isolated resolver restart in one generation and gives exact
centralized conflict outcomes. It serializes throughput on resolver persistence
and global finalization. Keep it as an oracle and fallback until RFC-0049 passes.

### Replay recent committed writes into replacement resolvers

This could avoid a full transaction-system generation change, but requires a
new barrier proving that every replacement has replayed the same conflict
window before serving. It adds a second recovery protocol and does not match
the simpler FoundationDB pattern.

### Replicate each resolver partition

Replication retains availability through one process failure but places
consensus inside the conflict-checking path. This may become appropriate for a
different latency and availability target. It is not required to test whether
stateless generation recovery is safe.

## Tradeoff

This contract optimizes for a batched, memory-only resolver path and one
well-tested generation recovery mechanism. It gives up transaction-system
availability during any resolver failure, exact centralized commit rates under
cross-partition contention, and preservation of old read transactions across
recovery.

## Unresolved questions

1. What recovery-time target makes whole-generation replacement operationally
   acceptable for a cell?
2. How are several commit proxies ordered at every resolver?
3. How are resolver-map split and merge synchronized with generation recovery?
4. What bounds the recent conflict window and safe false-conflict rate under
   hot-range workloads?
5. Which tLog certificate is sufficient to derive the successor recovery floor?

## Evaluation outcome

Candidate `b69b245` kept OTel-enabled run `e334c857` at 1,800 of 2,400
allowed events with zero anomalies and exact replay across seeds `1103`,
`2207`, and `3301`. The correct subjects evaluated 1,800 attempts through
228 ordered batches. They committed 699 transactions, rejected 1,098
conflicts, classified three conservative false conflicts, checked 2,706
resolver decisions, abandoned three candidates at resolver loss, and crossed
three replicated generation-fence markers. Every successor started three empty
memory-only resolver processes at the exact recovered floor. Old-generation
requests and replies failed closed. The unresolved work remained invisible and
retried under new identities.

The correct receipt used 27 process starts across three seeds, zero resolver
durable synchronizations, and zero resolver finalization RPCs. Visible rows and
commit-envelope chains matched the authoritative outcomes. Every partitioned
commit was allowed by the centralized oracle and every centralized conflict
was rejected.

Six clean controls replayed exactly and discarded with one anomaly per seed:

| Subject | Run |
|---|---|
| continue after resolver loss | `d2dde4c1` |
| activate the successor before the old fence | `e9551019` |
| count an old-generation resolver reply | `1fa4c4a9` |
| admit a read below the recovery floor | `ed58133e` |
| publish unresolved old-generation work | `0ea78ab3` |
| omit the durable head from recovery | `0cc71d81` |

Prometheus observed correctness anomalies `0`, availability `1`, 699 commits,
1,098 conflicts, 2,706 checked resolver decisions, and the exact candidate,
suite, profile, run, workload, and backend labels. The frozen source suite hash
is `7f2b60eb`; the evaluated suite hash is `5c74b499`; the profile hash is
`1a51aed8`.

This admits one commit-proxy controller, fixed resolver ranges, ordered batches
of eight, one resolver failure, one same-host replicated-authority fence marker,
and bounded deterministic histories. It does not compose the existing
authenticated tLog fence into the same run, admit multiple commit proxies,
measure recovery availability, move resolver ranges online, cover independent
hosts, or establish production identity and key custody.
