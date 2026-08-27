# RFC-0008: Transaction isolation model

- Status: draft
- Created: 2026-08-22

## Target

Strict serializability with explicit read/write conflict ranges, bounded
transaction lifetime, retry semantics, atomic mutations, and versionstamps.

The normal transaction domain is one tenant database inside one cell. A
transaction may cross any ranges and storage workers in that domain. It cannot
cross cells. Cell v0 may centralize commit and resolver throughput, but the
isolation contract must remain compatible with partitioned resolvers and logs.

## Cell v0 transaction authority

`[PROPOSED]` Cell v0 places one deterministic `TransactionAuthority` state
machine inside the single data `OpenRaft` group. The leader orders transaction
commands through Raft; every replica applies the same conflict decision,
commit version, mutations, and retry outcome at the same log position.

```text
client read at R
  -> TransactionCommand(R, read conflicts, write conflicts, mutations)
      -> quorum-durable OpenRaft entry
          -> deterministic TransactionAuthority apply
              -> committed(version) | conflict(version) | rejected(reason)
```

This optimizes for one recoverable semantic authority before partitioning. It
gives up horizontal commit throughput in Cell v0. Resolver partitioning and
multiple log groups remain later work and must preserve the same command and
history contracts.

The transaction command is value-native and contains no observed values or SQL
meaning. Its versioned encoding carries:

- one read version;
- canonical point or ordered-range read conflicts;
- canonical point or ordered-range write conflicts;
- ordered `Set`, `Clear`, or `ClearRange` mutations.

The outer replicated client command owns stable request identity, generation
fencing, and exact retry. The transaction authority owns conflict checking,
commit-version assignment, and atomic mutation application. A conflict result
is a durable outcome for that request identity and applies no mutation.

For the initial single-group implementation, an accepted commit version is its
applied Raft log index. Versions may therefore contain gaps for membership,
control, or rejected transaction entries. Ordering and uniqueness are required;
contiguity is not.

The authority retains committed write-conflict ranges newer than its conflict
GC floor. Cell v0 does not advance that floor until a bounded transaction-age
and read-version lease policy is implemented. This is intentionally safe and
unbounded for the first semantic process gate, not an admitted production
retention policy.

Snapshot compatibility is required before this state is persisted. The new
transaction-authority field must decode as empty when reading a pre-transaction
state-machine snapshot, and older readers must ignore the added field. Frozen
JSON fixtures cover both directions before the process gate is admitted.

Binary keys in transaction-authority snapshots and process status responses are
encoded as a key-ordered array of `{key, value}` entries, not as JSON object
members. The decoder also accepts the empty JSON object emitted by the initial
pre-process implementation. Non-empty legacy objects are rejected because they
never had a valid binary-key encoding.

## Read-version causality

`[PROPOSED]` A tenant session carries `CellId`, `TenantId`, `RoutingEpoch`, and
`min_known_version`, initially empty. A successful commit or exact read advances
that causal minimum. `ReadVersionService::get(min_known_version)` returns an
active-generation version at or above the minimum only after the transaction
system can serve a snapshot including every commit known complete before the
request, or it returns a retryable unavailable/fenced error.

A proxy never answers from an older cached generation. A serving worker may
return the exact requested version, `version_not_applied`, `version_too_old`, or
a routing/generation error. It never chooses a lower read version. Multi-proxy
work begins only after this rule passes stale-proxy and real-time-ordering
histories.

This optimizes for strict serializability and session handoff across proxies. It
gives up serving a nominally latest read from a lagging proxy or worker when the
caller has observed a newer commit.

## Questions to resolve

- Ordered versus hashed resolver partitions.
- Exact read-version authority protocol and bounded waiting policy.
- Read-your-writes behavior.
- Commit-unknown and idempotent retry contracts.
- Conflict-range representation and garbage collection.
- Threshold for moving beyond one ordered log.
- Conflict semantics and recovery when one transaction touches several resolver
  partitions and tagged log sets.
- Maximum transaction bytes, conflict bytes, duration, and range-read result.

## Executable semantic gate

`[CODE-COMPLETE]` `okv-history-oracle` defines a versioned transaction-history
schema and checks the centralized Cell v0 OCC contract independently of the
resolver subject. A passing history is strict serializable in commit-version
order only when all of the following hold:

1. Point and range reads match the exact declared read version.
2. Real-time predecessors precede the later transaction's read and commit
   versions.
3. Declared read conflicts cover every point or range read.
4. Declared write conflicts cover every written key.
5. No committed write intersects a read conflict between read and commit
   versions.
6. A committed transaction applies its complete write set and an aborted
   transaction applies none.

The deterministic subject emits 1,000 transactions per seed across four key
ranges. It includes overlapping multi-range point transactions and empty-range
reads that exercise phantom protection. Poison subjects accept point conflicts,
accept range phantoms, apply partial commits, omit read or write conflicts, and
use stale read versions.

`[CODE-COMPLETE]` The G0.4 process gate carries the same command and history
contract through one three-node OpenRaft data group. It covers point and empty
range conflicts, atomic multi-range mutation, a lost successful reply, accepting
leader death, successor outcome recovery, exact retry, and restarted-replica
state comparison. Conflict-acceptance and partial-apply faults are independently
detected by `okv-history-oracle`.

This does not cover resolver partitioning, automatic election, independent
failure domains, bounded conflict retention, snapshot installation, or an
external general-purpose checker against a large concurrent client history.
