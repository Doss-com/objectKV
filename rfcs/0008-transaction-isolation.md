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
