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

## Questions to resolve

- Ordered versus hashed resolver partitions.
- Read-version acquisition and real-time ordering.
- Read-your-writes behavior.
- Commit-unknown and idempotent retry contracts.
- Conflict-range representation and garbage collection.
- Threshold for moving beyond one ordered log.
- Conflict semantics and recovery when one transaction touches several resolver
  partitions and tagged log sets.
- Maximum transaction bytes, conflict bytes, duration, and range-read result.
