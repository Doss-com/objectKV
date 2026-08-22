# RFC-0008: Transaction isolation model

- Status: draft
- Created: 2026-08-22

## Target

Strict serializability with explicit read/write conflict ranges, bounded
transaction lifetime, retry semantics, atomic mutations, and versionstamps.

## Questions to resolve

- Ordered versus hashed resolver partitions.
- Read-version acquisition and real-time ordering.
- Read-your-writes behavior.
- Commit-unknown and idempotent retry contracts.
- Conflict-range representation and garbage collection.
- Threshold for moving beyond one ordered log.
