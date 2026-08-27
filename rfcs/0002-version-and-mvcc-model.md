# RFC-0002: Version and MVCC model

- Status: proposed; implementation evidence `[EVALUATING]`
- Authors: DOSS
- Created: 2026-08-22

## Decision

Within one cell, objectKV exposes a totally ordered `CommitVersion` represented as
`(generation: u64, sequence: u64)`. The stable value encoding is 16 bytes,
`generation_be || sequence_be`, so byte order equals logical order. `(0, 0)` is
reserved. Generations never decrease; sequences increase within one generation;
gaps are legal; and no committed version may ever be reused.

At 10 million committed versions per second, one `u64` sequence space lasts
more than 58,000 years. Version exhaustion is therefore an invalid state, not a
wraparound case. The containing durable batch envelope still requires an
explicit codec version before its layout is stable.

## Context and invariant

Snapshots, exact replay, recovery, CDC, analytical coverage, and GC need one
unambiguous order. A bare counter is insufficient because recovery must fence
the old transaction system before a new allocator can issue versions.

For a read version `R`, a key's visible value is the newest applicable mutation
with `version <= R`, after point and range tombstones are applied. All mutations
in one committed batch become visible atomically at one version.

## Proposed contract

### Allocation and ordering

- The bootstrap coordinator quorum owns the active generation.
- Version order is cell-scoped. Different cells have independent version spaces
  and no cross-cell snapshot or transaction order.
- A recovery generation is strictly greater than every generation the quorum
  has previously activated.
- Within one active generation, the sequencer allocates increasing sequence
  values. Allocation may leave gaps after reservation, failure, or recovery.
- WAL position is separate from commit version. Recovery detects a missing WAL
  suffix through contiguous log indexes and checksums, never by assuming commit
  versions are gapless.
- `(g1, s1) < (g2, s2)` when `g1 < g2`, or when `g1 == g2` and `s1 < s2`.
- The stable value encoding is 16-byte big endian. Inverted-order and generation
  rollover fixtures must fail any truncated or packed alternative.

### Mutation batches and replay

Each committed batch carries:

- commit version;
- client idempotency identity;
- canonical ordered mutations;
- batch fingerprint over the version, identity, and mutations;
- schema or codec version for the batch envelope.

The first application of a version records its fingerprint. Reapplying the same
version and fingerprint is a no-op success. Reapplying the version with a
different fingerprint is corruption and halts that apply stream. Mutation order
inside the canonical batch cannot affect the fingerprint.

Canonicalization sorts mutations by kind and bytewise key/range, removes exact
duplicates, and rejects two distinct point mutations for one key. A point
mutation wins over a covering range clear at the same version. This gives a
stable meaning equivalent to applying range clears before points without making
caller order part of commit identity.

Initial mutation kinds are `Set(key, value)`, `Clear(key)`, and
`ClearRange(begin, end)`, where ranges are half-open. Atomic arithmetic and
merge operations are resolved by the transaction layer into a committed value
before they enter the immutable entry stream. Large values are immutable,
checksum-addressed objects whose reference becomes visible in the same batch;
an unresolved reference makes the entire batch unavailable, not partially
visible.

### Reads

- `read_at(R)` is exact. It never falls back to an older snapshot.
- If `R > applied_version`, the worker returns `version_not_applied` with its
  current applied version and a retry hint.
- If `R < oldest_readable_version`, the worker returns `version_too_old`.
- A latest read first obtains a read version from the transaction authority,
  then performs `read_at(R)`. A worker cannot define latest independently.
- A transaction has a local write overlay, so it reads its own writes before
  commit without publishing a partial version.
- Read availability checks `R > applied_version` before
  `R < oldest_readable_version`. The oldest-readable boundary is inclusive.

### Retention and wall clock

`oldest_readable_version` is the minimum safe boundary after considering active
transactions, named snapshots, branches, backup roots, CDC positions, and
analytical materializations. A stalled consumer is governed by an explicit
lease and expiry policy; it cannot pin history forever by accident.

Commit versions are not timestamps. A separately persisted monotonic sample map
relates selected commit versions to wall-clock time for PITR and operations.
Clock movement cannot change transaction order.

The boundary advances monotonically, may advance through an unallocated version
gap, and cannot advance beyond applied state. Logical expiry does not require
immediate physical deletion.

## Worked failure cases

1. A sequencer reserves sequence 42 and dies before append. The next commit may
   use 43. Recovery does not invent commit 42 and does not report corruption.
2. Generation 7 acknowledges `(7, 91)`, loses leadership, and later wakes. The
   coordinator has activated generation 8, so every generation-7 publication
   is rejected even when its sequence is numerically higher than current work.
3. A worker at applied version `(8, 10)` receives a read at `(8, 12)`. Returning
   data from `(8, 10)` would create an impossible snapshot, so it returns
   `version_not_applied`.
4. A lost response causes a client to retry the same batch identity at the same
   version. The identical fingerprint succeeds idempotently. Different bytes at
   that version stop the apply stream as corruption.
5. A range clear at version 20 covers key `k`; an older set at 19 is hidden, and
   a later set at 21 is visible.
6. A range clear and point set both cover `k` at version 20. The point set is
   visible because same-version point precedence is part of canonical semantics.
7. The oldest-readable boundary is version 30. Reads at 30 remain valid, reads
   below 30 return `version_too_old`, and reads above applied state return
   `version_not_applied` even when both conditions could be inferred locally.

## Alternatives

- One gapless global `u64` is simpler for storage engines, but recovery would
  need to prove both gaplessness and non-reuse across authority changes.
- Hybrid logical clocks encode time and order together, but clock semantics add
  an unnecessary correctness dependency to a sequenced commit path.
- Per-range versions scale allocation but make global PostgreSQL snapshots and
  analytical coverage vectors materially more complex before scaling evidence
  exists.

## Executable evidence

- `[VERIFIED]` `okv-model` implements the 16-byte generation-aware value, exact
  replay identity, point and half-open range tombstones, scans, retention
  boundaries, and read-your-writes.
- `[VERIFIED]` `evals/suites/model-history.toml` runs five deterministic seeds,
  1,000 events per seed, against an independently normalized full-snapshot
  oracle. It does not reuse the model's replay fingerprint.
- `[VERIFIED]` The current developer diagnostic covers 5,000 events, 78,505
  point/scan reads, 925 range clears, 75 exact replays, 55 conflicting replays,
  65 future reads, 65 retention advances, 70 expired reads, 85 historical/gap
  reads, and 60 stale-generation attempts with zero divergence.
- `[VERIFIED]` Seven deliberate subjects separately break range clears, canonical
  replay order, conflicting replay, future-read availability, inclusive
  retention, expired-read rejection, and stale-generation fencing. Each is
  rejected at a deterministic prefix between steps 2 and 9.
- `[EVALUATING]` Run the same histories against storage-engine adapters after
  their range-clear, explicit-version read, and retention seams exist.

## Compatibility and migration

The repository has not published a stable durable batch envelope. The SlateDB
adapter accepts only generation zero because its public external sequence seam
is one `u64`; it now stores the complete logical version in private metadata and
fails explicitly on later generations. Accepting this RFC for durable objects
requires the versioned envelope and migration fixture.

## Unresolved questions

- Maximum version allocation rate and expected cluster lifetime.
- Lease duration and operator policy for stalled CDC, snapshots, and branches.
- Whether client-request deduplication needs a record separate from apply-stream
  replay identity before the transaction authority exists.
