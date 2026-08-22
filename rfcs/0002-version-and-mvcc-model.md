# RFC-0002: Version and MVCC model

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

objectKV exposes a totally ordered `CommitVersion` represented logically as
`(generation, sequence)`. Generations never decrease; sequences increase within
one generation; gaps are legal; and no committed version may ever be reused.
The current `Version(u64)` model and SlateDB sequence number are bootstrap
adapters, not the stable wire representation.

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
- A recovery generation is strictly greater than every generation the quorum
  has previously activated.
- Within one active generation, the sequencer allocates increasing sequence
  values. Allocation may leave gaps after reservation, failure, or recovery.
- WAL position is separate from commit version. Recovery detects a missing WAL
  suffix through contiguous log indexes and checksums, never by assuming commit
  versions are gapless.
- `(g1, s1) < (g2, s2)` when `g1 < g2`, or when `g1 == g2` and `s1 < s2`.
- The stable binary encoding must preserve this order and must not silently
  truncate either component. Its byte layout remains unresolved until the
  longevity and allocation-rate analysis is complete.

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

### Retention and wall clock

`oldest_readable_version` is the minimum safe boundary after considering active
transactions, named snapshots, branches, backup roots, CDC positions, and
analytical materializations. A stalled consumer is governed by an explicit
lease and expiry policy; it cannot pin history forever by accident.

Commit versions are not timestamps. A separately persisted monotonic sample map
relates selected commit versions to wall-clock time for PITR and operations.
Clock movement cannot change transaction order.

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

## Alternatives

- One gapless global `u64` is simpler for storage engines, but recovery would
  need to prove both gaplessness and non-reuse across authority changes.
- Hybrid logical clocks encode time and order together, but clock semantics add
  an unnecessary correctness dependency to a sequenced commit path.
- Per-range versions scale allocation but make global PostgreSQL snapshots and
  analytical coverage vectors materially more complex before scaling evidence
  exists.

## Eval plan

- Extend `okv-model` with generations, range clears, large-value references,
  read-your-writes, and retention boundaries.
- Generate histories with gaps, duplicate delivery, conflicting replay, stale
  generations, and range-clear overlap.
- Hard gates: zero model divergence, zero version reuse, exact failing-seed
  replay, and no silent read fallback.
- Negative controls: reuse one committed version and serve one future read from
  stale applied state. Both must fail.

## Compatibility and migration

The repository has not published a stable version encoding. The existing
`Version(u64)` remains an internal Phase 0 adapter. Accepting this RFC requires a
versioned binary envelope and migration fixture before durable objects are
declared stable.

## Unresolved questions

- Exact component widths and stable byte encoding.
- Maximum version allocation rate and expected cluster lifetime.
- Lease duration and operator policy for stalled CDC, snapshots, and branches.
