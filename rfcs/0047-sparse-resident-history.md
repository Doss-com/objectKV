# RFC-0047: Sparse post-frontier resident history

- Status: `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-30
- Supersedes: RFC-0040 full-history activation only
- Scope: native RocksDB RangeEngine format v2 and T27 corrective evaluation

## Decision

Replace the disposable native RangeEngine's full object-base duplication with
one current `head` plus sparse post-frontier `history`. Activation at object
frontier `O` writes every verified base outcome to `head` and writes no history
rows. Before the first post-`O` mutation of a key, the engine stores that key's
current outcome at version `O`; it then appends every tail mutation and updates
`head` and the applied frontier in the same RocksDB write batch.

This changes only disposable local state. Authoritative object and txLog
formats, the public `ResidentRangeEngine` contract, and exact snapshot semantics
remain unchanged.

## Context and invariant

The frozen T27 stratum `c50-z14-s3301` rejected resident format v1. Native p99
was 1.307614x direct RocksDB in AB order and 1.339897x in BA order, above the
1.20 limit. Throughput, CPU/read, physical bytes/read, read amplification,
correctness, pressure, and telemetry passed.

The failure sits at the cache-hit to cache-miss knee. Native incurred a median
9,980 misses per million reads, while control incurred 9,911 and 9,910. Native
local state was 2,215,101,820 bytes versus 1,099,175,660 for the direct control
because activation wrote the complete object base into both `head` and
`history`.

The invariant is:

> For every complete version `T` in `[O, C]`, a resident read returns the exact
> object-base plus txLog result without an object request, including values,
> tombstones, and true absence.

## Proposed contract

### Physical state

```text
object base through O
        │
        ▼
head
  key → latest value or tombstone through C

history
  first touch: key + O → value | tombstone | absent
  every tail mutation: key + commit identity → value | tombstone

metadata
  format v2 | generation | range | object root | O | applied C
```

The history encoding gains an explicit `absent` outcome. Absence and tombstone
remain distinct because a key inserted after `O` must read absent before its
first insertion, while a cleared base key must read its prior value before the
clear.

### Activation

1. Verify the complete named object closure through `O` as today.
2. Populate `head` only.
3. Leave `history` empty.
4. Persist format-v2 metadata and flush the base image.
5. Apply the retained suffix after `O` through the same first-touch rule used by
   live advancement.

The engine maintains an in-memory set of keys whose version-`O` history has
been seeded. It is bounded by keys touched since the current object frontier,
not by the complete base. The existing ordered `known_keys` set remains the
range-clear index for this slice.

### Point mutation

For the first point mutation of `key` after `O`:

1. Read the pre-mutation outcome from `head`.
2. Add `history(key, O) = pre-mutation outcome`, including explicit absence.
3. Add the mutation's versioned history outcome.
4. Update `head`.
5. Advance metadata only in the same atomic batch.

Later mutations skip step 2.

### Range clear

The current RangeEngine expands a range clear over `known_keys`. Before clearing
each first-touched key, format v2 seeds its version-`O` outcome. The clear and
all seeds remain in the same batch as the transaction identity and applied
frontier. This RFC does not change the range-clear algorithm or its eventual
scaling requirement.

### Reads

```text
T = current applied C
  → one head point lookup

O ≤ T < C
  → seek the key's descending history
     ├─ visible entry at or before T → return it
     ├─ no history entries          → key was untouched, return head
     └─ history exists, no O seed   → fail closed as corrupt state
```

Existing snapshot handles retain the transition-epoch sandwich. A read may not
fall back to `head` merely because newer history entries are invisible at `T`;
the required version-`O` seed must be present.

### Bounds and object-frontier movement

Sparse history grows with changed keys and tail mutations between `O` and `C`.
It is reclaimed when a newer authenticated object frontier is installed into a
new disposable RangeEngine image. RFC-0030 still owns safe txLog pop and object
frontier activation. This RFC does not allow local history to become the
durability source.

## Failure model

- Process death before the atomic mutation batch leaves no visible seed,
  mutation, or frontier movement.
- Process death after the batch is irrelevant to durability because recovery
  rebuilds from the named object closure plus retained txLog.
- A missing first-touch seed, malformed outcome tag, history entry below `O`,
  or history prefix with no visible outcome fails closed.
- A stale generation, partial txLog page, cursor gap, or batch-order mismatch
  remains rejected by the existing RangeEngine boundary.
- Local media loss reconstructs format v2 from authoritative state. No v1 or v2
  directory is adopted after process replacement.
- Object-store outage during rebuild follows the existing recovery and
  backpressure contract; it cannot make local history authoritative.

## Alternatives

### Keep the full history copy

Optimizes for: simple historical lookup because every base key has a version
`O` record.

Gives up: about 2x local bytes for a sparse tail, closer current-head physical
layout to direct RocksDB, and the object-native property that permanent history
does not need to be duplicated onto every disposable worker.

### Use RocksDB snapshots only

Optimizes for: no explicit history column family while one process remains
alive.

Gives up: version reconstruction after snapshot release, compaction, process
loss, or empty-worker recovery. This cannot satisfy exact reads throughout
`[O, C]`.

### Read the object base on historical misses

Optimizes for: smaller local history without first-touch seeds.

Gives up: the zero-object-request resident contract, predictable old-snapshot
latency, and correct handling of multiple tail mutations without additional
object and history coordination.

### Change the T27 percentile or limit

Optimizes for: admitting format v1 despite the observed cache boundary.

Gives up: the frozen experiment contract and hides a reproducible physical
layout difference. The gate remains unchanged.

## Eval plan

### V2.0 semantic and format tests

The implementation begins with failing tests that require:

1. zero history rows immediately after activation;
2. exact value, tombstone, and absence at `O` after first mutation;
3. exact intermediate versions across repeated mutations of one key;
4. exact batch and mutation order across point and range clears;
5. one version-`O` seed per changed key, never one per mutation;
6. a corrupt changed-key history without an `O` seed to fail closed;
7. the current snapshot to retain one `head` lookup per point;
8. old format-v1 value and tombstone fixtures to remain readable by the shared
   history decoder, while format v2 adds explicit absence;
9. the same authoritative object-base and txLog fixtures to rebuild exact state
   under both the retained v1 oracle and v2 candidate.

### V2.1 local physical preflight

Using one fixed logical base and sparse tail:

- native local bytes must be at most 1.25x direct RocksDB;
- history rows must equal unique first-touched keys plus tail mutations;
- current point reads must perform exactly one data-cache lookup;
- zero object operations may occur after activation;
- exact snapshots must pass at `O`, every tail commit, and `C`.

### V2.2 failed-stratum replay

Build a new provider identity and execution plan, then replay only
`c50-z14-s3301` on the same 1 GiB fixture and hardware class. Keep the original
T27 gates unchanged:

- throughput at least 0.80x direct RocksDB;
- p99 at most 1.20x in AB and BA order;
- CPU/read, physical bytes/read, and read amplification at most 1.25x;
- exact values, nonzero pressure, fresh process state, and complete OTel;
- native local bytes at most 1.25x direct RocksDB as an added correction gate.

If this replay fails, retain it and select the next physical-layout correction.
If it passes, freeze a new plan and restart all 27 direct-NVMe strata plus both
buffered sentinels. Results from provider v1 and v2 may not be combined into one
admission curve.

## Compatibility and migration

The provider string and local metadata move from
`rocksdb-11.8.1-native-resident-v1` to
`rocksdb-11.8.1-native-resident-v2`. Authoritative `OKVB`, `OKVI`, manifest,
txLog, version, and public API formats do not change.

Disposable directories are not migrated or opened across provider versions.
Upgrade and rollback both rebuild an empty directory from the same authenticated
object root and retained txLog. Format fixtures pin the v1 history tags and the
v2 explicit-absence tag so a future decoder change cannot silently reinterpret
historical outcomes.

## Unresolved questions

No unresolved question changes the first implementation slice. Replacing the
in-memory `known_keys` range-clear index, advancing `O` without complete worker
replacement, and native ordered scans remain separate measured decisions.
