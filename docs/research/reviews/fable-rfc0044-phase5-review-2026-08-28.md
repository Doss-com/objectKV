# Fable adversarial review: RFC-0044 phase 5

- Date: 2026-08-28
- Reviewer: Fable, Claude Code review lane
- Source revision inspected: `14b4f26`
- Disposition: accepted, implementation blocked pending five corrections

## Question

Can phase 5 isolate resident-layout performance under cache pressure without
accidental fixture regeneration, hidden caches, or page-cache reuse?

## Punchline

Not on the current runner. The suite becomes credible after the locator crosses
CLI invocations, the direct control owns no hidden native database, every ABBA
position is a fresh process, fixture and trace seeds are independent, and the
I/O treatment is explicit.

## Findings

1. `ObjectFixtureLocatorV1` has the required semantic fields, but phase 4 keeps
   it in a local variable. Preserved results do not provide a trusted locator
   envelope for another invocation.
2. The direct-control branch opens and populates the native resident engine
   first, then creates a second RocksDB and cache from the native snapshot. The
   control topology is therefore unmatched.
3. Reused-fixture repeats are multiple samples inside one process. Resetting
   the RocksDB block cache does not reset process, allocator, file descriptor,
   database, or Linux page-cache state.
4. A 1 GiB buffered database can fit in the runner's Linux page cache. Cache
   labels alone cannot establish NVMe pressure.
5. One seed currently drives fixture bytes, txLog tail values, and Zipf key
   selection, so data-layout and trace variance are coupled.

## Decisions

- D1: use one fixture seed, 4244, and independent trace seeds 1103, 2207, and
  3301.
- D2: use matched direct table reads for the admitting curve. Label it
  cache-to-NVMe mechanism evidence.
- D3: keep two separately gated buffered product sentinels with page-residency
  evidence.
- D4: emit one receipt per fresh subject process and gate all 27
  cache/skew/trace strata independently in both order positions.
- D5: implement locator serialization and the standalone direct subject before
  building the full controller.

## Counter

This review would be wrong if the current runner could already consume a
persisted placement locator across CLI invocations and prove exactly one
database and cache per subject. The inspected source does neither.

## Required first poisons

- missing locator;
- read-only GCS consumer attempting a write or LIST;
- direct subject with a hidden second database or cache;
- AABB receipts presented as ABBA;
- mismatched locator or trace identity;
- buffered execution labeled direct.

No source files were changed by the reviewer.
