# RFC-0021: SlateDB Phase 0 filesystem baseline

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

objectKV will establish its first executable physical-economics incumbent by
running a pinned, unmodified SlateDB revision against Apache `object_store`'s
local filesystem backend. The contract measures deterministic ingest, warm and
cold point reads, ordered scans, and empty-cache reopen. It does not claim
objectKV MVCC, distribution, durability, or compaction semantics.

## Context and invariant

The existing Phase 0 suite declares useful workloads, but its `set`, `get`,
`scan`, `overwrite_then_compact`, and `reopen_then_get` operations have no
runner implementations. Architecture work cannot establish whether object
storage is economically viable while the physical baseline is declarative.

SlateDB revision `e0161973d8d7ffdede7c44725729838811674e99` is already pinned
behind `okv-slate`. It exposes the required put, get, scan, flush, close, and
reopen APIs. It does not expose all objectKV historical-read, range-clear, or
distributed-authority seams. The invariant for this baseline is therefore
narrow: every measured read and scan must equal an independently generated
deterministic dataset before any economics result is admissible.

## Proposed contract

### Inputs

The local profile fixes:

- SlateDB source revision;
- filesystem object-store implementation;
- 8,192 lexicographically ordered keys;
- 8 MiB of logical values, distributed deterministically across the keys;
- seeds `1103`, `2207`, and `3301`;
- fixed point-read samples and a fixed 100-row scan per seed;
- a 60-second execution budget.

Keys and values are derived from the seed and ordinal without randomness from
the operating system. The expected value for any key is independently
reconstructable without reading SlateDB.

### Execution

For each seed, the runner:

1. creates an isolated filesystem object-store root and SlateDB path;
2. writes the deterministic dataset and waits for an explicit flush;
3. checks fixed point reads while the database and cache are warm;
4. checks one exact ordered 100-row scan;
5. closes the database;
6. constructs a new SlateDB instance with a new in-process cache over the same
   object-store root;
7. measures time from reopen start through the first verified correct read;
8. checks fixed point reads again through the reopened instance;
9. closes the reopened instance and records the object-store operation totals.

The first-correct-read timer includes database open and the verifying get. The
deterministic receipt excludes elapsed time and physical object names. It binds
the seed, dataset parameters, requested keys, expected values, scan rows, and
logical outcomes.

### Object I/O accounting

The filesystem object store is wrapped below SlateDB. The wrapper records
successful and failed request counts by API, bytes offered to writes, and byte
ranges returned to reads. Counts include metadata, manifest, WAL, and SST
traffic that reaches the backend. They exclude filesystem calls hidden inside
one `object_store` API invocation.

This contract records physical evidence but sets no cost ceiling. A later
comparison suite may set ceilings only after the same profile produces a stable
incumbent distribution.

### Observable errors

The run fails closed on:

- a missing, extra, out-of-order, or incorrect logical value;
- a failed flush, close, reopen, or object-store operation;
- reuse of the original SlateDB instance for the reopen phase;
- absent request or byte accounting;
- a different logical receipt from the repeated deterministic oracle pass;
- exceeding the fixed execution budget.

## Failure model

This first baseline covers process-local cache replacement through close and
reopen. It does not inject process death, partial filesystem writes, object
timeouts, stale listings, lost replies, disk-full faults, or stale owners.
Those belong to the object-store conformance and failure-recovery suites.

The deliberate negative control, `reuse_warm_db_for_reopen`, skips close and
fresh-instance construction before the timed first read. It can return the
right value with artificially cheap I/O, but must discard because the
`fresh_db_cache_on_reopen` gate fails.

## Alternatives

- Implement every workload in `phase0.toml` at once. This optimizes for breadth,
  but mixes unproved compaction forcing, cloud profiles, and champion
  comparison logic into the first executable baseline.
- Use SlateDB's benchmark binary directly. This optimizes for upstream reuse,
  but gives up objectKV's frozen suite, hard-gate, result-schema, source-identity,
  and OTel contracts.
- Begin with MinIO or GCS. This optimizes for remote-store realism, but adds
  credentials, network noise, and service configuration before the local
  oracle and instrumentation are trusted.
- Treat SlateDB as the objectKV kernel. This reduces initial code, but gives up
  sovereignty over explicit versions, historical reads, range clears,
  transaction authority, and durable formats.

## Eval plan

The fixed suite is `evals/suites/phase0-slate-filesystem.toml`. Its primary
metric is `recovery.first_correct_read_duration`. Correctness anomalies are a
hard zero gate. Secondary evidence includes per-API object-store requests,
read and write bytes, phase throughput, and cache-hit observations.

The independent oracle regenerates expected keys and values from the frozen
seed and ordinal. The normal workload must keep. The
`reuse_warm_db_for_reopen` workload must discard while preserving logical
result exactness, proving that the physical cache-state gate is active.

Compaction amplification is explicitly absent from this suite. A follow-up RFC
must define how a pinned SlateDB compactor is started, how completion is
observed, and which bytes belong to compaction before that lane can be admitted.

## Compatibility and migration

This suite is an incumbent measurement, not a public storage format. Any change
to the SlateDB revision, dataset generator, object-store accounting, cache-reset
procedure, oracle, or result schema changes the suite contract hash and starts a
new baseline. Old receipts remain valid only for their exact suite and profile
hashes.

## Unresolved questions

- Which cache and block-size settings should define the comparable MinIO and
  GCS profiles?
- What explicit completion signal makes SlateDB compaction amplification
  reproducible without inspecting private state?
- Should object bytes count requested ranges, delivered stream bytes, or both
  once remote backends are added?
- Which target workload and price snapshot should set the first Gate 1 cost
  ceilings?
