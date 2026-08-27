# RFC-0023: Resident ServingWorker hot-path gate

- Status: proposed
- Authors: DOSS
- Created: 2026-08-25

## Decision

`[EVALUATING]` G3.1 compares a bounded resident ServingWorker read seam with a
direct RocksDB control under the same deterministic dataset, access
distribution, RocksDB options, operation budget, and system library. A complete
resident range must perform exact point reads without invoking the object-base
fallback after warmup.

The first executable candidate performs these checks before its local RocksDB
read:

1. active cell-generation match;
2. assigned key-range membership;
3. recent-version overlay lookup;
4. resident-image coverage through the requested version;
5. point lookup in the local RocksDB image.

This is a serving-path experiment. It does not make RocksDB part of the durable
object format or turn the local image into authoritative state.

## Frozen profile

The `resident-dev` profile uses:

- 65,536 keys and deterministic incompressible 1,024-byte values, 64 MiB
  logical;
- deterministic 80/20 hotset reads;
- 100,000 warmup reads;
- 200,000 measured reads per repeat;
- four repeats for each of three fixed seeds;
- a 128 MiB local-byte ceiling;
- one process, one range, and one client thread.

Candidate and control execute in separate `okv-eval` processes in ABBA order.
The report aggregates two candidate and two control run medians. Dataset
population is excluded from the measured operation samples. Both paths disable
the population-only WAL and flush before warmup, because the local database is
a benchmark fixture rather than the kernel's replicated `txLog`.
The benchmark binary uses Cargo's optimized release profile.

## Hard gates

- every measured read returns a value with the expected key identity and
  checksum;
- the candidate records zero object-base fallback attempts after warmup;
- local RocksDB bytes remain within the declared profile budget;
- the poisoned candidate makes resident coverage incomplete only after warmup,
  records fallback attempts, and is discarded;
- the product-level comparison treats candidate throughput and p99 against the
  direct control as separate curves, not a blended score.

## What this gate can decide

The gate can reject a resident wrapper that adds a material in-process read
penalty, silently falls through to objects, returns incorrect values, or needs
an unbounded local image for the frozen range.

The gate cannot yet establish distributed SQL latency, Raft read latency,
concurrent-client scaling, recent-overlay behavior, hydration, demotion,
object-indexed cold reads, write throughput, PostgreSQL page behavior, or
production resource economics. G3.1 remains executable evidence until those
adjacent curves and clean committed receipts exist.

## Poison contract

The negative control warms a complete resident image, then lowers its coverage
watermark below the read version. Each measured read records an object-base GET
fallback attempt before returning from the same fixture database. This is an
instrumentation poison, not an object-store performance benchmark.

## Alternatives

- Comparing against an in-memory map would measure a different storage engine
  and would not isolate ServingWorker overhead.
- Sharing one RocksDB handle between paths reduces setup differences but allows
  cache and ordering interference. The executable gate uses separate processes.
- Adding network, consensus, and concurrency now would make a regression hard to
  attribute. Those curves follow only after this seam is stable.
