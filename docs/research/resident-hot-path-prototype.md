# Resident hot-path prototype finding

Status: `[EXISTS]` diagnostic evidence only. It is not a comparable G3.1 suite
receipt.

## Question answered

A minimal resident-range wrapper added generation, key-range, read-version,
coverage, and recent-overlay checks around the same RocksDB handle used by the
direct control. The question was whether that seam alone creates a visible
resident point-read tax.

## Profile

- Rust: `1.88.0`
- Rust crate: `rocksdb 0.25.0`
- System RocksDB: `11.8.1`
- Machine: Apple arm64
- Dataset: 65,536 keys, 1,024-byte values, 64 MiB logical
- Access: read-only resident point reads, deterministic 80/20 hotset
- Warmup: 100,000 reads
- Measurement: 200,000 reads per pass, four balanced passes per path
- Seeds: 1103, 2207, 3301
- Cache: one shared in-process RocksDB handle and block cache
- Value bodies: compressible zero-filled prototype data

## Results

| Seed | Control ops/s | Candidate ops/s | Throughput ratio | Control p99 | Candidate p99 | p99 ratio | Object fallbacks |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1103 | 1,251,597 | 1,255,701 | 1.003 | 1,375 ns | 1,333 ns | 0.969 | 0 |
| 2207 | 1,267,814 | 1,264,480 | 0.997 | 1,375 ns | 1,334 ns | 0.970 | 0 |
| 3301 | 1,145,363 | 1,217,627 | 1.063 | 1,542 ns | 1,416 ns | 0.918 | 0 |

Median candidate-to-control throughput ratio was `1.003`. Median p99 ratio was
`0.969`. Every read returned the same digest and no candidate read invoked the
object fallback.

## Interpretation

The wrapper did not create a measurable penalty in this profile. The candidate
appearing slightly faster is not treated as improvement. The absolute
differences are smaller than credible timer, code-layout, cache, and operating
system noise at roughly 1.3 to 1.5 microseconds per read.

This supports one narrow decision: keep the resident-range checks in the formal
G3.1 candidate. Do not optimize them away before a more complete serving path
exists.

The prototype's local-byte footprint is not evidence for the resident storage
budget because the zero-filled value bodies compressed heavily. The formal
runner replaces them with deterministic incompressible bytes.

## Not learned

- No network hop, routing service, Raft read policy, transaction overlay, or
  concurrent client was measured.
- The same RocksDB handle and cache served both paths.
- No object base, hydration, compaction, demotion, or cache budget was exercised.
- No writes, snapshots older than the resident image, process failures, or
  PostgreSQL pages were exercised.
- No OTel receipt or clean committed candidate exists.

## Absorption

RFC-0023 and the feature-gated `okv-eval` resident runner absorb the validated
seam. Separate-process control and candidate runs, local-byte measurement,
fallback poisoning, and registered telemetry are now part of the executable
contract. Concurrency, overlay, system-resource, and object-read curves remain
future gates.
