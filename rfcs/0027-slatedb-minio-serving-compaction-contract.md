# RFC-0027: SlateDB MinIO serving and compaction contract

- Status: accepted for local S3-compatible evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0024, RFC-0025, RFC-0026

## Decision

[ACTIVE-WORK] Run the already frozen `objectkv-serving-v1` writer, separate
coordinator, separate compaction worker, fresh reopen, and exact-read oracle
through the pinned MinIO S3-compatible service. Reuse the same store-independent
contract core as the local filesystem receipt.

Every workload receives a unique object prefix. The experiment does not delete
remote state. This keeps the first physical receipt recoverable and avoids
making garbage collection an implicit dependency.

## Question

Does the physical candidate retain exact data, format-compatible maintenance,
bounded fresh serving reads, and observable object economics when immutable SSTs
and manifests cross a real S3-compatible HTTP boundary?

RFC-0025 proves the same contract on a local object-store implementation.
RFC-0026 separately proves real operating-system worker death, persistent job
reclaim, and replacement-worker completion. This contract does not combine
worker-process death with MinIO. It isolates the remote physical path first.

## Frozen contract

For each seed:

1. Build the object store from `OKV_S3_*` environment configuration.
2. Require path-style S3 requests, HTTP for the local service, and ETag-match
   conditional puts.
3. Open the serving writer with object WAL and embedded maintenance disabled.
4. Write and explicitly flush eight equal key ranges totaling 8 MiB.
5. Require eight visible L0 SSTs.
6. Start a coordinator with no embedded worker and a separately built worker
   using 64 KiB SST blocks and whole-SST Bloom filters.
7. Require L0 reduction and at least one committed sorted run.
8. Reopen a fresh serving handle, verify the first point, then scan every key
   and value against the deterministic oracle.
9. Measure requests, bytes, maintenance write amplification, and open through
   first-correct-read latency by phase.

The initial seed is 1103. Held confirmation seeds are 2207 and 3301. The
negative control omits both maintenance roles and must discard on the intended
maintenance gates while retaining exact data.

## Hard gates

The hard gates are identical to RFC-0025:

- every explicit flush appears as an L0 SST;
- separate maintenance roles complete cleanly;
- maintenance reduces L0 and creates a sorted run;
- every key and value remains exact;
- maintenance object reads and writes are observed;
- serving reads write no objects;
- fresh open reads at most 1 MiB;
- the first cold point reads at most eight requests and 512 KiB;
- the frozen negative control discards for the intended reason.

## Interpretation

A pass is evidence that the selected physical geometry and maintenance seam
survive a real S3-compatible network service. It is not evidence for public
cloud latency, availability, egress cost, throttling, multi-region behavior,
read-only serving, garbage collection, or a complete transactional cell.

The local MinIO service removes network distance. Request count and bytes are
therefore the primary portable observations. Wall-clock latency is recorded as
a local development baseline only.

## Result

Candidate `abb2c64` passed the frozen contract through pinned MinIO across
seeds 1103, 2207, and 3301. Runs `229bfced` and `6f0e194b` kept with zero
anomalies. The missing-worker control `d1125f50` discarded on exactly the four
intended maintenance gates while every key and value remained readable.

| Observation | Seed 1103 | Seeds 2207, 3301 |
|---|---:|---:|
| Initial L0 SSTs | 8 | 8, 8 |
| Final L0 SSTs | 0 | 0, 0 |
| Final sorted runs | 1 | 1, 1 |
| Maintenance read bytes | 8,618,474 | 8,620,220; 8,618,474 |
| Maintenance written bytes | 8,617,071 | 8,617,071 each |
| Maintenance write amplification | 1.027x | 1.027x each |
| Fresh-open read bytes | 538 | 538 each |
| First-point requests | 5 | 5, 5 |
| First-point read bytes | 83,263 | 83,263; 83,264 |
| Open through first exact read | 19.65 ms | 18.02 ms; 19.30 ms |

The suite hash is
`61ff02669f9d3d405a8a36664df988201a2edd4a5423065c43bd184cdfcb4595`.
Every correct run exported OTel signals through the shared collector. The
result admits the physical format and role boundary through a local
S3-compatible HTTP service. It does not admit public-cloud economics,
availability, throttling, garbage collection, or a complete cell.

## Tradeoff

This optimizes for one attributable remote-store falsifier using the already
admitted logical contract. It gives up public-cloud distance, provider failure
modes, concurrent serving during compaction, coordinator death, and abandoned
output collection. Those remain separate experiments so a failure has one
interpretable cause.
