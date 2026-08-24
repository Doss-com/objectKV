# RFC-0025: SlateDB separate-role compaction contract

- Status: accepted for local evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0024

## Decision

Test maintenance outside the `objectkv-serving-v1` database handle. A local
coordinator schedules size-tiered compaction with its embedded worker disabled.
A separately constructed worker claims the job and writes SSTs using the same
64 KiB blocks, whole-SST Bloom filters, and compression setting as the serving
writer. Reopen a fresh serving handle only after the coordinator commits the
new manifest.

This is a separate-role proof inside one runtime. It is not a process-failure,
independent-host, or cloud-object-store proof.

## Context

RFC-0024 removed embedded compaction because the default 64 MiB serving reopen
performed dataset-sized maintenance. That result is useful only if maintenance
can run elsewhere without changing file geometry, breaking exact reads, or
restoring the reopen cliff.

SlateDB's public `Admin::run_compaction_worker_with_options` path does not
accept an SST block-size override. Using it would silently rewrite 64 KiB
serving SSTs as default 4 KiB SSTs. The contract therefore uses the public
`CompactionWorkerBuilder`, sets `SstBlockSize::Block64Kib`, and records that
setting in the raw receipt.

## Frozen local contract

For each seed:

1. Open `objectkv-serving-v1` with no embedded maintenance or object WAL.
2. Write eight equal contiguous key ranges and explicitly flush each range.
3. Close the writer and require at least eight visible L0 SSTs.
4. Start one coordinator with `worker = None` and one separately built worker.
5. Poll the authoritative manifest until L0 shrinks and a sorted run appears,
   or until the fixed timeout expires.
6. Stop both roles and require clean completion.
7. Reopen a fresh serving handle, verify the first point, then scan every key
   and value against the deterministic oracle.
8. Record maintenance I/O separately from reopen and read I/O.

The local dataset is 8 MiB across 8,192 keys. The initial seed is 1103 and the
held confirmation seeds are 2207 and 3301.

## Hard gates

- every explicit flush is visible as an initial L0 SST;
- the coordinator has no embedded worker and both roles stop cleanly;
- external maintenance reduces L0 and creates a sorted run;
- every key and value remains exact after compaction;
- maintenance produces separately measured object reads and writes;
- the first point and full verification produce no object writes;
- fresh open reads no more than 1 MiB;
- the first cold point uses no more than eight requests and 512 KiB.

Opening a writable SlateDB handle can update writer-control metadata. That
open-time write remains measured but is not classified as compaction. A future
zero-write read-serving path must use and evaluate SlateDB's read-only handle.

## Negative control

`skip_external_worker` creates the same L0 state but starts neither role. It
must discard because the separate-role, L0-reduction, sorted-run, and
maintenance-I/O gates fail. Exact data is expected to remain readable.

## Metrics and interpretation

The primary observation is maintenance write amplification, object bytes
written during the maintenance phase divided by logical bytes ingested. It is
reported without a product ceiling in this first contract. Object requests,
read bytes, written bytes, phase throughput, first-correct-read latency, and
hard-gate anomalies use the shared OTel registry.

A pass keeps the SlateDB segment candidate for overwrite, crash, MinIO, and GCS
falsification. It does not admit the maintenance design for production.

## Result

Candidate `b240b38` passed the frozen contract across seeds 1103, 2207, and
3301. Runs `d6425f5e` and `5431c0fe` kept with zero anomalies. The
missing-worker control `af37279a` discarded on the four intended maintenance
gates while every row remained readable.

| Observation | Seed 1103 | Seeds 2207, 3301 |
|---|---:|---:|
| Initial L0 SSTs | 8 | 8, 8 |
| Final L0 SSTs | 0 | 0, 0 |
| Final sorted runs | 1 | 1, 1 |
| Maintenance read bytes | 8,614,444 | 8,614,444 each |
| Maintenance written bytes | 8,616,533 | 8,616,533 each |
| Maintenance write amplification | 1.027x | 1.027x each |
| Fresh-open read bytes | 538 | 538 each |
| First-point requests | 5 | 5, 5 |
| First-point read bytes | 83,263 | 83,263, 83,264 |
| Open through first exact read | 2.91 ms | 3.19 ms, 2.92 ms |

Writable-handle open wrote a 538-byte manifest update. The point read and full
verification wrote no objects. This admits local format-compatible role
separation only. Worker-process death and reclaim, concurrent serving,
overwrites, garbage collection, and remote economics remain open.

## Tradeoff

This optimizes for a small, direct test of the role boundary and consistent
physical format. It gives up real process isolation, concurrent serving during
compaction, worker death and reclaim, garbage collection, overwrite pressure,
remote request latency, and provider cost evidence.
