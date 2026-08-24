# RFC-0026: SlateDB compaction worker process reclaim

- Status: accepted for the local process-failure candidate
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0024 and RFC-0025

## Hypothesis

A coordinator can reclaim a compaction from a killed standalone worker process,
permit a fresh worker identity to complete the same job, and preserve the
latest value for every overwritten key without restoring the fresh-read cliff.

## Frozen local contract

For each seed:

1. Write the same 8 MiB logical keyspace eight times, flushing after every
   overwrite round. This produces eight overlapping L0 SSTs and 64 MiB of
   logical ingest.
2. Close the serving writer and start a coordinator with no embedded worker.
3. Spawn the first standalone worker as a real `okv-eval` OS process with the
   `objectkv-serving-v1` block and filter settings.
4. Observe its persisted `Running` claim, then terminate and reap the process.
5. Require the coordinator to reset the silent claim to unowned `Scheduled`
   after the fixed 250 ms heartbeat timeout.
6. Spawn a second worker process with a fresh identity.
7. Require the same job to reach `Completed` and the authoritative manifest to
   contain fewer L0 SSTs plus at least one sorted run.
8. Terminate the idle replacement, stop the coordinator, open a fresh serving
   handle, and verify the latest overwrite for every key.

The initial profile uses seed 1103. Confirmation uses seeds 2207 and 3301. The
claim and reclaim timeouts are 10 seconds each. Replacement completion has a
60-second ceiling per seed.

## Hard gates

- all eight overlapping L0 rounds exist before maintenance;
- the first worker persisted a `Running` claim;
- the first OS process was terminated and reaped;
- the coordinator reclaimed the silent claim to unowned `Scheduled`;
- the replacement worker identity differs from the killed identity;
- the replacement completed the job and the coordinator committed its output;
- a fresh full scan returns only the latest overwrite for every key;
- fresh open reads no more than 1 MiB;
- the first cold point uses no more than eight requests and 512 KiB.

## Negative control

`skip_replacement_worker` kills and reclaims the first worker but starts no
replacement. It must discard on fresh replacement identity and completion
while the un-compacted L0 data remains exactly readable.

## Telemetry boundary

The parent controller exports reclaim duration, fresh-read duration, phase
throughput, controller-visible object I/O, and correctness anomalies through
the shared OTel registry. Object requests made inside the force-killed worker
cannot be flushed reliably and are explicitly excluded from this receipt.
RFC-0025 remains the local maintenance-I/O receipt. A later durable worker
telemetry design must export process-local counters independently of graceful
shutdown.

## Interpretation

A pass admits one local OS-process failure and reclaim path. It does not admit
host loss, independent storage failure, coordinator death, concurrent serving,
garbage collection of abandoned output, MinIO, GCS, or cloud cost.

## Result

Candidate `803de76` passed every gate across seeds 1103, 2207, and 3301. Runs
`238de077` and `882b1fcf` each kept with zero anomalies. Missing-replacement
control `af904d02` discarded only on replacement identity and completion while
the latest overwritten values remained exact in L0.

| Observation | Seed 1103 | Seeds 2207, 3301 |
|---|---:|---:|
| Initial overlapping L0 SSTs | 8 | 8, 8 |
| Final L0 SSTs | 0 | 0, 0 |
| Final sorted runs | 1 | 1, 1 |
| First worker claimed and killed | yes | yes, yes |
| Silent claim reclaimed | yes | yes, yes |
| Replacement identity fresh | yes | yes, yes |
| Kill through committed completion | 591 ms | 618 ms, 576 ms |
| Fresh-open read bytes | 538 | 538, 538 |
| First-point requests | 5 | 5, 5 |
| First-point read bytes | 83,263 | 83,263, 83,264 |
| Open through first exact read | 3.18 ms | 3.32 ms, 3.18 ms |

The result admits local worker-process failure detection, reclaim, and fresh
replacement for one overwrite compaction. Child-process object requests are
not included in controller counters. Coordinator death, partial-output
inventory and GC, concurrent writers, MinIO, and GCS remain open.
