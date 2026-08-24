# RFC-0028: SlateDB coordinator recovery contract

- Status: accepted for local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0025, RFC-0026

## Decision

[ACTIVE-WORK] Treat `Compacted` as a durable handoff between a disposable
compaction worker and a disposable coordinator. Kill the coordinator only after
the worker persists final output identities, but before the manifest references
those outputs. Start a fresh coordinator process with no worker and require it
to commit the already persisted output.

The first coordinator uses a deliberately slow one-second poll interval. This
creates an observable interval after the worker writes `Compacted` and before
the coordinator's next manifest-commit pass. The replacement uses the normal
25 ms development interval.

## Question

Can coordinator process death leave completed immutable output in a durable,
unambiguous state that a fresh coordinator adopts without rerunning compaction,
losing overwrites, or exposing an impossible serving manifest?

RFC-0026 proves the complementary worker failure: a coordinator reclaims a
silent worker claim and a new worker completes the job. This contract makes the
coordinator the failed process and keeps the worker result fixed.

## Frozen contract

For each seed:

1. Write eight overlapping 8 MiB logical snapshots and explicitly flush each.
2. Start one standalone coordinator process with no embedded worker.
3. Start one standalone worker process using the serving format.
4. Poll `.compactions` until the job is `Compacted` and records output SSTs.
5. Kill and reap the coordinator before the manifest changes.
6. Kill and reap the now-idle worker.
7. Require the manifest to retain all original L0 SSTs and no new sorted run.
8. Start a fresh coordinator process with no worker.
9. Require it to publish the same output SST identities into one sorted run.
10. Reopen a fresh serving handle and verify only the latest overwrite for every
    key.

The initial seed is 1103. Held confirmation seeds are 2207 and 3301. The
negative control omits the replacement coordinator. It must retain the exact
L0 data but fail the replacement identity and completion gates.

## Hard gates

- eight overlapping L0 SSTs exist before maintenance;
- a `Compacted` job and at least one output SST persist before coordinator death;
- the first coordinator process is killed and reaped;
- the serving manifest is unchanged before restart;
- the replacement coordinator has a distinct process identity;
- the replacement commits exactly the persisted output SST identities without
  a replacement worker;
- a fresh serving handle returns every latest overwrite exactly;
- fresh open reads at most 1 MiB;
- the first cold point reads at most eight requests and 512 KiB;
- the missing-restart control discards for the intended reason.

## Interpretation

A pass admits coordinator recovery at the worker-to-manifest handoff on one
local filesystem object store. It does not prove recovery during an ambiguous
manifest PUT, concurrent coordinators, coordinator fencing, public object-store
behavior, or collection of output that no job record references.

The final item is the next garbage-collection falsifier. Durable `Compacted`
output is recoverable work, not garbage. True abandoned output requires an
inventory rule that proves an object is absent from every manifest, active job,
checkpoint, and snapshot root before deletion.

## Result

Candidate `851decb` passed the frozen contract across seeds 1103, 2207, and
3301. Runs `ab8b22d4` and `e73b3458` kept with zero anomalies. Every first
coordinator was a real process, every replacement used a distinct process, and
every replacement committed the exact worker-produced SST without starting a
new worker. Missing-restart control `b2045e82` discarded only on replacement
identity and completion while every latest value remained exact in L0.

| Observation | Seed 1103 | Seeds 2207, 3301 |
|---|---:|---:|
| Initial L0 SSTs | 8 | 8, 8 |
| Persisted output SSTs | 1 | 1, 1 |
| Final L0 SSTs | 0 | 0, 0 |
| Final sorted runs | 1 | 1, 1 |
| Kill through manifest commit | 30.49 ms | 29.56 ms; 29.40 ms |
| Fresh-open read bytes | 538 | 538 each |
| First-point requests | 5 | 5; 3 |
| First-point read bytes | 83,263 | 83,263; 83,264 |
| Open through first exact read | 4.76 ms | 3.38 ms; 3.24 ms |

The suite hash is
`833ab8e591d95a2327c0314cd82febaac58616cb5a197c6cb1e3dea5eb92db69`.
The correct runs exported OTel metrics, traces, and logs through the shared
collector. Child-process object I/O remains outside the controller counters, so
RFC-0025 and RFC-0027 remain the maintenance-economics receipts.

## Tradeoff

This optimizes for one exact coordinator crash boundary and reuse of completed
work. It gives up a broader crash sweep, remote-store latency, and automatic
orphan collection until the state transition itself is proven.
