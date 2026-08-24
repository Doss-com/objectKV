# RFC-0030: SlateDB concurrent coordinator fencing

- Status: accepted for local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0025, RFC-0026, RFC-0028

## Decision

[ACTIVE-WORK] Require every compaction coordinator to acquire a durable,
monotonic authority epoch before changing compaction state or the serving
manifest. Starting a second coordinator against the same database must advance
the epoch and cause the older live process to exit on its next refresh or write.

The controller must not terminate the stale process in the correct subject. A
paired control kills it externally, reaches the same final data, and fails the
self-fencing gate. Operational convergence is not authority proof.

## Question

Can two real coordinator processes overlap without allowing the older process
to publish compaction work after a newer coordinator becomes authoritative?

## Frozen contract

For each seed:

1. Write eight overlapping 8 MiB logical snapshots and flush every snapshot.
2. Record the current compactor epoch and serving manifest.
3. Start coordinator A with no embedded worker.
4. Require A to advance the epoch and persist at least one compaction job.
5. Leave A live and start coordinator B against the same object root.
6. Require B to persist a strictly greater compactor epoch.
7. Require A to exit by detecting its stale epoch, without a controller signal.
8. Start one standalone worker and require B to commit compaction.
9. Require B to remain live through completion.
10. Reopen a fresh serving handle and verify every latest overwrite exactly.

The negative control kills A after B advances the epoch. It must retain exact
data and complete through B, but fail the stale-process self-fencing gate.

## Hard gates

- eight overlapping L0 SSTs exist before maintenance;
- coordinator A advances the compactor epoch and persists work;
- coordinator B advances to a strictly greater epoch while A is live;
- A exits without controller termination;
- B remains live and commits compaction;
- a fresh serving handle returns every latest overwrite exactly;
- fresh open reads at most 1 MiB;
- the first cold point reads at most eight requests and 512 KiB;
- the externally killed control discards for the intended reason.

## Interpretation

A pass admits one local overlap boundary for the pinned SlateDB coordinator
protocol. It does not prove a distributed object-store lease, host partition,
clock-skew tolerance, public-cloud conditional-write behavior, writer fencing,
or orphan collection.

## Tradeoff

This optimizes for an executable single-authority invariant before adding more
maintenance roles. It gives up leader availability during an epoch race and
does not claim that epoch fencing alone elects or monitors coordinators.

## Result

Candidate `2c6a854` passed the frozen contract across seeds 1103, 2207, and
3301. Runs `aaaecbb6` and `85672759` kept with zero anomalies. Every first
coordinator acquired epoch 1, every second live coordinator acquired epoch 2,
and every stale first process exited without a controller signal. Fence
detection took 13.56 to 21.61 ms after the second epoch became observable.

Every epoch-2 coordinator remained live through compaction, reduced eight L0
SSTs to one sorted run, and preserved every latest overwrite. Fresh open read
538 bytes and the first cold point used five requests and at most 83,264 bytes.

External-kill control `2899bb28` reached the same exact compacted data but
discarded only on `stale_coordinator_self_fenced`. This distinguishes authority
fencing from an operator removing the competing process.

The suite hash is
`f0c8c70372f77f333b62eb709391e88736c6b34930aa270f1e5897a208bb93db`.
The correct runs and control exported OTel metrics, traces, and logs through the
shared collector.
