# RFC-0005: Durability and WAL protocol

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

`COMMITTED` means the mutation batch and its commit version are fsynced by a
quorum of the declared WAL topology in the active generation. The initial
production topology is three replicas across three failure domains in one
region with a two-replica quorum. Object storage is the permanent tier, but the
retained WAL suffix `(O, C]` remains authoritative until objectification advances
the object-durable watermark `O`.

## Context and invariant

For latest committed version `C` and object-durable version `O`:

```text
O <= C
Database(C) = ObjectState(O) + WAL mutations in (O, C]
```

Every acknowledged mutation in `(O, C]` must remain reconstructable from the
WAL. WAL through `X` is reclaimable only after every affected range is
reconstructable from conditionally published object state through `X`.

## Commit protocol

1. The client supplies a globally unique idempotency identity and mutations.
2. The active sequencer validates generation and assigns a commit version.
3. The versioned canonical batch is appended to the active WAL generation.
4. `COMMITTED` is returned only after the leader and at least one other replica
   have fsynced the entry and the consensus protocol has committed it.
5. Lost replies produce `commit_unknown`. Retrying the same client identity
   returns the recorded outcome or completes the same logical commit; it cannot
   create a second commit.
6. Storage workers materialize committed entries into immutable segments and
   conditionally publish range manifests.
7. `O` advances conservatively through only the contiguous prefix durable for
   every affected range. WAL pop follows `O`, never an individual worker's
   applied version.

The idempotency-outcome retention window must be at least the maximum client
retry window and is itself durable state. A client that retries after expiry
must reconcile application identity; objectKV cannot promise exactly once for
an unbounded time.

## Durability and RPO statement

- Loss of one WAL replica or one failure domain does not lose acknowledged
  commits.
- Loss of two WAL failure domains before repair can lose acknowledged entries in
  `(O, C]`.
- Destruction of the whole WAL region has an RPO equal to the current
  objectification lag, bounded by retained WAL policy but not zero.
- Cross-region zero-RPO durability is not part of the initial contract.
- The system publishes current `C`, `O`, retained WAL bytes, oldest retained log
  index, replica placement, and estimated RPO as operator-visible state.

## Lag and capacity state machine

Every deployment profile must set byte and time thresholds for four states:

| State | Entry condition | Commit behavior | Exit condition |
|---|---|---|---|
| `normal` | below soft byte and lag bounds | admitted normally | remains below bounds |
| `rate_limited` | soft bound crossed | ratekeeper reduces admission as debt grows | below recovery hysteresis |
| `commit_refused` | hard byte or RPO bound crossed | new writes fail `objectification_backpressure` | repaired below recovery hysteresis |
| `recovery_only` | quorum or generation safety is uncertain | no client commit | new generation is safely activated |

The hard retained-byte bound cannot be disabled. Object-store failure must not
trigger a second unbounded spill to the same failed tier. Read availability for
versions already reconstructable may continue while commits are refused.

## Worked failure cases

1. The leader fsyncs an entry and replies before quorum commit, then dies. This
   violates the contract because the entry can disappear; the simulator must
   detect acknowledged loss.
2. A follower is disk-full. A two-replica quorum may still commit while the
   third is repaired, but the topology remains degraded and cannot tolerate a
   second failure.
3. The client times out after quorum commit. It retries the same idempotency
   identity and receives the original committed version.
4. One range materializer stalls while others advance. Global `O` remains at the
   last prefix durable for all affected ranges, so the stalled range prevents
   premature WAL pop.
5. Object PUTs return 503 for 30 minutes. Ratekeeping bends the commit-rate curve
   before the hard retained-byte bound, then refuses commits predictably.
6. The entire region is destroyed while `C - O` represents 17 seconds. Object
   recovery can lose up to that suffix; a zero-RPO claim would be false.

## Alternatives

- Synchronous object publication before acknowledge improves regional RPO but
  puts object-store tail latency and availability directly on every commit.
- A cross-region WAL can reduce regional RPO, but adds WAN latency, failure
  modes, and operator cost before the local-region kernel is proven.
- Multiple WAL groups increase throughput but complicate global ordering,
  recovery, and `O`. One group remains the bootstrap until metrics prove it is
  the limiting resource.

## Eval plan

`evals/suites/fault-recovery.toml` owns leader kill, follower disk-full, lost
ack, object PUT brownout, stale publisher, and range-materializer stall lanes.
Hard gates are zero acknowledged loss, exact seed replay, bounded retained WAL,
no watermark overstatement, and recovery to a serving state. Metrics include
commit p50/p99, `C - O`, retained bytes, admission rate, refusal time, election
window, RPO, and recovery duration.

The negative control acknowledges after leader-only fsync. One bounded seed set
must catch it before a WAL implementation is admitted.

## Compatibility and migration

WAL entries use a versioned envelope independent of the selected Raft library.
Changing consensus implementation requires dual-readable log fixtures or a
quiesced generation handoff. Old and new generations never append to one logical
log concurrently.

## Unresolved questions

- `raft-rs` versus OpenRaft after the simulator and persistence seam exist.
- Exact production retained-byte and lag thresholds from measured object-store
  recovery curves.
- Cross-region WAL mode and whether PostgreSQL consumers require a stronger RPO.
