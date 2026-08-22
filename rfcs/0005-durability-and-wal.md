# RFC-0005: Durability and WAL protocol

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

`COMMITTED` means the mutation batch and its commit version are fsynced by a
quorum of the declared WAL topology in the active cell generation. The initial
production topology is three replicas across three failure domains in one
region with a two-replica quorum. Object storage is the permanent tier, but the
retained WAL suffix `(O_cell, C_cell]` remains authoritative until
objectification advances the cell's object-durable watermark `O_cell`.

## Context and invariant

For one cell's latest committed version `C_cell` and conservative
object-durable version `O_cell`:

```text
O_cell <= C_cell
Database(C_cell) = ObjectState(O_cell) + WAL mutations in (O_cell, C_cell]
```

Every acknowledged mutation in `(O_cell, C_cell]` must remain reconstructable
from the WAL. WAL through `X` is reclaimable only after every affected range is
reconstructable from conditionally published object state through `X`.

`O_cell` is the safe global pop frontier for the bootstrap log. It is derived
from finer per-range materialization positions and per-consumer log positions.
The permanent tagged-log design may reclaim independently only when every
consumer for a tag proves a contiguous reconstructable prefix. A worker-local
applied position is never a safe WAL-pop authority.

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
7. `O_cell` advances conservatively through only the contiguous prefix durable
   for every affected range. WAL pop follows `O_cell`, never an individual
   worker's applied version.

The idempotency-outcome retention window must be at least the maximum client
retry window and is itself durable state. A client that retries after expiry
must reconcile application identity; objectKV cannot promise exactly once for
an unbounded time.

This retained-identity contract is intentionally stronger than FoundationDB's
general `commit_unknown_result` behavior. Before WAL implementation, the
mechanism must prove that the identity, canonical fingerprint, outcome, and
expiry are atomic with the commit, reconstructable during recovery,
generation-safe, and unavailable for unsafe reuse after retention expires. If
that proof is rejected, the public contract must weaken explicitly rather than
leaving RAM-only deduplication in the commit path. See FoundationDB's
[known limitations](https://apple.github.io/foundationdb/known-limitations.html#the-unknown-result-problem).

## Commit envelope freeze gate

No replicated WAL implementation begins until one versioned envelope fixes:

- `CellId`, `TenantId`, active generation, and commit version;
- client identity, canonical mutation fingerprint, and deduplication outcome;
- read and write conflict domains plus resolver-set identity;
- canonical mutations and every required durable-log or range tag;
- codec version, exact byte length, checksum, and previous log-chain identity;
- quorum acknowledgement evidence and the rules for replay after a lost reply.

The commit aggregator may acknowledge only after every required resolver has
accepted and every required log set has made the same envelope durable. Resolver
partitions may keep conservative conflict state after a transaction is rejected,
but no subset can publish a transaction outcome.

### Executable Cell v0 contract

`[EXISTS]` `crates/okv-sim/src/commit.rs` freezes a deterministic contract-model
encoding named `OKVC` at codec version 1. The byte order is:

```text
magic, codec_version, total_envelope_length,
cell_id, tenant_id, generation, commit_version, log_index,
client_id, request_id, resolver_set_id, logical_fingerprint,
length-prefixed read conflicts,
length-prefixed write conflicts,
length-prefixed canonical mutations,
required resolver IDs, required log tags,
previous log-chain digest, envelope checksum
```

The fingerprint binds the cell, tenant, generation, client request identity,
and exact conflict and mutation payloads. Decode rejects truncation, length or
checksum disagreement, generation/version disagreement, unsorted or empty
required sets, fingerprint disagreement, and trailing bytes. A durable record
combines this envelope with quorum acknowledgement evidence. Recovery rebuilds
the retained client outcome from quorum-certified records before accepting a
retry.

`[EXISTS]` The `cell-commit-contract-v1` eval exercises quorum acknowledge,
lost-reply recovery, conflicting retry, complete resolver acceptance, complete
log-tag routing, generation fencing, and leader-only fsync. Six negative
controls each fail at one bounded step.

`[PROPOSED]` This is not a production WAL format, consensus protocol, signature
scheme, or stable external compatibility promise. It freezes the information
and rejection rules that a Raft-backed implementation must preserve while its
framing and certificate representation remain replaceable.

### Executable local persistence seam

`[EXISTS]` `crates/okv-wal` wraps the opaque `OKVC` envelope in an `OKVW`
version-1 frame containing a log index, payload length, and SHA-256 checksum. A
three-file local topology writes and calls `sync_all` on each selected replica.
Fresh-open recovery scans each file, ignores an incomplete final frame, groups
checksum-valid payloads by index and digest, and returns only the contiguous
prefix with at least two matching copies. Complete corruption without a
remaining quorum and missing middle quorums fail closed.

The exact version-1 bytes are frozen in
`crates/okv-wal/fixtures/frame-v1.hex`; the compatibility test requires the
current encoder to reproduce the fixture and the current reader to recover it.

`[PROPOSED]` This seam does not establish consensus commit, replicate over a
network, elect or fence a leader, model separate disks or zones, repair a
replica, or survive a real machine crash. Quorum evidence is reconstructed from
matching local files rather than accepted from a self-declared certificate.
Those limits prevent this experiment from being described as a production
replicated WAL.

### Executable per-node consensus storage seam

`[EXISTS]` `crates/okv-wal` also owns the `OKVR` version-1 per-node journal.
It serializes and synchronizes durable vote, committed-log position, append,
conflicting-suffix truncate, and purged-prefix records. Recovery reconstructs
the logical log by replaying those operations. An incomplete final frame is
removed before any new append; complete checksum or semantic corruption fails
closed. Appends below the durable purge marker are ignored as obsolete, while
gaps above the retained suffix are rejected.

`[EXISTS]` `crates/okv-consensus` adapts that journal to OpenRaft `0.9.25`'s
`RaftLogStorage` contract. It passes OpenRaft's full upstream storage suite and
an objectKV gate that reopens votes, committed positions, entries, conflict
replacement, purge state, torn tails, and corruption across five seeds. The
exact journal bytes are frozen in
`crates/okv-wal/fixtures/node-journal-v1.hex`.

`[PROPOSED]` This seam still does not establish a three-node consensus commit,
network replication, leader election, generation takeover, cross-process
crash durability, disk independence, or repair. Those claims require the next
Turmoil and real-process gates.

## Durability and RPO statement

- Loss of one WAL replica or one failure domain does not lose acknowledged
  commits.
- Loss of two WAL failure domains before repair can lose acknowledged entries in
  `(O_cell, C_cell]`.
- Destruction of the whole WAL region has an RPO equal to the current
  objectification lag, bounded by retained WAL policy but not zero.
- Cross-region zero-RPO durability is not part of the initial contract.
- The system publishes current `C_cell`, `O_cell`, retained WAL bytes, oldest
  retained log index, replica placement, and estimated RPO as operator-visible
  state.

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
4. One range materializer stalls while others advance. Cell `O_cell` remains at
   the last prefix durable for all affected ranges, so the stalled range
   prevents premature WAL pop.
5. Object PUTs return 503 for 30 minutes. Ratekeeping bends the commit-rate curve
   before the hard retained-byte bound, then refuses commits predictably.
6. The entire region is destroyed while `C_cell - O_cell` represents 17 seconds. Object
   recovery can lose up to that suffix; a zero-RPO claim would be false.

## Alternatives

- Synchronous object publication before acknowledge improves regional RPO but
  puts object-store tail latency and availability directly on every commit.
- A cross-region WAL can reduce regional RPO, but adds WAN latency, failure
  modes, and operator cost before the local-region kernel is proven.
- Multiple WAL groups increase throughput but complicate global ordering,
  recovery, and `O_cell`. One group remains the bootstrap until metrics prove it
  is the limiting resource.

## Eval plan

`evals/suites/commit-contract.toml` owns the pre-WAL envelope, retry, resolver,
tag, generation, and quorum contract. `evals/suites/fault-recovery.toml` owns
leader kill, follower disk-full, lost ack, object PUT brownout, stale publisher,
and range-materializer stall lanes.
Hard gates are zero acknowledged loss, exact seed replay, bounded retained WAL,
no watermark overstatement, and recovery to a serving state. Metrics include
commit p50/p99, `C_cell - O_cell`, retained bytes, admission rate, refusal time, election
window, RPO, and recovery duration.

The negative control acknowledges after leader-only fsync. One bounded seed set
must catch it before a WAL implementation is admitted.

`evals/suites/persisted-wal.toml` owns the first local quorum-frame admission. Its
six negative subjects trust RAM-only deduplication, acknowledge one synchronized
copy, trust one recovered copy, promote a torn suffix, skip the envelope chain,
or ignore complete corruption. All must be discarded before the local
persistence seam is admitted.

`evals/suites/raft-storage.toml` owns the per-node OpenRaft storage admission.
Its six negative subjects lose vote or committed position on restart, retain a
conflicting suffix, lose a purge marker, accept a log gap, or ignore complete
corruption. All must be discarded before networked consensus work begins.

## Compatibility and migration

WAL entries use a versioned envelope independent of the selected Raft library.
Changing consensus implementation requires dual-readable log fixtures or a
quiesced generation handoff. Old and new generations never append to one logical
log concurrently.

## Unresolved questions

- Whether OpenRaft remains the production consensus implementation after the
  deterministic cluster, process-crash, and generation-takeover gates.
- Final strong retained-identity deduplication versus an explicit
  unknown-result contract that permits application-level reconciliation.
- Exact production retained-byte and lag thresholds from measured object-store
  recovery curves.
- Cross-region WAL mode and whether PostgreSQL consumers require a stronger RPO.
