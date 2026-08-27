# GP-G0 through GP-G6 architecture review

Status: `[EVALUATING]` for production strategy. The bounded local proof ladder
is `[VERIFIED]`.

## Readout

The implementation supports the central objectKV shape, but it has not yet
proved the composition that determines whether the product is economical. The
good signal is semantic: tiny application records, exact process recovery,
disposable RAM state, recursively verified immutable history, copy-on-write
forks, and safe branch reclamation now share explicit formats. The remaining
risk is architectural integration, not another isolated codec.

## Bottom-up module map

```text
okv-object
  immutable bytes, identity, guarded delete
      |
okv-publication
  intents, roots, pins, mark epochs, delete reservations
      |
okv-app-history
  application records, checkpoints, segments, manifests
      |
okv-consensus + okv-wal + okv-log
  ordered canonical envelopes, request identity, failover recovery
      |
okv-model
  exact versions, mutations, point and range reads
      |
Tetris and Chess
  materialized state, reducers, branches, lineage UI, receipts
```

`okv-playground-support` is evaluation composition code. It is not a product
service boundary. It connects the real in-memory object client and publication
state machine so GP-G5 and GP-G6 can test recursive lifecycle semantics without
claiming replicated authority or GCS behavior.

## What the curves say

1. Compact histories are real. Tetris falls from 2.52 MB of materialized txLog
   payload to 5.9 KB of delta and encoded checkpoint history, a 427.6x ratio.
2. RAM reads are cheap once state is materialized. The release harness measured
   p99 at 125 ns for Tetris and 84 ns for Chess, but this excludes RPC,
   concurrency, admission, and cache misses.
3. Rebuild cost follows state and retained history. Tetris rebuilt 2,000 actions
   in 19.2 ms; Chess rebuilt its tiny line in 35 us.
4. Copy-on-write branching is working at the object graph level. Both workloads
   published two new branch objects, copied zero prefix objects, and reclaimed
   exactly two child-only objects.
5. The fault scenario proves semantics, not throughput. Three commits across a
   leader failure completed in roughly 0.56 seconds on one host.

## Bounds and unresolved tradeoffs

| Boundary | Optimized for | Given up or not yet proven |
| --- | --- | --- |
| Reducer-specific deltas | Small history and cheap forks | Reducer/schema retention becomes part of recoverability. |
| Replicated txLog | Generic atomic recovery | Materialized effects still amplify bytes. Compaction/objectification must bound retention. |
| RAM serving image | Lowest hot-path CPU and device latency | Capacity is bounded; restart and eviction depend on exact object plus tail reconstruction. |
| Immutable objects | Portability, cheap history, independent compute | Cold GET count, bandwidth, publication lag, and outage debt are unmeasured on GCS. |
| Manifest branches | O(1) prefix sharing | Root/pin/GC correctness becomes an authority problem. |
| Bounded cell | Tenant-level serializability and contained recovery | No cross-cell transaction; applications own any higher-level coordination. |

## Directional decision

Continue, but only into one integrated stress slice. Do not expand toward
PostgreSQL, HTAP, or MultiRaft features until this path is measured:

```text
real replicated authority
  + real object backend
  + RAM and SSD serving controls
  + empty-worker rebuild
  + object outage and lag
  + branch publication and GC during failure
```

The strategy is invalidated if cold point reads grow with total database size,
objectification debt is unbounded under an admitted write rate, empty-worker
recovery requires a full database download, RAM cannot beat the SSD control
end to end, or the total cost has no material advantage over TiKV/RocksDB plus
an object tier.

## Next measured outcomes

1. Compose GP-G3 envelopes with the replicated publication authority and real
   GCS, retaining the exact Tetris and Chess traces.
2. Add an SSD-resident control with the same logical history, RPC path, resource
   cap, and durability contract as RAM.
3. Measure resident p50/p99, cold GET count and bytes, rebuild time by history
   size, objectification lag, retained txLog bytes, branch cost, and GC safety.
4. Inject process, host, lost-response, object throttling, and publication/GC
   failures.
5. Stop at the review gate. Admit GP-G7 only if the shape is both correct and
   materially better on at least one named workload.

## 2026-08-26 live Tetris and vision checkpoint

Status: `[EVALUATING]` product direction. No new production claim is admitted.

The live web path on ports 4279 and 4277 was checked without rebuilding or
resetting its state. At the start of the check it held active `branch-6` at
version 1,041, score 5,366, 33 lines, nine branches, 1,041 retained envelopes,
2,371,877 txLog bytes, and one prior exact replay recovery. A separate
no-build canary advanced main from v1 to v4, opened exact snapshot v2, forked a
child at v2, diverged its suffix, discarded its serving image, and reconstructed
the child game exactly from `okv-log`.

That live result validates the application boundary, not the production
topology:

```text
live now
Tetris HTTP adapter
  -> objectkv-boundary-v0
     -> volatile okv-log
        -> in-process okv-model MVCC image

target GP-G7
same Tetris boundary
  -> commit proxy and conflict resolution
     -> quorum-durable OpenRaft txLog
        -> bounded RAM or SSD RangeEngine
           -> authenticated objectifier
              -> immutable manifested GCS base
```

### Evidence added since the original review

1. Bounded commit-proxy batching reached 1,157 durable transactions per second
   locally at 76.1 ms maximum p99, then preserved exact object plus suffix
   reconstruction during controlled conflicts and object-frontier advancement.
2. Complete replicated snapshot state failed the media-economics gate at
   19.69x one logical copy. The current physical snapshot representation is
   discarded.
3. The C5 columnar RangeEngine candidate preserved exact local points and
   restart, used 0.353x the row control's point bytes, and reached 4.718x its
   projected-scan throughput. The direct DataFusion source doubled local scan
   throughput after coalescing stripes into bounded 256 KiB reads.
4. A real GCS cache-admission canary passed exactness and capacity gates. Ghost
   two-chance reduced post-scan requests from 161 to 128 and wall time from
   75.06 to 67.14 seconds relative to full admission. One seed, dirty source,
   and disabled OTel keep the result inconclusive.

### Big-vision readout

The useful objectKV product is not a generic replacement for every TiKV or
FoundationDB deployment. It is a bounded-cell transaction kernel whose unique
advantage must come from one committed history supporting all of the following:

```text
resident OLTP
  + bounded empty-worker recovery
  + metadata-scale branches
  + portable immutable object state
  + exact DataFusion base plus tail
```

The Tetris game is the right next vertical-slice client because its boundary is
already frozen and its long action history exposes amplification, replay,
branching, hot reads, and objectification debt. The next implementation should
replace the in-process backend behind that boundary, not add another game or a
new public API.

### D6. Make one integrated Tetris cell the next program gate

The next named outcome is `tetris-cell-v1`, frozen as
`evals/scenarios/objectkv-playground-golden-path-v4.toml`:

1. Keep `objectkv-boundary-v0` unchanged.
2. Commit each action through the existing bounded commit proxy and replicated
   txLog.
3. Serve point and range reads from a capacity-bounded RAM profile, with SSD as
   the same-durability control.
4. Objectify frozen prefixes into one authenticated manifested layout in GCS.
5. Kill the serving process and recover the exact game from
   `ManifestedObjectState(O) + txLog(O, C]`.
6. Fork one historical Tetris version by publishing a child root plus divergent
   suffix, without copying prefix objects.
7. Export commit, read, cache, objectification, recovery, branch, and cost
   telemetry through OTel.

The gate records commit p50/p99, resident read p50/p99, cache footprint, cold
GET count and bytes, time to first correct read, full range-ready time,
objectification lag, retained txLog bytes, object PUT bytes, branch-only bytes,
and cost per million actions.

### Stop conditions

Pivot to TiKV, FoundationDB, or PostgreSQL as the hot durable kernel if any of
the following remains true after the bounded integrated slice:

- retained txLog or replay work grows without bound during an admitted GCS
  slowdown;
- the system needs a second full durable page or row image to recover within
  the target bound;
- cold lookup work grows with total database size;
- RAM does not materially improve a named end-to-end workload over SSD;
- exact base plus tail requires duplicating an independently authoritative
  analytical history;
- branching, recovery, and independent compute do not offset the operational
  cost of owning the transaction kernel.

Current call: continue into `tetris-cell-v1`, but do not expand PostgreSQL,
MultiRaft, or metacluster scope until that slice either passes or selects the
hot-kernel pivot.
