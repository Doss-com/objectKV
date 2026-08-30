# T27 Zipf 1.4 tail-latency checkpoint, 2026-08-30

Status: `[VERIFIED]` diagnosis of one retained negative result. Five provider-v1
T27 strata passed individually; the sixth is a complete rejection. The T27
curve remains `[EVALUATING]`.

## Clarity

Question: Does the `c50-z14-s3301` p99 rejection identify a structural limit in
the native RangeEngine call path?

Punchline: The rejection is a reproducible physical-layout effect at the exact
one-percent cache-miss knee, and the first correction is to stop duplicating
the complete object base into resident history.

Counter: Sparse history may remove the 2.015x local-byte duplication without
equalizing current-head SST packing; if the replay still fails, value framing
and column-family construction order become the next isolated hypotheses.

Next: implement RFC-0047 under a new provider identity, replay this exact
stratum with the unchanged p99 gate, then restart the full curve only if it
passes.

## The completed third seed

The retained receipt is
[`c50-z14-s3301`](../artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s3301-failed-gcp-r0-2026-08-30/README.md).

| Metric | AB native/control | BA native/control | Gate | Result |
| --- | ---: | ---: | ---: | --- |
| Throughput | 0.995760x | 0.956539x | at least 0.80x | pass |
| P99 latency | 1.307614x | 1.339897x | at most 1.20x | reject |
| CPU ns/read | 1.017873x | 1.035078x | at most 1.25x | pass |
| Physical bytes/read | 1.006496x | 1.006547x | at most 1.25x | pass |
| Read amplification | 1.000000x | 1.000000x | at most 1.25x | pass |

All five AB p99 ratios exceeded 1.20. Four of five BA ratios exceeded 1.20.
Every correctness, identity, pressure, and telemetry gate passed. The runner
stopped before `c50-z20-s1103`, as required by the frozen plan.

## Cross-seed observation

| Seed | Native misses per 1M | Control misses per 1M | Native p99 range | Control p99 range | Outcome |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1103 | 9,910 to 9,914 | 9,958 to 9,961 | 37.108 to 46.233 us | 41.465 to 51.930 us | native wins |
| 2207 | 9,888 to 9,890 | 9,865 to 9,868 | 36.459 to 40.595 us | 30.639 to 37.760 us | native loses narrowly |
| 3301 | 9,979 to 9,981 | 9,910 to 9,912 | 49.899 to 65.111 us | 35.485 to 51.347 us | native rejects |

The sign and size of the p99 difference track which subject has more misses
near the one-percent boundary. Throughput, CPU/read, and total physical work do
not show a comparable discontinuity.

## Why p99 jumps

Every position retains and sorts one million measured read latencies. The
nearest-rank implementation selects zero-based index 990,000:

```text
sorted read latencies
  index 0 ... 990,000 ... 999,999
                    └─ p99

seed 3301 native
  9,980 misses leave about 20 upper-tail cache hits before the miss region

seed 3301 control
  9,910 misses leave about 90 upper-tail cache hits before the miss region
```

The native and control p50 ranges overlap almost completely. Their p99.9
ranges also overlap. The visible discontinuity occurs because p99 is measuring
the top edge of cache hits rather than a broad latency shift.

This is not a reason to replace p99 or waive the gate. A workload whose miss
rate sits at an application SLO percentile is a valid operating point. The
resident format must avoid unnecessary movement across that boundary.

## Physical-layout hypothesis

Resident format v1 builds:

```text
verified object base through O
  ├─ complete head copy
  └─ complete history copy at version O
```

The direct control stores one complete current image. In seed 3301, native used
2,215,101,820 local bytes and direct RocksDB used 1,099,175,660 bytes, a
2.015239x ratio. Both subjects use one database and one explicit cache, but
native has three column families and constructs two full logical copies during
activation.

RFC-0047 changes the local representation to:

```text
verified object base through O
  └─ complete head copy

post-O tail
  └─ history only for keys changed after O
```

Before a key's first mutation, the engine seeds its value, tombstone, or absence
at `O` in the same atomic batch. Untouched keys remain exact for every
historical snapshot because their head value still equals the object base.

## Ranked hypotheses after the third seed

1. **Full-history construction changes current-head SST and cache geometry.**
   Prediction: sparse history cuts local bytes toward direct RocksDB and moves
   seed-3301 misses and p99 toward the control.
2. **The native suffix value tag packs differently from the control's prefix
   tag.** Prediction: if sparse history does not equalize miss count, matching
   only the current-head codec will.
3. **Snapshot wrapper overhead shifts cache-hit latency.** Prediction: native
   p50, CPU/read, and p95 would remain consistently worse after physical layout
   equalizes. Current evidence shows only a small p95 shift and no p50 shift.
4. **Host scheduling or NVMe extent variance dominates.** Prediction: the
   subject direction would change inside ABBA blocks or p99.9 would diverge.
   Neither happened consistently.

The first hypothesis is selected because it removes a measured 2.015x local
duplication and aligns with the object-native architecture even if it does not
fully resolve p99.

## Evidence boundary

This diagnosis is `[VERIFIED]` for the frozen provider-v1 stratum and its two
preceding Zipf 1.4 seeds. RFC-0047 is `[PROPOSED]`. No provider-v2 performance
claim exists yet, and the five passing v1 strata may not be combined with a v2
curve.
