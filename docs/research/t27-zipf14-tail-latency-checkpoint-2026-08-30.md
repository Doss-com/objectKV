# T27 Zipf 1.4 tail-latency checkpoint, 2026-08-30

Status: `[EVALUATING]`. Two of three 50 percent-cache, Zipf 1.4 trace seeds are
`[VERIFIED]`; the third is running under the unchanged T27 execution envelope.

## Clarity

Question: Does the 1.188676x BA p99 result in `c50-z14-s2207` indicate that the
native RangeEngine architecture is approaching a structural tail-latency
limit?

Punchline: Not yet; native throughput and absolute p99 remained stable while
the paired direct-control p99 moved materially between seeds, so the near-gate
ratio is evidence of control-tail variance that the third seed must resolve,
not evidence for an architecture change.

Counter: If seed 3301 repeats the near-gate BA ratio, fails the 1.20x gate, or
shows native absolute p99 increasing while physical work remains matched, the
native call path becomes the primary hypothesis and requires profiling.

Next: complete `c50-z14-s3301` without changing the plan, then compare absolute
and paired p99 across all three seeds before selecting any optimization.

## What the fifth stratum measured

The fifth receipt is
[`c50-z14-s2207`](../artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s2207-gcp-r0-2026-08-30/README.md).
It passed with AB p99 at 1.075079x and BA p99 at 1.188676x direct RocksDB. The
BA result is 0.011324 below the frozen 1.20x ceiling.

The five paired blocks were:

| Block | AB p99 ratio | BA p99 ratio | AB native/control | BA native/control |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 1.045522x | 1.193032x | 37.919 / 36.268 us | 38.214 / 32.031 us |
| 1 | 1.177054x | 1.100951x | 40.227 / 34.176 us | 38.323 / 34.809 us |
| 2 | 1.031694x | 1.037093x | 36.556 / 35.433 us | 36.459 / 35.155 us |
| 3 | 1.075079x | 1.188676x | 40.595 / 37.760 us | 37.032 / 31.154 us |
| 4 | 1.139044x | 1.276576x | 38.060 / 33.414 us | 39.113 / 30.639 us |

The BA median is not caused by one bad native process. Native p99 stayed
between 36.459 and 40.595 us while the direct control ranged from 30.639 to
37.760 us. Three BA blocks exceeded 1.10x because their direct-control tail was
lower than the native tail.

## Cross-seed observation

The preceding verified seed is
[`c50-z14-s1103`](../artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s1103-gcp-r0-2026-08-30/README.md).

| Seed and subject | P99 minimum | P99 median | P99 maximum | Mean throughput |
| --- | ---: | ---: | ---: | ---: |
| 1103 native | 37.108 us | 41.748 us | 46.233 us | 1,285,161 reads/s |
| 1103 direct | 41.465 us | 46.301 us | 51.930 us | 1,324,044 reads/s |
| 2207 native | 36.459 us | 38.214 us | 40.595 us | 1,291,808 reads/s |
| 2207 direct | 30.639 us | 34.809 us | 37.760 us | 1,324,558 reads/s |

Mean throughput changed by about 0.5 percent for native and 0.04 percent for
direct between these seeds. Median p99 changed by about 8.5 percent for native
and 24.8 percent for direct. Physical bytes/read and cache misses remained
matched. The p99 ratio movement is therefore currently dominated by the
control's absolute tail shift rather than a throughput or physical-I/O shift.

## Measurement semantics

Every position retains all 1,000,000 measured read latencies across eight
threads, sorts them, and takes nearest-rank p99. The stratum gate takes the
median of five within-block native/control ratios independently for AB and BA.
The implementation is in
[`run_parallel_hot_read_window`](../../crates/okv-eval/src/serving_recovery_openraft.rs)
and [`build_t27_comparisons`](../../crates/okv-eval/src/t27_plan.rs).

This checkpoint does not change the frozen gate or declare the variability
solved. It prevents one passing but close ratio from selecting an optimization
before the third independent trace seed arrives.
