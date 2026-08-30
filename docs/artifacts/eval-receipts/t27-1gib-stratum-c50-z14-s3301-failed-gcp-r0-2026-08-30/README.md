# T27 1 GiB stratum c50-z14-s3301 retained rejection, GCP R0, 2026-08-30

Status: `[VERIFIED]` negative T27 result. The stratum receipt is complete and
authentic, but the native subject exceeded the frozen p99 limit in both
execution orders. The full T27 curve remains `[EVALUATING]`.

## Claim rejected

For the frozen 1 GiB object fixture, 50 percent RocksDB block-cache coverage,
Zipf 1.4 access, trace seed 3301, eight concurrent readers, and direct local
NVMe reads, the current native resident snapshot does not remain within 1.20x
the p99 latency of a standalone directly owned RocksDB control.

This is a mechanism rejection, not an objectKV program rejection. Throughput,
CPU/read, physical bytes/read, read amplification, correctness, cache pressure,
process isolation, and telemetry all passed.

## Result

| Metric | AB native/control | BA native/control | Gate | Result |
| --- | ---: | ---: | ---: | --- |
| Throughput | 0.995760x | 0.956539x | at least 0.80x | pass |
| P99 latency | 1.307614x | 1.339897x | at most 1.20x | **reject** |
| CPU ns/read | 1.017873x | 1.035078x | at most 1.25x | pass |
| Physical bytes/read | 1.006496x | 1.006547x | at most 1.25x | pass |
| RocksDB read amplification | 1.000000x | 1.000000x | at most 1.25x | pass |
| Cache or physical pressure | nonzero | nonzero | required | pass |

Every AB block exceeded the p99 limit. Four of five BA blocks exceeded it.

| Block | AB p99 | BA p99 | AB throughput | BA throughput |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 1.417861x | 1.339897x | 0.995760x | 0.956539x |
| 1 | 1.307614x | 1.765563x | 0.935504x | 0.915340x |
| 2 | 1.299653x | 1.459639x | 1.017768x | 0.917698x |
| 3 | 1.834888x | 1.287019x | 0.946407x | 0.964361x |
| 4 | 1.294540x | 1.122364x | 0.997509x | 0.968719x |

The native median cache-miss count was 9,980 per million reads in both orders.
The control medians were 9,911 and 9,910. Native physical reads were
82.505728 bytes per logical read, while control reads were 81.969152 and
81.960960 bytes. The small physical-work difference is real and within the
declared limit.

## Tail-latency localization

The p99 estimator selects zero-based index 990,000 from one million sorted
latencies. This trace places both subjects immediately below a one-percent
cache-miss rate:

```text
native   9,980 misses  → p99 is about 20 cache hits from the miss boundary
control  9,910 misses  → p99 is about 90 cache hits from the miss boundary
```

Native p50 ranged from 3.463 to 3.637 us, versus 3.493 to 3.559 us for the
control. Native p99.9 ranged from 190.527 to 201.614 us, versus 193.538 to
202.713 us for the control. The broad distribution and far tail remain close;
the formal rejection occurs at the cache-hit to cache-miss knee.

The subject-specific miss direction also changes across trace seeds. Seed
1103 gave native fewer misses than control and native won p99. Seed 2207 gave
native slightly more misses and native lost p99 narrowly. Seed 3301 gave native
about 70 more misses and native lost p99 decisively. This makes resident-image
layout the primary correction target. It does not justify changing the frozen
percentile or gate.

## Selected correction hypothesis

The current native activation materializes the complete object base into both
the `head` and `history` column families. Position 100 used 2,215,101,820 local
bytes for native state; position 101 used 1,099,175,660 bytes for direct
RocksDB, a 2.015239x ratio.

RFC-0047 proposes storing the object base once in `head`, then seeding a key's
version-`O` history only before its first post-`O` mutation. This should make
the current-head SST construction closer to the direct control while keeping
exact old snapshots. It is a hypothesis until the same failed stratum passes
under a new provider identity and frozen plan.

## Evidence integrity

All 20 positions used unique measured worker processes and fresh mutable
directories. Each performed 200,000 warmup reads and 1,000,000 measured reads.
The run started at `2026-08-30T02:19:56Z` and finished at
`2026-08-30T03:25:22Z`. Maximum controller-process-tree RSS was 5,626,132 KiB.
The subject scratch directory was empty after completion, and the next planned
stratum did not start.

All 20 positions emitted telemetry under run ID
`912fb7e5-35db-4f63-99db-cdd8201f23a9`. Exporter flush and shutdown succeeded
for logs, metrics, and traces. Independent collector inspection found 21 log,
63 metric, and 20 trace JSONL exports containing the run ID.

```text
source revision
  95dedb0249a69567e7c390f4c191d079f07b6d90

runtime source archive SHA-256
  060a2deea1320819b4038c87891024586b34d31989a353566df449d8fd68a459

runtime executable SHA-256
  aac675c7b54974014985fe00095fea5fb31657a12f3082a61babc22c93a12b74

plan semantic SHA-256
  40d4559af1b51db13f3dad85a089c81af84d645794eb1bd19d8079bb89b115be

portable workload SHA-256
  7019d0e12bff05e721c867dfe3277dc87a44902d26636a5eaf08f2f54f03fb26

execution SHA-256
  2f3864365f25aadd42012757a52f677713a2ca0a88571f23b98ed28a104944f7

stratum semantic receipt SHA-256
  224a430eea2b735a4c5144eb340c5e527119207b17d7cf8260bee1c58e747ee8

stratum receipt file SHA-256
  dcdb016bb7f25a59981173331e83b4d4e66891d07ef0095696706ab2bdc54754
```

## Immutable artifacts

Both objects were uploaded with create-only preconditions and read back by
generation for SHA-256 verification.

| Artifact | GCS generation | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| Full failed-stratum evidence archive | `1788060421647494` | 18,851 | `c225c87a3575ff584c76bab79b86554fbd3255ffbea988cbffafbf92104899b6` |
| Standalone failed-stratum receipt | `1788060423851601` | 4,574 | `dcdb016bb7f25a59981173331e83b4d4e66891d07ef0095696706ab2bdc54754` |

Root:
`gs://doss-objectkv-dev-okv-evals/runs/rfc0044-t27-admission-r0-20260829/strata/c50-z14-s3301/`.

## Next

Keep T27 `[EVALUATING]`. Preserve the five prior passing strata as evidence for
provider v1, but do not combine them with a corrected provider v2 admission.
Implement RFC-0047 behind a new provider identity, replay this exact stratum
first, then freeze and restart the complete 27-stratum curve if the correction
passes.
