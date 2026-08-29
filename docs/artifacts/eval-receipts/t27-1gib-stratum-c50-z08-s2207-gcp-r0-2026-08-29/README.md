# T27 1 GiB stratum c50-z08-s2207, GCP R0, 2026-08-29

Status: `[VERIFIED]` for one complete T27 stratum. The full 27-stratum T27
curve remains `[EVALUATING]`.

## Claim admitted

For the frozen 1 GiB object fixture, 50 percent RocksDB block-cache coverage,
Zipf 0.8 access, trace seed 2207, eight concurrent readers, and direct local
NVMe reads, the objectKV native resident snapshot remains within the declared
throughput, p99, CPU/read, physical-read, and read-amplification bounds of a
standalone directly owned RocksDB control in both ABBA execution orders.

This receipt does not admit other cache levels, skews, seeds, buffered reads,
cold object reads, writes, transaction commits, multi-range behavior, or HTAP.

## Result

| Metric | AB native/control | BA native/control | Gate | Result |
| --- | ---: | ---: | ---: | --- |
| Throughput | 1.012558x | 0.998886x | at least 0.80x | pass |
| P99 latency | 0.989567x | 0.998296x | at most 1.20x | pass |
| CPU ns/read | 1.015743x | 1.006168x | at most 1.25x | pass |
| Physical bytes/read | 0.999567x | 0.999454x | at most 1.25x | pass |
| RocksDB read amplification | 1.000000x | 1.000000x | at most 1.25x | pass |
| Cache or physical pressure | nonzero | nonzero | required | pass |

The native median physical-read values were 1,371.385856 and 1,371.045888
bytes per measured read. Control values were 1,371.738112 and 1,371.942912.
Native cache misses were 0.165926 and 0.165887 per measured read; control was
0.165936 and 0.165958.

Each order contains five within-block candidate/control ratios. Each of the 20
positions used a unique measured worker process and a fresh mutable directory.
Every position performed 200,000 warmup reads followed by 1,000,000 measured
reads. All position, raw-report, runtime, fixture, trace, treatment, machine,
device, and execution identities passed.

All 20 positions emitted telemetry under run ID
`bde16597-8fe7-434b-95b4-dfdc7c58d267`. Exporter flush and shutdown passed for
logs, metrics, and traces. Independent collector inspection found 21 log
records, 63 metric records, and 20 trace records carrying that run ID. The
stratum receipt passed.

## Frozen identities

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
  58b3af2b08b18c5d3ac2b59881563ec7f6ae1a1c92fa97d4c75f03dabadd641e

stratum receipt file SHA-256
  ebe7f05c87bfa7338ac925a09d1fc6fd74a55626281a8576bf9f850abc90c233
```

## Infrastructure

```text
private runner
  GCP doss-objectkv-dev, us-central1-a
  n2-standard-8, 32 GiB RAM
  instance 141366064138072137
  375 GiB local NVMe, ext4
  200 GiB pd-ssd
  no public IP

private collector
  e2-standard-2
  instance 1678812039385886793
  OTLP HTTP at 10.77.0.3:4318
  no public IP

object fixture
  regional versioned GCS
  1,073,741,824 logical bytes
  1,101,701,925 physical bytes
  266 objects
```

The successful retry ran from `2026-08-29T21:54:10Z` through
`2026-08-29T22:59:51Z`, 1 hour 5 minutes 41 seconds. Maximum
controller-process-tree RSS reported by GNU time was 5,625,272 KiB. The queued
driver began the next stratum only after this receipt passed and released its
host-global lease. Infrastructure remains leased for the remaining T27 strata
and is not reported as torn down.

## Retained failed attempt

The first invocation failed closed before measurement because its transient
service omitted `OKV_GCS_BUCKET`. It made no performance claim. The failure
archive was retained before the corrected service was started:

```text
object
  gs://doss-objectkv-dev-okv-evals/runs/rfc0044-t27-admission-r0-20260829/
    failures/c50-z08-s2207/
    t27a2-r0-c50-z08-s2207-95dedb0-failed-missing-bucket.tgz

generation
  1788040429798296

SHA-256
  55f6f8458aba6fc1b63c893052408397d752b2d2bd921b5a397537ad8514f899
```

## Immutable artifacts

All objects are generation-pinned and were uploaded with create-only
preconditions.

| Artifact | GCS generation | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| Source archive | `1788034891457272` | 1,981,628 | `060a2deea1320819b4038c87891024586b34d31989a353566df449d8fd68a459` |
| Runtime executable | `1788034940015727` | 215,947,032 | `aac675c7b54974014985fe00095fea5fb31657a12f3082a61babc22c93a12b74` |
| Machine receipt | `1788035385588095` | 2,243 | `530ed01a0f54e752e076d5f1cec4650c099b3c505887d3f8934a0fe4eed3c3f5` |
| Execution plan | `1788035389178682` | 343,793 | `08a9bab025990380a1e69c8b788d61c2215694f092ebbaa632af4fdb7b9ee89e` |
| Full stratum evidence archive | `1788044539265220` | 23,193 | `9875c4ae41926f103dae8ed4adb3e4a040b9818352fd1fd8eab894a5667d39e2` |
| Standalone stratum receipt | `1788044541560876` | 4,562 | `ebe7f05c87bfa7338ac925a09d1fc6fd74a55626281a8576bf9f850abc90c233` |

Root:
`gs://doss-objectkv-dev-okv-evals/runs/rfc0044-t27-admission-r0-20260829/`.

## Next

Keep T27 `[EVALUATING]`. Two of 27 direct-NVMe strata and zero of two buffered
sentinels are complete. Execute the remaining 25 direct-NVMe strata against the
same plan, workload digest, executable, machine incarnation, and lease. A
failed stratum is retained and selects the next correction; it does not erase
either passing result.
