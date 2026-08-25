# Provider-bound range-image I/O, 2026-08-25

Status: `[EXISTS]` portable local-file release result. `[ACTIVE-WORK]`
hardware-controlled NVMe and whole-worker memory curves.

## Answer

The sparse-index range-image mechanism passes its first load-bearing local
test. A 33.70 MB immutable image opened under a 4 MiB application-memory
budget, served exact points with one explicit 57,530-byte file read at p99,
and reached 124.25 us local-file point p99 on the uniform workload. The full
scan sustained 65,714 rows/s, about 513 MiB/s of logical values. No
object-provider work occurred after readiness.

This admits the mechanism, not the production performance claim. The host OS
page cache was uncontrolled. Median peak process RSS delta was 40.4 MB in the
uniform workload even though the audited reader state was 4,142,150 bytes.
Physical NVMe latency, queueing, concurrent throughput, total Range Engine
memory, recent-mutation overlays, and GCS hydration remain unproven.

## Frozen question

Can an assigned-range image at least eight times larger than its reader-memory
budget open cheaply, serve exact points with at most two explicit file reads
and 64 KiB per miss, remain below 1 ms local-file p99, scan in order, and
perform zero provider work?

Contract commit: `2d1bfbf99f8107961bc24123233ebe23f7229a4e`.

Constraint correction: `26bca0b7d9ca8b8cd35d6d8500af49ca5ae910da`.

Sparse-image implementation: `4416b1fac32092912ebb85c06e11401f65da0ce3`.

Evaluated candidate:
`7e7247053e832ab7ca188c4d69da42e3052e6412`.

Suite: `provider-bound-range-image-io-v0`.

Suite hash:
`9f2f2eb1a458f848f996b9913532520c7df3145cedfd48be4e98ddfdaaad18f2`.

Backend: retained local range-image file, fresh worker processes, explicit
positional file reads, uncontrolled OS page cache.

Seeds: `724851`, `724877`, `724901`, `724921`, `724939`.

## Measured path

```text
authority-bound placed-ready receipt
  -> retained immutable range-image file
  -> fixed header and checksummed sparse index
  -> bounded decoded block cache
  -> one checksummed block read on a cache miss
  -> exact point or ordered scan output

object provider
  -> absent after placed readiness
```

The 33,704,472-byte full image contained 586 independently checksummed data
blocks and a 41,032-byte sparse index. Image overhead over the 32 MiB logical
fixture was 0.447 percent.

## Curve

| Workload | Audited reader bytes | Open p50 | Point p99 p50 | Point p99 range | App-cache hit | File reads p99 | File bytes p99 | Verdict |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| full uniform, 4 MiB budget | 4,142,150 | 208.8 us | 124.3 us | 119.5 to 136.8 us | 11.79% | 1 | 57,530 | keep |
| full Zipf 0.99, 4 MiB budget | 4,142,150 | 210.3 us | 123.4 us | 115.7 to 129.9 us | 65.21% | 1 | 57,530 | keep |
| quarter uniform, 1 MiB budget | 992,580 | 80.4 us | 118.1 us | 117.9 to 121.4 us | 11.28% | 1 | 57,530 | keep |
| full fresh-process reopen | 4,142,150 | 211.5 us | 128.2 us | 117.6 to 136.5 us | 11.79% | 1 | 57,530 | keep |
| full ordered scan | 4,150,436 | 217.4 us | 115.1 us | 112.3 to 120.5 us | 85.69% | 1 | 57,530 | keep |

The ordered scan median was 65,714 rows/s with median absolute deviation 799
rows/s and a 63,851 to 66,990 rows/s range. At 8 KiB per value, that is about
513 MiB/s of logical value throughput.

Open required three explicit reads and 41,200 bytes for the full image. The
quarter-range image required three reads and 10,486 bytes. Every correct run
verified the root-bound receipt, index checksum, touched block checksums,
exact points, exact scan boundaries, semantic replay, outside-range refusal,
and scratch cleanup.

## Controls

| Subject | Observed boundary | Verdict |
| --- | ---: | --- |
| decode complete image | 37,846,622 audited resident bytes against 4 MiB | discard |
| linear point scan | 33,367,400 file bytes median p99 and 61.90 ms latency p99 | discard |
| accept corrupt index | root and checksum gates failed | discard |
| skip block checksum | exact-point and checksum gates failed | discard |

The linear control completed within its frozen 900-second budget at 820.73
seconds. It establishes that the evaluator distinguishes the indexed path
from a semantically tempting full-prefix scan.

## Throughput bound

The current format reads one approximately 57.5 KiB block for an 8 KiB value
on a cache miss. The uniform workload missed the application cache on 88.21
percent of points, so its average explicit local-file traffic was about 49.6
KiB per logical read.

| Uniform point rate | Approximate explicit file traffic | Approximate file reads/s |
| ---: | ---: | ---: |
| 10,000/s | 484 MiB/s | 8,821 |
| 20,000/s | 0.945 GiB/s | 17,642 |
| 100,000/s | 4.73 GiB/s | 88,208 |

These are arithmetic projections from the measured miss ratio and bytes, not
measured throughput. They identify the next likely bottleneck. A 64 KiB-class
block can be acceptable for skewed or scan-heavy access, but uniform high-QPS
OLTP will require a higher RAM hit ratio, smaller point blocks, request
coalescing, more Range Engines, or some combination.

## Economic interpretation

The serving read path issued zero object requests after readiness, so direct
GCS request cost is not the hot-read constraint in this design. The economic
question moves to how many complete local serving copies are needed, how much
NVMe bandwidth each copy sustains, how quickly an empty worker hydrates, and
whether object authority plus disposable local copies costs less than two or
three authoritative local replicas.

The result does not prove that local bytes disappear. A hot assigned range
currently needs nearly one complete local image. objectKV can still win on
replica count, independent compute scaling, cold-range placement, rebuild
operations, and object-native history, but each advantage needs its own cost
curve against RocksDB or TiKV.

## Decision and next gate

Keep the sparse-index block-image direction for the next experiment. Do not
select this custom format as permanent and do not report the 124 us result as
physical NVMe latency.

Freeze and run a hardware-controlled NVMe matrix with:

1. 8, 16, 32, and 64 KiB-class data blocks;
2. cold-media and warm-page-cache states reported separately;
3. concurrency and queue depths of 1, 8, and 32;
4. uniform, Zipfian, and ordered-scan traces;
5. total worker RSS alongside audited reader memory;
6. latency p50, p95, and p99, IOPS, bytes/s, CPU, checksum cost, and exactness;
7. one incumbent RocksDB or TiKV local-read curve on the same host and data.

Advance to GCS hydration only after one configuration provides a credible
per-worker point-throughput envelope without violating memory or scan
throughput. Then measure hydration time, transferred bytes, object operations,
worker-replacement time, and cost for the exact retained format.

## Verification

- five correct release workloads: keep;
- four unsafe controls: discard;
- deterministic semantic replay: exact;
- strict workspace Clippy across all targets: passed;
- object-provider requests and bytes after readiness: zero;
- OTel collector: not configured for the portable profile;
- physical NVMe claim: false;
- cloud claim: false.
