# GP3.1.2 optimized native cache-pressure calibration, 2026-08-28

Status: `[VERIFIED]` for the 64 MiB calibration, the one-probe correction,
both process orders, all explicit comparison constraints, exact workload hard
gates, required OTel correlation, durable evidence, and complete lease
teardown. T27 and master-matrix row 1 remain `[EVALUATING]` because cache
coverage, skew, eviction, and physical-read curves are not yet complete.

## Result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
stable volume:              200 GiB pd-ssd
serving scratch:            375 GiB GCP local SSD, NVMe, ext4
object base:                regional versioned GCS
collector:                  private e2-standard-2 with 20 GiB pd-balanced
source revision:            a4cf9a8a8d86a1dfa84d5af01eb514149dce1ed8
parent revision:            0d494849a1340ab441b880adfc75ced73421287b
source bundle sha256:       235e7e6b78065f37de7d2ac7682a1afe3842b01ae0c557734a3e868ec2447065
binary sha256:              a94f5f4a7079aae8b1645c97046ed3a2298f11d2ec050ad48c14b8f2977125b2
machine receipt sha256:     55d354ff986485412298905c64cac8acfc112246a1cdb47a71c5af64cc77ae0a
suite hash:                 b47cb8b8470c1914f184ad6abaced2c0d65b3e6b9fd884349735f0417ef24193
profile hash:               ab928cb84dec50e39ff9ba72ec83bba33af28e0bafbd015de0382a0cead435e9
workload profile hash:      46981dfa3c1e39cb109616c4c38a27aa473d9f4d3ca5c8ca9991d67d0d858774
lockfile hash:              85fd5d79ab99965dd3eac6fbba955d57045de2b48e4d2a4bc3ab1d30e2698201
seed:                       1103
samples per subject/order:  15
measured reads/receipt:     15,000,000
measured reads total:       60,000,000

AB native:                  1,722,232 reads/s, 16,073 ns p99, 4,461 CPU ns/read
AB direct RocksDB:          1,825,966 reads/s, 15,394 ns p99, 4,214 CPU ns/read
AB ratios:                  0.9432 throughput, 1.0441 p99, 1.0586 CPU/read

BA direct RocksDB:          1,800,166 reads/s, 15,814 ns p99, 4,277 CPU ns/read
BA native:                  1,752,426 reads/s, 15,733 ns p99, 4,404 CPU ns/read
BA ratios:                  0.9735 throughput, 0.9949 p99, 1.0298 CPU/read

workload hard gates:        84 pass, 0 fail
comparison constraints:     8 pass, 0 fail
physical read bytes/read:   0 for all four subjects
OTel correlation:           all four run IDs in logs, metrics, and traces
durable evidence:           31 objects, 4,911,598 bytes
lease teardown:             9 resources destroyed, 0 Terraform entries remain
local provider cache:       removed, worktree returned to 7.5 MiB
```

The calibration requires at least 0.80x throughput, at most 1.20x p99, at
most 1.25x CPU/read, and an exact match to a zero physical-read control. Both
process orders passed all four bounds. The worst observed margins were 0.9432x
throughput, 1.0441x p99, and 1.0586x CPU/read.

The comparison receipts report `inconclusive` for the primary directional
verdict because neither subject is at least 20 percent faster than the other.
That verdict detects a material improvement. GP3.1.2 is a non-inferiority gate,
so admission is determined by the eight explicit constraints, all of which
passed. This run does not claim that native objectKV is faster than RocksDB.

## Mechanism finding

The negative calibration wrote the immutable base, forced every small tail
advance into another SST, then looked through both files for every untouched
latest key. Its native subject issued about two cache probes per measured read.
The direct control issued about one.

The correction keeps recent disposable tail state in RocksDB's mutable layer.
txLog remains the durability authority, so forcing the serving image to flush
on every advance added read cost without protecting acknowledged state. A
focused R0 regression performed 256 untouched latest reads and observed exactly
256 cache probes. All eight `okv-serving-rocksdb` package tests passed.

Relative to the negative calibration, the minimum throughput ratio improved
from 0.5659x to 0.9432x. The worst CPU ratio improved from 1.7460x to 1.0586x.
This attributes most of the prior gap to file geometry created by the serving
implementation, not to the version-bound snapshot API itself.

## What this proves and does not prove

`[VERIFIED]` The latest-read native boundary can preserve exact object-base plus
txLog recovery semantics while staying close to direct owned-value RocksDB on
one real eight-vCPU host. Every result used the same source, suite, machine,
cache capacity, trace, process topology, and workload identity. No measured
read reached object storage.

`[EVALUATING]` The fixture was 64 MiB with a 32 MiB RocksDB block cache and
Zipf 1.4 access. Linux reported zero physical read bytes because the operating
system page cache served the RocksDB misses. This is not an isolated NVMe curve,
and it does not verify the complete cache-coverage, skew, or eviction matrix.

Optimizes for: preserving the public version and recovery semantics within a
small single-digit CPU and latency tax relative to the owned-value engine
control.

Gives up: a claim of outperforming RocksDB, proof under physical NVMe reads,
multi-range scaling, replicated commit, and complete-cell economics.

## Next gate

Keep T27 and matrix row 1 `[EVALUATING]`. First reuse one content-addressed
fixture across native, control, and both process orders. Then execute the
frozen 1 GiB cache-coverage and skew sweep with a reviewed direct-read or
page-cache-control mode so the physical-byte metric can become non-zero when
the workload leaves the block cache. Do not let four independent fixture
reconstructions multiply the next run's setup cost.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/runs/gp312opt-r0-20260828/
```

`GCS-EVIDENCE.tsv` binds the four workload receipts, both comparisons, machine
receipt, source bundle, and all three OTel signals to SHA-256 digests. Bucket
versioning is enabled. The runner, collector, persistent disks, local NVMe,
firewalls, router, NAT, subnet, lease-scoped SSH key, copied telemetry, and
local Terraform provider cache were removed after evidence capture.
