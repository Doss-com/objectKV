# GP3.1.1 native concurrent-read admission, 2026-08-27

Status: `[VERIFIED]` for the single-range native resident read boundary at 8
and 32 concurrent clients, both process orders, exact owned values, zero-object
hot reads, bounded local state, required OTel signals, durable evidence,
scratch cleanup, and lease teardown. The admitted single-client GP3.1 receipt
is the regression anchor. This is not a cache-pressure, RPC, replicated-commit,
multi-range, or complete-cell performance claim.

## Result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
stable volume:              200 GiB pd-ssd at /var/lib/objectkv
serving scratch:            375 GiB GCP local SSD, NVMe, ext4
object base:                regional versioned GCS
source revision:            e478806314049145db70207761d331b0be5c1ff1
source bundle sha256:       f5c91cad4f9089235891857c423841694be63dadcff26cf84d0839b2f66c6124
binary sha256:              4f2cb74f25c9e18df3bb6b84579d4f1d6f931ef130d2f2507489d835478d4c01
machine receipt sha256:     40e1aed8101986214f490ac5bfb5a5c666a2844746338ce86cbf092ff18d7a3f
suite hash:                 f1f7aa6a503ef2132567bc61cc41ec47a045c9f9f04f4ca09e65941ee922f341
samples per subject/order:  15
measured reads/subject:     3,000,000 per order
measured reads total:       24,000,000

8-client AB native:         2,283,696 reads/s, 5,856 ns p99
8-client AB control:        2,595,764 reads/s, 4,945 ns p99
8-client AB ratios:         0.8798 throughput, 1.1842 p99
8-client BA native:         2,304,217 reads/s, 5,150 ns p99
8-client BA control:        2,638,279 reads/s, 4,590 ns p99
8-client BA ratios:         0.8734 throughput, 1.1220 p99

32-client AB native:        2,134,702 reads/s, 5,224 ns p99
32-client AB control:       2,425,028 reads/s, 4,718 ns p99
32-client AB ratios:        0.8803 throughput, 1.1072 p99
32-client BA native:        2,155,744 reads/s, 5,350 ns p99
32-client BA control:       2,420,521 reads/s, 4,661 ns p99
32-client BA ratios:        0.8906 throughput, 1.1478 p99

all workload gates:         128 pass, 0 fail
OTel correlation:           all eight run IDs in logs, metrics, and traces
current scratch removed:    384 objects, 550,912,512 bytes
durable run evidence:       28 objects, 3,278,822 bytes
lease teardown:             9 resources destroyed, 0 Terraform entries remain
```

Native retained 87.34 through 89.06 percent of direct RocksDB throughput and
kept p99 between 1.1072x and 1.1842x control. Every explicit 0.80x throughput
and 1.20x p99 constraint passed in both orders. The 8-client AB p99 result has
the least headroom, 1.58 percentage points below the ceiling. Cache pressure
and larger working sets therefore remain material open risks.

The comparator's overall verdict is `inconclusive`, not `better`, because its
generic primary-metric rule asks whether the candidate improved by at least 20
percent. GP3.1.1 is an envelope claim. Its executable throughput and p99
constraints are the admission authority.

## What was matched

Every subject ran after complete GCS object-base verification, txLog catch-up,
worker kill, and empty replacement with six live local authority processes.
Candidate and control shared source, binary, machine receipt, lockfile, suite,
profile, seeds, operation keys, 1 KiB owned values, sample count, total
operation budget, client count, and batch identity. The measured-path
difference was:

```text
N synchronized clients
  -> native: version-bound ResidentRangeEngine snapshot -> owned value
  -> control: direct RocksDB DB::get                    -> owned value
```

The native image used at most 12,564,950 local bytes for 4,098 records. Both
subjects issued zero object requests during measured reads.

## Latency shape

| Clients | Order | Subject | p50 | p95 | p99 | p99.9 |
|---:|---|---|---:|---:|---:|---:|
| 8 | AB | native | 3,305 ns | 3,855 ns | 5,856 ns | 16,256 ns |
| 8 | AB | direct RocksDB | 2,875 ns | 3,416 ns | 4,945 ns | 16,120 ns |
| 8 | BA | direct RocksDB | 2,849 ns | 3,310 ns | 4,590 ns | 14,853 ns |
| 8 | BA | native | 3,287 ns | 3,768 ns | 5,150 ns | 15,615 ns |
| 32 | AB | native | 3,263 ns | 3,741 ns | 5,224 ns | 17,059 ns |
| 32 | AB | direct RocksDB | 2,833 ns | 3,303 ns | 4,718 ns | 15,977 ns |
| 32 | BA | direct RocksDB | 2,830 ns | 3,279 ns | 4,661 ns | 16,157 ns |
| 32 | BA | native | 3,270 ns | 3,774 ns | 5,350 ns | 16,991 ns |

The native boundary adds roughly 412 to 456 ns at p50. Aggregate throughput
falls from about 2.29 million reads/s at 8 clients to 2.15 million at 32,
roughly the same saturation shape as the direct control. The runner reports
wall throughput and merged per-operation latency; it does not yet report CPU
time, scheduler delay, RocksDB block-cache behavior, or read amplification.

## Failures caught before admission

1. The first upload was a macOS ARM binary and could not execute on the x86_64
   Linux runner. The exact source bundle was built on the runner before any
   admitted measurement.
2. The base runner image lacked Git, the Rust toolchain, and native build
   dependencies. Setup installed them and built the exact detached revision.
3. The local NVMe mount root was not writable by the benchmark user. The
   measured runs used an owned subdirectory on the same machine-receipted NVMe
   device. The two failed attempts ended before measurement and have no result
   receipts.

These are infrastructure-harness defects, not kernel failures. The next harness
revision should build or install an architecture-matched lean eval binary,
preinstall its build dependencies, create the benchmark scratch directory at
startup, and record the Rust version instead of `unknown`.

## Architectural consequence

Concurrent resident reads do not currently force a FoundationDB transaction
plane. The native version-bound read boundary remains within the frozen direct
RocksDB envelope through 32 clients on one eight-vCPU machine. This result
admits one mechanism, not native distributed transaction authority.

The next read gate is cache pressure with a declared RocksDB block-cache budget,
a reusable immutable fixture larger than that budget, CPU time, physical bytes,
read amplification, and object-fetch attribution. In parallel, the next
transaction gate is a native three-node replicated commit path against a
same-durability control. FoundationDB remains the strict-serializability oracle
and fallback profile until those gates pass.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/runs/gp311-r0-20260827/receipts/
gs://doss-objectkv-dev-okv-evals/runs/gp311-r0-20260827/otel/
gs://doss-objectkv-dev-okv-evals/runs/gp311-r0-20260827/source/e478806.bundle
```

`GCS-EVIDENCE.tsv` records the exact remote object URIs and hashes. Bucket
versioning retains deleted scratch generations. The 384 current scratch
objects, lease-scoped SSH key, runner, collector, disks, firewall rules,
router, NAT, and subnet were removed after evidence capture.
