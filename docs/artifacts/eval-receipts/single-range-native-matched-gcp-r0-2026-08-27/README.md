# GP3.1 native matched-topology admission, 2026-08-27

Status: `[VERIFIED]` for the single-range native resident read boundary, both
process orders, exact owned values, zero-object hot reads, bounded local state,
required OTel signals, durable evidence, scratch cleanup, and lease teardown.
This is not a replicated-commit, multi-range, cache-pressure, or complete-cell
performance claim.

## Result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
stable volume:              200 GiB pd-ssd at /var/lib/objectkv
serving scratch:            375 GiB GCP local SSD, NVMe, ext4
object base:                regional versioned GCS
source revision:            418b776de26de8b1428d42069d6a7dd7cb6b9f0b
source bundle sha256:       2d0ab77679a2b2e05fb9057e3562e3cbef62835e7757367cfd86eac36a07e7ac
binary sha256:              09bfbab97d7444b10a80d5acc308b40c264c72900ac2f0445687e486e4df2255
machine receipt sha256:     d73768b2493c8a36841f2c1d877b742a2fbcb191f1014a8fa4f55be1efcb2e07
suite hash:                 63a0842334d02bf1d00936d75633b251f340e56785c13c842009f79d59123cba
samples per subject/order:  15
measured reads/subject:     3,000,000 per order

order AB native:            656,662 reads/s, 1,920 ns p99
order AB direct control:    722,443 reads/s, 2,102 ns p99
order AB ratios:            0.9089 throughput, 0.9134 p99

order BA native:            660,783 reads/s, 1,883 ns p99
order BA direct control:    718,492 reads/s, 2,062 ns p99
order BA ratios:            0.9197 throughput, 0.9132 p99

all workload gates:         64 pass, 0 fail
OTel correlation:           all four run IDs in logs, metrics, and traces
current scratch removed:    192 objects, 275,456,256 bytes
durable run evidence:       14 objects, 122,327 bytes
lease teardown:             9 resources destroyed, 0 instances or disks remain
```

Native retained 90.89 percent and 91.97 percent of direct RocksDB throughput,
inside the frozen 80 percent floor. Native p99 was 8.66 percent and 8.68
percent lower than control, inside the frozen 1.20x ceiling. Both explicit
comparison constraints passed in both orders.

The comparator's overall verdict is `inconclusive`, not `better`, because its
generic primary-metric rule asks whether the candidate improved by at least 20
percent. GP3.1 is an envelope claim, not an improvement claim. Its executable
throughput and p99 constraints are the admission authority. A later harness
change should represent parity gates directly instead of overloading the
improvement verdict.

## What was matched

Both subjects ran after the same complete GCS object-base verification, txLog
catch-up, worker kill, empty replacement, and six live local authority
processes. Both used the same source, machine receipt, lockfile, suite, profile,
seeds, operation keys, 1 KiB owned values, sample count, and batch identity.
The only measured-path difference was:

```text
recovered replacement worker
  -> native: version-bound ResidentRangeEngine snapshot -> owned value
  -> control: direct RocksDB DB::get                    -> owned value
```

The native engine used 11,995,653 local bytes for 4,098 records and issued zero
object requests during measured reads. The direct control also issued zero
object requests during measured reads. All four results passed their 16 hard
gates.

## Latency shape

| Order | Subject | p50 | p95 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|
| AB | native | 1,440 ns | 1,676 ns | 1,920 ns | 5,523 ns |
| AB | direct RocksDB | 1,270 ns | 1,750 ns | 2,102 ns | 5,637 ns |
| BA | native | 1,433 ns | 1,664 ns | 1,883 ns | 5,559 ns |
| BA | direct RocksDB | 1,286 ns | 1,703 ns | 2,062 ns | 5,607 ns |

The native boundary adds about 147 to 170 ns at p50, while p95 through p99.9
are equal or lower in this working set. This does not yet establish the cause.
The next latency study needs concurrent clients, CPU per read, and larger
working sets before the curve can be generalized.

## Failures caught before admission

1. RocksDB could remove an obsolete SST between directory enumeration and
   `metadata()`. Local-byte accounting now disables file deletion during the
   bounded scan and always re-enables it.
2. The golden-path program named a backend string the implemented runner did
   not accept. The first cloud attempt stopped before measurement. Revision
   `418b776` binds both subjects to the implemented native-resident NVMe backend.
3. The runner release build currently links the full DataFusion and bundled
   RocksDB graph. Setup took substantially longer than the benchmark. A lean
   resident-kernel eval binary is needed for iteration speed, but did not alter
   this frozen run.

Revision `418b776` changes only the golden-path TOML backend identity relative
to the compiled parent. No Rust source changed, so the captured binary remains
the exact implementation exercised by that revision.

## Architectural consequence

The earlier unmatched GP3.1 result charged the native subject for a full
recovery topology while comparing it with a bare direct RocksDB process. Its
p99 failure is not authority for abandoning the native plane. Once process
topology and owned-value semantics are matched, the native read boundary is
within the frozen envelope in both orders.

This admits the single-range read mechanism, not the distributed system.
objectKV remains native-first, with FoundationDB retained as the strict
serializability oracle and fallback profile. The next native gates are
concurrent and cache-pressure read curves, then a three-node replicated commit
path against a same-durability control. Multi-range transactionality remains
blocked until those pass.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/runs/gp31match-r0-20260827/receipts/
gs://doss-objectkv-dev-okv-evals/runs/gp31match-r0-20260827/telemetry/
gs://doss-objectkv-dev-okv-evals/bundles/native-matched/418b776.bundle
```

The four exact scratch prefixes have zero current objects after cleanup. Bucket
versioning retains recovery history. The lease-scoped SSH key and both VMs were
destroyed after evidence capture.

`GCS-EVIDENCE.tsv` records the exact remote object URIs and hashes. The local
`SHA256SUMS` file verifies this readout and that remote evidence index from the
artifact directory.
