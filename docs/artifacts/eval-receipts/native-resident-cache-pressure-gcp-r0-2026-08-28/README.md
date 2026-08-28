# GP3.1.2 native resident cache-pressure calibration, 2026-08-28

Status: `[VERIFIED]` for execution of the frozen 64 MiB calibration, its
negative performance result, exact semantic gates, required OTel signals,
durable evidence, scratch cleanup, and lease teardown. T27 remains
`[EVALUATING]`. The current native point-read implementation did not clear the
frozen performance envelope, and no 1 GiB admission claim was attempted.

## Result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
stable volume:              200 GiB pd-ssd at /var/lib/objectkv
serving scratch:            375 GiB GCP local SSD, NVMe, ext4
object base:                regional versioned GCS
source revision:            76d2cae98d846901c1833a3c038be983509c7f6a
source bundle sha256:       d48515347fec846c0c5f9b115097ceee26232e642a7b784b8e96038ce8f43de3
binary sha256:              c4441e0531faa19ac73743ad38b01335e9171f451e460497b6fd2457ec81afd4
machine receipt sha256:     7051803674b307097d6b5d2cb9673222c2a003c8d80880876245a0ef2b361cbc
suite hash:                 9ac7b9e7d0307d3415dc03d10dd721dbdfb412b12d9bf69b03a515c38899e2ec
profile hash:               ab928cb84dec50e39ff9ba72ec83bba33af28e0bafbd015de0382a0cead435e9
workload profile hash:      46981dfa3c1e39cb109616c4c38a27aa473d9f4d3ca5c8ca9991d67d0d858774
seed:                       1103
samples per subject/order:  15
measured reads/receipt:     15,000,000
measured reads total:       60,000,000

AB native:                  1,012,794 reads/s, 22,533 ns p99
AB direct RocksDB:          1,696,914 reads/s, 16,927 ns p99
AB ratios:                  0.5968 throughput, 1.3312 p99, 1.6685 CPU/read

BA direct RocksDB:          1,786,883 reads/s, 15,703 ns p99
BA native:                  1,011,245 reads/s, 24,445 ns p99
BA ratios:                  0.5659 throughput, 1.5567 p99, 1.7460 CPU/read

workload hard gates:        84 pass, 0 fail
comparison verdicts:        worse in AB and BA
OTel correlation:           all four run IDs in logs, metrics, and traces
current scratch removed:    152 objects, 550,853,784 bytes
durable run evidence:       19 objects, 4,758,698 bytes
lease teardown:             9 resources destroyed, 0 Terraform entries remain
```

The frozen envelope required native throughput of at least 0.80x control and
native p99 of at most 1.20x control. Native missed both constraints in both
orders. AB throughput was 40.32 percent lower and p99 was 33.12 percent higher.
BA throughput was 43.41 percent lower and p99 was 55.67 percent higher. The
order reversal rules out a simple first-run or second-run explanation.

RFC-0043 also limits CPU time per read to 1.25x control for T27 admission.
Observed CPU ratios were 1.6685x and 1.7460x, so that bound also fails. The
calibration receipt reports CPU, but its comparator currently executes only
the throughput and p99 cross-result constraints. CPU and physical-byte
constraints must become executable before the 1 GiB admission.

## What the run proves

Every subject reconstructed exact object base plus txLog state, killed a
worker, opened an empty replacement, returned exact owned 1 KiB values, and
issued zero object requests during measured reads. Candidate and control shared
source, binary, machine, suite, profile, seed, cache capacity, key and value
trace, operation count, client count, process topology, and batch identity.

```text
64 MiB exact logical values
        ↓
six local OpenRaft authority processes
        ↓
killed worker and empty replacement
        ↓
32 MiB explicit RocksDB block cache
        ↓
15 independently warmed windows × 1,000,000 reads
        ↓
native snapshot boundary ↔ direct owned-value RocksDB control
```

The paired comparator therefore identifies a real cost inside the current
native read composition. It does not show that object-backed serving is
infeasible, because no measured read reached object storage or physical NVMe.

## Attribution

| Order | Subject | Cache hit ratio | CPU ns/read | Peak RSS | Physical bytes/read |
|---|---|---:|---:|---:|---:|
| AB | native | 0.997260 | 7,477 | 399,355,904 | 0 |
| AB | direct RocksDB | 0.994449 | 4,482 | 398,934,016 | 0 |
| BA | direct RocksDB | 0.994503 | 4,291 | 399,151,104 | 0 |
| BA | native | 0.997263 | 7,492 | 399,134,720 | 0 |

The native subject used nearly identical peak memory and a slightly higher
block-cache hit ratio, yet consumed 1.67x to 1.75x CPU per read. Linux reported
zero physical read bytes for every subject. This calibration is therefore a
combined RocksDB and operating-system page-cache curve, not an isolated NVMe
curve. The immediate bottleneck is above physical media, in the native
snapshot, head, history, metadata, or value-return path. A CPU profile is
required before selecting the owning function.

Zipf 1.4 also produced more than 99.4 percent block-cache hits despite a
working set twice the declared block cache. The calibration did not create a
meaningful physical-read signal. A separate reviewed experiment must use a
broader trace, larger fixture, direct I/O, controlled page-cache treatment, or
a combination of these. The frozen result itself is not changed.

## Operational findings

The first cloud attempt,
`27069e4c-930d-491f-b07a-90739ad4dfe1`, was discarded before a receipt. The
old three-seed setup rebuilt too many replicated histories to finish inside the
bounded command budget. The calibration contract was reduced to one seed and
15 measurement windows without changing the 1 GiB admission requirement.

Each completed receipt still constructed and semantically replayed its own
64 MiB fixture. Aggregate three-replica transaction-authority scratch reached
about 1.2 GiB per fixture, roughly 19x logical values, and setup took 35 to 40
minutes per receipt. This is outside measured reads, but it makes iteration
unnecessarily expensive. The next harness slice must persist one
content-addressed fixture and reopen or clone it across candidate, control, and
both process orders.

The first long IAP SSH transport disconnected after about 28 minutes while the
remote run continued. Detached execution completed the reverse-order runs.
This is an infrastructure-runner defect, not a workload failure.

## Decision and next gate

Do not expand to the 1 GiB admission or upper-stack workload while the current
native path misses the 64 MiB calibration this decisively. The next bounded
slice is:

1. Profile native and direct point reads on the exact frozen fixture.
2. Remove or amortize the owning native CPU cost without weakening generation,
   range, frontier, snapshot, or owned-value semantics.
3. Persist one fixture and reuse it across all four subjects.
4. Rerun this exact AB and BA gate.
5. Freeze a separate physical-read experiment only after this envelope passes.

FoundationDB remains the semantic oracle and fallback. The result does not
reverse D56 by itself, but native-first progression is blocked at T27 until the
same calibration clears its constraints.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/runs/gp312-r0-20260828/objectkv-gp312-r0-evidence/
```

`GCS-EVIDENCE.tsv` records the exact remote object URIs and hashes. Bucket
versioning retains deleted scratch generations. The 152 current scratch
objects, lease-scoped SSH key, runner, collector, disks, firewall rules,
router, NAT, subnet, and local Terraform provider cache were removed after
evidence capture.
