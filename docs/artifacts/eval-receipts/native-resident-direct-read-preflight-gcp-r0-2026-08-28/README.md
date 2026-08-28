# T27 matched direct-read mechanism preflight, 2026-08-28

Status: `[VERIFIED]` for matched direct-table-read configuration, Linux
physical-read attribution, exact point reads, frozen source identity, durable
evidence, and complete lease teardown. This is one smoke sample per subject.
It is not a cache-pressure performance admission, and T27 remains
`[EVALUATING]`.

## Result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
hot scratch:                375 GiB GCP local SSD, NVMe, ext4
stable volume:              200 GiB pd-ssd
collector:                  private e2-standard-2 with 20 GiB pd-balanced
source revision:            a56c838947a702ab6f1a66b719e7c8fd53bc4efe
source bundle sha256:       d6be2259eb1cdfff4110539ef2102b21319b6e1ab8a22576c2fe0de3971689e6
binary sha256:              e9749570bea3742ca95e70d9992baef3e4a1270c03ecd8678036efae5f0082e1
machine receipt sha256:     89f074e36dc925d88315c2f996169601b266a590726b350c6ef2e1c819f46e06
suite:                      native-resident-direct-read-preflight-v1
suite hash:                 24d46a5965594d8e1a4196ba8b0a39f005167e8e2a396cd4735848ebefa3600b
profile:                    linux-direct-io-preflight
profile hash:               2aa614932a169d1f891a968681e843536f513773fdc93f3fc9db44123d81a2d8
seed:                       1103
logical values:             16 MiB
block cache:                4 MiB
access trace:               Zipf 0.8
clients:                    8
measured reads/subject:     100,000
samples/subject:            1
direct table reads:         true for native and control

native throughput:          122,702 reads/s
control throughput:         117,440 reads/s
native/control throughput:  1.0448x

native p99:                 217,130 ns
control p99:                224,993 ns
native/control p99:         0.9651x

native CPU/read:            10,928 ns
control CPU/read:           10,572 ns
native/control CPU/read:    1.0337x

native physical bytes/read: 2,960.75
control physical bytes/read:2,966.00
native/control physical:    0.9982x

workload hard gates:        44 pass, 0 fail
correctness anomalies:      0
measured object operations: 0
lease teardown:             9 resources destroyed, 0 Terraform entries remain
durable evidence:           8 objects, 2,681,699 bytes
local provider cache:       removed, worktree returned to 7.5 MiB
```

## Finding

RocksDB direct table reads are now a matched evaluator treatment. The native
RangeEngine and the owned-value control both received `direct_reads=true`,
reported that value in the measured sample, and passed a fail-closed profile
gate. Linux attributed approximately 2.96 KiB of physical device reads to each
logical read. The preceding buffered calibration reported zero because the
operating-system page cache satisfied the remaining RocksDB misses.

The native and control paths produced nearly identical physical bytes per read
in this sample. That verifies the measurement mechanism and option parity. It
does not establish a stable throughput or latency ratio because the smoke
profile has one repeat, one seed, 100,000 reads, and no AB/BA comparison.

An earlier diagnostic run observed the same non-zero physical path but was
discarded because a single sample incorrectly declared fixture reuse. Source
`a56c838` corrected the declaration before the admitted preflight. No workload
or direct-I/O mechanism changed.

## Decision

Retain buffered reads as the portable product default and use direct table
reads as an explicit evaluation lane when the question is physical NVMe cost.
Direct reads optimize for attributable device behavior. They give up the
operating-system page cache and are not equally portable across every provider
or filesystem.

The next T27 slice must create one persisted, content-addressed resident
fixture that is reused by native, control, A/B, and B/A. The 1 GiB coverage and
skew sweep can then use this direct-read treatment without paying replicated
fixture construction four times. Row 2 does not start until row 1 is admitted.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/runs/gp312dio-r0-20260828/
```

`GCS-EVIDENCE.tsv` binds the exact source bundle, machine receipt, workload
receipts, stdout, and Linux build tests to SHA-256 digests. Bucket versioning
is enabled. The runner, collector, persistent disks, local NVMe, firewalls,
router, NAT, subnet, lease-scoped SSH key, local source copies, and Terraform
provider cache were removed after evidence capture.
