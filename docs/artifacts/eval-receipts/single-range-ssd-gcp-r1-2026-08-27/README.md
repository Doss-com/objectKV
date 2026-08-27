# GP3.1 GCP R0 optimization run, 2026-08-27

Status: `[VERIFIED]` for the complete-image routing optimization, frozen
candidate/control comparison, executable p99 constraint, and teardown.
`[EVALUATING]` for GP3.1 admission because p99 failed in both process orders.

This is optimization run 1 on the R0 infrastructure shape. It is not the
three-data-machine infrastructure rung named R1.

## Decision result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
stable volume:              200 GiB pd-ssd at /var/lib/objectkv
serving scratch:            375 GiB GCP local SSD, NVMe, ext4
object base:                regional versioned GCS
source revision:            88e221b9d1a593d1e6877ef5de70abdcfa0b23b4
source bundle sha256:       37357655888294dea9bb99e82aa2ff1adcf95e479277a90fb34a2f5846bda22d
binary sha256:              088930b0f4353e1384ed11a1f8b042c969b9857f43612de2e90f7ffb13b1cf95
machine receipt sha256:     cddbf2fcb8af94479d5c3f0ab3ecb4752e47013a221b2024a3afa529c2ba05ca
suite hash:                 5a9c5efdb3a176f95ea10b90e48540afadbb68c0df0624e5b63f126c898b9e76
samples per subject/order:  15
measured reads per subject: 3,000,000

order AB candidate:         575,498 reads/s, 2,490 ns p99
order AB control:           713,304 reads/s, 1,841 ns p99
order AB ratios:            0.8068 throughput, 1.3525 p99
order AB verdict:           worse, p99 constraint failed

order BA candidate:         573,999 reads/s, 2,427 ns p99
order BA control:           717,362 reads/s, 1,867 ns p99
order BA ratios:            0.8002 throughput, 1.2999 p99
order BA verdict:           worse, p99 constraint failed

candidate local image:      4,351,739 bytes
post-activation object ops: 0
correctness anomalies:      0
lease teardown:             9 resources destroyed, 0 benchmark resources remain
```

Throughput entered the frozen 20 percent envelope in both orders, by 0.68 and
0.02 percentage points. P99 remained 30.0 to 35.3 percent above direct
RocksDB, outside the 20 percent limit. Every comparability check passed.

Relative to the prior clean R0 candidate, bypassing manifest lookup and
reference cloning after a complete serving image is active improved mean
candidate throughput by 11.18 percent. Mean candidate p99 improved by only
0.95 percent. This supports the routing-cost hypothesis for throughput and
rejects it as the dominant explanation for tail latency.

## What was verified

- The comparison engine binds each result to the current suite hash.
- The candidate and control share source, lockfile, machine, seeds, profile,
  batch, metric, sample count, and hard-gate identity.
- The p99 ratio is an executable lower-is-better constraint in the receipt.
- Missing or non-finite secondary metrics invalidate a comparison.
- The complete GCS closure, retained txLog suffix, killed worker, empty
  replacement, bounded RocksDB activation, exact reads, and zero-object hot
  window passed in both candidate runs.
- All four run IDs occur in OTel logs, metrics, and traces.
- Four core `okv` tests and three evaluator comparison tests passed.

## Architectural consequence

GP3.1 remains `[EVALUATING]`, and its predeclared stop condition fired. Do not
continue adding point-read checks around a resident RocksDB value one at a
time. The next bounded design slice is a native resident-engine data plane:
materialize the authoritative object base plus txLog tail into the local
engine, bind correctness at activation and frontier transitions, and let the
resident engine own the steady-state point lookup. `okv-log`, publication,
branching, recovery, and exact historical views remain objectKV-owned.

If that native-engine boundary cannot clear the same p99 and throughput gate,
the serving and transaction data plane moves to TiKV or FoundationDB. The
object-native lifecycle and history layers remain reusable above it. RAM,
multi-range, PostgreSQL, and HTAP work do not inherit a hot-path performance
claim from this result.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/results/gp31nvme2-r1/receipts/
gs://doss-objectkv-dev-okv-evals/results/gp31nvme2-r1/objectkv-otel-evidence/
gs://doss-objectkv-dev-okv-evals/results/gp31nvme2-r1/verification/
gs://doss-objectkv-dev-okv-evals/bundles/sha256/37357655888294dea9bb99e82aa2ff1adcf95e479277a90fb34a2f5846bda22d.bundle
```

The live benchmark scratch closure contained 96 objects. Those current
scratch objects were removed after evidence capture; bucket versioning keeps
the deletion recoverable. The retained evidence totals less than 250 KiB.
