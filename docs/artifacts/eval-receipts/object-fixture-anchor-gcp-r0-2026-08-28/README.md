# RFC-0044 object fixture anchor, GCP R0, 2026-08-28

Status: `[VERIFIED]` semantic bootstrap gate. This is not a performance
admission for T27.

## Question

Can independent fresh transaction authorities establish the same canonical
object-base version with one empty transaction, retain no base values, and
recover the exact original result after the commit response is lost?

## Frozen source and machine

```text
source commit:       30f65b566547cbbe151a07fa51eba21df7866ee3
parent commit:       8a68adc9af67910b2470fd8d50031d42a56bc7a7
suite:               object-fixture-anchor-v1
suite hash:          3069e1ccc7930723c8ca908ebd2f955f4dfa1223cdabdabeed8620941dd94f6a
machine:             objectkv-anchor-rfc0044-r0
machine type:        c3-highcpu-22, 22 vCPU, 44 GiB RAM
boot media:          60 GB pd-balanced
operating system:    Debian 12, Linux 6.1.0-52-cloud-amd64
rustc:               1.88.0
okv-eval SHA-256:    889f538148d8382f90c6c3b761196d2e572e88951effe30bd334403b42b98316
```

The release build completed in 285.872 seconds. The evaluator and every
authority process used that same 202,293,656-byte binary.

## Result

The candidate started 20 fresh authority clusters sequentially, with three
real OpenRaft processes per cluster. All 20 independently assigned `O=2`.
Each authority retained exactly one empty transaction record, zero mutations,
and zero live keys. The evaluator observed the committed record after the
reply was deliberately dropped and before issuing the exact retry. That retry
and a second exact retry both returned the original commit version.

```text
fresh authorities:                  20
authority processes started:        60
distinct anchor versions:            1
anchor version:                       2
retained records per authority:       1
mutations per authority:              0
live keys per authority:              0
correctness anomalies:                0
formal candidate budget observed:     0.774596 s
candidate verdict:                    keep
candidate semantic SHA-256:           97e56fd23766a9e28cd47fd48fb90a043fe3fce39861502c10f74aa194ffa643
```

The negative control first passed the evaluator freshness guard, then bypassed
that guard with a changed request identity. The authority committed a second
empty transaction, retained two records, and the oracle detected the illegal
shape.

```text
poison authority processes started:   3
second identity bypass detected:       true
correctness anomalies:                 0
formal poison budget observed:         0.043628 s
poison verdict:                         keep
poison semantic SHA-256:                9670648ef7d4f5b6735aaac70e272210b2785e2b90b64ce5257b47d1a5811386
```

Both schema-validated suite receipts passed 12 of 12 hard gates. OTel export
was intentionally optional for this semantic contract profile. The later 64
MiB and 1 GiB workload profiles still require correlated logs, metrics, and
traces.

## Evidence

```text
local receipt directory:
  docs/artifacts/eval-receipts/object-fixture-anchor-gcp-r0-2026-08-28/

durable GCS prefix:
  gs://doss-objectkv-dev-okv-evals/runs/rfc0044-anchor-r0-20260828/
```

SHA-256:

```text
machine.json                                  3801d7626d9a84c1d6020530f830f0e963e2d015735ca719a6424d126514415b
object-fixture-anchor-candidate-receipt.json  5fbb9cc68d55e52861ff19441b6357d57a7d6770faf40890fe94ca54e6519468
object-fixture-anchor-candidate.json          c24b124b045d223470820c79d9df02c91a0f641b5da75cec021f000bd882a7e7
object-fixture-anchor-poison-receipt.json     0f4ead49fd71ef8e2dd2c07b8af8addb67d19323ddd6c15914a3b25427eca251
object-fixture-anchor-poison.json             0ac6aee44e3347bebd76fd0e6ed7604d990bc17aac1cb247281a7253b566a312
objectkv-anchor-rfc0044.bundle                 fb80d03088496d894749ae6a41c1525548edb9508e95973c7bc8bb1453427121
```

## Decision

The fresh-authority falsifier passed, so RFC-0044 descriptor and closure work
may begin. T27 remains `[EVALUATING]`. This result does not verify fixture
persistence, subject-image construction, the 64 MiB setup preflight, or the 1
GiB performance curve.
