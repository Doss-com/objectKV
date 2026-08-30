# RFC-0046 T28 corrected point curve on GCS

Status: end-to-end gate `[EVALUATING]`; local-overhead addendum `[VERIFIED]`.

Date: 2026-08-30

## Result

The single execution required by the frozen plan completed all 60 fresh
positions, 15 paired blocks, and 61,440 measured reads against the existing 1
GiB content-addressed GCS fixture. The corrected seed matrix used ABBA and BAAB
orders and inverted seed 2207's starting subject.

The original gate remains rejected. Thirteen of 15 blocks kept candidate
end-to-end p99 at or below 1.25x raw GCS. Seed 1103 block 0 measured 1.594936x;
seed 3301 block 3 measured 1.383290x. Their provider-only ratios were 1.597901x
and 1.385882x.

The precommitted local-residual addendum passed all 15 blocks. Its maximum
block ratio was 1.078302x and its maximum positive candidate increment was
33.932 microseconds, below the frozen 1.25x and 250-microsecond limits. This
verifies bounded objectKV-local work. It does not replace or pass the original
end-to-end gate.

| Percentile | Candidate end to end | Raw GCS | Ratio |
|---|---:|---:|---:|
| p50 | 26.841 ms | 26.437 ms | 1.015x |
| p95 | 43.770 ms | 41.743 ms | 1.049x |
| p99 | 62.304 ms | 56.964 ms | 1.094x |
| p99.9 | 151.128 ms | 108.421 ms | 1.394x |

| Percentile | Candidate provider | Raw provider | Candidate local | Raw local |
|---|---:|---:|---:|---:|
| p50 | 26.495 ms | 26.098 ms | 339.557 us | 333.545 us |
| p95 | 43.421 ms | 41.407 ms | 391.796 us | 383.531 us |
| p99 | 61.970 ms | 56.617 ms | 446.575 us | 439.678 us |
| p99.9 | 150.797 ms | 108.013 ms | 531.161 us | 525.669 us |

## Correctness and physical work

| Check | Observed |
|---|---:|
| Fresh process identities | 60 |
| Measured reads | 61,440 |
| Measured provider attempts | 61,440 |
| Measured response bytes | 3,994,975,620 |
| Metadata warmup attempts | 7,980 |
| Metadata warmup bytes | 94,478,700 |
| Full-data requests | 0 |
| LIST, PUT, or DELETE requests | 0 |
| Correctness anomalies | 0 |

Every read returned the independently expected value through exactly one
planned GCS byte-range request. Data blocks were not retained between reads.

## Identity and telemetry

- Plan ID: `t28-point-curve-addendum-v1`
- Plan SHA-256:
  `b597f94da7cc1abd5a3fef6bf4ae353ed73b9f38bc99108b59c366a0743546ea`
- Controller run ID: `e393cc6f-f121-45de-9ef1-baebe0443d71`
- Candidate commit: `afbfb693f79b246bf00560a76aa51480bed206ef`
- Executable SHA-256:
  `3762be094aa0108505adc662d9dbd02e907b878449dda3f2bd13bb8cf358b241`
- Aggregate receipt SHA-256:
  `0a4e0d1fbe7b0207789732686dae68696c5498364ca065af9f51b263efb4b0c7`
- Runner machine ID: `1115800effc744faa0199cde1db52a82`
- Runner boot ID: `587e8408-74d8-4fc7-8df0-9e2921d57634`

All six exporter flush and shutdown checks passed. Independent collector
inspection found the run ID in 61 log records, 8 metric records, and 60 trace
records. The collector confirmation is in
`collector-confirmation-v1.json`.

## Durable evidence

- Receipts:
  `gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0046-t28-corrected-v1-gcp-r0-20260830/receipts.tar.gz#1788075462625275`
  (1,983,721 bytes, MD5 `ANuHxB41rS3b05GYewT8Gg==`)
- Runtime source:
  `gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0046-t28-corrected-v1-gcp-r0-20260830/runtime-source.tar.gz#1788075466312280`
  (145,642 bytes, MD5 `fpsVz2eKvtAj51+kNv00tg==`)

## Program consequence

Row 2 remains `[EVALUATING]` because its original every-block cloud-latency
gate and cache-refill lane are not verified. The local indexed-read overhead is
`[VERIFIED]` under the addendum. Repeating the same GCS allocation comparison
would not answer the next product question, so row 3, matched row-versus-column
object-layout geometry, becomes the active frontier. T38 receives no admitted
object-tier point from this run.
