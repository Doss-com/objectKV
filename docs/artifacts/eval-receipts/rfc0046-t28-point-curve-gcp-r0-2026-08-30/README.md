# RFC-0046 T28 point curve on GCS

Status: `[EVALUATING]`. The frozen admission gate rejected 2 of 15 blocks.

Date: 2026-08-30

## Result

The run used one existing 1 GiB content-addressed fixture, three deterministic
1,024-operation plans, eight concurrent clients per position, four fresh
processes per block, five paired blocks per seed, and alternating ABBA or BAAB
order. Candidate and raw control consumed the same sealed data ranges.

| Percentile | objectKV candidate | Raw GCS range control | Ratio |
|---|---:|---:|---:|
| p50 | 26.760 ms | 26.758 ms | 1.000x |
| p95 | 44.202 ms | 43.666 ms | 1.012x |
| p99 | 61.752 ms | 58.920 ms | 1.048x |
| p99.9 | 140.124 ms | 116.206 ms | 1.206x |

The frozen primary gate required every block's pooled candidate p99 to remain
at or below 1.25x its paired raw control. Thirteen blocks passed. Seed 2207,
block 0 rejected at 1.3775x; seed 3301, block 0 rejected at 1.2978x. The block
ratio distribution was 0.8479x minimum, 1.0033x median, 1.3775x nearest-rank
p95, and 1.3775x maximum.

Provider-only p99 ratios on the two rejected blocks were 1.3792x and 1.2990x.
Across all samples, candidate provider p99 was 61.386 ms and raw-control
provider p99 was 58.567 ms, also 1.048x. The end-to-provider latency gap was
about 0.35 ms on both subjects. The rejected end-to-end ratios therefore track
the measured GCS call rather than a separately visible objectKV-local stage.
This attribution is diagnostic; it does not change the frozen rejection.

### Exact local-residual diagnostic

After the frozen curve, the runner was changed so every operation recorded its
own end-to-end time, GCS call time, and exact saturated difference. One fresh
1,024-read position per subject produced:

| Percentile | Candidate local residual | Raw-control local residual | Added candidate time |
|---|---:|---:|---:|
| p50 | 340.150 us | 331.069 us | 9.081 us |
| p95 | 384.621 us | 361.978 us | 22.643 us |
| p99 | 428.507 us | 407.242 us | 21.265 us |
| p99.9 | 514.751 us | 464.407 us | 50.344 us |

Candidate end-to-end p99 was 95.611 ms versus 62.276 ms raw, while provider
p99 was 95.269 ms versus 61.950 ms. The 33.335 ms end-to-end difference tracks
the 33.319 ms provider difference; the exact local-residual difference was
21.265 microseconds. This pair proves the attribution mechanism and is not a
precommitted admission curve.

The two receipts are retained at
`gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0046-t28-local-attribution-gcp-r0-20260830/receipts.tar.gz#1788073633546913`
(26,886 bytes, MD5 `aD/OM8Nr706wRUN21a5B9w==`). The release executable
SHA-256 was
`42d1a971b62235850394f52332393fb7ac3f1d24186697ff0808b1c4df62adbf`.

## Correctness and physical work

| Check | Observed |
|---|---:|
| Fresh position processes | 60 distinct PID and start-tick pairs |
| Measured reads | 61,440 total, 30,720 per subject |
| Measured provider attempts | 61,440 |
| Measured response bytes | 3,994,975,620 |
| Metadata warmup attempts | 7,980 |
| Metadata warmup bytes | 94,478,700 |
| Full-data requests | 0 |
| LIST, PUT, or DELETE requests | 0 |
| Provider retries | 0 |
| Correctness anomalies | 0 |

Every measured read returned the independent expected-value digest and used
exactly one planned GCS byte range. Selected authenticated sparse indexes were
retained in RAM before measurement; data blocks were not retained.

## Identity

- GCP project: `doss-objectkv-dev`
- Bucket: `doss-objectkv-dev-okv-evals`
- Runner: `objectkv-bench-t27a2-r0-runner`, `us-central1-a`
- Machine ID: `1115800effc744faa0199cde1db52a82`
- Boot ID: `587e8408-74d8-4fc7-8df0-9e2921d57634`
- Fixture envelope SHA-256:
  `768e1a9b8ee91a16615dd69b89d15ba581667a9d5ab6e5190b5de663efcc024d`
- Reader IAM receipt SHA-256:
  `f383977a0f13ddf791ebc6ac97381ffc903268f45416689fe7eb23db22f2c1e9`
- Seed 1103 plan SHA-256:
  `f1e3612a8480b852a82ade64f030f7b7b4e347ae56e130f865e62544594ba3b1`
- Seed 2207 plan SHA-256:
  `dd7488b537c7fa71b685025defacf9f58b9b528b6d517d048e890392f5d3eb1d`
- Seed 3301 plan SHA-256:
  `b90022799885d3443d5fb51e4caa38a45341ab6934779d118057c00c0b6cec20`
- Release executable SHA-256:
  `b953b2647a7b9f81a87a110d448e230e87f737498ac6a6411077a7b20a560166`
- Base commit: `929202b`

The release executable included the fresh-position instrumentation on top of
the named base commit. Its exact runtime source and lockfile are retained with
the raw evidence.

## Durable evidence

- Raw receipts and plans:
  `gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0046-t28-point-curve-gcp-r0-20260830/raw-receipts-and-plans.tar.gz#1788073356501337`
  (1,204,835 bytes, MD5 `pnMC7XBJg+EJ1Or+LDun5g==`)
- Runtime source and lockfile:
  `gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0046-t28-point-curve-gcp-r0-20260830/runtime-source.tar.gz#1788073372869399`
  (148,506 bytes, MD5 `rXZajqYIVRU9KNHXDlSPXA==`)

## What remains open

1. Preserve this rejected result without changing its all-block threshold.
2. Use the now code-complete per-operation local-residual receipt in a
   precommitted variance-aware addendum. Do not select its
   statistic from this result.
3. Bind any admitted curve to the declared OTel evidence contract.
4. Keep T38 unchanged until a curve is admitted.
