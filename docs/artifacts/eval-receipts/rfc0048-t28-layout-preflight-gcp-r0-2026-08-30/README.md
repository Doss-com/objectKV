# RFC-0048 typed layout GCS preflight

Status: `[VERIFIED]` retained resource-guard rejection. The C5 performance lane
remains `[EVALUATING]`; no admission positions ran.

Date: 2026-08-30

## Result

One objectViewer-only GCP runner opened the same generation-pinned C0 and C5
fixture from fresh processes. Each point process executed the first 256
seed-5701 operations across eight tasks. Each scan process executed the frozen
DataFusion query through a single sequential range-fetch scheduler.

C5 passed every correctness, request, byte, metadata, and scan resource gate.
It missed the point resource guard by 1.61 percent, so RFC-0048 correctly stops
before its 15 admission blocks.

```text
C0 point p99  90.791 ms
C5 point p99 230.636 ms
ratio          2.540x
guard         <= 2.500x

C0 scan        2,128 rows/s
C5 scan       67,227 rows/s
ratio          31.595x
guard          >= 1.250x
```

## Point lane

| Percentile | C0 indexed row | C5 columnar main | C5/C0 |
|---|---:|---:|---:|
| p50 | 35.178 ms | 64.677 ms | 1.839x |
| p95 | 53.300 ms | 99.649 ms | 1.870x |
| p99 | 90.791 ms | 230.636 ms | 2.540x |
| p99.9 | 108.619 ms | 455.187 ms | 4.191x |

| Physical work | C0 | C5 | C5/C0 |
|---|---:|---:|---:|
| Provider attempts | 256 | 505 | 1.973x |
| Response bytes | 16,585,027 | 5,819,521 | 0.351x |
| Conservative maximum bytes/point | 65,524 | 23,178 | 0.354x |
| Full-object requests | 0 | 0 | 0 |
| Correctness anomalies | 0 | 0 | 0 |

Provider-call p99 was 90.450 ms for C0 and 95.907 ms for C5, a 1.060x
ratio. C5 point lookup performs a projection-stripe GET followed by a payload-
page GET. The 2.540x end-to-end p99 therefore localizes to sequential request
composition and provider-tail exposure, rather than materially slower
individual range reads. The next format candidate should place payload-page
coordinates in the resident primary index so the two ranges can be fetched
concurrently.

## Projected-scan lane

| Metric | C0 indexed row | C5 columnar main | C5/C0 |
|---|---:|---:|---:|
| Rows | 15,742 | 15,742 | 1.000x |
| Query time | 7.398 s | 0.234 s | 0.032x |
| Rows/s | 2,127.8 | 67,227.3 | 31.595x |
| GCS range GETs | 203 | 6 | 0.030x |
| Response bytes | 13,105,844 | 1,527,824 | 0.117x |
| Peak fetch | 65,524 | 257,628 | 3.932x |
| Peak source batch rows | 88 | 92 | 1.045x |
| Opaque-payload GETs | 0 | 0 | 0 |
| Peak in-flight fetches | 1 | 1 | 1.000x |
| Correctness anomalies | 0 | 0 | 0 |

Both scans reproduced the independent ordered-projection digest and the exact
quantity sum of `67,524,278`. C5 stayed below the 256 KiB fetch limit and the
128-row source-batch limit.

## Identity

- Project: `doss-objectkv-dev`
- Runner: `objectkv-bench-t27a2-r0-runner`, `us-central1-a`
- Runtime authority: `roles/storage.objectViewer` only
- Fixture ID:
  `5d933648e3190b3bd6768c36c1d9022596c69c621c2347fa648a0754dc5431b0`
- Placement envelope:
  `1d9ddeff4a4885511f3a4e7cdf11507a45cd47e17525911448e0f094a6343f69`
- Execution-plan semantic SHA-256:
  `2e04d69775f67cb7561b59374d27bf2082909ca2df23a72f40e209728131c797`
- Execution-plan file SHA-256:
  `e74aa8c739558e38d4be1beb06d5a538baa02031c5eca3d1994a845a283232f6`
- Planner commit: `fc64b82`
- Point-position commit: `0c1935f`
- Scan-position commit: `62e7549`
- Point executable SHA-256:
  `67025226f32880bd63737b8846cb6bca275013a7d72996338f5a9188c7adda35`
- Scan executable SHA-256:
  `30af77730db0a801616347969f62e0e850168181a56e3fd23d9c9978e932781e`

## Durable evidence

The typed root, postpublication plan, publication and authority receipts, and
all four point/scan outputs are archived at:

`gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0048-t28-layout-r0-f3bd0b6/preflight-receipts.tar.gz#1788080681096029`

Archive SHA-256:
`bde28a2e8bb8d69fc95f9ca6a38ed3ab43c7f44f4dda8b64110b5fe38a9a0069`

## Decision

D1. Preserve this preflight as a rejection and do not run RFC-0048 admission
positions.

D2. Preserve C5's columnar media and scan path. Its 31.595x scan result and
0.117x scan bytes establish material leverage.

D3. Design a new compatible C5 point-index revision that exposes projection
and payload page ranges before data access, then test concurrent two-range
gather under a new immutable fixture and execution plan.

Optimizes for: retaining the demonstrated columnar advantage while removing
the sequential cold-point penalty.

Gives up: claiming that the current C5 v1 format is the admitted general object
base.
