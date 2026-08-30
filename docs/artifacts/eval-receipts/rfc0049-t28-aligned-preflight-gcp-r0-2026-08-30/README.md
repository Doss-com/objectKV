# RFC-0049 aligned columnar GCS preflight

Status: `[VERIFIED]` immutable publication, exact viewer-only point and scan
preflight, concurrent pair evidence, and writer revocation. C5v2 admission
remains `[EVALUATING]`; these uncounted samples do not replace the frozen
15-block curve.

Date: 2026-08-30

## Result

C5v2 corrects the retained C5v1 point-path rejection without giving up its
columnar scan advantage. The same eight-task, 256-read preflight measured C5v2
point p99 at 0.869x C0. Every point issued one projection and one payload range
GET concurrently. The projected scan remained 31.692x C0 throughput and read
zero opaque-payload bytes.

```text
reused immutable C0 closure
  + new aligned projection and payload frames
  -> new generation-pinned root
  -> revoke runtime objectCreator
  -> fresh viewer-only C0 and C5v2 processes
  -> exact point and DataFusion scan receipts
```

## Point lane

| Percentile | C0 indexed row | C5v2 aligned columnar | C5v2/C0 |
|---|---:|---:|---:|
| p50 | 36.737 ms | 38.113 ms | 1.037x |
| p95 | 82.928 ms | 65.777 ms | 0.793x |
| p99 | 130.172 ms | 113.082 ms | 0.869x |
| p99.9 | 251.138 ms | 135.772 ms | 0.541x |

| Physical work | C0 | C5v2 | C5v2/C0 |
|---|---:|---:|---:|
| Provider attempts | 256 | 512 | 2.000x |
| Response bytes | 16,585,027 | 4,422,951 | 0.267x |
| Maximum bytes per point | 65,524 | 17,880 | 0.273x |
| Provider-call p99 | 129.812 ms | 89.613 ms | 0.690x |
| Overlapping projection/payload pairs | n/a | 256/256 | 1.000x |
| Correctness anomalies | 0 | 0 | 0 |

The frozen preflight point guard is C5v2/C0 p99 at most 2.50x. The observed
ratio is 0.869x.

## Projected-scan lane

| Metric | C0 indexed row | C5v2 aligned columnar | C5v2/C0 |
|---|---:|---:|---:|
| Rows | 15,742 | 15,742 | 1.000x |
| Query time | 8.348 s | 0.263 s | 0.032x |
| Rows/s | 1,885.6 | 59,758.5 | 31.692x |
| GCS range GETs | 203 | 7 | 0.034x |
| Response bytes | 13,105,844 | 1,701,414 | 0.130x |
| Peak fetch | 65,524 | 262,134 | 4.001x |
| Peak source batch rows | 88 | 26 | 0.295x |
| Opaque-payload GETs | 0 | 0 | 0 |
| Peak in-flight fetches | 1 | 1 | 1.000x |
| Correctness anomalies | 0 | 0 | 0 |

Both scans reproduced the independent 15,742-row ordered projection and exact
quantity sum of `67,524,278`. C5v2 stayed under the 256 KiB fetch ceiling and
the 128-row Arrow batch ceiling. The frozen scan guard requires at least 1.25x
C0 throughput.

## Immutable media and authority

- Project: `doss-objectkv-dev`
- Bucket: `doss-objectkv-dev-okv-evals`
- Runner: `objectkv-bench-t27a2-r0-runner`, `us-central1-a`, `n2-standard-8`
- Runtime principal:
  `objectkv-eval-runner@doss-objectkv-dev.iam.gserviceaccount.com`
- Publication source: `94d55bb`
- Preflight source: `8ff14a2`
- Release executable SHA-256:
  `4a2e543955fe4cba33f5075324e485f1e47123315ee24cef827072619c0155a0`
- New root SHA-256:
  `524cb3303748b2b04f37bc3c25a1e20dc27db82119f8cb357da53780661d23fd`
- New root generation: `1788083524547258`
- Placement envelope SHA-256:
  `d2bc16dd8b7b58db292bf33763ad8a962ad89259ea28916dca00187f85684550`
- C5v2 media: 13,695,766 bytes, or 1.043x the reused 13,125,073-byte C0
- C5v2 resident metadata: 20,176 bytes, or 1.049x C0

The runner temporarily held `roles/storage.objectCreator` for create-only
publication. The role was removed before every measured process. A second
publication under a fresh probe prefix failed with `permission_denied`; the
probe prefix contains zero objects. The retained runtime binding is
`roles/storage.objectViewer` only.

## Durable evidence

The root locator, publication receipt, source locator and execution plan, four
fresh-process receipts, process outputs, denied-create evidence, binary digest,
and derived preflight summary are archived at:

`gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0049-t28-aligned-r0-8ff14a2/preflight-receipts.tar.gz#1788084094078007`

Archive SHA-256:
`4b4e1e2acab2cebdb303f8cf6d101e1ae55e07b7aabe25cb86b05db91217d151`

## Decision

D1. C5v2 passes the uncounted resource preflight. Preserve C5v1 as a retained
rejection and advance only C5v2 to the frozen 15-block admission curve.

D2. Keep the aligned primary index and two-object gather. It reduced maximum
point bytes to 0.273x C0 and overlapped all 256 provider pairs.

D3. Keep projection-only DataFusion scans. The new proof-bearing frame format
still delivered 31.692x C0 scan throughput with zero payload access.

Optimizes for: one authenticated columnar object base that supports bounded
point reads and material projected-scan leverage.

Gives up: claiming admission from a single preflight sample. The next run must
execute all 15 paired point blocks and 15 paired scan ratios with OTel collector
confirmation.
