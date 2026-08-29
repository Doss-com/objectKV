# RFC-0044 persisted GCS fixture preflight, GCP R0, 2026-08-28

Status: `[VERIFIED]`

This evidence set verifies RFC-0044 phase 4. One 64 MiB logical fixture was
published to regional GCS, then native, direct control, direct control, and
native fresh worker processes reconstructed the same logical image. The first
subject established the fixture. The next three opened the exact persisted
descriptor instead of regenerating the base.

```text
64 MiB generated base
        │
        ▼
20 immutable GCS objects, 68,857,626 bytes
        │
        ├─ native worker A, establish fixture
        ├─ direct worker B, exact descriptor reopen
        ├─ direct worker B, exact descriptor reopen
        └─ native worker A, exact descriptor reopen
```

## Authority

| Field | Value |
|---|---|
| Source | `6f812ddf3d261d30cc9698b6baed3f97876ace45` |
| Suite | `object-fixture-gcs-preflight-v1` |
| Suite hash | `1c15f35b9089a21b491ca83e73680cef15f4e1285b954a23af6ba2358f0213ac` |
| Candidate run | `ffe07640-13e7-4631-bd8d-9ff64aef133d` |
| Poison run | `84674b28-5292-4c39-a854-da629fbce642` |
| Binary SHA-256 | `2543c70b2e558c691bb67fb90df94e3901d1ccf056f1e5be18d8482265217281` |
| Machine receipt SHA-256 | `aaa96fc8f710c11f5a0f947afa619fc7ba8438c604d08b8dc0cbbb8df6e28f2f` |
| Runner | GCP `c3-highcpu-22`, Debian 12, 100 GiB `pd-balanced` |
| Object store | `doss-objectkv-dev-okv-evals`, `US-CENTRAL1`, versioned |
| Network during receipt and runs | no public IP, IAP operator path, Private Google Access |
| OTel collector | `otelcol-contrib` 0.157.0, co-located for this bounded preflight |

## Candidate result

The clean release candidate returned `keep` with 19 of 19 hard gates passing.

| Measurement | Observed | Gate |
|---|---:|---:|
| Total workload time | 55.906526 s | at most 300 s |
| Maximum fixture setup time | 11.696264 s | at most 300 s |
| Median fixture setup time | 4.845308 s | observed only |
| Exact persisted descriptor reopens | 3 | at least 3 |
| Maximum transaction-authority bytes | 108,918 B | ratio gate below |
| Transaction-authority scratch ratio | 0.001623x | at most 0.25x |
| Logical fixture | 67,108,864 B | frozen input |
| Persisted fixture | 68,857,626 B across 20 objects | observed only |
| Fixture response bytes across four subjects | 413,165,382 B | observed setup cost |
| Measured-window object requests | 0 | exactly 0 |

Every authority retained one empty anchor at `O=2`, one anchor record, zero
anchor mutations, zero base-value txLog records, and zero base-value txLog
mutation bytes. All four subject reports carry fixture and tail identity
`634db09bd09fe95129f7ca45b8d9d75fc3e41ec062f8c31dda3612c72f0c14b8`
and one equal complete resident logical digest.

## Negative control

The reuse-bypass control deliberately regenerated the second 4 MiB subject
instead of opening the persisted descriptor. It observed two exact reopens,
below the required three, and the poison oracle returned `keep`. Its maximum
setup time was 3.286295 seconds, transaction-authority scratch was 108,912
bytes, and all 19 control gates passed.

## Telemetry and files

Both run IDs occur in traces, metrics, and logs. The candidate and poison each
occur once in the trace and metric export and twice in the log export. The
captured files are:

- `candidate-64m.json` and `candidate-64m.stderr.jsonl`
- `reuse-poison.json` and `reuse-poison.stderr.jsonl`
- `machine.json`
- `traces.jsonl`, `metrics.jsonl`, and `logs.jsonl`

The duplicate `*.stdout.json` files are the evaluator's standard-output copies
of the schema-validated result. They are retained so the executed process
stream can be compared byte-for-byte with the named receipt.

The durable provider locations are:

```text
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-gcs-preflight-r0-20260828/candidate-6f812dd/
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-gcs-preflight-r0-20260828/reuse-poison-6f812dd/
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-gcs-preflight-r0-20260828/receipts/
```

## Claim boundary

This is `[VERIFIED]` mechanism and setup evidence. It is one seed, one repeat,
one GCP machine, a co-located collector, and a 64 MiB fixture. It does not admit
T27 throughput, p99, CPU/read, cache coverage, skew, cold-read, three-machine
commit, or economics claims. The result removes the fixture-construction
blocker for freezing the 1 GiB native versus direct-control T27 curve.

The evaluator recorded `rustc` as `unknown` because the non-login run did not
include the Rust toolchain directory on `PATH`; the release binary's exact
SHA-256 and source revision are recorded. Future cloud controllers must export
the compiler path before formal execution.

Cloud cleanup is `[VERIFIED]`. The receipt prefix was uploaded first. The exact
VM, auto-delete boot disk, firewall rule, and temporary SSH key were then
deleted and verified absent. The immutable fixture and receipt prefixes remain
in versioned GCS as evidence.
