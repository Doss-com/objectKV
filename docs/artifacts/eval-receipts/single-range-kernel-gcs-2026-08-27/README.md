# SingleRange GCS smoke receipt, 2026-08-27

Status: `[EVALUATING]` the public `okv::SingleRange` object-base plus txLog-tail
path completed against real GCS with required OTel signals. The source was
dirty, compute and all six authority processes shared one local machine, and
the suite has one sample. This is a correctness and plumbing receipt, not a
competitive performance result.

## Result

```text
run:                    6723ce8a-ea92-48d6-8020-60f12575fcb8
batch:                  single-range-gcs-smoke-20260827-a
suite:                  single-range-kernel-gcs-smoke-v1
suite file sha256:      4e71213e1f4def23c0e605f20a5a761341b50b2176237855b77e1e9524e22562
suite receipt hash:     ad4e54fc0631970b5836d8e875250083b9ea6c885abd06f6f94e9e8fa08d6c73
profile hash:           ce31e2412401d6181e14f6dbc358d35f25444d726ddf91654a05fa0ae3a04e63
eval binary sha256:     6c5863ece797a7d0523c02e1476da9fa0c69a5274beab82ac4530ad014660ed3
stored result sha256:   d9a376d2fbdadba1d5c66fdf322b7b77088883bb91384ff5b8d658dc106620ed
source identity:        a56442ad800deedd72a404a0886e88831eb308a0+dirty
hard gates:             12 pass, 0 fail
wall time:              7.253939042 seconds
first correct read:     0.756949583 seconds
object response bytes:  6,177
txLog response bytes:   4,403
```

The immutable semantic digest was
`80a34828a140c8e2b5922a9df8b5cc9109be4f0ca2f7c2866e1448597af7191f`,
matching the local object-base run.

## Exact path observed

```text
3 publication-authority processes
  + 3 transaction-authority processes
  + public SingleRange commit
  + killed first serving worker
  + distinct empty-scratch replacement
  + GCS manifest GET
  + GCS index GET
  + one selected GCS data range GET
  + retained txLog catch-up through concurrent commits
  -> exact values, point clears, insertion, and range clear
```

The replacement issued one manifest request, one index request, one data range
request, zero complete-data requests, and zero LIST requests. The retained
stream required seven requests and five resumes inside shared commit-version
batches.

The run and deterministic replay wrote 14 scratch objects totaling 84,610
bytes below:

```text
gs://doss-objectkv-dev-okv-evals/scratch/single-range-kernel/6723ce8a-ea92-48d6-8020-60f12575fcb8/
```

The bucket lifecycle deletes the `scratch/` prefix after 30 days.

## OTel evidence

The isolated collector received all required signals:

```text
logs:     2 records
traces:   1 span
metrics:  8 metrics, 8 data points
```

Prometheus exposed the exact run, suite, profile, backend, batch, and source
identity. It recorded zero correctness anomalies and a
`recovery.first_correct_read_duration` sum of `0.756949583` seconds.

The failure-closed control omitted `OTEL_EXPORTER_OTLP_ENDPOINT`, exited with
status 1, emitted `profile local-controller-gcs-smoke requires telemetry`, and
created no result receipt.

## Adjacent local control

Local-filesystem run `260a5f44-ec3d-4443-ac03-1592aef6b0fb` passed the same 12
behavioral gates after the backend change. It reached first correct read in
`0.108626542` seconds and completed in `1.713956666` seconds. The GCS run took
`0.756949583` and `7.253939042` seconds respectively.

These values are not a paired performance comparison. The suites and backends
differ, there is one sample each, the machine is an uncontrolled Mac, and the
source is dirty. They only show that remote object latency is visible and must
be isolated into request-level curves on R0.

## Failure found

Ports 4317 and 4318 were already held by other local DOSS collectors. The
objectKV compose boundary now accepts `OKV_OTEL_GRPC_PORT`,
`OKV_OTEL_HTTP_PORT`, and `OKV_OTEL_PROMETHEUS_PORT`, preserving the default
ports while allowing an isolated receipt collector.

## Next admission step

Reproduce this exact suite from one clean digest-addressed bundle on the R0 GCP
runner, add a machine receipt, and collect at least five samples only after a
one-variable size calibration fits the two-minute target. A local-controller
GCS latency number cannot become `[VERIFIED]` performance evidence.
