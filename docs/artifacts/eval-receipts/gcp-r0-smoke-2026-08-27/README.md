# GCP R0 smoke receipt, 2026-08-27

Status: `[VERIFIED]` the private runner, GCS storage-layout path, required OTel
signals, result schema, machine identity, and durable receipt path completed in
one bounded diagnostic run. `[EVALUATING]` performance and clean-source
comparability.

## Result

```text
run:                 367e1c06-3bc0-418c-8b2c-1c27487acb5d
batch:               r0-gcs-01-smoke
suite:               storage-layout-gcs-smoke-v1
suite file sha256:   3696712af6488d1cb1e612d26e8345de83596b48d2ef5eceb5cc80597313ab02
suite receipt hash:  cb5ad40ee959b4914c0a64ad3d42a498dbf88c364bb0d7642db0e1a5ce55c14b
machine receipt:     5b4414c0c9c7596a1a748ab089cddaba5489a8e281bc3046ddaf678fd2e32583
eval binary sha256:  24e2c18045045fc57f065acb28321eb5988cacf5b42620020f08b50234cc47b6
result sha256:       30c3ee2ac7d87795c9ba314f95e8ff8019131d23bc27b3d15a1d371fc0a4bd6f
OTel archive sha256: 289cd13ebd1935ab6da8e816df7f8126d7d55698e40163c36cb32670d72533f8
source identity:     ec9afad0c6147e080f9b3ec35d09476ea7cdc335+dirty
wall time:           6.924945801 seconds
budget:              120 seconds
hard gates:          24 pass, 0 fail
GCS objects:         13
GCS bytes:           852,280
OTel correlation:    1 trace line, 1 metric line, 2 log lines
lease teardown:      9 resources destroyed, 0 benchmark resources remain
```

The result verdict is `inconclusive` because this was an explicitly allowed
dirty-source diagnostic. The throughput and ratio fields prove metric plumbing,
not a performance result. No candidate-versus-control claim is admitted from
this receipt.

Durable evidence:

```text
gs://doss-objectkv-dev-okv-evals/runs/r0-gcs-01/receipts/machine-5b4414c0c9c7596a1a748ab089cddaba5489a8e281bc3046ddaf678fd2e32583.json
gs://doss-objectkv-dev-okv-evals/runs/r0-gcs-01/receipts/layout-smoke.json
gs://doss-objectkv-dev-okv-evals/runs/r0-gcs-01/telemetry/otel-jsonl.tgz
```

## Failures found before the passing smoke

| Failure | Direct observation | Owning correction |
|---|---|---|
| IAP API absent | private SSH path could not start | declare `iap.googleapis.com` in the project foundation |
| Local key unusable unattended | SSH server accepted the RSA key, but the encrypted private key required an unavailable prompt | optional lease-scoped unencrypted ED25519 operator key |
| Collector files unwritable | collector image runs as UID/GID `10001:10001`, while the mounted directory was root-owned | install the OTel directory with the runtime UID/GID |
| OTLP blocked on the host | VPC firewall allowed traffic, COS host iptables did not | install a source-CIDR host rule for ports 4317, 4318, and 13133 |
| Machine parser used stale fields | current `gcloud storage buckets describe` keys did not match the receipt parser | accept current and prior storage-class and versioning fields |
| Eval errors hid the cause | orchestration reported only a missing primary metric | surface the workload error that occurred before metric recording |
| Runner receipts unwritable | GCS execution passed, then result persistence failed with `EACCES` | create eval, receipt, and scratch directories as `objectkv:objectkv` |

The first attempted admission batch also exceeded its 1,800 second budget after
entering only 13 of 30 sample namespaces. It was terminated and is not a result.
This establishes the required order:

```text
one tiny smoke
  -> repeat smoke from a clean bundle
  -> one-variable scale calibration
  -> five-sample performance admission
  -> second batch for drift
```
