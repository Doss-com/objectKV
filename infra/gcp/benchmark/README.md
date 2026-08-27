# objectKV benchmark runner v1

Status: `[VERIFIED]` one private-runner GCS smoke and the sequential GP2.5.3
provider-media-loss lifecycle produced machine-bound results with all three
required OTel signals. `[CODE-COMPLETE]` the isolated Terraform root validates
and defaults to zero compute resources. `[EVALUATING]` scale calibration. The
completed leases destroyed all nine managed resources and left no benchmark
compute running.

This root owns the first stable real-infrastructure benchmark environment. It
does not share Terraform state with the existing project and bucket root. State
is stored under `gs://doss-objectkv-dev-okv-evals/terraform/benchmark-runner-v1`.

```text
local operator
  -> IAP SSH
     -> n2-standard-8 benchmark runner + 200 GiB pd-ssd
     -> e2-standard-2 OTel collector + 20 GiB pd-balanced

runner
  -> regional GCS bucket
  -> OTLP/HTTP over private VPC -> collector
```

The runner and collector have private addresses only. The collector is separate
so serialization, batching, and file export do not consume benchmark-runner CPU.
The runner and control execute sequentially on the same machine. This optimizes
for paired-measurement stability. It gives up concurrent solution-stack testing
until the one-runner curves reproduce.

For GP2.5.3, `runner_phase=source` and `runner_phase=restore` replace that runner
sequentially. The phase changes both the VM name and persistent provider-disk
name. Terraform destroys the source identities before creating the restore
identities; the collector and regional GCS objects remain. This keeps only one
data VM active while an external controller can prove the exact source media is
absent. These correctness phases set `enable_local_ssd=false`; local NVMe is an
unrelated serving-path variable and is not provisioned for this gate.

The verified `gp253-r0` execution wrote an exact 950-record closure on the
source phase, replaced the source VM and provider disk, observed the source VM,
boot disk, and data disk absent, then reconstructed the exact digest on a fresh
restore phase. Its formal positive passed 16 gates; the hidden-source-media
control was discarded. After capture, the root destroyed nine managed
resources and returned to an empty Terraform state. Receipts are under
`docs/artifacts/eval-receipts/provider-media-loss-r0-2026-08-27/`.

OS Login is the default operator path. If OS Login is unavailable, an explicit
break-glass key may be passed as `operator_ssh_public_key`. This creates only the
`objectkv` account on the two leased private machines. Set
`OKV_GCP_SSH_USER=objectkv` and `OKV_GCP_SSH_KEY_FILE` when capturing the
receipt. Use a lease-scoped, unencrypted key for unattended runs. Do not use
this fallback for an admitted run without recording the access-path deviation.

## Guarded lifecycle

Do not provision from a dirty tree. The `benchmark_revision` must name the clean
source used to build `/opt/objectkv/bin/okv-eval`.

```bash
terraform -chdir=infra/gcp/benchmark init

terraform -chdir=infra/gcp/benchmark plan \
  -var=create=true \
  -var=run_label=r0-layout-01 \
  -var=benchmark_revision=<clean-git-sha> \
  -var=lease_expires_epoch=<utc-epoch> \
  -out=/private/tmp/objectkv-r0-layout-01.tfplan

terraform -chdir=infra/gcp/benchmark apply \
  /private/tmp/objectkv-r0-layout-01.tfplan
```

The lease is recorded, not automatically enforced. Before applying, assign one
operator to destroy the resources by that epoch. This avoids hidden automation
deleting a live failure experiment, but it gives up automatic cost containment.

Install the exact release binary and record its digest:

```bash
gcloud compute scp /path/to/okv-eval \
  objectkv-bench-r0-layout-01-runner:/tmp/okv-eval \
  --project=doss-objectkv-dev \
  --zone=us-central1-a \
  --tunnel-through-iap

gcloud compute ssh objectkv-bench-r0-layout-01-runner \
  --project=doss-objectkv-dev \
  --zone=us-central1-a \
  --tunnel-through-iap \
  --command='sudo install -m 0755 /tmp/okv-eval /opt/objectkv/bin/okv-eval && sha256sum /opt/objectkv/bin/okv-eval'
```

Capture the machine authority before any comparable run:

```bash
infra/gcp/benchmark/capture-machine-receipt.sh \
  r0-layout-01 \
  /private/tmp/objectkv-r0-layout-01-machine.json
```

All four receipt checks must be true. Upload the receipt beside results, then
destroy through the exact command emitted by `terraform output destroy_command`.

Copy the validated receipt to the runner and bind every cloud result to its
SHA-256 digest:

```bash
gcloud compute scp /private/tmp/objectkv-r0-layout-01-machine.json \
  objectkv-bench-r0-layout-01-runner:/tmp/objectkv-machine.json \
  --project=doss-objectkv-dev \
  --zone=us-central1-a \
  --tunnel-through-iap

gcloud compute ssh objectkv-bench-r0-layout-01-runner \
  --project=doss-objectkv-dev \
  --zone=us-central1-a \
  --tunnel-through-iap \
  --command='sudo install -m 0444 /tmp/objectkv-machine.json /var/lib/objectkv/receipts/machine.json'

export OKV_EVAL_MACHINE_RECEIPT=/var/lib/objectkv/receipts/machine.json
```

## First rung, infrastructure smoke

Start with the bounded diagnostic suite. It uses one seed and one repeat to
prove plumbing and correctness. It does not admit a performance claim.

```bash
export OKV_GCP_PROJECT=doss-objectkv-dev
export OKV_GCS_BUCKET=doss-objectkv-dev-okv-evals
export OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector-private-ip>:4318
export OKV_EVAL_MACHINE_RECEIPT=/var/lib/objectkv/receipts/machine.json

timeout --signal=TERM 150s /opt/objectkv/bin/okv-eval run \
  evals/suites/storage-layout-gcs-smoke.toml \
  --profile objectkv-dev-gcs-smoke \
  --workload split-projection-sidecar-vs-indexed-row-smoke \
  --backend gcs+observed-range-reads \
  --batch-id r0-layout-smoke \
  --output /var/lib/objectkv/receipts/layout-smoke.json
```

Run admission only after a clean smoke and a one-variable calibration stay
inside the declared wall-clock budget.

## Later rung, performance admission

The first admitted profile is the paired GCS storage-layout workload. The suite
now requires OTel and five repeats:

```bash
export OKV_GCP_PROJECT=doss-objectkv-dev
export OKV_GCS_BUCKET=doss-objectkv-dev-okv-evals
export OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector-private-ip>:4318
export OKV_EVAL_MACHINE_RECEIPT=/var/lib/objectkv/receipts/machine.json

/opt/objectkv/bin/okv-eval run \
  evals/suites/storage-layout-gcs-admission.toml \
  --profile objectkv-dev-gcs \
  --workload split-projection-sidecar-vs-indexed-row \
  --backend gcs+observed-range-reads \
  --batch-id r0-layout-01-a \
  --output /var/lib/objectkv/receipts/layout.json
```

The repository is not copied to the runner. The binary, suite, metric registry,
schemas, and contract files must be uploaded as one digest-addressed experiment
bundle. Binary-only installation is enough for readiness, not enough for a
comparable run.
