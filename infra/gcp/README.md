# objectKV GCP playground

Status: `[EXISTS]` project, bucket, isolated network, keyless runner identity,
and the first in-region provider-bound GCS receipt. `[ACTIVE-WORK]` remote
Terraform state and repeatable ephemeral-runner provisioning.

The project display name is `objectKV-dev`. The project ID is
`doss-objectkv-dev`, and the eval bucket is
`doss-objectkv-dev-okv-evals` in `us-central1`.

## Provisioned boundary

- one organization-owned, billing-linked development project;
- one single-region private GCS bucket for comparable database evals;
- uniform bucket-level IAM, public access prevention, versioning, and seven-day
  soft delete;
- deletion protection on the project and bucket;
- one keyless eval-runner service account with bucket-scoped object access plus
  OTel export roles for Cloud Monitoring, Logging, and Trace;
- one custom `10.41.0.0/24` subnet with Private Google Access and no default
  network;
- scratch-prefix cleanup after 30 days, without force-destroy behavior.

No service-account keys or credentials are created or stored in the repository.

## First run receipt

Candidate `257fe2a` ran the frozen provider-bound suite on an ephemeral
`n2-standard-8` VM in `us-central1-a`. Empty-cache first-point latency was 48.6
ms median and 53.4 ms maximum across five seeds. Persistent-NVMe first-point
latency was 294.5 us median with zero serving-path GCS reads. All six identity
controls discarded, OTel exported metrics, traces, and logs, and the final live
bucket listing was empty.

Versioning and seven-day soft delete retained 218 deleted or noncurrent object
generations totaling 1,464,840,385 bytes after the matrix and controls. This is
retained storage, even though no live scratch name remains. A separate
short-retention scratch bucket is `[PROPOSED]` before frequent scheduled runs.

The temporary VM, boot disk, IAP-only SSH firewall, project SSH metadata, local
SSH key, Cargo target, and Terraform download cache were removed after the run.
The project, bucket, network, service account, and Terraform state remain.

## Guarded apply sequence

```bash
gcloud auth login
gcloud organizations list
gcloud billing accounts list
gcloud projects describe doss-objectkv-dev

cp infra/gcp/terraform.tfvars.example infra/gcp/terraform.tfvars
terraform -chdir=infra/gcp init
terraform -chdir=infra/gcp plan -out=objectkv-dev.tfplan
terraform -chdir=infra/gcp apply objectkv-dev.tfplan
```

Before applying in another environment, replace the organization and billing
IDs with observed values. Do not infer ownership from the display name.

After apply:

```bash
export OKV_GCP_PROJECT="$(terraform -chdir=infra/gcp output -raw project_id)"
export OKV_GCS_BUCKET="$(terraform -chdir=infra/gcp output -raw eval_bucket)"
gcloud auth application-default login

cargo run -p okv-object -- --backend gcs --profile authority
cargo run -p okv-eval -- run evals/suites/object-store.toml \
  --profile gcs-authority \
  --workload named-object-authority-contract \
  --backend gcs

cargo run --release -p okv-eval -- run \
  evals/suites/cell-range-cache-eviction-process.toml \
  --profile gcs-dev \
  --workload range-cache-eviction-correct \
  --backend gcs-process+authority-bound-slatedb+shared-bounded-cache
```

The eviction worker creates a unique `scratch/range-cache-eviction/` prefix per
process and deletes every object before returning a passing GCS receipt. Remote
scratch cleanup is a hard gate.

The command substitutions above read Terraform outputs only. They do not expose
credentials.

## Tradeoff

D1: keep the first playground single-region and intentionally small. This
optimizes for repeatable object-request, byte, latency, and recovery curves. It
gives up multi-region evidence until the single-region durability and economics
gates are credible.

D2: use local Terraform state for the first guarded apply by one operator. This
optimizes for getting the isolated experiment project online. It gives up safe
multi-operator changes, so remote state is mandatory before a second operator
applies infrastructure.
