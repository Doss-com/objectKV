# objectKV GCP playground

Status: `[ACTIVE-WORK]` the reviewed Terraform boundary exists. The Google Cloud
project and bucket do not yet exist because the active `wiley@doss.com` gcloud
session requires interactive reauthentication.

The project display name is `objectKV-dev`. The final project ID is selected only
after checking global availability and DOSS organization conventions. The
example candidate is `doss-objectkv-dev`.

## Provisioned boundary

- one organization-owned, billing-linked development project;
- one single-region private GCS bucket for comparable database evals;
- uniform bucket-level IAM, public access prevention, versioning, and seven-day
  soft delete;
- deletion protection on the project and bucket;
- one keyless eval-runner service account with bucket-scoped object access plus
  OTel export roles for Cloud Monitoring, Logging, and Trace;
- scratch-prefix cleanup after 30 days, without force-destroy behavior.

No service-account keys or credentials are created or stored in the repository.

## Guarded apply sequence

```bash
gcloud auth login wiley@doss.com
gcloud organizations list
gcloud billing accounts list
gcloud projects describe doss-objectkv-dev

cp infra/gcp/terraform.tfvars.example infra/gcp/terraform.tfvars
terraform -chdir=infra/gcp init
terraform -chdir=infra/gcp plan -out=objectkv-dev.tfplan
terraform -chdir=infra/gcp apply objectkv-dev.tfplan
```

Before applying, replace the example organization and billing IDs with values
observed from the reauthenticated account. A failed `projects describe` with
`NOT_FOUND` is the availability check. Do not infer ownership from the display
name.

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
```

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
