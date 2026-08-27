# objectKV GCP playground

Status: `[CODE-COMPLETE]` for the reviewed Terraform boundary. `[EVALUATING]`
for live use: project `doss-objectkv-dev` and versioned single-region bucket
`doss-objectkv-dev-okv-evals` were observed through GCP APIs on 2026-08-26, and
the first namespaced GCS cache-admission canary completed. Object-authority
conformance, clean-source repetition, and required OTel export remain open.

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

The separate `infra/gcp/benchmark` root owns the zero-by-default R0 runner,
collector, disks, subnet, firewall, and NAT resources. It uses a distinct GCS
state prefix so the previously created project and bucket are never inferred
into an empty local state. See `docs/REAL-INFRA-EVALS.md` for the benchmark and
failure-mode contract.

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

## Three-machine transaction gate

Status: `[CODE-COMPLETE]` the controller boundary and lifecycle-hook contract
are implemented. `[EVALUATING]` no GCP machine receipt exists because compute,
network, disk, binary-delivery, controller, and required OTel resources have not
been provisioned. Project and bucket access are working.

The G5.2 topology is three data machines in distinct zones plus one controller
machine. Each data machine uses its own persistent SSD root. The controller is
outside all three data-machine identities and runs the exact G0.4 transaction
history and independent oracle. Execution is intentionally deferred until the
resident, cold-object, empty-worker recovery, and branch-leverage gates clear
their controls.

```text
controller
  -> node 1, zone A, persistent SSD
  -> node 2, zone B, persistent SSD
  -> node 3, zone C, persistent SSD
  -> frozen history + independent oracle
```

The repository does not claim that the current Terraform provisions these
machines. Compute resources, machine types, disks, network policy, binary
delivery, and TTL cleanup must be present and recorded before running the gate.
The lifecycle hook assumes:

- node instances named `objectkv-node-1` through `objectkv-node-3`;
- private connectivity from the controller through `gcloud compute ssh
  --internal-ip`;
- `/opt/objectkv/okv-eval` installed with one recorded SHA-256 digest on every
  data machine;
- `jq`, `gcloud`, `systemd-run`, and OS Login available to the controller;
- data roots restricted to `/var/lib/objectkv/evals/`;
- the controller service account can get, start, and stop only the eval node
  instances.

Generate a runtime configuration from provider-observed instance IDs and IPs,
starting with `transaction-machine-config.example.json`. Install
`okv-transaction-machine-hook.sh` at the absolute path named by the config.
Then run:

```bash
export OKV_TRANSACTION_MACHINE_CONFIG=/opt/objectkv/transaction-machine-config.json
export OKV_EVAL_ARTIFACT_DIR=/var/lib/objectkv/eval-receipts/g5-2

okv-eval run evals/suites/serializability-machines.toml \
  --profile gcp-three-zone \
  --workload openraft-machine-transaction-serializability \
  --backend gcp-three-zone-pd-ssd+openraft
```

Run the two poison workloads only after the correct subject completes. The
machine runner invokes `prepare`, `start`, `kill`, and `cleanup` with a bounded
timeout. `kill` stops the accepting VM, so the result cannot pass by killing
only a client connection or a worker on the controller.
