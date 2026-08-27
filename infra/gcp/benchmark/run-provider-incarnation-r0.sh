#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <run-label> <run-id> <new-output-directory>" >&2
  exit 64
fi

run_label="$1"
run_id="$2"
output_root="$3"
project="${OKV_GCP_PROJECT:-doss-objectkv-dev}"
zone="${OKV_GCP_ZONE:-us-central1-a}"
bucket="${OKV_GCS_BUCKET:-doss-objectkv-dev-okv-evals}"
ssh_user="${OKV_GCP_SSH_USER:-}"
ssh_key_file="${OKV_GCP_SSH_KEY_FILE:-}"
eval_binary="${OKV_EVAL_BIN:-}"
seed="${OKV_PROVIDER_INCARNATION_SEED:-2026082704}"
record_count="${OKV_PROVIDER_INCARNATION_RECORDS:-1000}"

if [[ ! "${run_label}" =~ ^[a-z][a-z0-9-]{0,19}$ ]]; then
  echo "run label must be 1 to 20 lowercase letters, digits, or hyphens" >&2
  exit 64
fi
if [[ ! "${run_id}" =~ ^[a-zA-Z0-9-]{1,64}$ ]]; then
  echo "run ID must contain only letters, digits, and hyphens" >&2
  exit 64
fi
if [[ "${output_root}" != /* ]]; then
  echo "output directory must be an absolute path" >&2
  exit 64
fi
if [[ -e "${output_root}" ]]; then
  echo "output directory already exists: ${output_root}" >&2
  exit 73
fi
if [[ -z "${eval_binary}" || ! -x "${eval_binary}" ]]; then
  echo "OKV_EVAL_BIN must name the executable built from the clean candidate" >&2
  exit 66
fi
if [[ -z "${OTEL_EXPORTER_OTLP_ENDPOINT:-}" ]]; then
  echo "OTEL_EXPORTER_OTLP_ENDPOINT is required for formal receipts" >&2
  exit 66
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${repo_root}"
source_instance="objectkv-bench-${run_label}-source"
destination_instance="objectkv-bench-${run_label}-restore"
install -d "${output_root}"

ssh_args=(--project="${project}" --zone="${zone}" --tunnel-through-iap --quiet)
scp_args=(--project="${project}" --zone="${zone}" --tunnel-through-iap --quiet)
if [[ -n "${ssh_key_file}" ]]; then
  ssh_args+=(--ssh-key-file="${ssh_key_file}")
  scp_args+=(--ssh-key-file="${ssh_key_file}")
fi

target() {
  local instance="$1"
  if [[ -n "${ssh_user}" ]]; then
    printf '%s@%s' "${ssh_user}" "${instance}"
  else
    printf '%s' "${instance}"
  fi
}

remote_exec() {
  local instance="$1"
  local command="$2"
  gcloud compute ssh "$(target "${instance}")" "${ssh_args[@]}" --command="${command}"
}

copy_to() {
  local instance="$1"
  local destination="$2"
  shift 2
  gcloud compute scp "$@" "$(target "${instance}"):${destination}" "${scp_args[@]}"
}

copy_from() {
  local instance="$1"
  local source="$2"
  local destination="$3"
  gcloud compute scp "$(target "${instance}"):${source}" "${destination}" "${scp_args[@]}"
}

for instance in "${source_instance}" "${destination_instance}"; do
  remote_exec "${instance}" "test -f /var/lib/objectkv/runner-ready.json"
  copy_to "${instance}" /tmp/ \
    "${repo_root}/experiments/provider-plane/configure-foundationdb-r0.sh" \
    "${repo_root}/experiments/provider-plane/foundationdb_lifecycle_r0.py" \
    "${repo_root}/experiments/provider-plane/foundationdb_media_loss_r0.py" \
    "${repo_root}/experiments/provider-plane/foundationdb_incarnation_r0.py"
done

remote_exec "${source_instance}" \
  "sudo bash /tmp/configure-foundationdb-r0.sh '${run_label}' source"
remote_exec "${destination_instance}" \
  "sudo bash /tmp/configure-foundationdb-r0.sh '${run_label}' restore"

"${repo_root}/infra/gcp/benchmark/provider-media-topology.sh" \
  capture "${run_label}" source "${output_root}/source-identity-before.json"
"${repo_root}/infra/gcp/benchmark/provider-media-topology.sh" \
  capture "${run_label}" restore "${output_root}/destination-identity.json"

provider_python=/opt/objectkv/provider-venv/bin/python
provider_env="PYTHONPATH=/tmp"
receipt_root=/var/lib/objectkv/receipts
remote_exec "${source_instance}" \
  "sudo env '${provider_env}' '${provider_python}' /tmp/foundationdb_incarnation_r0.py source --run-id '${run_id}' --bucket '${bucket}' --record-count '${record_count}' --output '${receipt_root}/source-phase.json'"
copy_from "${source_instance}" "${receipt_root}/source-phase.json" \
  "${output_root}/source-phase.json"

copy_to "${destination_instance}" /tmp/ "${output_root}/source-phase.json"
remote_exec "${destination_instance}" \
  "sudo env '${provider_env}' '${provider_python}' /tmp/foundationdb_incarnation_r0.py restore --run-id '${run_id}' --bucket '${bucket}' --source-receipt /tmp/source-phase.json --output '${receipt_root}/restore-phase.json'"
copy_from "${destination_instance}" "${receipt_root}/restore-phase.json" \
  "${output_root}/restore-phase.json"

"${eval_binary}" provider-incarnation-trace --seed "${seed}" --mode correct \
  >"${output_root}/authority-positive.json"
"${eval_binary}" provider-incarnation-trace --seed "${seed}" \
  --mode accept_stale_source_incarnation \
  >"${output_root}/authority-poison.json"
jq -e '.anomaly_count == 0 and .mode == "correct"' \
  "${output_root}/authority-positive.json" >/dev/null
jq -e '.anomaly_count >= 3 and .mode == "accept_stale_source_incarnation"' \
  "${output_root}/authority-poison.json" >/dev/null

remote_exec "${source_instance}" \
  "sudo env '${provider_env}' '${provider_python}' /tmp/foundationdb_incarnation_r0.py fence --run-id '${run_id}' --source-receipt '${receipt_root}/source-phase.json' --output '${receipt_root}/fence-phase.json'"
copy_from "${source_instance}" "${receipt_root}/fence-phase.json" \
  "${output_root}/fence-phase.json"

copy_to "${destination_instance}" /tmp/ \
  "${output_root}/fence-phase.json" \
  "${output_root}/authority-positive.json"
remote_exec "${destination_instance}" \
  "sudo env '${provider_env}' '${provider_python}' /tmp/foundationdb_incarnation_r0.py activate --run-id '${run_id}' --bucket '${bucket}' --source-receipt /tmp/source-phase.json --restore-receipt '${receipt_root}/restore-phase.json' --fence-receipt /tmp/fence-phase.json --authority-report /tmp/authority-positive.json --output '${receipt_root}/activation-phase.json'"
copy_from "${destination_instance}" "${receipt_root}/activation-phase.json" \
  "${output_root}/activation-phase.json"

"${repo_root}/infra/gcp/benchmark/provider-media-topology.sh" \
  restart-source "${run_label}" "${output_root}/restart-observation.json"
remote_exec "${source_instance}" \
  "for attempt in \$(seq 1 60); do sudo fdbcli -C /etc/foundationdb/fdb.cluster --exec 'status minimal' >/dev/null 2>&1 && exit 0; sleep 1; done; exit 1"
"${repo_root}/infra/gcp/benchmark/provider-media-topology.sh" \
  capture "${run_label}" source "${output_root}/source-identity-after.json"

copy_to "${source_instance}" /tmp/ \
  "${output_root}/activation-phase.json" \
  "${output_root}/restart-observation.json"
remote_exec "${source_instance}" \
  "sudo env '${provider_env}' '${provider_python}' /tmp/foundationdb_incarnation_r0.py resurrect --run-id '${run_id}' --source-receipt '${receipt_root}/source-phase.json' --fence-receipt '${receipt_root}/fence-phase.json' --activation-receipt /tmp/activation-phase.json --restart-observation /tmp/restart-observation.json --output '${receipt_root}/resurrection-phase.json'"
copy_from "${source_instance}" "${receipt_root}/resurrection-phase.json" \
  "${output_root}/resurrection-phase.json"

remote_exec "${source_instance}" "sudo install -d -m 0755 /tmp/gp254-assemble"
copy_to "${source_instance}" /tmp/gp254-assemble/ \
  "${output_root}/source-phase.json" \
  "${output_root}/restore-phase.json" \
  "${output_root}/fence-phase.json" \
  "${output_root}/activation-phase.json" \
  "${output_root}/resurrection-phase.json" \
  "${output_root}/authority-positive.json" \
  "${output_root}/authority-poison.json" \
  "${output_root}/source-identity-before.json" \
  "${output_root}/source-identity-after.json" \
  "${output_root}/destination-identity.json" \
  "${output_root}/restart-observation.json"

assemble=/tmp/gp254-assemble
remote_exec "${source_instance}" \
  "sudo env '${provider_env}' '${provider_python}' /tmp/foundationdb_incarnation_r0.py assemble-positive --run-id '${run_id}' --source-receipt '${assemble}/source-phase.json' --restore-receipt '${assemble}/restore-phase.json' --fence-receipt '${assemble}/fence-phase.json' --activation-receipt '${assemble}/activation-phase.json' --resurrection-receipt '${assemble}/resurrection-phase.json' --authority-report '${assemble}/authority-positive.json' --source-identity-before '${assemble}/source-identity-before.json' --source-identity-after '${assemble}/source-identity-after.json' --destination-identity '${assemble}/destination-identity.json' --restart-observation '${assemble}/restart-observation.json' --output '${receipt_root}/provider-incarnation-positive.json'"
copy_from "${source_instance}" "${receipt_root}/provider-incarnation-positive.json" \
  "${output_root}/provider-incarnation-positive.json"

set +e
remote_exec "${source_instance}" \
  "sudo env '${provider_env}' '${provider_python}' /tmp/foundationdb_incarnation_r0.py assemble-poison --run-id '${run_id}' --source-receipt '${assemble}/source-phase.json' --source-identity '${assemble}/source-identity-before.json' --authority-report '${assemble}/authority-poison.json' --output '${receipt_root}/provider-incarnation-poison.json'"
poison_status=$?
set -e
if [[ "${poison_status}" -ne 1 ]]; then
  echo "provider-incarnation poison assembler returned ${poison_status}, expected 1" >&2
  exit 1
fi
copy_from "${source_instance}" "${receipt_root}/provider-incarnation-poison.json" \
  "${output_root}/provider-incarnation-poison.json"

export OKV_PROVIDER_INCARNATION_RECEIPT="${output_root}/provider-incarnation-positive.json"
"${eval_binary}" run "${repo_root}/evals/suites/provider-incarnation-r0.toml" \
  --profile gcp-r0-foundationdb-incarnation \
  --workload foundationdb-provider-incarnation \
  --backend foundationdb-7.4.6+external-incarnation-authority+gcs \
  --batch-id gp254-r0-positive \
  --output "${output_root}/eval-positive.json"

export OKV_PROVIDER_INCARNATION_POISON_RECEIPT="${output_root}/provider-incarnation-poison.json"
"${eval_binary}" run "${repo_root}/evals/suites/provider-incarnation-r0.toml" \
  --profile gcp-r0-foundationdb-incarnation \
  --workload foundationdb-provider-incarnation-stale-source-poison \
  --backend foundationdb-7.4.6+external-incarnation-authority+gcs \
  --batch-id gp254-r0-poison \
  --output "${output_root}/eval-poison.json"

jq -e '.verdict == "keep" and .primary_metric.value == 0' \
  "${output_root}/eval-positive.json" >/dev/null
jq -e '.verdict == "discard" and .primary_metric.value >= 3' \
  "${output_root}/eval-poison.json" >/dev/null
sha256sum "${output_root}"/*.json >"${output_root}/SHA256SUMS"
printf 'provider-incarnation R0 complete: %s\n' "${output_root}"
