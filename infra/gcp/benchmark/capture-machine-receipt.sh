#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 <run-label> <output.json> [standard|source|restore]" >&2
  exit 64
fi

run_label="$1"
output="$2"
runner_phase="${3:-standard}"
if [[ "${runner_phase}" != "standard" && "${runner_phase}" != "source" && "${runner_phase}" != "restore" ]]; then
  echo "runner phase must be standard, source, or restore" >&2
  exit 64
fi
project="${OKV_GCP_PROJECT:-doss-objectkv-dev}"
region="${OKV_GCP_REGION:-us-central1}"
zone="${OKV_GCP_ZONE:-us-central1-a}"
bucket="${OKV_GCS_BUCKET:-doss-objectkv-dev-okv-evals}"
ssh_user="${OKV_GCP_SSH_USER:-}"
ssh_key_file="${OKV_GCP_SSH_KEY_FILE:-}"
runner_suffix="runner"
runner_disk_suffix="data"
if [[ "${runner_phase}" != "standard" ]]; then
  runner_suffix="${runner_phase}"
  runner_disk_suffix="${runner_phase}-data"
fi
runner="objectkv-bench-${run_label}-${runner_suffix}"
collector="objectkv-bench-${run_label}-collector"
runner_target="${runner}"
collector_target="${collector}"
if [[ -n "${ssh_user}" ]]; then
  runner_target="${ssh_user}@${runner}"
  collector_target="${ssh_user}@${collector}"
fi
ssh_args=(--project="${project}" --zone="${zone}" --tunnel-through-iap --quiet)
if [[ -n "${ssh_key_file}" ]]; then
  ssh_args+=(--ssh-key-file="${ssh_key_file}")
fi

runner_json="$(gcloud compute instances describe "${runner}" --project="${project}" --zone="${zone}" --format=json)"
collector_json="$(gcloud compute instances describe "${collector}" --project="${project}" --zone="${zone}" --format=json)"
runner_boot_json="$(gcloud compute disks describe "${runner}" --project="${project}" --zone="${zone}" --format=json)"
collector_boot_json="$(gcloud compute disks describe "${collector}" --project="${project}" --zone="${zone}" --format=json)"
runner_disk_json="$(gcloud compute disks describe "objectkv-bench-${run_label}-${runner_disk_suffix}" --project="${project}" --zone="${zone}" --format=json)"
collector_disk_json="$(gcloud compute disks describe "objectkv-bench-${run_label}-otel" --project="${project}" --zone="${zone}" --format=json)"
bucket_json="$(gcloud storage buckets describe "gs://${bucket}" --project="${project}" --format=json)"

source_revision="$(jq -r '.metadata.items[] | select(.key == "objectkv-revision") | .value' <<<"${runner_json}")"
lease_expires="$(jq -r '.metadata.items[] | select(.key == "objectkv-lease-expires") | .value' <<<"${runner_json}")"
runner_ready_document="$(gcloud compute ssh "${runner_target}" "${ssh_args[@]}" --command='cat /var/lib/objectkv/runner-ready.json' 2>/dev/null || printf '{}')"
runner_ready="$(jq -e '.status == "ready" and (.hot_bytes // 0) > 0 and (.hot_interface // "") == "nvme"' <<<"${runner_ready_document}" >/dev/null 2>&1 && printf true || printf false)"
binary_sha256="$(gcloud compute ssh "${runner_target}" "${ssh_args[@]}" --command='sha256sum /opt/objectkv/bin/okv-eval 2>/dev/null | cut -d " " -f 1' 2>/dev/null || true)"
runner_runtime="$(gcloud compute ssh "${runner_target}" "${ssh_args[@]}" --command='uname -srvmo' 2>/dev/null || true)"
collector_runtime="$(gcloud compute ssh "${collector_target}" "${ssh_args[@]}" --command='sudo docker inspect objectkv-otel --format={{.Image}} 2>/dev/null' 2>/dev/null || true)"
collector_ip="$(jq -r '.networkInterfaces[0].networkIP' <<<"${collector_json}")"
collector_healthy="$(gcloud compute ssh "${runner_target}" "${ssh_args[@]}" --command="timeout 5 bash -c '</dev/tcp/${collector_ip}/13133'" >/dev/null 2>&1 && printf true || printf false)"

jq -n \
  --arg captured_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg run_label "${run_label}" \
  --arg project "${project}" \
  --arg region "${region}" \
  --arg zone "${zone}" \
  --arg source_revision "${source_revision}" \
  --arg bucket_name "${bucket}" \
  --argjson bucket "${bucket_json}" \
  --argjson runner "${runner_json}" \
  --argjson collector "${collector_json}" \
  --argjson runner_boot "${runner_boot_json}" \
  --argjson collector_boot "${collector_boot_json}" \
  --argjson runner_disk "${runner_disk_json}" \
  --argjson collector_disk "${collector_disk_json}" \
  --argjson runner_ready_document "${runner_ready_document}" \
  --arg lease_expires "${lease_expires}" \
  --arg binary_sha256 "${binary_sha256}" \
  --arg runner_runtime "${runner_runtime}" \
  --arg collector_runtime "${collector_runtime}" \
  --argjson runner_ready "${runner_ready}" \
  --argjson collector_healthy "${collector_healthy}" \
  '
    def tail: split("/")[-1];
    def public_ip: .networkInterfaces[0].accessConfigs[0].natIP // null;
    def machine($instance; $boot; $disk; $runtime; $binary; $hot): {
      name: $instance.name,
      instance_id: $instance.id,
      machine_type: ($instance.machineType | tail),
      cpu_platform: $instance.cpuPlatform,
      image: $boot.sourceImage,
      internal_ip: $instance.networkInterfaces[0].networkIP,
      public_ip: ($instance | public_ip),
      service_account: $instance.serviceAccounts[0].email,
      data_disk_type: ($disk.type | tail),
      data_disk_gib: ($disk.sizeGb | tonumber),
      lease_expires_epoch: $lease_expires,
      binary_sha256: (if $binary == "" then null else $binary end),
      runtime_identity: (if $runtime == "" then null else $runtime end),
      hot_scratch: $hot
    };
    {
      schema_version: 1,
      captured_at: $captured_at,
      run_label: $run_label,
      project: $project,
      region: $region,
      zone: $zone,
      source_revision: $source_revision,
      bucket: {
        name: $bucket_name,
        location: $bucket.location,
        storage_class: ($bucket.storageClass // $bucket.default_storage_class),
        versioning_enabled: ($bucket.versioning.enabled // $bucket.versioning_enabled // false)
      },
      runner: machine(
        $runner;
        $runner_boot;
        $runner_disk;
        $runner_runtime;
        $binary_sha256;
        (if $runner_ready_document.status == "ready" then {
          kind: "gcp-local-ssd",
          device: $runner_ready_document.hot_device,
          interface: $runner_ready_document.hot_interface,
          filesystem: $runner_ready_document.hot_filesystem,
          mount: $runner_ready_document.hot_mount,
          bytes: $runner_ready_document.hot_bytes
        } else null end)
      ),
      collector: machine($collector; $collector_boot; $collector_disk; $collector_runtime; ""; null),
      checks: {
        runner_ready: $runner_ready,
        binary_sha256_recorded: ($binary_sha256 | length == 64),
        collector_healthy: $collector_healthy,
        no_public_ips: (($runner | public_ip) == null and ($collector | public_ip) == null)
      }
    }
  ' >"${output}"

jq empty "${output}"
