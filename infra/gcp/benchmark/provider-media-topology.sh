#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 capture <run-label> <source|restore> <output.json>" >&2
  echo "       $0 observe-loss <source-identity.json> <output.json>" >&2
  echo "       $0 restart-source <run-label> <output.json>" >&2
  exit 64
}

[[ $# -ge 1 ]] || usage
command_name="$1"
shift

project="${OKV_GCP_PROJECT:-doss-objectkv-dev}"
zone="${OKV_GCP_ZONE:-us-central1-a}"
ssh_user="${OKV_GCP_SSH_USER:-}"
ssh_key_file="${OKV_GCP_SSH_KEY_FILE:-}"
cluster_file="${OKV_FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"

capture() {
  [[ $# -eq 3 ]] || usage
  local run_label="$1"
  local phase="$2"
  local output="$3"
  if [[ "${phase}" != "source" && "${phase}" != "restore" ]]; then
    echo "capture phase must be source or restore" >&2
    exit 64
  fi

  local instance="objectkv-bench-${run_label}-${phase}"
  local data_disk="objectkv-bench-${run_label}-${phase}-data"
  local target="${instance}"
  if [[ -n "${ssh_user}" ]]; then
    target="${ssh_user}@${instance}"
  fi
  local ssh_args=(--project="${project}" --zone="${zone}" --tunnel-through-iap --quiet)
  if [[ -n "${ssh_key_file}" ]]; then
    ssh_args+=(--ssh-key-file="${ssh_key_file}")
  fi

  local instance_json
  local data_disk_json
  local boot_disk_name
  local boot_disk_json
  local cluster_text
  instance_json="$(gcloud compute instances describe "${instance}" --project="${project}" --zone="${zone}" --format=json)"
  data_disk_json="$(gcloud compute disks describe "${data_disk}" --project="${project}" --zone="${zone}" --format=json)"
  boot_disk_name="$(jq -r '.disks[] | select(.boot == true) | .source | split("/")[-1]' <<<"${instance_json}")"
  boot_disk_json="$(gcloud compute disks describe "${boot_disk_name}" --project="${project}" --zone="${zone}" --format=json)"
  cluster_text="$(gcloud compute ssh "${target}" "${ssh_args[@]}" --command="sudo cat '${cluster_file}'")"
  cluster_text="${cluster_text//$'\r'/}"
  cluster_text="${cluster_text//$'\n'/}"
  local cluster_id
  local cluster_file_sha256
  cluster_id="${cluster_text#*:}"
  cluster_id="${cluster_id%%@*}"
  if [[ ! "${cluster_id}" =~ ^[0-9a-f]{32}$ ]]; then
    echo "FoundationDB cluster file did not contain a 32-character lowercase cluster ID" >&2
    exit 65
  fi
  cluster_file_sha256="$(printf '%s\n' "${cluster_text}" | sha256sum | cut -d ' ' -f 1)"

  jq -n \
    --arg captured_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg run_label "${run_label}" \
    --arg phase "${phase}" \
    --arg project "${project}" \
    --arg zone "${zone}" \
    --arg cluster_id "${cluster_id}" \
    --arg cluster_file_sha256 "${cluster_file_sha256}" \
    --argjson instance "${instance_json}" \
    --argjson boot_disk "${boot_disk_json}" \
    --argjson data_disk "${data_disk_json}" \
    '
      {
        schema_version: 1,
        kind: "objectkv_provider_media_identity_r0",
        captured_at: $captured_at,
        run_label: $run_label,
        phase: $phase,
        project: $project,
        zone: $zone,
        identity: {
          cluster_id: $cluster_id,
          cluster_file_sha256: $cluster_file_sha256,
          instance_name: $instance.name,
          instance_id: $instance.id,
          boot_disk_name: $boot_disk.name,
          boot_disk_id: $boot_disk.id,
          data_disk_name: $data_disk.name,
          data_disk_id: $data_disk.id
        }
      }
    ' >"${output}"
  jq empty "${output}"
}

observe_loss() {
  [[ $# -eq 2 ]] || usage
  local source_identity="$1"
  local output="$2"
  jq -e '
    .schema_version == 1 and
    .kind == "objectkv_provider_media_identity_r0" and
    .phase == "source"
  ' "${source_identity}" >/dev/null
  local source_project
  local source_zone
  local instance
  local boot_disk
  local data_disk
  source_project="$(jq -r '.project' "${source_identity}")"
  source_zone="$(jq -r '.zone' "${source_identity}")"
  instance="$(jq -r '.identity.instance_name' "${source_identity}")"
  boot_disk="$(jq -r '.identity.boot_disk_name' "${source_identity}")"
  data_disk="$(jq -r '.identity.data_disk_name' "${source_identity}")"
  if [[ "${source_project}" != "${project}" || "${source_zone}" != "${zone}" ]]; then
    echo "source identity project or zone does not match the active controller scope" >&2
    exit 65
  fi

  local instance_absent=true
  local boot_disk_absent=true
  local data_disk_absent=true
  if gcloud compute instances describe "${instance}" --project="${project}" --zone="${zone}" >/dev/null 2>&1; then
    instance_absent=false
  fi
  if gcloud compute disks describe "${boot_disk}" --project="${project}" --zone="${zone}" >/dev/null 2>&1; then
    boot_disk_absent=false
  fi
  if gcloud compute disks describe "${data_disk}" --project="${project}" --zone="${zone}" >/dev/null 2>&1; then
    data_disk_absent=false
  fi

  jq -n \
    --arg observed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg project "${project}" \
    --arg zone "${zone}" \
    --arg source_instance_name "${instance}" \
    --arg source_boot_disk_name "${boot_disk}" \
    --arg source_data_disk_name "${data_disk}" \
    --argjson source_instance_absent "${instance_absent}" \
    --argjson source_boot_disk_absent "${boot_disk_absent}" \
    --argjson source_data_disk_absent "${data_disk_absent}" \
    '
      {
        schema_version: 1,
        kind: "objectkv_provider_media_loss_observation_r0",
        observed_at: $observed_at,
        project: $project,
        zone: $zone,
        source_instance_name: $source_instance_name,
        source_boot_disk_name: $source_boot_disk_name,
        source_data_disk_name: $source_data_disk_name,
        source_instance_absent: $source_instance_absent,
        source_boot_disk_absent: $source_boot_disk_absent,
        source_data_disk_absent: $source_data_disk_absent
      }
    ' >"${output}"
  jq empty "${output}"
  jq -e '
    .source_instance_absent and
    .source_boot_disk_absent and
    .source_data_disk_absent
  ' "${output}" >/dev/null
}

restart_source() {
  [[ $# -eq 2 ]] || usage
  local run_label="$1"
  local output="$2"
  local instance="objectkv-bench-${run_label}-source"
  local before
  local after
  local stopped_at
  local started_at
  before="$(gcloud compute instances describe "${instance}" --project="${project}" --zone="${zone}" --format=json)"
  stopped_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  gcloud compute instances stop "${instance}" \
    --project="${project}" \
    --zone="${zone}" \
    --quiet
  if [[ "$(gcloud compute instances describe "${instance}" --project="${project}" --zone="${zone}" --format='value(status)')" != "TERMINATED" ]]; then
    echo "source provider did not reach TERMINATED" >&2
    exit 1
  fi
  gcloud compute instances start "${instance}" \
    --project="${project}" \
    --zone="${zone}" \
    --quiet
  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$(gcloud compute instances describe "${instance}" --project="${project}" --zone="${zone}" --format='value(status)')" != "RUNNING" ]]; then
    echo "source provider did not return to RUNNING" >&2
    exit 1
  fi
  after="$(gcloud compute instances describe "${instance}" --project="${project}" --zone="${zone}" --format=json)"
  local before_instance_id
  local after_instance_id
  local before_boot_disk
  local after_boot_disk
  local before_data_disk
  local after_data_disk
  before_instance_id="$(jq -r '.id' <<<"${before}")"
  after_instance_id="$(jq -r '.id' <<<"${after}")"
  before_boot_disk="$(jq -r '.disks[] | select(.boot == true) | .source' <<<"${before}")"
  after_boot_disk="$(jq -r '.disks[] | select(.boot == true) | .source' <<<"${after}")"
  before_data_disk="$(jq -r '.disks[] | select(.deviceName == "objectkv-data") | .source' <<<"${before}")"
  after_data_disk="$(jq -r '.disks[] | select(.deviceName == "objectkv-data") | .source' <<<"${after}")"
  local identities_retained=false
  if [[ "${before_instance_id}" == "${after_instance_id}" && "${before_boot_disk}" == "${after_boot_disk}" && "${before_data_disk}" == "${after_data_disk}" ]]; then
    identities_retained=true
  fi
  jq -n \
    --arg stopped_at "${stopped_at}" \
    --arg started_at "${started_at}" \
    --arg project "${project}" \
    --arg zone "${zone}" \
    --arg instance_name "${instance}" \
    --arg instance_id "${after_instance_id}" \
    --arg boot_disk "${after_boot_disk##*/}" \
    --arg data_disk "${after_data_disk##*/}" \
    --argjson identities_retained "${identities_retained}" \
    '{
      schema_version: 1,
      kind: "objectkv_provider_restart_observation_r0",
      stopped_at: $stopped_at,
      started_at: $started_at,
      project: $project,
      zone: $zone,
      instance_name: $instance_name,
      instance_id: $instance_id,
      boot_disk_name: $boot_disk,
      data_disk_name: $data_disk,
      stop_succeeded: true,
      start_succeeded: true,
      identities_retained: $identities_retained
    }' >"${output}"
  jq -e '.stop_succeeded and .start_succeeded and .identities_retained' "${output}" >/dev/null
}

case "${command_name}" in
  capture)
    capture "$@"
    ;;
  observe-loss)
    observe_loss "$@"
    ;;
  restart-source)
    restart_source "$@"
    ;;
  *)
    usage
    ;;
esac
