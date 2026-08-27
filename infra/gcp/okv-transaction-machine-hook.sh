#!/usr/bin/env bash
set -euo pipefail

action="${1:?lifecycle action is required}"
node_id="${2:?node ID is required}"
config_json="${3:-}"

project_id="${OKV_GCP_PROJECT:?OKV_GCP_PROJECT is required}"
instance_prefix="${OKV_GCP_NODE_PREFIX:-objectkv-node}"

case "$node_id" in
  1) zone="${OKV_GCP_NODE_1_ZONE:-us-central1-a}" ;;
  2) zone="${OKV_GCP_NODE_2_ZONE:-us-central1-b}" ;;
  3) zone="${OKV_GCP_NODE_3_ZONE:-us-central1-c}" ;;
  *) echo "node ID must be 1, 2, or 3" >&2; exit 64 ;;
esac

instance="${instance_prefix}-${node_id}"
unit="objectkv-eval-node-${node_id}"

instance_status() {
  gcloud compute instances describe "$instance" \
    --project "$project_id" \
    --zone "$zone" \
    --format='value(status)'
}

ensure_running() {
  if [[ "$(instance_status)" != "RUNNING" ]]; then
    gcloud compute instances start "$instance" \
      --project "$project_id" \
      --zone "$zone" \
      --quiet
  fi
}

remote() {
  gcloud compute ssh "$instance" \
    --project "$project_id" \
    --zone "$zone" \
    --internal-ip \
    --quiet \
    --command "$1"
}

case "$action" in
  prepare)
    [[ -n "$config_json" ]] || { echo "prepare requires node configuration" >&2; exit 64; }
    root="$(jq -er '.root | select(startswith("/var/lib/objectkv/evals/"))' <<<"$config_json")"
    root_b64="$(printf '%s' "$root" | base64 | tr -d '\n')"
    ensure_running
    remote "set -euo pipefail; sudo systemctl stop '${unit}.service' 2>/dev/null || true; root=\$(printf '%s' '${root_b64}' | base64 --decode); case \"\$root\" in /var/lib/objectkv/evals/*) ;; *) exit 64 ;; esac; sudo install -d -m 0750 \"\$root\"; sudo find \"\$root\" -mindepth 1 -depth -delete"
    ;;
  start)
    [[ -n "$config_json" ]] || { echo "start requires node configuration" >&2; exit 64; }
    config_b64="$(printf '%s' "$config_json" | base64 | tr -d '\n')"
    ensure_running
    remote "set -euo pipefail; config=\$(printf '%s' '${config_b64}' | base64 --decode); sudo systemctl stop '${unit}.service' 2>/dev/null || true; sudo systemctl reset-failed '${unit}.service' 2>/dev/null || true; sudo systemd-run --unit='${unit}' --collect --property=Restart=no /opt/objectkv/okv-eval consensus-node --config-json \"\$config\""
    ;;
  kill)
    gcloud compute instances stop "$instance" \
      --project "$project_id" \
      --zone "$zone" \
      --quiet
    ;;
  cleanup)
    if [[ "$(instance_status)" == "RUNNING" ]]; then
      remote "sudo systemctl stop '${unit}.service' 2>/dev/null || true"
    fi
    ;;
  *) echo "action must be prepare, start, kill, or cleanup" >&2; exit 64 ;;
esac
