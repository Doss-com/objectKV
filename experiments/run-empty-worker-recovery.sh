#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner_target="${OKV_EMPTY_WORKER_TARGET_DIR:-$(mktemp -d /tmp/okv-empty-worker-target.XXXXXX)}"
if [[ -n "${OKV_EMPTY_WORKER_OUTPUT_DIR:-}" ]]; then
  result_dir="$OKV_EMPTY_WORKER_OUTPUT_DIR"
  clean_results=0
  mkdir -p "$result_dir"
else
  result_dir="$(mktemp -d /tmp/okv-empty-worker-results.XXXXXX)"
  clean_results=1
fi
if [[ -n "${OKV_EMPTY_WORKER_TARGET_DIR:-}" ]]; then
  clean_target=0
else
  clean_target=1
fi

cleanup() {
  if [[ "$clean_target" == "1" ]]; then
    cargo clean --quiet --target-dir "$runner_target" 2>/dev/null || true
    rmdir "$runner_target" 2>/dev/null || true
  fi
  if [[ "$clean_results" == "1" ]]; then
    find "$result_dir" -depth -delete 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$runner_target"
export CARGO_PROFILE_RELEASE_DEBUG=0

dirty_args=()
if [[ "${OKV_ALLOW_DIRTY:-0}" == "1" ]]; then
  dirty_args+=(--allow-dirty)
fi

read -r -a profiles <<< "${OKV_EMPTY_WORKER_PROFILES:-recovery-1mib-dev recovery-8mib-dev recovery-dev}"

run_workload() {
  local profile="$1"
  local workload="$2"
  local backend="$3"
  local output="$4"
  cargo run --quiet --release --manifest-path "$repo_root/Cargo.toml" \
    -p okv-eval -- \
    run "$repo_root/evals/suites/product-thesis.toml" \
    --profile "$profile" \
    --workload "$workload" \
    --backend "$backend" \
    "${dirty_args[@]}" \
    --output "$output" >/dev/null
}

receipts=()
for profile in "${profiles[@]}"; do
  profile_dir="$result_dir/$profile"
  mkdir -p "$profile_dir"
  run_workload \
    "$profile" \
    empty-worker-first-read \
    serving-worker-empty-local-fs \
    "$profile_dir/candidate.json"
  run_workload \
    "$profile" \
    full-local-restore-control \
    full-local-restore \
    "$profile_dir/control.json"
  run_workload \
    "$profile" \
    empty-worker-full-hydration-poison \
    serving-worker-empty-local-fs \
    "$profile_dir/poison.json"
  receipts+=(
    "$profile_dir/candidate.json"
    "$profile_dir/control.json"
    "$profile_dir/poison.json"
  )
done

jq -s '
  map({
    run_id,
    profile: .profile.id,
    mode: .artifact_refs[0],
    verdict,
    configured_statistic: .primary_metric.statistic,
    first_read_statistic_seconds: .primary_metric.value,
    first_read_p50_seconds: .primary_metric.median,
    first_read_samples_seconds: .primary_metric.samples,
    response_bytes: .secondary_metrics["recovery.response_bytes.median"],
    data_closure_bytes: .secondary_metrics["recovery.data_closure_bytes"],
    index_closure_bytes: .secondary_metrics["recovery.index_closure_bytes"],
    manifest_bytes: .secondary_metrics["recovery.manifest_bytes"],
    segments: .secondary_metrics["recovery.segment_count"],
    failed_gates: [.hard_gates[] | select(.status == "fail") | .id]
  })
' "${receipts[@]}"
