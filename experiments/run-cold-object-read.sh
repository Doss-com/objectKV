#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner_target="${OKV_COLD_READ_TARGET_DIR:-$(mktemp -d /tmp/okv-cold-read-target.XXXXXX)}"
if [[ -n "${OKV_COLD_READ_OUTPUT_DIR:-}" ]]; then
  result_dir="$OKV_COLD_READ_OUTPUT_DIR"
  clean_results=0
  mkdir -p "$result_dir"
else
  result_dir="$(mktemp -d /tmp/okv-cold-read-results.XXXXXX)"
  clean_results=1
fi
if [[ -n "${OKV_COLD_READ_TARGET_DIR:-}" ]]; then
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

read -r -a profiles <<< "${OKV_COLD_READ_PROFILES:-elastic-1mib-dev elastic-8mib-dev elastic-dev}"

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
    elastic-indexed-point-read \
    serving-worker-object-local-fs \
    "$profile_dir/candidate.json"
  run_workload \
    "$profile" \
    indexed-object-reader-control \
    indexed-object-reader-local-fs \
    "$profile_dir/control.json"
  run_workload \
    "$profile" \
    elastic-scan-object-poison \
    serving-worker-object-local-fs \
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
    requests_per_operation: .primary_metric.median,
    bytes_per_operation: .secondary_metrics["cold.bytes_per_operation"],
    data_object_bytes: .secondary_metrics["cold.data_object_bytes"],
    max_data_object_bytes: .secondary_metrics["cold.max_data_object_bytes"],
    segments: .secondary_metrics["cold.segment_count"],
    metadata_bytes: .secondary_metrics["cold.metadata_bytes"],
    p50_ns: .secondary_metrics["cold.latency_ns.p50"],
    p99_ns: .secondary_metrics["cold.latency_ns.p99"],
    failed_gates: [.hard_gates[] | select(.status == "fail") | .id]
  })
' "${receipts[@]}"
