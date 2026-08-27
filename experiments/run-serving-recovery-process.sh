#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner_target="${OKV_SERVING_RECOVERY_TARGET_DIR:-$(mktemp -d /private/tmp/okv-serving-recovery-target.XXXXXX)}"
if [[ -n "${OKV_SERVING_RECOVERY_OUTPUT_DIR:-}" ]]; then
  result_dir="$OKV_SERVING_RECOVERY_OUTPUT_DIR"
  clean_results=0
  mkdir -p "$result_dir"
else
  result_dir="$(mktemp -d /private/tmp/okv-serving-recovery-results.XXXXXX)"
  clean_results=1
fi
if [[ -n "${OKV_SERVING_RECOVERY_TARGET_DIR:-}" ]]; then
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

run_workload() {
  local workload="$1"
  local output="$2"
  cargo run --quiet --release --manifest-path "$repo_root/Cargo.toml" \
    -p okv-eval -- \
    run "$repo_root/evals/suites/serving-recovery-process.toml" \
    --profile local-fs \
    --workload "$workload" \
    --backend object-store-local-fs+authority-openraft+quorum-wal-files \
    "${dirty_args[@]}" \
    --output "$output" >/dev/null
}

run_workload replacement-worker-object-plus-tail "$result_dir/candidate.json"
run_workload replacement-worker-full-hydration-control "$result_dir/control.json"
run_workload replacement-worker-skip-tail-poison "$result_dir/poison.json"

jq -s '
  map({
    run_id,
    workload: .artifact_refs[0],
    verdict,
    configured_statistic: .primary_metric.statistic,
    first_read_seconds: .primary_metric.value,
    first_read_samples_seconds: .primary_metric.samples,
    object_response_bytes: .secondary_metrics["serving_recovery.object_response_bytes.median"],
    txlog_physical_bytes: .secondary_metrics["serving_recovery.txlog_physical_bytes.median"],
    row_data_closure_bytes: .secondary_metrics["serving_recovery.row_data_closure_bytes"],
    correctness_anomalies: .secondary_metrics["serving_recovery.correctness_anomalies"],
    failed_gates: [.hard_gates[] | select(.status == "fail") | .id]
  })
' "$result_dir/candidate.json" "$result_dir/control.json" "$result_dir/poison.json"
