#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner_target="${OKV_G45_TARGET_DIR:-$(mktemp -d /private/tmp/okv-g45-target.XXXXXX)}"
if [[ -n "${OKV_G45_OUTPUT_DIR:-}" ]]; then
  result_dir="$OKV_G45_OUTPUT_DIR"
  clean_results=0
  mkdir -p "$result_dir"
else
  result_dir="$(mktemp -d /private/tmp/okv-g45-results.XXXXXX)"
  clean_results=1
fi
if [[ -n "${OKV_G45_TARGET_DIR:-}" ]]; then
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

dirty_args=()
if [[ "${OKV_ALLOW_DIRTY:-0}" == "1" ]]; then
  dirty_args+=(--allow-dirty)
fi

run_workload() {
  local workload="$1"
  local output="$2"
  cargo run --quiet --manifest-path "$repo_root/Cargo.toml" \
    -p okv-eval -- \
    run "$repo_root/evals/suites/transaction-authority-state-scale.toml" \
    --profile local-process \
    --workload "$workload" \
    --backend data-openraft-local-process \
    "${dirty_args[@]}" \
    --output "$output" >/dev/null
}

run_workload ideal-stream-pop-complete-authority "$result_dir/candidate.json"
run_workload no-pop-complete-authority-control "$result_dir/control.json"
run_workload retained-only-accounting-poison "$result_dir/poison.json"

jq -s '
  map({
    run_id,
    workload: .artifact_refs[0],
    verdict,
    reason,
    snapshot_growth_ratio: .primary_metric.value,
    projected_snapshot_bytes: {
      c256: .secondary_metrics["authority.stream_popped_snapshot_bytes.c256"],
      c1024: .secondary_metrics["authority.stream_popped_snapshot_bytes.c1024"],
      c4096: .secondary_metrics["authority.stream_popped_snapshot_bytes.c4096"]
    },
    actual_snapshot_bytes: {
      c256: .secondary_metrics["authority.actual_snapshot_bytes.c256"],
      c1024: .secondary_metrics["authority.actual_snapshot_bytes.c1024"],
      c4096: .secondary_metrics["authority.actual_snapshot_bytes.c4096"]
    },
    failed_gates: [.hard_gates[] | select(.status == "fail") | .id]
  })
' "$result_dir/candidate.json" "$result_dir/control.json" "$result_dir/poison.json"
