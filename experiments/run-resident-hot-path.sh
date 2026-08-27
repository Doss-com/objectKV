#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runner_target="$(mktemp -d /tmp/okv-resident-runner-target.XXXXXX)"
if [[ -n "${OKV_RESIDENT_OUTPUT_DIR:-}" ]]; then
  result_dir="$OKV_RESIDENT_OUTPUT_DIR"
  clean_results=0
  mkdir -p "$result_dir"
else
  result_dir="$(mktemp -d /tmp/okv-resident-results.XXXXXX)"
  clean_results=1
fi

cleanup() {
  find "$runner_target" -depth -delete 2>/dev/null || true
  if [[ "$clean_results" == "1" ]]; then
    find "$result_dir" -depth -delete 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$runner_target"
export CARGO_PROFILE_RELEASE_DEBUG=0
export ROCKSDB_LIB_DIR="${ROCKSDB_LIB_DIR:-/opt/homebrew/opt/rocksdb/lib}"
export ROCKSDB_INCLUDE_DIR="${ROCKSDB_INCLUDE_DIR:-/opt/homebrew/opt/rocksdb/include}"
export LIBCLANG_PATH="${LIBCLANG_PATH:-/Library/Developer/CommandLineTools/usr/lib}"
export OKV_ROCKSDB_VERSION="${OKV_ROCKSDB_VERSION:-11.8.1}"

run_workload() {
  local workload="$1"
  local backend="$2"
  local output="$3"
  cargo run --quiet --release --manifest-path "$repo_root/Cargo.toml" \
    --features resident-rocksdb -p okv-eval -- \
    run "$repo_root/evals/suites/product-thesis.toml" \
    --profile ssd-resident-dev \
    --workload "$workload" \
    --backend "$backend" \
    --allow-dirty \
    --output "$output" >/dev/null
}

run_workload \
  direct-nvme-rocksdb-control \
  rocksdb-11.8.1-local-fs \
  "$result_dir/control-a.json"
run_workload \
  ssd-resident-serving-image \
  serving-worker-rocksdb-11.8.1-local-fs \
  "$result_dir/candidate-a.json"
run_workload \
  ssd-resident-serving-image \
  serving-worker-rocksdb-11.8.1-local-fs \
  "$result_dir/candidate-b.json"
run_workload \
  direct-nvme-rocksdb-control \
  rocksdb-11.8.1-local-fs \
  "$result_dir/control-b.json"
run_workload \
  ssd-resident-object-read-poison \
  serving-worker-rocksdb-11.8.1-local-fs \
  "$result_dir/poison.json"

jq -n \
  --slurpfile control_a "$result_dir/control-a.json" \
  --slurpfile control_b "$result_dir/control-b.json" \
  --slurpfile candidate_a "$result_dir/candidate-a.json" \
  --slurpfile candidate_b "$result_dir/candidate-b.json" \
  --slurpfile poison "$result_dir/poison.json" \
  'def average(a; b): (a + b) / 2;
  def maximum(a; b): if a > b then a else b end;
  (average($control_a[0].primary_metric.median; $control_b[0].primary_metric.median)) as $control_throughput |
  (average($candidate_a[0].primary_metric.median; $candidate_b[0].primary_metric.median)) as $candidate_throughput |
  (average($control_a[0].secondary_metrics["resident.latency_ns.p99"]; $control_b[0].secondary_metrics["resident.latency_ns.p99"])) as $control_p99 |
  (average($candidate_a[0].secondary_metrics["resident.latency_ns.p99"]; $candidate_b[0].secondary_metrics["resident.latency_ns.p99"])) as $candidate_p99 |
  {
    status: "diagnostic-dirty-source",
    process_order: ["control-a", "candidate-a", "candidate-b", "control-b", "poison"],
    control: {
      verdicts: [$control_a[0].verdict, $control_b[0].verdict],
      operations_per_second_run_median_mean: $control_throughput,
      latency_ns_p99_run_median_mean: $control_p99,
      object_fallbacks: ($control_a[0].secondary_metrics["resident.object_fallbacks"] + $control_b[0].secondary_metrics["resident.object_fallbacks"]),
      max_local_bytes: maximum($control_a[0].secondary_metrics["resident.max_local_bytes"]; $control_b[0].secondary_metrics["resident.max_local_bytes"])
    },
    candidate: {
      verdicts: [$candidate_a[0].verdict, $candidate_b[0].verdict],
      operations_per_second_run_median_mean: $candidate_throughput,
      latency_ns_p99_run_median_mean: $candidate_p99,
      object_fallbacks: ($candidate_a[0].secondary_metrics["resident.object_fallbacks"] + $candidate_b[0].secondary_metrics["resident.object_fallbacks"]),
      max_local_bytes: maximum($candidate_a[0].secondary_metrics["resident.max_local_bytes"]; $candidate_b[0].secondary_metrics["resident.max_local_bytes"])
    },
    comparison: {
      throughput_ratio_candidate_over_control: ($candidate_throughput / $control_throughput),
      p99_ratio_candidate_over_control: ($candidate_p99 / $control_p99)
    },
    poison: {
      verdict: $poison[0].verdict,
      object_fallbacks: $poison[0].secondary_metrics["resident.object_fallbacks"],
      zero_object_gate: (
        $poison[0].hard_gates[] |
        select(.id == "zero_object_requests_after_warmup") |
        .status
      )
    }
  }'
