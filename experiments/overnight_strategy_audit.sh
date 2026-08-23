#!/usr/bin/env bash

set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root" || exit 2

expected_head="${OKV_AUDIT_EXPECTED_HEAD:-$(git rev-parse HEAD)}"
duration_seconds="${OKV_AUDIT_DURATION_SECONDS:-43200}"
interval_seconds="${OKV_AUDIT_INTERVAL_SECONDS:-1800}"
started_epoch="$(date +%s)"
started_utc="$(date -u +%Y%m%dT%H%M%SZ)"
run_root="${OKV_AUDIT_OUTPUT_DIR:-/tmp/okv-overnight-strategy-${started_utc}}"
records_file="$run_root/records.jsonl"
summary_file="$run_root/summary.json"
status_file="$run_root/status.json"

for command_name in cargo curl git jq; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$command_name" >&2
    exit 2
  fi
done

if ! [[ "$duration_seconds" =~ ^[0-9]+$ ]] || ((duration_seconds < 1)); then
  printf 'OKV_AUDIT_DURATION_SECONDS must be a positive integer\n' >&2
  exit 2
fi
if ! [[ "$interval_seconds" =~ ^[0-9]+$ ]] || ((interval_seconds < 60)); then
  printf 'OKV_AUDIT_INTERVAL_SECONDS must be an integer of at least 60\n' >&2
  exit 2
fi
if [[ "$(git rev-parse HEAD)" != "$expected_head" ]]; then
  printf 'expected HEAD %s, found %s\n' "$expected_head" "$(git rev-parse HEAD)" >&2
  exit 2
fi
if [[ -n "$(git status --porcelain)" ]]; then
  printf 'overnight audit requires a clean worktree\n' >&2
  exit 2
fi

mkdir -p "$run_root/cycles" "$run_root/controls"
: >"$records_file"

export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4318}"
export OKV_S3_ENDPOINT="${OKV_S3_ENDPOINT:-http://127.0.0.1:19110}"
export OKV_S3_BUCKET="${OKV_S3_BUCKET:-okv-dev}"
export OKV_S3_ACCESS_KEY_ID="${OKV_S3_ACCESS_KEY_ID:-okvdev}"
export OKV_S3_SECRET_ACCESS_KEY="${OKV_S3_SECRET_ACCESS_KEY:-okv-dev-only-secret}"
export OKV_OBJECT_SERVER_VERSION="${OKV_OBJECT_SERVER_VERSION:-RELEASE.2025-09-07T16-13-09Z}"

if ! curl -fsS "$OKV_S3_ENDPOINT/minio/health/ready" >/dev/null; then
  printf 'MinIO is not ready at the configured endpoint\n' >&2
  exit 2
fi
if ! curl -fsS http://127.0.0.1:8889/metrics >/dev/null; then
  printf 'the local OTel Prometheus endpoint is not ready\n' >&2
  exit 2
fi

write_summary() {
  jq -s \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg candidate_commit "$expected_head" \
    '{
      generated_at: $generated_at,
      candidate_commit: $candidate_commit,
      records: length,
      expected_matches: (map(select(.expected_match)) | length),
      unexpected_results: (map(select(.expected_match | not)) | length),
      by_label: (
        sort_by(.label)
        | group_by(.label)
        | map({
            label: .[0].label,
            expected_verdict: .[0].expected_verdict,
            runs: length,
            expected_matches: (map(select(.expected_match)) | length),
            primary_samples: map(.primary_median | select(. != null)),
            operation_seconds: map(.operation_seconds | select(. != null)),
            run_ids: map(.run_id | select(. != null))
          })
      )
    }' "$records_file" >"$summary_file"
}

write_status() {
  local state="$1"
  local cycle="$2"
  local detail="$3"
  jq -n \
    --arg state "$state" \
    --arg cycle "$cycle" \
    --arg detail "$detail" \
    --arg candidate_commit "$expected_head" \
    --arg started_at "$started_utc" \
    --arg updated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg output_dir "$run_root" \
    '{
      state: $state,
      cycle: $cycle,
      detail: $detail,
      candidate_commit: $candidate_commit,
      started_at: $started_at,
      updated_at: $updated_at,
      output_dir: $output_dir
    }' >"$status_file"
}

record_missing() {
  local cycle="$1"
  local label="$2"
  local expected_verdict="$3"
  local exit_code="$4"
  local log_file="$5"
  jq -cn \
    --arg observed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg cycle "$cycle" \
    --arg label "$label" \
    --arg expected_verdict "$expected_verdict" \
    --arg log_file "$log_file" \
    --argjson exit_code "$exit_code" \
    '{
      observed_at: $observed_at,
      cycle: $cycle,
      label: $label,
      expected_verdict: $expected_verdict,
      verdict: null,
      expected_match: false,
      exit_code: $exit_code,
      run_id: null,
      primary_median: null,
      operation_seconds: null,
      result_file: null,
      log_file: $log_file,
      failed_gates: ["missing-result"]
    }' >>"$records_file"
}

run_one() {
  local cycle="$1"
  local label="$2"
  local expected_verdict="$3"
  local suite="$4"
  local profile="$5"
  local workload="$6"
  local backend="$7"
  local result_dir="$run_root/cycles/$cycle"
  if [[ "$cycle" == "controls" ]]; then
    result_dir="$run_root/controls"
  fi
  mkdir -p "$result_dir"
  local result_file="$result_dir/$label.json"
  local log_file="$result_dir/$label.log"

  if [[ "$(git rev-parse HEAD)" != "$expected_head" ]] || [[ -n "$(git status --porcelain)" ]]; then
    record_missing "$cycle" "$label" "$expected_verdict" 97 "$log_file"
    write_summary
    write_status "stopped" "$cycle" "source identity changed"
    exit 97
  fi

  cargo run -q -p okv-eval -- run "$suite" \
    --profile "$profile" \
    --workload "$workload" \
    --backend "$backend" \
    --output "$result_file" >"$log_file" 2>&1
  local exit_code=$?

  if ((exit_code != 0)) || ! jq -e . "$result_file" >/dev/null 2>&1; then
    record_missing "$cycle" "$label" "$expected_verdict" "$exit_code" "$log_file"
    write_summary
    return 0
  fi

  local verdict
  local expected_match=false
  local primary_median
  local operation_seconds
  local secondary_metrics
  local failed_gates
  local phase0_detail=null
  local raw_artifact
  verdict="$(jq -r '.verdict' "$result_file")"
  primary_median="$(jq -c '.primary_metric.median // null' "$result_file")"
  operation_seconds="$(jq -c '.secondary_metrics["operation.duration.median"] // null' "$result_file")"
  secondary_metrics="$(jq -c '.secondary_metrics' "$result_file")"
  failed_gates="$(jq -c '[.hard_gates[] | select(.status != "pass") | .id]' "$result_file")"
  if [[ "$verdict" == "$expected_verdict" ]]; then
    expected_match=true
  fi
  raw_artifact="$(jq -r '.artifact_refs[0] // empty' "$result_file")"
  if [[ "$raw_artifact" == target/* ]] && [[ -f "$raw_artifact" ]]; then
    phase0_detail="$(jq -c '{
      logical_bytes,
      key_count,
      reopen_first_correct_read_seconds: .seeds[0].reopen_first_correct_read_seconds,
      reopen_open_seconds: .seeds[0].reopen_open.elapsed_seconds,
      reopen_open_requests: ((.seeds[0].reopen_open.io.successful_requests | to_entries | map(.value) | add // 0) + (.seeds[0].reopen_open.io.failed_requests | to_entries | map(.value) | add // 0)),
      reopen_open_read_bytes: (.seeds[0].reopen_open.io.read_bytes | to_entries | map(.value) | add // 0),
      first_read_requests: ((.seeds[0].first_correct_read.io.successful_requests | to_entries | map(.value) | add // 0) + (.seeds[0].first_correct_read.io.failed_requests | to_entries | map(.value) | add // 0)),
      first_read_bytes: (.seeds[0].first_correct_read.io.read_bytes | to_entries | map(.value) | add // 0),
      cold_point_requests: ((.seeds[0].cold_point.io.successful_requests | to_entries | map(.value) | add // 0) + (.seeds[0].cold_point.io.failed_requests | to_entries | map(.value) | add // 0)),
      cold_point_read_bytes: (.seeds[0].cold_point.io.read_bytes | to_entries | map(.value) | add // 0)
    }' "$raw_artifact")"
  fi

  jq -cn \
    --arg observed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg cycle "$cycle" \
    --arg label "$label" \
    --arg expected_verdict "$expected_verdict" \
    --arg verdict "$verdict" \
    --arg run_id "$(jq -r '.run_id' "$result_file")" \
    --arg result_file "$result_file" \
    --arg log_file "$log_file" \
    --arg suite_hash "$(jq -r '.suite_hash' "$result_file")" \
    --argjson expected_match "$expected_match" \
    --argjson exit_code "$exit_code" \
    --argjson primary_median "$primary_median" \
    --argjson operation_seconds "$operation_seconds" \
    --argjson secondary_metrics "$secondary_metrics" \
    --argjson failed_gates "$failed_gates" \
    --argjson phase0 "$phase0_detail" \
    '{
      observed_at: $observed_at,
      cycle: $cycle,
      label: $label,
      expected_verdict: $expected_verdict,
      verdict: $verdict,
      expected_match: $expected_match,
      exit_code: $exit_code,
      run_id: $run_id,
      suite_hash: $suite_hash,
      primary_median: $primary_median,
      operation_seconds: $operation_seconds,
      secondary_metrics: $secondary_metrics,
      phase0: $phase0,
      result_file: $result_file,
      log_file: $log_file,
      failed_gates: $failed_gates
    }' >>"$records_file"
  write_summary
}

stop_audit() {
  write_summary
  write_status "stopped" "signal" "terminated before the 12-hour deadline"
  exit 130
}
trap stop_audit INT TERM

write_status "running" "controls" "running one-time negative controls"
run_one controls slate-warm-cache-poison discard \
  evals/suites/phase0-slate-filesystem-scale.toml scale-8mib \
  slatedb-filesystem-scale-reuse-warm-db slatedb-local-fs
run_one controls generation-single-signer-poison discard \
  evals/suites/generation-certificates.toml local-fs \
  generation-certificate-single-signer-fence process-local-fs
run_one controls publication-convergence-only-poison discard \
  evals/suites/object-publication-publisher-publish-recovery.toml local-fs \
  publisher-publish-unknown-convergence-only object-store-local-fs+process-openraft
run_one controls htap-materialization-poison discard \
  evals/suites/htap-streaming.toml local-fs \
  zebradb-streaming-materialize-inputs datafusion-local-fs

deadline_epoch=$((started_epoch + duration_seconds))
cycle=0
while (( $(date +%s) < deadline_epoch )); do
  cycle=$((cycle + 1))
  write_status "running" "$cycle" "running normal strategy audit cycle"
  run_one "$cycle" slate-scale-1mib keep \
    evals/suites/phase0-slate-filesystem-scale.toml scale-1mib \
    slatedb-filesystem-scale-baseline slatedb-local-fs
  run_one "$cycle" slate-scale-8mib keep \
    evals/suites/phase0-slate-filesystem-scale.toml scale-8mib \
    slatedb-filesystem-scale-baseline slatedb-local-fs
  run_one "$cycle" slate-scale-64mib keep \
    evals/suites/phase0-slate-filesystem-scale.toml scale-64mib \
    slatedb-filesystem-scale-baseline slatedb-local-fs
  run_one "$cycle" minio-authority keep \
    evals/suites/object-store.toml minio-authority \
    named-object-authority-contract minio
  run_one "$cycle" generation-handoff keep \
    evals/suites/generation-certificates.toml local-fs \
    generation-certificate-handoff process-local-fs
  run_one "$cycle" publication-lost-reply keep \
    evals/suites/object-publication-publisher-publish-recovery.toml local-fs \
    publisher-publish-unknown-restart object-store-local-fs+process-openraft
  run_one "$cycle" htap-streaming keep \
    evals/suites/htap-streaming.toml local-fs \
    zebradb-streaming-overlay datafusion-local-fs

  next_cycle_epoch=$((started_epoch + cycle * interval_seconds))
  now_epoch="$(date +%s)"
  if ((next_cycle_epoch > now_epoch && next_cycle_epoch < deadline_epoch)); then
    write_status "sleeping" "$cycle" "waiting for the next fixed-cadence cycle"
    sleep $((next_cycle_epoch - now_epoch))
  fi
done

write_summary
write_status "complete" "$cycle" "12-hour strategy audit finished"
printf '%s\n' "$run_root"
