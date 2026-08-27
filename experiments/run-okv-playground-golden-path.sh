#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_root="$(mktemp -d /private/tmp/okv-playground-golden-target.XXXXXX)"
tetris_actions="${OKV_TETRIS_GOLDEN_ACTIONS:-2000}"
object_actions="${OKV_TETRIS_OBJECT_ACTIONS:-512}"
build_profile="${OKV_PLAYGROUND_BUILD_PROFILE:-release}"

cleanup() {
  if [[ -d "$build_root" ]]; then
    find "$build_root" -depth -delete
  fi
}
trap cleanup EXIT INT TERM

cd "$repo_root"
if [[ "$build_profile" == "release" ]]; then
  CARGO_TARGET_DIR="$build_root" cargo build --quiet --release \
    -p okv-tetris-example \
    -p okv-chess-example
  binary_root="$build_root/release"
elif [[ "$build_profile" == "debug" ]]; then
  CARGO_TARGET_DIR="$build_root" cargo build --quiet \
    -p okv-tetris-example \
    -p okv-chess-example
  binary_root="$build_root/debug"
else
  printf '%s\n' "OKV_PLAYGROUND_BUILD_PROFILE must be release or debug" >&2
  exit 1
fi

tetris_receipt="$("$binary_root/okv-tetris-example" --golden "$tetris_actions")"
tetris_delta_receipt="$("$binary_root/okv-tetris-example" --delta-golden "$tetris_actions")"
tetris_transaction_receipt="$("$binary_root/okv-tetris-example" --transactional-golden "$tetris_actions")"
tetris_consensus_receipt="$("$binary_root/okv-tetris-example" --consensus-golden)"
tetris_serving_receipt="$("$binary_root/okv-tetris-example" --serving-golden "$tetris_actions")"
tetris_object_receipt="$("$binary_root/okv-tetris-example" --object-golden "$object_actions")"
tetris_branch_receipt="$("$binary_root/okv-tetris-example" --branch-gc-golden "$object_actions")"
chess_receipt="$("$binary_root/okv-chess-example" --golden)"
chess_delta_receipt="$("$binary_root/okv-chess-example" --delta-golden)"
chess_transaction_receipt="$("$binary_root/okv-chess-example" --transactional-golden)"
chess_consensus_receipt="$("$binary_root/okv-chess-example" --consensus-golden)"
chess_serving_receipt="$("$binary_root/okv-chess-example" --serving-golden)"
chess_object_receipt="$("$binary_root/okv-chess-example" --object-golden)"
chess_branch_receipt="$("$binary_root/okv-chess-example" --branch-gc-golden)"

printf '%s\n' "$tetris_receipt"
printf '%s\n' "$tetris_delta_receipt"
printf '%s\n' "$tetris_transaction_receipt"
printf '%s\n' "$tetris_consensus_receipt"
printf '%s\n' "$tetris_serving_receipt"
printf '%s\n' "$tetris_object_receipt"
printf '%s\n' "$tetris_branch_receipt"
printf '%s\n' "$chess_receipt"
printf '%s\n' "$chess_delta_receipt"
printf '%s\n' "$chess_transaction_receipt"
printf '%s\n' "$chess_consensus_receipt"
printf '%s\n' "$chess_serving_receipt"
printf '%s\n' "$chess_object_receipt"
printf '%s\n' "$chess_branch_receipt"

command -v jq >/dev/null 2>&1 || {
  printf '%s\n' "jq is required to compare golden receipts" >&2
  exit 1
}

tetris_api="$(printf '%s' "$tetris_receipt" | jq -er '.api_version')"
chess_api="$(printf '%s' "$chess_receipt" | jq -er '.api_version')"
[[ "$tetris_api" == "$chess_api" ]]
printf '%s' "$tetris_receipt" | jq -e \
  '.status == "VERIFIED" and .record_kind == "materialized-kv" and .snapshot_round_trip and .replay_exact' >/dev/null
printf '%s' "$tetris_delta_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G1" and .record_kind == "application-delta" and .delta_bytes_per_action == 2 and .checkpoint_identity_exact and .poison_controls == 7 and .snapshot_round_trip and .replay_exact' >/dev/null
printf '%s' "$tetris_transaction_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G2" and .application_record_bytes_per_commit == 2 and .record_and_mutations_atomic and .atomic_abort_no_effect and .records_aligned and .application_replay_exact' >/dev/null
printf '%s' "$tetris_consensus_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G3" and .scope == "three-process-openraft-one-host" and .canonical_envelope_atomic and .anomalies == 0 and .process_kills == 1 and .dropped_replies == 1 and .duplicate_retries == 1 and .caught_up_nodes == 3 and .exact_game_replay' >/dev/null
printf '%s' "$tetris_serving_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G4" and .serving_profile == "ram" and .discarded_image_rebuild_exact and .historical_read_after_rebuild_exact and .local_data_files == 0' >/dev/null
printf '%s' "$tetris_object_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G5" and .object_puts == 3 and .recursive_closure_verified_before_publish and .cold_reopen_exact and .root_exact' >/dev/null
printf '%s' "$tetris_branch_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G6" and .branch_new_puts == 2 and .copied_prefix_puts == 0 and .deleted_branch_objects == 2 and .main_after_gc_exact and .branch_before_gc_exact and .pin_from_root_used and .exact_root_removal_used' >/dev/null
printf '%s' "$chess_receipt" | jq -e \
  '.status == "VERIFIED" and .record_kind == "materialized-kv" and .snapshot_round_trip and .branch_diverged and .main_switch_exact and .branch_switch_exact and .replay_exact' >/dev/null
printf '%s' "$chess_delta_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G1" and .record_kind == "application-delta" and .delta_bytes_per_action == 4 and .branch_suffix_bytes == 4 and .checkpoint_identity_exact and .poison_controls == 7 and .snapshot_round_trip and .branch_diverged and .main_switch_exact and .branch_switch_exact and .replay_exact' >/dev/null
printf '%s' "$chess_transaction_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G2" and .application_record_bytes_per_commit == 4 and .record_and_mutations_atomic and .atomic_abort_no_effect and .records_aligned and .application_replay_exact and .main_switch_exact and .branch_switch_exact' >/dev/null
printf '%s' "$chess_consensus_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G3" and .scope == "three-process-openraft-one-host" and .canonical_envelope_atomic and .anomalies == 0 and .process_kills == 1 and .dropped_replies == 1 and .duplicate_retries == 1 and .caught_up_nodes == 3 and .exact_game_replay' >/dev/null
printf '%s' "$chess_serving_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G4" and .serving_profile == "ram" and .discarded_image_rebuild_exact and .historical_read_after_rebuild_exact and .main_switch_exact and .branch_switch_exact and .local_data_files == 0' >/dev/null
printf '%s' "$chess_object_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G5" and .object_puts == 3 and .recursive_closure_verified_before_publish and .cold_reopen_exact and .root_exact' >/dev/null
printf '%s' "$chess_branch_receipt" | jq -e \
  '.status == "VERIFIED" and .rung == "G6" and .branch_new_puts == 2 and .copied_prefix_puts == 0 and .deleted_branch_objects == 2 and .main_after_gc_exact and .branch_before_gc_exact and .pin_from_root_used and .exact_root_removal_used' >/dev/null

tetris_fingerprint="$(printf '%s' "$tetris_receipt" | jq -er '.fingerprint')"
tetris_delta_fingerprint="$(printf '%s' "$tetris_delta_receipt" | jq -er '.fingerprint')"
chess_fingerprint="$(printf '%s' "$chess_receipt" | jq -er '.fingerprint')"
chess_delta_fingerprint="$(printf '%s' "$chess_delta_receipt" | jq -er '.fingerprint')"
tetris_transaction_fingerprint="$(printf '%s' "$tetris_transaction_receipt" | jq -er '.fingerprint')"
chess_transaction_fingerprint="$(printf '%s' "$chess_transaction_receipt" | jq -er '.fingerprint')"
[[ "$tetris_fingerprint" == "$tetris_delta_fingerprint" ]]
[[ "$chess_fingerprint" == "$chess_delta_fingerprint" ]]
[[ "$tetris_fingerprint" == "$tetris_transaction_fingerprint" ]]
[[ "$chess_fingerprint" == "$chess_transaction_fingerprint" ]]
tetris_trace="$(printf '%s' "$tetris_receipt" | jq -er '.trace_sha256')"
[[ "$tetris_trace" == "$(printf '%s' "$tetris_delta_receipt" | jq -er '.trace_sha256')" ]]
[[ "$tetris_trace" == "$(printf '%s' "$tetris_transaction_receipt" | jq -er '.trace_sha256')" ]]
[[ "$tetris_trace" == "$(printf '%s' "$tetris_serving_receipt" | jq -er '.trace_sha256')" ]]

jq -cn \
  --argjson tetris_materialized "$tetris_receipt" \
  --argjson tetris_delta "$tetris_delta_receipt" \
  --argjson tetris_transaction "$tetris_transaction_receipt" \
  --argjson tetris_consensus "$tetris_consensus_receipt" \
  --argjson tetris_serving "$tetris_serving_receipt" \
  --argjson tetris_object "$tetris_object_receipt" \
  --argjson tetris_branch "$tetris_branch_receipt" \
  --argjson chess_materialized "$chess_receipt" \
  --argjson chess_delta "$chess_delta_receipt" \
  --argjson chess_transaction "$chess_transaction_receipt" \
  --argjson chess_consensus "$chess_consensus_receipt" \
  --argjson chess_serving "$chess_serving_receipt" \
  --argjson chess_object "$chess_object_receipt" \
  --argjson chess_branch "$chess_branch_receipt" \
  --arg build_profile "$build_profile" \
  '{
    golden_path: "objectkv-playground-v3",
    status: "VERIFIED",
    scope: "G0-G6 bounded local profiles",
    build_profile: $build_profile,
    production_admission: "EVALUATING",
    rungs: {
      G0: {status: "VERIFIED", scope: "frozen reducer encodings"},
      G1: {status: "VERIFIED", scope: "volatile ordered history and checkpoint replay"},
      G2: {status: "VERIFIED", scope: "single-process transaction model"},
      G3: {status: "VERIFIED", scope: "three OpenRaft processes on one host"},
      G4: {status: "VERIFIED", scope: "single-process RAM serving image; SSD proposed"},
      G5: {status: "VERIFIED", scope: "memory object adapter plus pure authority; GCS and replicated authority proposed"},
      G6: {status: "VERIFIED", scope: "memory object adapter plus pure authority; process authority proposed"},
      G7: {status: "FUTURE"}
    },
    hard_gates: {
      differential_state_exact: true,
      checkpoint_identity_exact: true,
      poison_controls_per_workload: 7,
      application_record_and_mutations_atomic: true,
      atomic_abort_no_effect: true,
      process_consensus_failover_exact: true,
      ram_image_rebuild_exact: true,
      recursive_object_closure_exact: true,
      shared_prefix_branch_gc_exact: true,
      tetris_snapshot_and_replay: true,
      chess_snapshot_branch_switch_and_replay: true
    },
    tetris: {
      actions: $tetris_materialized.actions,
      materialized_bytes: $tetris_materialized.txlog_bytes,
      delta_and_checkpoint_bytes: $tetris_delta.logical_bytes,
      logical_size_ratio: ($tetris_materialized.txlog_bytes / $tetris_delta.logical_bytes),
      application_record_bytes_per_commit: $tetris_transaction.application_record_bytes_per_commit,
      transactional_txlog_bytes: $tetris_transaction.txlog_bytes,
      consensus_scenario_millis: $tetris_consensus.scenario_duration_millis,
      consensus_caught_up_nodes: $tetris_consensus.caught_up_nodes,
      ram_reads_per_second: $tetris_serving.reads_per_second,
      ram_point_read_p99_nanos: $tetris_serving.point_read_latency_nanos_p99,
      ram_rebuild_micros: $tetris_serving.rebuild_micros,
      object_history_put_bytes: $tetris_object.put_bytes,
      branch_new_puts: $tetris_branch.branch_new_puts,
      branch_deleted_objects: $tetris_branch.deleted_branch_objects
    },
    chess: {
      materialized_branch_bytes: $chess_materialized.txlog_bytes,
      delta_and_checkpoint_branch_bytes: $chess_delta.logical_bytes,
      logical_size_ratio: ($chess_materialized.txlog_bytes / $chess_delta.logical_bytes),
      divergent_suffix_bytes: $chess_delta.branch_suffix_bytes,
      application_record_bytes_per_commit: $chess_transaction.application_record_bytes_per_commit,
      transactional_txlog_bytes: $chess_transaction.txlog_bytes,
      consensus_scenario_millis: $chess_consensus.scenario_duration_millis,
      consensus_caught_up_nodes: $chess_consensus.caught_up_nodes,
      ram_reads_per_second: $chess_serving.reads_per_second,
      ram_point_read_p99_nanos: $chess_serving.point_read_latency_nanos_p99,
      ram_rebuild_micros: $chess_serving.rebuild_micros,
      object_history_put_bytes: $chess_object.put_bytes,
      branch_new_puts: $chess_branch.branch_new_puts,
      branch_deleted_objects: $chess_branch.deleted_branch_objects
    }
  }'
