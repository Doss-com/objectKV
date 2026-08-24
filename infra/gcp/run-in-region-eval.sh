#!/usr/bin/env bash
set -euo pipefail

metadata() {
  curl --fail --silent --show-error \
    -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"
}

candidate_commit="$(metadata okv-candidate-commit)"
workload="$(metadata okv-workload)"
repo_url="https://github.com/Doss-com/objectKV.git"
otel_version="0.157.0"
source_root="/opt/objectkv"
cargo_target="/opt/objectkv-target"
otel_root="/opt/objectkv-otel"
collector_pid=""

finish() {
  status=$?
  if [[ -n "$collector_pid" ]]; then
    kill -TERM "$collector_pid" 2>/dev/null || true
    wait "$collector_pid" 2>/dev/null || true
  fi
  echo "OKV_EVAL_EXIT_STATUS=$status"
}
trap finish EXIT

echo "OKV_EVAL_PHASE=bootstrap"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq build-essential ca-certificates cmake curl git jq pkg-config >/dev/null

curl --proto '=https' --tlsv1.2 --silent --show-error --fail \
  https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.88.0 >/dev/null
export PATH="/root/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$cargo_target"

mkdir -p "$source_root" "$otel_root"
git -C "$source_root" init -q
if git -C "$source_root" remote get-url origin >/dev/null 2>&1; then
  git -C "$source_root" remote set-url origin "$repo_url"
else
  git -C "$source_root" remote add origin "$repo_url"
fi
git -C "$source_root" fetch -q --depth=2 origin "$candidate_commit"
git -C "$source_root" checkout -q --detach FETCH_HEAD

curl --silent --show-error --fail --location \
  "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${otel_version}/otelcol-contrib_${otel_version}_linux_amd64.tar.gz" \
  | tar -xz -C "$otel_root" otelcol-contrib

"$otel_root/otelcol-contrib" \
  --config="$source_root/infra/otel/otel-collector.yaml" \
  >/var/log/objectkv-otel.log 2>&1 &
collector_pid=$!

for _ in {1..30}; do
  if curl --fail --silent http://127.0.0.1:8889/metrics >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent http://127.0.0.1:8889/metrics >/dev/null

echo "OKV_EVAL_PHASE=build"
cd "$source_root" || exit 1
cargo build --locked --release -p okv-eval

echo "OKV_EVAL_PHASE=run"
export OKV_GCP_PROJECT=doss-objectkv-dev
export OKV_GCS_BUCKET=doss-objectkv-dev-okv-evals
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export RUST_LOG=warn,okv_eval=info
"$cargo_target/release/okv-eval" run \
  evals/suites/provider-bound-range-read.toml \
  --profile gcs-dev \
  --workload "$workload" \
  --backend gcs-generation-bound-process

sleep 3
curl --fail --silent http://127.0.0.1:8889/metrics \
  | grep -E 'okv_eval_provider_bound_(first_point_duration|object_requests|object_bytes|estimated_cost)' \
  | tail -n 80 || true
tail -n 40 /var/log/objectkv-otel.log || true
echo "OKV_EVAL_PHASE=complete"
