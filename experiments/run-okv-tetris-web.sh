#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_root="$(mktemp -d /private/tmp/okv-tetris-web-target.XXXXXX)"
runtime_root="$(mktemp -d /private/tmp/okv-tetris-web-runtime.XXXXXX)"
port="${OKV_TETRIS_PORT:-4267}"

cleanup() {
  if [[ -d "$build_root" ]]; then
    find "$build_root" -depth -delete
  fi
  if [[ -d "$runtime_root" ]]; then
    find "$runtime_root" -depth -delete
  fi
}
trap cleanup EXIT INT TERM

cd "$repo_root"
CARGO_TARGET_DIR="$build_root" cargo build --quiet --release -p okv-tetris-example
install -m 755 "$build_root/release/okv-tetris-example" "$runtime_root/okv-tetris-example"
strip "$runtime_root/okv-tetris-example" 2>/dev/null || true
find "$build_root" -depth -delete
"$runtime_root/okv-tetris-example" --web --port "$port"
