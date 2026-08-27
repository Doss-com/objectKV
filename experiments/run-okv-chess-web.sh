#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_root="$(mktemp -d /private/tmp/okv-chess-web-target.XXXXXX)"
runtime_root="$(mktemp -d /private/tmp/okv-chess-web-runtime.XXXXXX)"
port="${OKV_CHESS_PORT:-4268}"

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
CARGO_TARGET_DIR="$build_root" cargo build --quiet --release -p okv-chess-example
install -m 755 "$build_root/release/okv-chess-example" "$runtime_root/okv-chess-example"
strip "$runtime_root/okv-chess-example" 2>/dev/null || true
find "$build_root" -depth -delete
"$runtime_root/okv-chess-example" --web --port "$port"
