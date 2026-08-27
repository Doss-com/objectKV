#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_root="$(mktemp -d /private/tmp/okv-tetris-target.XXXXXX)"

cleanup() {
  find "$build_root" -depth -delete
}
trap cleanup EXIT INT TERM

cd "$repo_root"
CARGO_TARGET_DIR="$build_root" cargo run --quiet -p okv-tetris-example -- "$@"

