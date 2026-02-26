#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: smoke-binary.sh --binary <path> --version <semver>
USAGE
  exit 1
}

BINARY_PATH=""
VERSION=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      BINARY_PATH="${2:-}"
      shift 2
      ;;
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      ;;
  esac
done

[[ -n "$BINARY_PATH" && -n "$VERSION" ]] || usage
[[ -x "$BINARY_PATH" ]] || {
  echo "Binary is not executable: $BINARY_PATH" >&2
  exit 1
}

observed_version="$($BINARY_PATH --version)"
[[ "$observed_version" == "$VERSION" ]] || {
  echo "Version mismatch: expected $VERSION, got $observed_version" >&2
  exit 1
}

"$BINARY_PATH" --help >/dev/null

TEMP_HOME="$(mktemp -d)"
trap 'rm -rf "$TEMP_HOME"' EXIT
set +e
status_output="$(HOME="$TEMP_HOME" "$BINARY_PATH" status 2>&1)"
status_code=$?
set -e

[[ $status_code -ne 0 ]] || {
  echo "Expected lore status to fail for fresh home" >&2
  exit 1
}

echo "$status_output" | grep -q "NOT_INITIALIZED" || {
  echo "Expected NOT_INITIALIZED error in status output" >&2
  exit 1
}

HOME="$TEMP_HOME" "$BINARY_PATH" init --yes --no-skills --no-heartbeat --json >/dev/null

log_path="$TEMP_HOME/studio-smoke.log"
started=0
for _ in 1 2 3 4 5; do
  port="$((10000 + RANDOM % 50000))"
  HOME="$TEMP_HOME" "$BINARY_PATH" studio --no-open --port "$port" >"$log_path" 2>&1 &
  studio_pid=$!
  sleep 1
  if curl -fsSL "http://127.0.0.1:$port/" >/dev/null; then
    started=1
    kill "$studio_pid" || true
    wait "$studio_pid" || true
    break
  fi
  kill "$studio_pid" || true
  wait "$studio_pid" || true
done
[[ "$started" -eq 1 ]] || {
  cat "$log_path"
  exit 1
}
