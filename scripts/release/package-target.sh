#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: package-target.sh --version <semver> --os <linux|darwin> --arch <x64|arm64> --binary <path> --out-dir <dir>
USAGE
  exit 1
}

VERSION=""
OS_NAME=""
ARCH_NAME=""
BINARY_PATH=""
OUT_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --os)
      OS_NAME="${2:-}"
      shift 2
      ;;
    --arch)
      ARCH_NAME="${2:-}"
      shift 2
      ;;
    --binary)
      BINARY_PATH="${2:-}"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      ;;
  esac
done

[[ -n "$VERSION" && -n "$OS_NAME" && -n "$ARCH_NAME" && -n "$BINARY_PATH" && -n "$OUT_DIR" ]] || usage
[[ -f "$BINARY_PATH" ]] || {
  echo "Binary not found: $BINARY_PATH" >&2
  exit 1
}

mkdir -p "$OUT_DIR"
ARCHIVE_NAME="lore_${VERSION}_${OS_NAME}_${ARCH_NAME}.tar.gz"
ARCHIVE_PATH="$OUT_DIR/$ARCHIVE_NAME"

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
cp "$BINARY_PATH" "$TEMP_DIR/lore"
chmod 755 "$TEMP_DIR/lore"
tar -czf "$ARCHIVE_PATH" -C "$TEMP_DIR" lore

printf '%s\n' "$ARCHIVE_PATH"
