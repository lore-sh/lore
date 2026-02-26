#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: generate-checksums.sh --version <semver> --artifacts-dir <dir>
USAGE
  exit 1
}

VERSION=""
ARTIFACTS_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --artifacts-dir)
      ARTIFACTS_DIR="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      ;;
  esac
done

[[ -n "$VERSION" && -n "$ARTIFACTS_DIR" ]] || usage
[[ -d "$ARTIFACTS_DIR" ]] || {
  echo "Artifacts directory not found: $ARTIFACTS_DIR" >&2
  exit 1
}

if command -v sha256sum >/dev/null 2>&1; then
  HASH_CMD=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  HASH_CMD=(shasum -a 256)
else
  echo "Missing checksum tool: require sha256sum or shasum" >&2
  exit 1
fi

shopt -s nullglob
ARCHIVES=("$ARTIFACTS_DIR"/lore_"$VERSION"_*.tar.gz)
shopt -u nullglob
[[ ${#ARCHIVES[@]} -gt 0 ]] || {
  echo "No archives found for version $VERSION under $ARTIFACTS_DIR" >&2
  exit 1
}

CHECKSUM_FILE="$ARTIFACTS_DIR/lore_${VERSION}_checksums.txt"
: > "$CHECKSUM_FILE"
for archive in "${ARCHIVES[@]}"; do
  checksum="$(${HASH_CMD[@]} "$archive" | awk '{print $1}')"
  printf '%s  %s\n' "$checksum" "$(basename "$archive")" >> "$CHECKSUM_FILE"
done

printf '%s\n' "$CHECKSUM_FILE"
