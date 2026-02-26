#!/usr/bin/env bash
set -euo pipefail

LORE_REPO="${LORE_REPO:-lore-sh/lore}"
LORE_VERSION="${LORE_VERSION:-}"
LORE_INSTALL_DIR="${LORE_INSTALL_DIR:-$HOME/.local/bin}"
LORE_RELEASE_BASE_URL="${LORE_RELEASE_BASE_URL:-https://github.com/${LORE_REPO}/releases/download}"
LORE_LATEST_URL="${LORE_LATEST_URL:-https://github.com/${LORE_REPO}/releases/latest}"
LORE_INSTALL_TEMP_DIR=""

log() {
  printf 'lore-install: %s\n' "$*" >&2
}

die() {
  log "$*"
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

normalize_os() {
  local machine_os
  machine_os="$(uname -s)"
  case "$machine_os" in
    Darwin) printf 'darwin\n' ;;
    Linux) printf 'linux\n' ;;
    *) die "Unsupported OS: $machine_os (supported: macOS, Linux)" ;;
  esac
}

reject_musl() {
  if [[ "$1" != "linux" ]]; then
    return
  fi
  if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
    die "musl libc is not supported yet. Please use a glibc-based Linux distribution."
  fi
}

normalize_arch() {
  local machine_arch
  machine_arch="$(uname -m)"
  case "$machine_arch" in
    x86_64|amd64) printf 'x64\n' ;;
    arm64|aarch64) printf 'arm64\n' ;;
    *) die "Unsupported architecture: $machine_arch (supported: x64, arm64)" ;;
  esac
}

normalize_tag() {
  local version="$1"
  if [[ -z "$version" ]]; then
    printf '\n'
    return
  fi
  if [[ "$version" == v* ]]; then
    printf '%s\n' "$version"
    return
  fi
  printf 'v%s\n' "$version"
}

resolve_tag_from_latest() {
  local resolved
  resolved="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$LORE_LATEST_URL")"
  [[ -n "$resolved" ]] || die "Failed to resolve latest stable release"
  printf '%s\n' "${resolved##*/}"
}

resolve_release_tag() {
  if [[ -n "$LORE_VERSION" ]]; then
    normalize_tag "$LORE_VERSION"
    return
  fi
  resolve_tag_from_latest
}

compute_sha256() {
  local file_path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file_path" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file_path" | awk '{print $1}'
    return
  fi
  die "Missing checksum tool: sha256sum or shasum"
}

cleanup() {
  if [[ -n "${LORE_INSTALL_TEMP_DIR:-}" ]]; then
    rm -rf "$LORE_INSTALL_TEMP_DIR"
  fi
}

main() {
  require_cmd curl
  require_cmd tar
  require_cmd uname
  require_cmd mktemp

  local os_name
  local arch_name
  local release_tag
  local release_version
  local archive_name
  local checksums_name
  local release_url
  local archive_path
  local checksums_path
  local expected_hash
  local actual_hash
  local extracted_binary
  local install_path

  os_name="$(normalize_os)"
  reject_musl "$os_name"
  arch_name="$(normalize_arch)"
  release_tag="$(resolve_release_tag)"
  release_version="${release_tag#v}"
  archive_name="lore_${release_version}_${os_name}_${arch_name}.tar.gz"
  checksums_name="lore_${release_version}_checksums.txt"
  release_url="${LORE_RELEASE_BASE_URL}/${release_tag}"

  LORE_INSTALL_TEMP_DIR="$(mktemp -d)"
  trap cleanup EXIT

  archive_path="$LORE_INSTALL_TEMP_DIR/$archive_name"
  checksums_path="$LORE_INSTALL_TEMP_DIR/$checksums_name"

  log "Downloading ${archive_name}"
  curl -fsSL -o "$archive_path" "$release_url/$archive_name" || die "Failed to download release archive"

  log "Downloading ${checksums_name}"
  curl -fsSL -o "$checksums_path" "$release_url/$checksums_name" || die "Failed to download checksums"

  expected_hash="$(grep "  ${archive_name}$" "$checksums_path" | awk '{print $1}' | head -n1)"
  [[ -n "$expected_hash" ]] || die "Checksum entry not found for ${archive_name}"

  actual_hash="$(compute_sha256 "$archive_path")"
  [[ "$expected_hash" == "$actual_hash" ]] || die "Checksum verification failed for ${archive_name}"

  tar -xzf "$archive_path" -C "$LORE_INSTALL_TEMP_DIR" || die "Failed to extract archive"
  extracted_binary="$LORE_INSTALL_TEMP_DIR/lore"
  [[ -f "$extracted_binary" ]] || die "Release archive did not contain lore binary"

  mkdir -p "$LORE_INSTALL_DIR"
  install_path="$LORE_INSTALL_DIR/lore"
  cp "$extracted_binary" "$install_path"
  chmod 755 "$install_path"

  "$install_path" --version >/dev/null || die "Installed binary failed self-check"

  log "Installed lore ${release_version} to ${install_path}"
  if [[ ":$PATH:" != *":${LORE_INSTALL_DIR}:"* ]]; then
    log "Add ${LORE_INSTALL_DIR} to PATH to run lore globally"
  fi

  "$install_path" --version
}

main "$@"
