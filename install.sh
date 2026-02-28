#!/usr/bin/env bash
set -euo pipefail

LORE_REPO="${LORE_REPO:-lore-sh/lore}"
LORE_VERSION="${LORE_VERSION:-}"
LORE_INSTALL_DIR="${LORE_INSTALL_DIR:-$HOME/.local/bin}"
LORE_RELEASE_BASE_URL="${LORE_RELEASE_BASE_URL:-https://github.com/${LORE_REPO}/releases/download}"
LORE_LATEST_URL="${LORE_LATEST_URL:-https://github.com/${LORE_REPO}/releases/latest}"
LORE_SELF_CHECK_TIMEOUT="${LORE_SELF_CHECK_TIMEOUT:-10}"
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

usage() {
  cat <<'USAGE'
Usage: install.sh [--version <semver>] [-h|--help]

Environment variables:
  LORE_VERSION              version to install (for example: 0.1.2 or v0.1.2)
  LORE_INSTALL_DIR          install destination directory (default: ~/.local/bin)
  LORE_SELF_CHECK_TIMEOUT   seconds for binary self-check timeout (default: 10)
USAGE
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h|--help)
        usage
        exit 0
        ;;
      --version)
        [[ -n "${2:-}" ]] || die "Missing value for --version"
        LORE_VERSION="$2"
        shift 2
        ;;
      *)
        die "Unknown option: $1"
        ;;
    esac
  done
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
  local machine_os
  machine_arch="$(uname -m)"
  machine_os="$(uname -s)"
  case "$machine_arch" in
    x86_64|amd64)
      if [[ "$machine_os" == "Darwin" ]]; then
        local rosetta_translated
        rosetta_translated="$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)"
        if [[ "$rosetta_translated" == "1" ]]; then
          printf 'arm64\n'
          return
        fi
      fi
      printf 'x64\n'
      ;;
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

run_version_check() {
  local binary_path="$1"
  local timeout_seconds="$LORE_SELF_CHECK_TIMEOUT"
  local pid
  local elapsed=0

  if ! [[ "$timeout_seconds" =~ ^[0-9]+$ ]] || [[ "$timeout_seconds" -lt 1 ]]; then
    timeout_seconds=10
  fi

  "$binary_path" --version >/dev/null &
  pid=$!

  while kill -0 "$pid" >/dev/null 2>&1; do
    if [[ "$elapsed" -ge "$timeout_seconds" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  wait "$pid"
}

main() {
  parse_args "$@"
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
  local staged_path
  local backup_path

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
  staged_path="$(mktemp "$LORE_INSTALL_DIR/.lore.new.XXXXXX")"
  backup_path=""

  cp "$extracted_binary" "$staged_path" || {
    rm -f "$staged_path"
    die "Failed to stage lore binary"
  }
  chmod 755 "$staged_path" || {
    rm -f "$staged_path"
    die "Failed to set execute permission on staged binary"
  }

  if run_version_check "$staged_path"; then
    :
  else
    local check_status=$?
    rm -f "$staged_path"
    if [[ "$check_status" -eq 124 ]]; then
      die "Downloaded binary self-check timed out"
    fi
    die "Downloaded binary failed self-check"
  fi

  if [[ -f "$install_path" ]]; then
    backup_path="$(mktemp "$LORE_INSTALL_DIR/.lore.backup.XXXXXX")"
    cp "$install_path" "$backup_path" || {
      rm -f "$staged_path" "$backup_path"
      die "Failed to back up current lore binary"
    }
  fi

  mv -f "$staged_path" "$install_path" || {
    rm -f "$staged_path"
    [[ -n "$backup_path" ]] && rm -f "$backup_path"
    die "Failed to install lore binary"
  }

  if run_version_check "$install_path"; then
    :
  else
    local check_status=$?
    if [[ -n "$backup_path" ]]; then
      mv -f "$backup_path" "$install_path" || die "Installed binary failed self-check and rollback failed"
      if [[ "$check_status" -eq 124 ]]; then
        die "Installed binary self-check timed out and was rolled back"
      fi
      die "Installed binary failed self-check and was rolled back"
    fi
    if [[ "$check_status" -eq 124 ]]; then
      die "Installed binary self-check timed out"
    fi
    die "Installed binary failed self-check"
  fi

  [[ -n "$backup_path" ]] && rm -f "$backup_path"

  log "Installed lore ${release_version} to ${install_path}"
  if [[ ":$PATH:" != *":${LORE_INSTALL_DIR}:"* ]]; then
    log "Add ${LORE_INSTALL_DIR} to PATH to run lore globally"
  fi

  "$install_path" --version
}

main "$@"
