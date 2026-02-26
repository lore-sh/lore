#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SOURCE="$ROOT_DIR/install.sh"
TARGET="$ROOT_DIR/packages/web/public/install"

cp "$SOURCE" "$TARGET"
chmod 644 "$TARGET"
