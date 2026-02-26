#!/usr/bin/env bash
set -euo pipefail

bun run ./scripts/release/build-target.ts "$@"
