#!/usr/bin/env bash
set -euo pipefail

# Kept as compatibility shim because lesson3 was copied to bootstrap lesson4.
exec "$(cd "$(dirname "$0")" && pwd)/lesson4_smoke.sh" "$@"
