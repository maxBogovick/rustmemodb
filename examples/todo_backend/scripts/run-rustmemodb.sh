#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export DATABASE_BACKEND="${DATABASE_BACKEND:-rustmemodb}"
export DATABASE_URL="${DATABASE_URL:-rustmemodb://admin:adminpass@localhost:5432/rustmemodb}"
export RUSTMEMODB_USERNAME="${RUSTMEMODB_USERNAME:-admin}"
export RUSTMEMODB_PASSWORD="${RUSTMEMODB_PASSWORD:-adminpass}"
export APP_HOST="${APP_HOST:-0.0.0.0}"
export APP_PORT="${APP_PORT:-8080}"
export DB_MAX_CONNECTIONS="${DB_MAX_CONNECTIONS:-10}"

cargo run --bin todo_backend
