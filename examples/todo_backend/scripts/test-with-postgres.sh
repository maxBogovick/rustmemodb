#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export DATABASE_BACKEND="${DATABASE_BACKEND:-postgres}"
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/todo_db}"
export TEST_DATABASE_URL="${TEST_DATABASE_URL:-$DATABASE_URL}"

cleanup() {
  docker compose down --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose up -d postgres

for _ in {1..30}; do
  if docker compose exec -T postgres pg_isready -U postgres -d todo_db >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

cargo run --bin migrate
cargo test --all-targets -- --nocapture
