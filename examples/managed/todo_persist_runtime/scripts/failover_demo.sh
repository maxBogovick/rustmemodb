#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="$(cd -- "${EXAMPLE_DIR}/../../.." && pwd)"
MANIFEST_PATH="${EXAMPLE_DIR}/Cargo.toml"

PRIMARY_PORT="${PRIMARY_PORT:-18090}"
FAILOVER_PORT="${FAILOVER_PORT:-18091}"
DATA_ROOT="${DATA_ROOT:-${EXAMPLE_DIR}/.data/failover_demo}"
PRIMARY_DIR="${DATA_ROOT}/primary"
REPLICA_DIR="${DATA_ROOT}/replica"
PRIMARY_LOG="${DATA_ROOT}/primary.log"
FAILOVER_LOG="${DATA_ROOT}/failover.log"

PRIMARY_PID=""
FAILOVER_PID=""

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_http_ready() {
  local url="$1"
  local name="$2"
  local log_file="$3"
  local attempts=120

  for ((i = 1; i <= attempts; i++)); do
    if curl --silent --fail "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done

  echo "service did not become ready: $name ($url)" >&2
  if [[ -f "$log_file" ]]; then
    echo "--- tail $log_file ---" >&2
    tail -n 40 "$log_file" >&2 || true
  fi
  exit 1
}

stop_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  set +e
  stop_pid "$PRIMARY_PID"
  stop_pid "$FAILOVER_PID"
}
trap cleanup EXIT INT TERM

need_cmd cargo
need_cmd curl

if [[ -z "$DATA_ROOT" || "$DATA_ROOT" == "/" ]]; then
  echo "unsafe DATA_ROOT value: '$DATA_ROOT'" >&2
  exit 1
fi

mkdir -p "$DATA_ROOT"
rm -rf "$PRIMARY_DIR" "$REPLICA_DIR" "$PRIMARY_LOG" "$FAILOVER_LOG"
mkdir -p "$PRIMARY_DIR" "$REPLICA_DIR"

echo "starting primary node on 127.0.0.1:${PRIMARY_PORT}"
(
  cd "$REPO_DIR"
  TODO_BIND_ADDR="127.0.0.1:${PRIMARY_PORT}" \
  TODO_DATA_DIR="$PRIMARY_DIR" \
  TODO_DURABILITY_MODE="strict" \
  TODO_SNAPSHOT_EVERY_OPS="1" \
  TODO_REPLICATION_MODE="sync" \
  TODO_REPLICA_DIRS="$REPLICA_DIR" \
  cargo run --manifest-path "$MANIFEST_PATH"
) >"$PRIMARY_LOG" 2>&1 &
PRIMARY_PID=$!

wait_http_ready "http://127.0.0.1:${PRIMARY_PORT}/health" "primary" "$PRIMARY_LOG"
echo "primary is ready"

CREATE_RESPONSE="$(curl --silent --show-error --fail \
  -X POST "http://127.0.0.1:${PRIMARY_PORT}/api/v1/todos" \
  -H "content-type: application/json" \
  -d '{"title":"Replication failover demo","priority":5}')"

TODO_ID="$(echo "$CREATE_RESPONSE" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
if [[ -z "$TODO_ID" ]]; then
  echo "failed to extract todo id from create response: $CREATE_RESPONSE" >&2
  exit 1
fi
echo "created todo id=$TODO_ID on primary"

echo "stopping primary to simulate failover"
stop_pid "$PRIMARY_PID"
PRIMARY_PID=""

echo "starting promoted replica node on 127.0.0.1:${FAILOVER_PORT}"
(
  cd "$REPO_DIR"
  TODO_BIND_ADDR="127.0.0.1:${FAILOVER_PORT}" \
  TODO_DATA_DIR="$REPLICA_DIR" \
  TODO_DURABILITY_MODE="strict" \
  TODO_SNAPSHOT_EVERY_OPS="1" \
  TODO_REPLICATION_MODE="sync" \
  TODO_REPLICA_DIRS="" \
  cargo run --manifest-path "$MANIFEST_PATH"
) >"$FAILOVER_LOG" 2>&1 &
FAILOVER_PID=$!

wait_http_ready "http://127.0.0.1:${FAILOVER_PORT}/health" "promoted replica" "$FAILOVER_LOG"
echo "promoted replica is ready"

LIST_RESPONSE="$(curl --silent --show-error --fail "http://127.0.0.1:${FAILOVER_PORT}/api/v1/todos")"
if ! echo "$LIST_RESPONSE" | grep -q "$TODO_ID"; then
  echo "recovery check failed, todo id not found on promoted replica" >&2
  echo "response: $LIST_RESPONSE" >&2
  exit 1
fi

echo "failover successful: todo recovered on promoted replica"
echo "primary log:  $PRIMARY_LOG"
echo "failover log: $FAILOVER_LOG"
