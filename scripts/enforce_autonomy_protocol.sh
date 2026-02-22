#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-}"
WORKSPACE_KEY="$(printf "%s" "${ROOT_DIR}" | shasum -a 256 | awk '{print substr($1,1,16)}')"
STATE_FILE="/tmp/rustmemodb_autonomy_preflight_state_${WORKSPACE_KEY}"

required_files=(
  "${ROOT_DIR}/llm/FAILURE_POSTMORTEM_AND_BANS.md"
  "${ROOT_DIR}/llm/STRICT_EXECUTION_PROTOCOL.md"
  "${ROOT_DIR}/llm/PRE_FINAL_RESPONSE_CHECKLIST.md"
  "${ROOT_DIR}/llm/AUTONOMY_DX_CONTRACT.md"
)

usage() {
  echo "Usage: $0 <pre|post>" >&2
  exit 2
}

if [[ "${MODE}" != "pre" && "${MODE}" != "post" ]]; then
  usage
fi

check_files_exist() {
  for file in "${required_files[@]}"; do
    if [[ ! -f "${file}" ]]; then
      echo "FAIL: required governance file is missing: ${file}" >&2
      exit 1
    fi
  done
}

print_hashes() {
  local hashes="$1"
  echo "Governance file fingerprints:"
  echo "${hashes}"
}

run_core_guard() {
  "${ROOT_DIR}/scripts/guard_lesson4_no_persistence_leak.sh"
}

compute_hashes() {
  shasum -a 256 "${required_files[@]}"
}

compute_bundle_hash() {
  local hashes="$1"
  printf "%s\n" "${hashes}" | shasum -a 256 | awk '{print $1}'
}

load_state() {
  if [[ ! -f "${STATE_FILE}" ]]; then
    echo "FAIL: postflight requires successful preflight in this workspace. Run: $0 pre" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "${STATE_FILE}"
  if [[ -z "${PRE_TS:-}" || -z "${PRE_BUNDLE_HASH:-}" ]]; then
    echo "FAIL: preflight state file has invalid format. Run: $0 pre" >&2
    exit 1
  fi
}

if [[ "${MODE}" == "pre" ]]; then
  check_files_exist
  HASHES="$(compute_hashes)"
  BUNDLE_HASH="$(compute_bundle_hash "${HASHES}")"
  PRE_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  print_hashes "${HASHES}"
  run_core_guard

  cat > "${STATE_FILE}" <<EOF
PRE_TS='${PRE_TS}'
PRE_BUNDLE_HASH='${BUNDLE_HASH}'
EOF
  echo "OK: preflight completed at ${PRE_TS}"
  echo "State file: ${STATE_FILE}"
  exit 0
fi

load_state
check_files_exist
HASHES="$(compute_hashes)"
BUNDLE_HASH="$(compute_bundle_hash "${HASHES}")"

if [[ "${BUNDLE_HASH}" != "${PRE_BUNDLE_HASH:-}" ]]; then
  echo "FAIL: governance files changed after preflight." >&2
  echo "Run '$0 pre' again to acknowledge updated governance context." >&2
  exit 1
fi

print_hashes "${HASHES}"
run_core_guard

echo "OK: postflight completed (preflight recorded at ${PRE_TS})"
echo "State file: ${STATE_FILE}"
