#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_APP="${ROOT_DIR}/education/habit-hero-ab/lesson4/product-api/src/application"

if [[ ! -d "${TARGET_APP}" ]]; then
  echo "ERROR: target path not found: ${TARGET_APP}" >&2
  exit 2
fi

if [[ -f "${TARGET_APP}/user_workspace_store.rs" ]]; then
  echo "FAIL: forbidden adapter file exists: ${TARGET_APP}/user_workspace_store.rs" >&2
  exit 1
fi

PATTERN='snapshot_for_external_transaction|restore_snapshot_for_external_transaction|shared_session|on_external_mutation_committed|PersistSession|_with_session\(|_with_tx\(|open_user_workspace_store|UserWorkspaceStore|PersistUserStore|Repository|execute_intent_if_match_auto_audit|execute_intent_for_many_auto_audit'

if rg -n "${PATTERN}" "${TARGET_APP}"; then
  echo "FAIL: forbidden persistence-leak patterns found in lesson4 product application layer." >&2
  exit 1
fi

echo "OK: lesson4 product application layer has no forbidden persistence-leak patterns."
