#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <base_url>"
  echo "Example: $0 http://127.0.0.1:18080"
  exit 1
fi

base_url="$1"

pretty_print() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  else
    cat
  fi
}

extract_json_field() {
  local json="$1"
  local field="$2"
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$json" | jq -r ".${field} // empty"
  else
    printf '%s' "$json" | sed -n "s/.*\"${field}\":\"\{0,1\}\([^\",}]*\)\"\{0,1\}.*/\1/p"
  fi
}

printf '\n[1/8] Healthcheck\n'
curl -sS -X GET "${base_url}/health" | pretty_print

printf '\n[2/8] Create user\n'
created_json=$(curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"lesson4@example.com","display_name":"Lesson Four"}')
printf '%s' "$created_json" | pretty_print
user_id=$(extract_json_field "$created_json" "id")
user_version=$(extract_json_field "$created_json" "version")

printf '\n[3/8] Single command without If-Match (must be 400)\n'
curl -sS -X POST "${base_url}/api/v1/users/${user_id}/commands" \
  -H "content-type: application/json" \
  -d '{"command":"deactivate"}' | pretty_print

printf '\n[4/8] Single command with current If-Match\n'
command_json=$(curl -sS -X POST "${base_url}/api/v1/users/${user_id}/commands" \
  -H "if-match: ${user_version}" \
  -H "content-type: application/json" \
  -d '{"command":"deactivate"}')
printf '%s' "$command_json" | pretty_print

printf '\n[5/8] List user events\n'
curl -sS -X GET "${base_url}/api/v1/users/${user_id}/events?limit=10" | pretty_print

printf '\n[6/8] Bulk command (activate existing + one missing)\n'
curl -sS -X POST "${base_url}/api/v1/users/commands/bulk-lifecycle" \
  -H "content-type: application/json" \
  -d "{\"ids\":[\"${user_id}\",\"00000000-0000-0000-0000-000000000000\"],\"command\":\"activate\"}" | pretty_print

printf '\n[7/8] Get user after bulk command\n'
curl -sS -X GET "${base_url}/api/v1/users/${user_id}" | pretty_print

printf '\n[8/8] List user events again\n'
curl -sS -X GET "${base_url}/api/v1/users/${user_id}/events?limit=10" | pretty_print

printf '\nDone\n'
