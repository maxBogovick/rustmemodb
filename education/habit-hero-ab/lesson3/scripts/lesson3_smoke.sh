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

printf '\n[1/9] Healthcheck\n'
curl -sS -X GET "${base_url}/health" | pretty_print

printf '\n[2/9] Create user\n'
created_json=$(curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"lesson3@example.com","display_name":"Lesson Three"}')
printf '%s' "$created_json" | pretty_print
user_id=$(extract_json_field "$created_json" "id")
user_version=$(extract_json_field "$created_json" "version")

printf '\n[3/9] Get by id\n'
curl -sS -X GET "${base_url}/api/v1/users/${user_id}" | pretty_print

printf '\n[4/9] Patch without If-Match (must be 400 problem+json)\n'
curl -sS -X PATCH "${base_url}/api/v1/users/${user_id}" \
  -H "content-type: application/json" \
  -d '{"display_name":"Updated Name","active":false}' | pretty_print

printf '\n[5/9] Patch with stale If-Match (must be 409 problem+json)\n'
curl -sS -X PATCH "${base_url}/api/v1/users/${user_id}" \
  -H "if-match: 999" \
  -H "content-type: application/json" \
  -d '{"display_name":"Updated Name","active":false}' | pretty_print

printf '\n[6/9] Patch with current If-Match\n'
patched_json=$(curl -sS -X PATCH "${base_url}/api/v1/users/${user_id}" \
  -H "if-match: ${user_version}" \
  -H "content-type: application/json" \
  -d '{"display_name":"Updated Name","active":false}')
printf '%s' "$patched_json" | pretty_print
patched_version=$(extract_json_field "$patched_json" "version")

printf '\n[7/9] Delete with stale If-Match (must be 409 problem+json)\n'
curl -sS -X DELETE "${base_url}/api/v1/users/${user_id}" \
  -H "if-match: ${user_version}" | pretty_print

printf '\n[8/9] Delete with current If-Match\n'
curl -sS -X DELETE "${base_url}/api/v1/users/${user_id}" \
  -H "if-match: ${patched_version}" | pretty_print

printf '\n[9/9] Get deleted user (must be 404 problem+json)\n'
curl -sS -X GET "${base_url}/api/v1/users/${user_id}" | pretty_print

printf '\nDone\n'
