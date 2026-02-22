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

printf '\n[1/7] Healthcheck\n'
curl -sS -X GET "${base_url}/health" | pretty_print

printf '\n[2/7] Create Alice\n'
alice_json=$(curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"alice@example.com","display_name":"Alice"}')
printf '%s' "$alice_json" | pretty_print
if command -v jq >/dev/null 2>&1; then
  alice_id=$(printf '%s' "$alice_json" | jq -r '.id // empty')
else
  alice_id=$(printf '%s' "$alice_json" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
fi

printf '\n[3/7] Create Bob\n'
bob_json=$(curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"bob@example.com","display_name":"Bob"}')
printf '%s' "$bob_json" | pretty_print

printf '\n[4/7] Duplicate email (must be 409 problem+json)\n'
curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"alice@example.com","display_name":"Alice Duplicate"}' | pretty_print

printf '\n[5/7] Get user by id\n'
curl -sS -X GET "${base_url}/api/v1/users/${alice_id}" | pretty_print

printf '\n[6/7] List users sorted by email asc (page 1, per_page 2)\n'
curl -sS -X GET "${base_url}/api/v1/users?page=1&per_page=2&sort_by=email&order=asc" | pretty_print

printf '\n[7/7] Filter users email_contains=bob active=true\n'
curl -sS -X GET "${base_url}/api/v1/users?page=1&per_page=10&email_contains=bob&active=true" | pretty_print

printf '\nDone\n'
