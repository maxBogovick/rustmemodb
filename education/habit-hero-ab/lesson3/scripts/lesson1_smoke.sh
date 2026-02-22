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

printf '\n[1/4] Healthcheck\n'
curl -sS -X GET "${base_url}/health" | pretty_print

printf '\n[2/4] Create user\n'
curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"alice@example.com","display_name":"Alice"}' | pretty_print

printf '\n[3/4] Duplicate email (must be 409 problem+json)\n'
curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"alice@example.com","display_name":"Alice Duplicate"}' | pretty_print

printf '\n[4/4] Invalid email (must be 400 problem+json)\n'
curl -sS -X POST "${base_url}/api/v1/users" \
  -H "content-type: application/json" \
  -d '{"email":"not-email","display_name":"Broken"}' | pretty_print

printf '\nDone\n'
