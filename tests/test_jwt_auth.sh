#!/usr/bin/env bash

set -euo pipefail

BASE_URL="${TAASIM_API_URL:-http://localhost:8000}"
JWT_SECRET="${JWT_SECRET:-taasim-dev-jwt-secret}"

login_token() {
  local username="$1"
  local password="$2"

  curl -sS \
    -d "username=${username}" -d "password=${password}" \
    "${BASE_URL}/auth/token" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])'
}

make_expired_token() {
  JWT_SECRET="$JWT_SECRET" python3 - <<'PY'
import base64
import hashlib
import hmac
import json
import os
import time

secret = os.environ["JWT_SECRET"].encode("utf-8")
header = {"alg": "HS256", "typ": "JWT"}
payload = {"sub": "admin", "role": "admin", "exp": int(time.time()) - 60}

def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

head = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
body = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
signature = hmac.new(secret, f"{head}.{body}".encode("ascii"), hashlib.sha256).digest()
print(f"{head}.{body}.{b64url(signature)}")
PY
}

assert_status() {
  local expected="$1"
  shift

  local response_file
  response_file="$(mktemp)"
  local actual
  actual="$(curl -sS -o "$response_file" -w '%{http_code}' "$@")"
  if [[ "$actual" != "$expected" ]]; then
    echo "Expected HTTP ${expected} but got ${actual}" >&2
    echo "--- Response body ---" >&2
    cat "$response_file" >&2
    rm -f "$response_file"
    exit 1
  fi
  rm -f "$response_file"
}

echo "Fetching demo tokens..."
rider_token="$(login_token rider1 riderpass)"
admin_token="$(login_token admin adminpass)"
expired_token="$(make_expired_token)"
tampered_token="${admin_token%?}x"

echo "Verifying JWT protection..."
assert_status 401 "$BASE_URL/api/v1/vehicles/zone/1"
assert_status 403 "$BASE_URL/api/v1/vehicles/zone/1" -H "Authorization: Bearer ${rider_token}"
assert_status 200 "$BASE_URL/api/v1/vehicles/zone/1" -H "Authorization: Bearer ${admin_token}"
assert_status 202 -X POST "$BASE_URL/api/v1/trips" -H 'Content-Type: application/json' -H "Authorization: Bearer ${rider_token}" -d '{"origin_zone":1,"destination_zone":2,"rider_id":"rider1"}'
assert_status 202 -X POST "$BASE_URL/api/v1/trips" -H 'Content-Type: application/json' -H "Authorization: Bearer ${admin_token}" -d '{"origin_zone":1,"destination_zone":2,"rider_id":"admin"}'
assert_status 401 -X POST "$BASE_URL/api/v1/trips" -H 'Content-Type: application/json' -H "Authorization: Bearer ${tampered_token}" -d '{"origin_zone":1,"destination_zone":2,"rider_id":"admin"}'
assert_status 401 -X POST "$BASE_URL/api/v1/trips" -H 'Content-Type: application/json' -H "Authorization: Bearer ${expired_token}" -d '{"origin_zone":1,"destination_zone":2,"rider_id":"admin"}'

assert_status 403 -X POST "$BASE_URL/api/v1/demand/forecast" -H "Authorization: Bearer ${rider_token}" -d ''
assert_status 202 -X POST "$BASE_URL/api/v1/demand/forecast" -H "Authorization: Bearer ${admin_token}" -d ''

echo "JWT auth smoke tests passed."