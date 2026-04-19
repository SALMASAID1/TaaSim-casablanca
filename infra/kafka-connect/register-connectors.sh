#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"

wait_for_connect() {
  echo "--- Waiting for Kafka Connect at ${CONNECT_URL} ---"

  i=0
  while ! curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then
      echo "ERROR: Kafka Connect not ready after 120s" >&2
      exit 1
    fi
    sleep 2
  done
}

put_connector() {
  name="$1"
  cfg_file="$2"

  echo "--- Applying connector config: ${name} ---"
  curl -sf -X PUT \
    -H "Content-Type: application/json" \
    --data "@${cfg_file}" \
    "${CONNECT_URL}/connectors/${name}/config" \
    >/dev/null
}

wait_for_connect

put_connector "s3-sink-raw-gps" "/connectors/s3-sink-raw-gps.json"
put_connector "s3-sink-raw-trips" "/connectors/s3-sink-raw-trips.json"

echo "--- Registered connectors ---"
curl -sf "${CONNECT_URL}/connectors" | cat
