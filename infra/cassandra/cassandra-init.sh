#!/bin/bash
set -euo pipefail

until cqlsh cassandra 9042 -e "describe cluster" > /dev/null 2>&1; do
  echo "Waiting for Cassandra..."
  sleep 5
done

CQL_FILE="${CQL_FILE:-/init/cassandra_init.cql}"

if [ ! -f "$CQL_FILE" ]; then
  echo "ERROR: CQL init file not found: $CQL_FILE" >&2
  exit 1
fi

cqlsh cassandra 9042 -f "$CQL_FILE"

echo '--- Cassandra schema applied ---'
