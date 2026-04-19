#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
RETENTION_MS="${RETENTION_MS:-604800000}"

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic raw.gps           --partitions 4 --replication-factor 1 --config "retention.ms=$RETENTION_MS"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic raw.trips         --partitions 4 --replication-factor 1 --config "retention.ms=$RETENTION_MS"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic processed.demand  --partitions 2 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic processed.gps     --partitions 4 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic processed.matches --partitions 2 --replication-factor 1

echo '--- Topics created ---'
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list
