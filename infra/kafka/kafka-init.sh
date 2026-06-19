#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
RETENTION_MS="${RETENTION_MS:-604800000}"

CMD_CFG="--command-config /init/admin.properties"

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --create --if-not-exists --topic raw.gps           --partitions 4 --replication-factor 1 --config "retention.ms=$RETENTION_MS"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --create --if-not-exists --topic raw.trips         --partitions 4 --replication-factor 1 --config "retention.ms=$RETENTION_MS"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --create --if-not-exists --topic processed.demand  --partitions 2 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --create --if-not-exists --topic processed.gps     --partitions 4 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --create --if-not-exists --topic processed.matches --partitions 2 --replication-factor 1

echo '--- Topics created ---'
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --list

echo '--- Creating ACLs ---'
# GPS Producer
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --add --allow-principal User:gps-producer --operation Write --topic raw.gps
# Trip Producer
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --add --allow-principal User:trip-producer --operation Write --topic raw.trips
# Flink Jobs
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --add --allow-principal User:flink --operation Read --topic "raw." --resource-pattern-type prefixed
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --add --allow-principal User:flink --operation Read --topic "processed." --resource-pattern-type prefixed
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --add --allow-principal User:flink --operation Write --topic "processed." --resource-pattern-type prefixed
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --add --allow-principal User:flink --operation Write --topic "raw.unmatched"
# Admin
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --add --allow-principal User:admin --operation All --topic "*" --resource-pattern-type prefixed

echo '--- ACLs created ---'
mkdir -p /seed/metadata/docs || true
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" $CMD_CFG --list > /seed/metadata/docs/kafka-acls-list.txt
cat /seed/metadata/docs/kafka-acls-list.txt
