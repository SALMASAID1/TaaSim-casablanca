#!/usr/bin/env bash
set -euo pipefail

# Installs the Confluent S3 Sink connector if missing, then starts Kafka Connect.
#
# NOTE: This downloads the connector from Confluent Hub at first start.
# If you run in an offline environment, you must pre-bundle the plugin.

CONNECT_S3_VERSION="${CONNECT_S3_VERSION:-10.5.0}"

PLUGIN_DIR="/usr/share/confluent-hub-components/confluentinc-kafka-connect-s3"

if [ ! -d "$PLUGIN_DIR" ]; then
  echo "--- Installing confluentinc/kafka-connect-s3:${CONNECT_S3_VERSION} ---"
  confluent-hub install --no-prompt "confluentinc/kafka-connect-s3:${CONNECT_S3_VERSION}"
else
  echo "--- S3 connector already installed (${PLUGIN_DIR}) ---"
fi

exec /etc/confluent/docker/run
