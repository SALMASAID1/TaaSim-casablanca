#!/bin/bash
# ---------------------------------------------------------------------------
# TaaSim — Generate self-signed TLS certificate for FastAPI HTTPS demo
#
# Sprint 5, Task 06
#
# Usage:
#   bash infra/ssl/generate_certs.sh
#
# Output:
#   infra/ssl/server.key   — RSA private key (2048-bit)
#   infra/ssl/server.crt   — Self-signed certificate (valid 365 days)
#
# The certificate is used by the API container for HTTPS (demo only).
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEY_FILE="${SCRIPT_DIR}/server.key"
CERT_FILE="${SCRIPT_DIR}/server.crt"

echo "Generating self-signed TLS certificate for TaaSim API..."

openssl req -x509 \
  -newkey rsa:2048 \
  -keyout "${KEY_FILE}" \
  -out "${CERT_FILE}" \
  -days 365 \
  -nodes \
  -subj "/C=MA/ST=Casablanca/L=Casablanca/O=TaaSim/OU=Engineering/CN=localhost"

echo "✅ Certificate generated:"
echo "   Key:  ${KEY_FILE}"
echo "   Cert: ${CERT_FILE}"
echo ""
echo "To use with the API container, ensure docker-compose.yml mounts these files."
