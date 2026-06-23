#!/usr/bin/env bash
# Enregistre le S3 sink connector qui matérialise le bronze layer sur MinIO.
# Usage : ./scripts/register-s3-sink.sh
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${SCRIPT_DIR}/../connectors/s3-sink-bronze.json"

echo "Deploying or updating S3 sink connector at ${CONNECT_URL}..."

curl -fsS -X PUT \
  -H "Content-Type: application/json" \
  --data "$(jq -c '.config' "${CONFIG}")" \
  "${CONNECT_URL}/connectors/s3-sink-bronze/config" | jq .

echo
echo "Status:"
sleep 2
curl -fsS "${CONNECT_URL}/connectors/s3-sink-bronze/status" | jq .
