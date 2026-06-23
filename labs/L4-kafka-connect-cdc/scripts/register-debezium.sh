#!/usr/bin/env bash
# Enregistre le source connector Debezium Postgres auprès de Connect.
# Usage : ./scripts/register-debezium.sh
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="${SCRIPT_DIR}/../connectors/debezium-postgres-source.json"

echo "Deploying or updating Debezium source connector at ${CONNECT_URL}..."

curl -fsS -X PUT \
  -H "Content-Type: application/json" \
  --data "$(jq -c '.config' "${CONFIG}")" \
  "${CONNECT_URL}/connectors/debezium-postgres-source/config" | jq .

echo
echo "Status:"
sleep 2
curl -fsS "${CONNECT_URL}/connectors/debezium-postgres-source/status" | jq .
