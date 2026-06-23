#!/usr/bin/env bash
# Redémarre un connector et toutes ses tasks (utile après échec FAILED).
# Usage : ./scripts/restart-connector.sh <connector-name>
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <connector-name>" >&2
  exit 1
fi

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="$1"

echo "Restart connector ${NAME} (includeTasks=true, onlyFailed=false)..."
curl -fsS -X POST \
  "${CONNECT_URL}/connectors/${NAME}/restart?includeTasks=true&onlyFailed=false" \
  -o /dev/null -w "HTTP %{http_code}\n"

sleep 2
curl -fsS "${CONNECT_URL}/connectors/${NAME}/status" | jq .
