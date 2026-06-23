#!/usr/bin/env bash
# Supprime un connector (n'efface pas les topics ni les fichiers MinIO).
# Usage : ./scripts/delete-connector.sh <connector-name>
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <connector-name>" >&2
  exit 1
fi

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="$1"

echo "Delete connector ${NAME}..."
curl -fsS -X DELETE "${CONNECT_URL}/connectors/${NAME}" -o /dev/null -w "HTTP %{http_code}\n"

echo "Restant :"
curl -fsS "${CONNECT_URL}/connectors" | jq .
