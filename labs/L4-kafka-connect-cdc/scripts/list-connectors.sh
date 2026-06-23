#!/usr/bin/env bash
# Liste les connectors et leur état (RUNNING/PAUSED/FAILED) avec leur task count.
# Usage : ./scripts/list-connectors.sh
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

mapfile -t CONNECTORS < <(curl -fsS "${CONNECT_URL}/connectors" | jq -r '.[]')

if [[ ${#CONNECTORS[@]} -eq 0 ]]; then
  echo "Aucun connector déployé sur ${CONNECT_URL}."
  exit 0
fi

printf "%-35s %-12s %-12s %s\n" "NAME" "STATE" "TASKS" "WORKER"
printf "%-35s %-12s %-12s %s\n" "----" "-----" "-----" "------"

for c in "${CONNECTORS[@]}"; do
  STATUS_JSON=$(curl -fsS "${CONNECT_URL}/connectors/${c}/status")
  STATE=$(echo "${STATUS_JSON}" | jq -r '.connector.state')
  WORKER=$(echo "${STATUS_JSON}" | jq -r '.connector.worker_id')
  TASKS=$(echo "${STATUS_JSON}" | jq -r '[.tasks[].state] | join(",")')
  printf "%-35s %-12s %-12s %s\n" "${c}" "${STATE}" "${TASKS:-none}" "${WORKER}"
done
