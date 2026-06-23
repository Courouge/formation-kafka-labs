#!/usr/bin/env bash
# Lit les messages Avro d'un topic CDC via kafka-avro-console-consumer.
# Usage : ./scripts/inspect-topic.sh <topic> [max-messages]
# Exemples :
#   ./scripts/inspect-topic.sh ecommerce.public.customers
#   ./scripts/inspect-topic.sh ecommerce.public.orders 20
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic> [max-messages]" >&2
  exit 1
fi

TOPIC="$1"
MAX="${2:-10}"

# kafka-avro-console-consumer est disponible dans l'image schema-registry.
docker exec -i schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --topic "${TOPIC}" \
  --from-beginning \
  --max-messages "${MAX}" \
  --property schema.registry.url=http://schema-registry:8081 \
  --property print.key=true \
  --property print.headers=true \
  --property key.separator=" | "
