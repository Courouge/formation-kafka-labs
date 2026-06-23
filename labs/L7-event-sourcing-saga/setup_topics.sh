#!/usr/bin/env bash
# Crée les topics du lab L7 avec les bonnes configurations.
# - orders.commands : 3 partitions, RF=3, rétention 7 jours
# - orders.events   : 6 partitions, RF=3, rétention 30 jours (audit log + replay)
# - orders.snapshots: 3 partitions, RF=3, COMPACTÉ (dernière vue par order_id)

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka1}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-kafka1:29092}"

run_kafka() {
    docker exec -e KAFKA_OPTS= "$KAFKA_CONTAINER" "$@"
}

create_or_alter() {
    local topic="$1"
    local partitions="$2"
    local rf="$3"
    shift 3
    local extra_configs=("$@")

    if run_kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep -q "^${topic}$"; then
        echo "[=] topic ${topic} déjà présent, skip create"
    else
        echo "[+] création topic ${topic} (partitions=${partitions}, RF=${rf})"
        local cmd=(kafka-topics --bootstrap-server "$BOOTSTRAP" --create
                   --topic "$topic" --partitions "$partitions" --replication-factor "$rf")
        for cfg in "${extra_configs[@]}"; do
            cmd+=(--config "$cfg")
        done
        run_kafka "${cmd[@]}"
    fi
}

create_or_alter "orders.commands" 3 3 \
    "retention.ms=604800000"

create_or_alter "orders.events" 6 3 \
    "retention.ms=2592000000"

create_or_alter "orders.snapshots" 3 3 \
    "cleanup.policy=compact" \
    "min.cleanable.dirty.ratio=0.1" \
    "segment.ms=60000" \
    "delete.retention.ms=100"

echo
echo "=== topics créés ==="
run_kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep "^orders\."
echo
echo "=== description orders.snapshots ==="
run_kafka kafka-configs --bootstrap-server "$BOOTSTRAP" \
    --entity-type topics --entity-name orders.snapshots --describe
