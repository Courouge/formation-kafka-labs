"""Snapshot writer — squelette à compléter (lab L7 étape 9).

Lit orders.events, maintient l'agrégat de chaque order en mémoire, et publie
périodiquement un OrderSnapshot dans orders.snapshots (compaction enabled).

Au démarrage d'un service consommateur :
    1. lire orders.snapshots (lecture rapide, dernière version par order_id),
    2. puis rejouer orders.events depuis la version du snapshot.
"""

from __future__ import annotations

import logging
import signal
import time
from typing import Any

from confluent_kafka import KafkaError
from confluent_kafka.serialization import MessageField, SerializationContext

from kafka_setup import (
    load_schema,
    make_avro_deserializer,
    make_avro_serializer,
    make_consumer,
    make_producer,
    string_serializer,
)
from models import TOPIC_EVENTS, TOPIC_SNAPSHOTS, OrderState

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("snapshot_writer")

GROUP_ID = "snapshot-writer"
CLIENT_ID = "snapshot-writer"
SNAPSHOT_EVERY_N = 5
SNAPSHOT_FLUSH_SECONDS = 5

_running = True


def _stop(*_: Any) -> None:
    global _running
    _running = False


def _to_snapshot_payload(state: OrderState) -> dict:
    return {
        "order_id": state.order_id,
        "version": state.version,
        "status": state.status.value,
        "customer_id": state.customer_id,
        "total": state.total,
        "currency": state.currency,
        "snapshot_at": int(time.time() * 1000),
        "transaction_id": state.transaction_id,
        "reservation_id": state.reservation_id,
        "tracking_number": state.tracking_number,
        "cancel_reason": state.cancel_reason,
    }


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = make_consumer(GROUP_ID)
    consumer.subscribe([TOPIC_EVENTS])
    avro_deser = make_avro_deserializer()
    producer = make_producer(client_id=CLIENT_ID)
    snap_ser = make_avro_serializer(load_schema("order_snapshot"))
    key_ser = string_serializer()

    states: dict[str, OrderState] = {}
    counters: dict[str, int] = {}

    log.info("snapshot writer started (every %d events or %ds)",
             SNAPSHOT_EVERY_N, SNAPSHOT_FLUSH_SECONDS)
    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            ctx_v = SerializationContext(msg.topic(), MessageField.VALUE)
            value = avro_deser(msg.value(), ctx_v)

            order_id = value.get("order_id")
            # TODO 1 : appliquer l'event sur l'agrégat (réutiliser apply_event de replay_order)
            # state = apply_event(states.get(order_id, OrderState(order_id=order_id)), value)
            # states[order_id] = state
            # counters[order_id] = counters.get(order_id, 0) + 1

            # TODO 2 : si counters[order_id] >= SNAPSHOT_EVERY_N, publier le snapshot
            # if counters[order_id] >= SNAPSHOT_EVERY_N:
            #     ctx_snap_v = SerializationContext(TOPIC_SNAPSHOTS, MessageField.VALUE)
            #     ctx_snap_k = SerializationContext(TOPIC_SNAPSHOTS, MessageField.KEY)
            #     producer.produce(
            #         topic=TOPIC_SNAPSHOTS,
            #         key=key_ser(order_id, ctx_snap_k),
            #         value=snap_ser(_to_snapshot_payload(state), ctx_snap_v),
            #     )
            #     counters[order_id] = 0
            #     producer.poll(0)

            consumer.commit(msg, asynchronous=False)
    finally:
        producer.flush(timeout=10)
        consumer.close()


if __name__ == "__main__":
    main()
