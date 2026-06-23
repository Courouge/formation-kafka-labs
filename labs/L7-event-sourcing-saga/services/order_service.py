"""OrderService — squelette à compléter (lab L7).

Responsabilités :
    1. Lire les commandes PlaceOrder sur orders.commands.
    2. Émettre OrderCreated sur orders.events (clé = order_id).
    3. Écouter orders.events et réagir en aval :
       - PaymentFailed / StockOutOfStock -> émettre OrderCancelled.
       - OrderShipped / OrderDelivered  -> mettre à jour l'état interne (logging).

Idempotence : maintenir un set d'event_id déjà traités.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
from typing import Any

from confluent_kafka import KafkaError
from confluent_kafka.serialization import MessageField, SerializationContext

from kafka_setup import (
    load_schema,
    make_avro_deserializer,
    make_avro_serializer,
    make_consumer,
    make_producer,
    string_deserializer,
    string_serializer,
)
from models import (
    TOPIC_COMMANDS,
    TOPIC_EVENTS,
    OrderCancelled,
    OrderCreated,
    PlaceOrder,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("order_service")

GROUP_COMMANDS = "order-service-commands"
GROUP_EVENTS = "order-service-events"
CLIENT_ID = "order-service"


_running = True
_processed_event_ids: set[str] = set()


def _stop(*_: Any) -> None:
    global _running
    _running = False


def consume_commands() -> None:
    """Lit orders.commands (sérialisé en JSON pour les commandes — léger pour le lab)
    et émet un OrderCreated en réponse à un PlaceOrder.
    """
    consumer = make_consumer(GROUP_COMMANDS)
    consumer.subscribe([TOPIC_COMMANDS])
    producer = make_producer(client_id=f"{CLIENT_ID}-cmd")
    key_ser = string_serializer()
    value_ser = make_avro_serializer(load_schema("order_created"))

    log.info("OrderService listening on %s", TOPIC_COMMANDS)
    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue

            try:
                payload = json.loads(msg.value())
                command = PlaceOrder(**payload)
            except Exception as exc:
                log.error("Invalid command payload: %s", exc)
                consumer.commit(msg, asynchronous=False)
                continue

            # TODO 1 : construire un OrderCreated à partir de la command
            #     event = OrderCreated(
            #         order_id=command.order_id,
            #         version=1,
            #         customer_id=command.customer_id,
            #         items=command.items,
            #         total=command.total,
            #         currency=command.currency,
            #     )

            # TODO 2 : produire l'event sur TOPIC_EVENTS avec key=order_id
            #     ctx_v = SerializationContext(TOPIC_EVENTS, MessageField.VALUE)
            #     ctx_k = SerializationContext(TOPIC_EVENTS, MessageField.KEY)
            #     producer.produce(
            #         topic=TOPIC_EVENTS,
            #         key=key_ser(command.order_id, ctx_k),
            #         value=value_ser(event.model_dump(mode="json"), ctx_v),
            #     )
            #     producer.poll(0)

            consumer.commit(msg, asynchronous=False)
    finally:
        producer.flush(timeout=5)
        consumer.close()


def consume_events() -> None:
    """Lit orders.events pour réagir aux décisions des autres services."""
    consumer = make_consumer(GROUP_EVENTS)
    consumer.subscribe([TOPIC_EVENTS])
    avro_deser = make_avro_deserializer()
    key_deser = string_deserializer()
    producer = make_producer(client_id=f"{CLIENT_ID}-evt")
    cancel_ser = make_avro_serializer(load_schema("order_cancelled"))
    key_ser = string_serializer()

    log.info("OrderService reactor listening on %s", TOPIC_EVENTS)
    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            ctx_v = SerializationContext(msg.topic(), MessageField.VALUE)
            ctx_k = SerializationContext(msg.topic(), MessageField.KEY)
            try:
                value = avro_deser(msg.value(), ctx_v)
                key = key_deser(msg.key(), ctx_k) if msg.key() else None
            except Exception as exc:
                log.error("Deserialization failed: %s", exc)
                consumer.commit(msg, asynchronous=False)
                continue

            event_id = value.get("event_id")
            # TODO 3 : idempotence — skip si déjà traité
            #     if event_id in _processed_event_ids:
            #         consumer.commit(msg, asynchronous=False)
            #         continue
            #     _processed_event_ids.add(event_id)

            # TODO 4 : matcher sur le type de record Avro pour déclencher les compensations
            # Indication : les schémas définis dans schemas/*.avsc utilisent un namespace
            # commun ; vous pouvez détecter le type via une convention (par exemple en
            # ajoutant un champ event_type, ou en vous appuyant sur la présence de champs
            # discriminants : reason, missing_sku, tracking_number, ...).
            #
            # Exemple :
            #     if "reason" in value and "missing_sku" not in value:
            #         # PaymentFailed -> annuler
            #         _publish_cancelled(producer, cancel_ser, key_ser, key, value, "payment_failed")
            #     elif "missing_sku" in value:
            #         # StockOutOfStock -> annuler
            #         _publish_cancelled(producer, cancel_ser, key_ser, key, value, "out_of_stock")

            consumer.commit(msg, asynchronous=False)
    finally:
        producer.flush(timeout=5)
        consumer.close()


def _publish_cancelled(producer, value_ser, key_ser, order_id: str, source_event: dict, reason: str) -> None:
    cancelled = OrderCancelled(
        order_id=order_id,
        version=source_event.get("version", 1) + 1,
        reason=reason,
    )
    ctx_v = SerializationContext(TOPIC_EVENTS, MessageField.VALUE)
    ctx_k = SerializationContext(TOPIC_EVENTS, MessageField.KEY)
    producer.produce(
        topic=TOPIC_EVENTS,
        key=key_ser(order_id, ctx_k),
        value=value_ser(cancelled.model_dump(mode="json"), ctx_v),
    )
    producer.poll(0)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    threads = [
        threading.Thread(target=consume_commands, daemon=True, name="orders-cmd"),
        threading.Thread(target=consume_events, daemon=True, name="orders-evt"),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
