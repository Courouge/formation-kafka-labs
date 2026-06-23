"""PaymentService — squelette à compléter (lab L7).

Responsabilités :
    1. Écouter orders.events filtré sur OrderCreated.
    2. Décider PaymentCompleted / PaymentFailed (règle simulée : refus si total > 1000).
    3. Idempotence : ne pas charger deux fois le même order_id.
    4. Compensation : sur OrderCancelled après PaymentCompleted, émettre un PaymentRefunded
       (proposé en challenge dans la solution complète).
"""

from __future__ import annotations

import logging
import signal
import uuid
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
from models import TOPIC_EVENTS, PaymentCompleted, PaymentFailed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("payment_service")

GROUP_ID = "payment-service"
CLIENT_ID = "payment-service"

MAX_AMOUNT = 1000.0  # règle simulée pour pédagogie

_running = True
_processed_event_ids: set[str] = set()
_charged_orders: set[str] = set()


def _stop(*_: Any) -> None:
    global _running
    _running = False


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = make_consumer(GROUP_ID)
    consumer.subscribe([TOPIC_EVENTS])
    avro_deser = make_avro_deserializer()
    key_deser = string_deserializer()
    producer = make_producer(client_id=CLIENT_ID)
    completed_ser = make_avro_serializer(load_schema("payment_completed"))
    failed_ser = make_avro_serializer(load_schema("payment_failed"))
    key_ser = string_serializer()

    log.info("PaymentService listening on %s", TOPIC_EVENTS)
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
            ctx_k = SerializationContext(msg.topic(), MessageField.KEY)
            try:
                value = avro_deser(msg.value(), ctx_v)
                key = key_deser(msg.key(), ctx_k) if msg.key() else None
            except Exception as exc:
                log.error("deserialization error: %s", exc)
                consumer.commit(msg, asynchronous=False)
                continue

            event_id = value.get("event_id")

            # TODO 1 : idempotence par event_id
            # if event_id in _processed_event_ids: ...
            # _processed_event_ids.add(event_id)

            # TODO 2 : ne traiter que les OrderCreated (présence du champ "items")
            # if "items" not in value:
            #     consumer.commit(msg, asynchronous=False)
            #     continue

            # TODO 3 : éviter le double charge sur un même order
            order_id = value.get("order_id")
            # if order_id in _charged_orders: ...
            # _charged_orders.add(order_id)

            total = value.get("total", 0.0)

            # TODO 4 : décider Completed vs Failed et publier
            # if total <= MAX_AMOUNT:
            #     evt = PaymentCompleted(
            #         order_id=order_id,
            #         version=value.get("version", 1) + 1,
            #         transaction_id=str(uuid.uuid4()),
            #         amount=total,
            #     )
            #     producer.produce(
            #         topic=TOPIC_EVENTS,
            #         key=key_ser(order_id, ctx_k),
            #         value=completed_ser(evt.model_dump(mode="json"), ctx_v),
            #     )
            # else:
            #     evt = PaymentFailed(
            #         order_id=order_id,
            #         version=value.get("version", 1) + 1,
            #         reason=f"amount {total} exceeds {MAX_AMOUNT}",
            #     )
            #     producer.produce(
            #         topic=TOPIC_EVENTS,
            #         key=key_ser(order_id, ctx_k),
            #         value=failed_ser(evt.model_dump(mode="json"), ctx_v),
            #     )
            # producer.poll(0)

            consumer.commit(msg, asynchronous=False)
    finally:
        producer.flush(timeout=5)
        consumer.close()


if __name__ == "__main__":
    main()
