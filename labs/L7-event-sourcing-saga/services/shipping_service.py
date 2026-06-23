"""ShippingService — squelette à compléter (lab L7).

Responsabilités :
    1. Écouter orders.events filtré sur StockReserved (présence reservation_id, pas reason).
    2. Émettre OrderShipped avec un tracking_number.
    3. Après quelques secondes (simulation), émettre OrderDelivered.
"""

from __future__ import annotations

import logging
import random
import signal
import string
import threading
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
    string_deserializer,
    string_serializer,
)
from models import TOPIC_EVENTS, OrderDelivered, OrderShipped

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("shipping_service")

GROUP_ID = "shipping-service"
CLIENT_ID = "shipping-service"
DELIVERY_DELAY_SECONDS = 2

_running = True
_processed_event_ids: set[str] = set()


def _stop(*_: Any) -> None:
    global _running
    _running = False


def _random_tracking() -> str:
    return "TRK-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def _deliver_after_delay(producer, deliver_ser, key_ser, order_id: str, version: int) -> None:
    time.sleep(DELIVERY_DELAY_SECONDS)
    delivered = OrderDelivered(order_id=order_id, version=version + 1)
    ctx_v = SerializationContext(TOPIC_EVENTS, MessageField.VALUE)
    ctx_k = SerializationContext(TOPIC_EVENTS, MessageField.KEY)
    producer.produce(
        topic=TOPIC_EVENTS,
        key=key_ser(order_id, ctx_k),
        value=deliver_ser(delivered.model_dump(mode="json"), ctx_v),
    )
    producer.flush(timeout=5)
    log.info("OrderDelivered order_id=%s", order_id)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = make_consumer(GROUP_ID)
    consumer.subscribe([TOPIC_EVENTS])
    avro_deser = make_avro_deserializer()
    key_deser = string_deserializer()
    producer = make_producer(client_id=CLIENT_ID)
    shipped_ser = make_avro_serializer(load_schema("order_shipped"))
    deliver_ser = make_avro_serializer(load_schema("order_delivered"))
    key_ser = string_serializer()

    log.info("ShippingService listening on %s", TOPIC_EVENTS)
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
            except Exception as exc:
                log.error("deserialization error: %s", exc)
                consumer.commit(msg, asynchronous=False)
                continue

            event_id = value.get("event_id")
            # TODO 1 : idempotence

            # TODO 2 : ne traiter que StockReserved (champ reservation_id, sans reason)
            # if "reservation_id" not in value or "reason" in value:
            #     consumer.commit(msg, asynchronous=False)
            #     continue

            order_id = value.get("order_id")
            version = value.get("version", 1)

            # TODO 3 : émettre OrderShipped
            # shipped = OrderShipped(order_id=order_id, version=version+1,
            #                        tracking_number=_random_tracking())
            # producer.produce(topic=TOPIC_EVENTS,
            #                  key=key_ser(order_id, ctx_k),
            #                  value=shipped_ser(shipped.model_dump(mode="json"), ctx_v))
            # producer.flush(timeout=5)
            # log.info("OrderShipped order_id=%s tracking=%s", order_id, shipped.tracking_number)

            # TODO 4 : déclencher OrderDelivered après un délai (thread)
            # threading.Thread(target=_deliver_after_delay,
            #                  args=(producer, deliver_ser, key_ser, order_id, version+1),
            #                  daemon=True).start()

            consumer.commit(msg, asynchronous=False)
    finally:
        producer.flush(timeout=5)
        consumer.close()


if __name__ == "__main__":
    main()
