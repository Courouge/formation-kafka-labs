"""StockService — squelette à compléter (lab L7).

Responsabilités :
    1. Écouter orders.events filtré sur PaymentCompleted (présence de transaction_id).
    2. Pour chaque order, charger le stock courant (en mémoire pour le lab).
    3. Si stock OK pour tous les SKU -> StockReserved et décrémenter.
    4. Sinon -> StockOutOfStock (avec missing_sku).
    5. Compensation : sur OrderCancelled après StockReserved, émettre StockReleased
       et restituer la quantité.

Note : pour récupérer les items d'un order, il faut idéalement maintenir un cache
order_id -> items en consommant aussi OrderCreated.
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
from models import TOPIC_EVENTS, StockOutOfStock, StockReleased, StockReserved

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("stock_service")

GROUP_ID = "stock-service"
CLIENT_ID = "stock-service"

# Stock simulé en mémoire — en prod : Postgres ou Redis avec verrou optimiste.
_stock: dict[str, int] = {
    "SKU-001": 10,
    "SKU-002": 5,
    "SKU-003": 0,  # toujours en rupture, pour le test de compensation
}
_orders_cache: dict[str, list[dict]] = {}  # order_id -> items
_reservations: dict[str, dict[str, int]] = {}  # order_id -> {sku: qty}
_processed_event_ids: set[str] = set()

_running = True


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
    reserved_ser = make_avro_serializer(load_schema("stock_reserved"))
    out_ser = make_avro_serializer(load_schema("stock_out_of_stock"))
    released_ser = make_avro_serializer(load_schema("stock_released"))
    key_ser = string_serializer()

    log.info("StockService listening on %s (initial stock=%s)", TOPIC_EVENTS, _stock)
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
            # if event_id in _processed_event_ids: ...

            order_id = value.get("order_id")

            # TODO 2 : si OrderCreated (champ items présent), mémoriser les items
            # if "items" in value:
            #     _orders_cache[order_id] = value["items"]
            #     consumer.commit(...) ; continue

            # TODO 3 : si PaymentCompleted (champ transaction_id), tenter la réservation
            # if "transaction_id" in value:
            #     items = _orders_cache.get(order_id, [])
            #     missing = next((i["sku"] for i in items
            #                     if _stock.get(i["sku"], 0) < i["quantity"]), None)
            #     if missing is None:
            #         for i in items:
            #             _stock[i["sku"]] -= i["quantity"]
            #         _reservations[order_id] = {i["sku"]: i["quantity"] for i in items}
            #         evt = StockReserved(order_id=order_id, version=value["version"]+1,
            #                             reservation_id=str(uuid.uuid4()))
            #         producer.produce(topic=TOPIC_EVENTS,
            #                          key=key_ser(order_id, ctx_k),
            #                          value=reserved_ser(evt.model_dump(mode="json"), ctx_v))
            #     else:
            #         evt = StockOutOfStock(order_id=order_id, version=value["version"]+1,
            #                               missing_sku=missing)
            #         producer.produce(topic=TOPIC_EVENTS,
            #                          key=key_ser(order_id, ctx_k),
            #                          value=out_ser(evt.model_dump(mode="json"), ctx_v))
            #     producer.poll(0)

            # TODO 4 : si OrderCancelled et qu'on avait une réservation, émettre StockReleased
            # if "reason" in value and order_id in _reservations:
            #     for sku, qty in _reservations[order_id].items():
            #         _stock[sku] = _stock.get(sku, 0) + qty
            #     released = StockReleased(order_id=order_id, version=value["version"]+1,
            #                              reservation_id="released-"+order_id)
            #     producer.produce(topic=TOPIC_EVENTS,
            #                      key=key_ser(order_id, ctx_k),
            #                      value=released_ser(released.model_dump(mode="json"), ctx_v))
            #     del _reservations[order_id]
            #     producer.poll(0)

            consumer.commit(msg, asynchronous=False)
    finally:
        producer.flush(timeout=5)
        consumer.close()


if __name__ == "__main__":
    main()
