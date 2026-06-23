"""Fixtures pytest pour les tests saga.

Ces fixtures publient une commande PlaceOrder et lisent les events qui en
résultent, sans démarrer les services en thread (on suppose qu'ils tournent
déjà via `python -m services.<name>` dans des terminaux séparés).
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

# Permet d'importer kafka_setup et models depuis le répertoire parent (lab root).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, Producer  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from kafka_setup import (  # noqa: E402
    BOOTSTRAP,
    make_avro_deserializer,
    make_consumer,
    make_producer,
    string_deserializer,
)
from models import TOPIC_COMMANDS, TOPIC_EVENTS  # noqa: E402


@pytest.fixture
def producer() -> Producer:
    return make_producer(client_id=f"test-producer-{uuid.uuid4().hex[:6]}")


@pytest.fixture
def event_consumer() -> Consumer:
    """Consumer dédié aux tests, group.id unique pour ne pas perturber les services."""
    consumer = make_consumer(group_id=f"test-{uuid.uuid4().hex[:8]}")
    consumer.subscribe([TOPIC_EVENTS])
    yield consumer
    consumer.close()


def publish_place_order(producer: Producer, *, customer_id: str = "cust-test",
                        items: list[dict] | None = None,
                        order_id: str | None = None) -> str:
    """Publie une command PlaceOrder en JSON sur orders.commands. Retourne l'order_id."""
    order_id = order_id or str(uuid.uuid4())
    items = items or [{"sku": "SKU-001", "quantity": 1, "unit_price": 50.0}]
    payload = {
        "command_type": "PlaceOrder",
        "order_id": order_id,
        "customer_id": customer_id,
        "items": items,
        "currency": "EUR",
    }
    producer.produce(
        topic=TOPIC_COMMANDS,
        key=order_id.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush(timeout=5)
    return order_id


def collect_events_for_order(consumer: Consumer, order_id: str, *,
                             expected_types: list[str], timeout_s: float = 30.0) -> list[dict]:
    """Lit orders.events jusqu'à voir tous les types attendus pour cet order_id ou timeout."""
    avro_deser = make_avro_deserializer()
    key_deser = string_deserializer()

    events: list[dict] = []
    seen_types: set[str] = set()
    deadline = time.time() + timeout_s

    while time.time() < deadline and not all(t in seen_types for t in expected_types):
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        ctx_v = SerializationContext(msg.topic(), MessageField.VALUE)
        ctx_k = SerializationContext(msg.topic(), MessageField.KEY)
        value = avro_deser(msg.value(), ctx_v)
        key = key_deser(msg.key(), ctx_k) if msg.key() else None

        if key != order_id:
            continue

        events.append(value)
        # Approximation de type via champs discriminants
        if "items" in value:
            seen_types.add("OrderCreated")
        elif "transaction_id" in value:
            seen_types.add("PaymentCompleted")
        elif value.get("reason") and "missing_sku" not in value:
            seen_types.add("PaymentFailed" if "items" not in value else "OrderCancelled")
            seen_types.add("OrderCancelled")
        elif "missing_sku" in value:
            seen_types.add("StockOutOfStock")
        elif "reservation_id" in value:
            seen_types.add("StockReserved")
        elif "tracking_number" in value:
            seen_types.add("OrderShipped")
        elif "delivered_at" in value:
            seen_types.add("OrderDelivered")

    return events
