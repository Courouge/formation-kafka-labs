"""Producer Kafka avec sérialisation Avro et Schema Registry.

À compléter par l'apprenant : zones marquées TODO.
"""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer_avro")

# --- Configuration -----------------------------------------------------------
BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders.avro"
SCHEMA_FILE = Path(__file__).parent / "schemas" / "order_v1.avsc"
NB_MESSAGES = 20


def order_to_dict(order: dict[str, Any], ctx: SerializationContext) -> dict[str, Any]:
    """Hook appelé par AvroSerializer pour transformer l'objet Python en dict Avro."""
    return order


def delivery_report(err, msg) -> None:
    if err is not None:
        log.error("delivery failed: %s", err)
        return
    log.info(
        "delivered topic=%s partition=%s offset=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def build_order(i: int) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "customer_id": f"cust-{i % 5:03d}",
        "total": round(10.0 + i * 3.5, 2),
        "currency": "EUR",
        "created_at": int(time.time() * 1000),
    }


def main() -> None:
    # TODO 1 : créer le client Schema Registry
    # schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # TODO 2 : lire le schéma Avro depuis SCHEMA_FILE
    # schema_str = SCHEMA_FILE.read_text(encoding="utf-8")

    # TODO 3 : instancier AvroSerializer et StringSerializer (clé)
    # avro_serializer = AvroSerializer(schema_registry_client, schema_str, order_to_dict)
    # key_serializer = StringSerializer("utf-8")

    # Configuration producer
    producer_conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "producer-avro-lab2",
        "acks": "all",
        # Pour l'étape 6 : décommenter pour activer l'idempotence
        # "enable.idempotence": True,
        # "max.in.flight.requests.per.connection": 5,
        "linger.ms": 5,
        "compression.type": "lz4",
    }
    producer = Producer(producer_conf)

    for i in range(NB_MESSAGES):
        order = build_order(i)
        # TODO 4 : sérialiser key et value avec les serializers ci-dessus puis produire
        # ctx_value = SerializationContext(TOPIC, MessageField.VALUE)
        # ctx_key = SerializationContext(TOPIC, MessageField.KEY)
        # producer.produce(
        #     topic=TOPIC,
        #     key=key_serializer(order["customer_id"], ctx_key),
        #     value=avro_serializer(order, ctx_value),
        #     on_delivery=delivery_report,
        # )
        producer.poll(0)

    remaining = producer.flush(timeout=10)
    log.info("flush done, %d message(s) restant(s)", remaining)


if __name__ == "__main__":
    main()
