"""Producer Kafka simple en JSON brut, sans Schema Registry.

But pédagogique : montrer la base avant d'introduire Avro.
Limites visibles : aucune validation de schéma, payload verbeux.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer_simple")

# --- Configuration -----------------------------------------------------------
CONFIG: dict[str, Any] = {
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
    "client.id": "producer-simple-lab2",
    # acks=all = tous les ISR doivent confirmer (durabilité maximale)
    "acks": "all",
    "linger.ms": 5,
    "compression.type": "lz4",
}
TOPIC = "orders.json"
NB_MESSAGES = 20


def delivery_report(err, msg) -> None:
    """Callback invoqué par librdkafka pour chaque message produit."""
    if err is not None:
        log.error("delivery failed: %s", err)
        return
    log.info(
        "delivered topic=%s partition=%s offset=%s key=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
        msg.key().decode() if msg.key() else None,
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
    producer = Producer(CONFIG)
    for i in range(NB_MESSAGES):
        order = build_order(i)
        producer.produce(
            topic=TOPIC,
            key=order["customer_id"].encode(),
            value=json.dumps(order).encode(),
            on_delivery=delivery_report,
        )
        # poll(0) sert le callback queue sans bloquer
        producer.poll(0)
    remaining = producer.flush(timeout=10)
    log.info("flush done, %d message(s) restant(s)", remaining)


if __name__ == "__main__":
    main()
