"""
Producer SASL/SCRAM-SHA-256 (lab L8 — partie 2).

Identité : producer-app
ACL : WRITE + DESCRIBE sur 'orders', IDEMPOTENT_WRITE sur cluster.

Lance 10 messages JSON sur le topic 'orders' du cluster sécurisé (port 19092).
"""
from __future__ import annotations

import json
import logging
import sys
from typing import Final

from confluent_kafka import Producer

LOG: Final = logging.getLogger("producer-sasl")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

BOOTSTRAP: Final = "localhost:19092"
TOPIC: Final = "orders"
USERNAME: Final = "producer-app"
PASSWORD: Final = "producer-secret"  # nosec - lab credentials


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
        "client.id": "producer-app-demo",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "zstd",
    })


def delivery_cb(err, msg) -> None:
    if err is not None:
        LOG.error("delivery failed: %s", err)
    else:
        LOG.info(
            "delivered topic=%s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(), msg.key().decode(),
        )


def main() -> int:
    producer = build_producer()
    LOG.info("envoi de 10 messages sur %s en tant que %s", TOPIC, USERNAME)
    for i in range(10):
        key = f"order-{i}".encode()
        value = json.dumps({"id": i, "amount": 42 + i, "currency": "EUR"}).encode()
        producer.produce(TOPIC, key=key, value=value, on_delivery=delivery_cb)
        producer.poll(0)
    producer.flush(10)
    LOG.info("terminé.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
