"""
Consumer SASL/SCRAM-SHA-256 (lab L8 — partie 2).

Identité : consumer-app
ACL : READ + DESCRIBE sur 'orders', READ sur les groupes prefixed 'analytics-'.

Consomme 10 messages depuis 'orders' avec le groupe 'analytics-team-a',
puis quitte proprement après commit explicite.
"""
from __future__ import annotations

import json
import logging
import signal
import sys
from typing import Final

from confluent_kafka import Consumer, KafkaError

LOG: Final = logging.getLogger("consumer-sasl")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

BOOTSTRAP: Final = "localhost:19092"
TOPIC: Final = "orders"
GROUP: Final = "analytics-team-a"
USERNAME: Final = "consumer-app"
PASSWORD: Final = "consumer-secret"  # nosec - lab credentials


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "consumer-app-demo",
    })


def main() -> int:
    consumer = build_consumer()
    consumer.subscribe([TOPIC])
    LOG.info("abonné à %s en tant que %s (group=%s)", TOPIC, USERNAME, GROUP)

    stop = False

    def _stop(*_args) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    received = 0
    target = 10
    try:
        while not stop and received < target:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                LOG.error("kafka error: %s", msg.error())
                continue
            value = json.loads(msg.value())
            LOG.info(
                "reçu offset=%d partition=%d key=%s value=%s",
                msg.offset(), msg.partition(), msg.key().decode(), value,
            )
            received += 1
        consumer.commit(asynchronous=False)
        LOG.info("commit OK, %d messages traités.", received)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
