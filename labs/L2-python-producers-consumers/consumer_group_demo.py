"""Démo de rebalance de consumer group.

Lancer 2 instances dans 2 terminaux distincts :

    # Terminal 1
    python consumer_group_demo.py

    # Terminal 2 (au moment où le 1 est déjà connecté)
    python consumer_group_demo.py

Observer dans les logs :
- Le 1er consumer reçoit toutes les partitions (assignment).
- Quand le 2ème join, on voit revoke puis re-assign sur les deux instances.
- Quand l'un des deux est tué (Ctrl+C), l'autre récupère toutes les partitions.

Le `client.id` change automatiquement pour distinguer les instances dans Kafka UI.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger(f"consumer-{os.getpid()}")

BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders.avro"
GROUP_ID = "lab2-rebalance-demo"
SCHEMA_FILE = Path(__file__).parent / "schemas" / "order_v1.avsc"


_running = True


def _stop(*_: Any) -> None:
    global _running
    _running = False


def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    log.info("ASSIGN: %s", [(p.topic, p.partition) for p in partitions])


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    log.info("REVOKE: %s", [(p.topic, p.partition) for p in partitions])


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    reader_schema = SCHEMA_FILE.read_text(encoding="utf-8")
    avro_deserializer = AvroDeserializer(schema_registry_client, reader_schema)

    consumer_conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        # client.id différent par process => visible dans Kafka UI
        "client.id": f"{socket.gethostname()}-{os.getpid()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # CooperativeStickyAssignor = rebalance incrémental, plus doux
        "partition.assignment.strategy": "cooperative-sticky",
    }
    consumer = Consumer(consumer_conf)
    # Les callbacks on_assign / on_revoke permettent d'observer le rebalance
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    log.info("Started, group=%s, awaiting messages...", GROUP_ID)

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("kafka error: %s", msg.error())
                continue
            ctx_v = SerializationContext(msg.topic(), MessageField.VALUE)
            value = avro_deserializer(msg.value(), ctx_v)
            log.info(
                "p=%s o=%s id=%s customer=%s",
                msg.partition(),
                msg.offset(),
                value["id"],
                value["customer_id"],
            )
            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()
        log.info("closed")


if __name__ == "__main__":
    main()
