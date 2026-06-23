"""Consumer Kafka avec désérialisation Avro.

À compléter par l'apprenant : zones marquées TODO.
"""

from __future__ import annotations

import logging
import signal
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer_avro")

BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders.avro"
GROUP_ID = "lab2-orders-consumer"
SCHEMA_FILE = Path(__file__).parent / "schemas" / "order_v1.avsc"


_running = True


def _stop(*_: Any) -> None:
    global _running
    _running = False


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    # TODO 1 : client Schema Registry + AvroDeserializer (avec le schéma local pour reader)
    # schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    # reader_schema = SCHEMA_FILE.read_text(encoding="utf-8")
    # avro_deserializer = AvroDeserializer(schema_registry_client, reader_schema)
    # key_deserializer = StringDeserializer("utf-8")

    consumer_conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        # earliest = repartir du début si le group n'existe pas encore
        "auto.offset.reset": "earliest",
        # On commit nous-mêmes pour bien voir le cycle "process puis commit"
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])
    log.info("Subscribed to %s with group=%s", TOPIC, GROUP_ID)

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

            # TODO 2 : désérialiser key et value avec les deserializers
            # ctx_v = SerializationContext(msg.topic(), MessageField.VALUE)
            # ctx_k = SerializationContext(msg.topic(), MessageField.KEY)
            # key = key_deserializer(msg.key(), ctx_k)
            # value = avro_deserializer(msg.value(), ctx_v)
            # log.info("partition=%s offset=%s key=%s value=%s",
            #          msg.partition(), msg.offset(), key, value)

            # TODO 3 : commit synchrone après traitement (at-least-once propre)
            # consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()
        log.info("consumer closed")


if __name__ == "__main__":
    main()
