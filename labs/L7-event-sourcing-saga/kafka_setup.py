"""Utilitaires de configuration Kafka pour le lab L7.

Centralise la création de Producer/Consumer Avro pour éviter de répéter
le boilerplate dans chaque service.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, record_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

log = logging.getLogger("kafka_setup")


BOOTSTRAP = os.environ.get(
    "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
)
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://localhost:8081"
)
SCHEMAS_DIR = Path(__file__).parent / "schemas"


def schema_registry() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def load_schema(name: str) -> str:
    """Charge un schéma .avsc depuis le dossier schemas/ par nom (sans extension)."""
    path = SCHEMAS_DIR / f"{name}.avsc"
    return path.read_text(encoding="utf-8")


def make_producer(client_id: str, *, transactional_id: Optional[str] = None) -> Producer:
    """Construit un Producer avec idempotence + acks=all.

    Args:
        client_id: identifiant logique du producer (visible en monitoring).
        transactional_id: si fourni, active les transactions Kafka.
    """
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 5,
        "compression.type": "lz4",
    }
    if transactional_id:
        conf["transactional.id"] = transactional_id
    return Producer(conf)


def make_consumer(group_id: str, *, auto_offset_reset: str = "earliest") -> Consumer:
    """Construit un Consumer avec commit explicite (at-least-once propre)."""
    conf: dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
        "max.poll.interval.ms": 30000,
    }
    return Consumer(conf)


def make_avro_serializer(schema_str: str) -> AvroSerializer:
    """Construit un AvroSerializer avec **RecordNameStrategy**.

    Pourquoi RecordNameStrategy plutot que TopicNameStrategy (defaut) ?
    Dans L7 plusieurs types d'events (OrderCreated, PaymentCompleted, StockReserved...)
    coexistent sur le meme topic `orders.events`. Avec TopicNameStrategy, le subject
    serait toujours `orders.events-value` -- la deuxieme inscription echouerait :
    `Schema being registered is incompatible with an earlier schema for subject ...`.

    RecordNameStrategy fait que chaque type d'event a son propre subject base sur
    le `namespace.name` du schema Avro (ex: `fr.formation.kafka.OrderCreated`).
    Ainsi N events differents = N subjects independants, chacun avec sa propre
    histoire de versions et de compatibilite.
    """
    return AvroSerializer(
        schema_registry_client=schema_registry(),
        schema_str=schema_str,
        to_dict=lambda obj, ctx: obj if isinstance(obj, dict) else obj.model_dump(mode="json"),
        conf={"subject.name.strategy": record_subject_name_strategy},
    )


def make_avro_deserializer(schema_str: Optional[str] = None) -> AvroDeserializer:
    """Deserializer Avro generique avec **RecordNameStrategy** pour matcher
    les producers (cf make_avro_serializer)."""
    return AvroDeserializer(
        schema_registry_client=schema_registry(),
        schema_str=schema_str,
    )


def string_serializer() -> StringSerializer:
    return StringSerializer("utf-8")


def string_deserializer() -> StringDeserializer:
    return StringDeserializer("utf-8")
