"""
Démonstration des refus d'autorisation (ACL) — lab L8.

1. Tente de CONSOMMER 'orders' avec les credentials 'producer-app'
   (qui n'a que WRITE/DESCRIBE) -> TopicAuthorizationException.

2. Tente de PRODUIRE sur 'orders' avec les credentials 'consumer-app'
   (qui n'a que READ/DESCRIBE) -> TopicAuthorizationException.

Les deux erreurs apparaissent côté broker dans les logs (authorizer logger).
"""
from __future__ import annotations

import json
import logging
import sys
from typing import Final

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

LOG: Final = logging.getLogger("unauthorized")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

BOOTSTRAP: Final = "localhost:19092"
TOPIC: Final = "orders"


def base_sasl(username: str, password: str) -> dict:
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": username,
        "sasl.password": password,
    }


def attempt_consume_as_producer_app() -> None:
    LOG.info("=== test 1 : consumer avec creds producer-app (devrait être REFUSÉ) ===")
    cfg = {
        **base_sasl("producer-app", "producer-secret"),
        "group.id": "rogue-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(cfg)
    consumer.subscribe([TOPIC])

    deadline = 0
    try:
        while deadline < 30:
            msg = consumer.poll(timeout=1.0)
            deadline += 1
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    LOG.warning("REFUS attendu : %s", err.str())
                    return
                LOG.info("erreur intermédiaire : %s", err.str())
                if err.code() == KafkaError.GROUP_AUTHORIZATION_FAILED:
                    LOG.warning("REFUS group : %s", err.str())
                    return
            else:
                LOG.error("INATTENDU : reçu un message ! ACL mal configurée ?")
                return
        LOG.error("timeout sans refus explicite : vérifier les ACLs")
    finally:
        consumer.close()


def attempt_produce_as_consumer_app() -> None:
    LOG.info("=== test 2 : producer avec creds consumer-app (devrait être REFUSÉ) ===")
    producer = Producer({
        **base_sasl("consumer-app", "consumer-secret"),
        "acks": "all",
    })

    refused = {"flag": False}

    def cb(err, msg) -> None:
        if err is not None:
            refused["flag"] = True
            LOG.warning("REFUS attendu : %s", err)
        else:
            LOG.error("INATTENDU : message livré offset=%d", msg.offset())

    try:
        producer.produce(
            TOPIC,
            key=b"rogue",
            value=json.dumps({"hack": True}).encode(),
            on_delivery=cb,
        )
        producer.flush(10)
    except KafkaException as exc:
        LOG.warning("REFUS attendu (exception) : %s", exc)
        refused["flag"] = True

    if not refused["flag"]:
        LOG.error("aucun refus reçu, vérifier les ACLs.")


def main() -> int:
    attempt_consume_as_producer_app()
    attempt_produce_as_consumer_app()
    return 0


if __name__ == "__main__":
    sys.exit(main())
