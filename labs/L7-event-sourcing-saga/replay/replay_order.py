"""Reconstruit l'état d'un order par replay des events depuis le début du topic.

Pédagogie (lab L7) :
    - Crée un consumer éphémère (group.id aléatoire) avec auto.offset.reset=earliest.
    - Lit toutes les partitions de orders.events jusqu'aux end-offsets connus au démarrage.
    - Filtre par order_id, applique chaque event sur un OrderState (fold).

Usage :
    python -m replay.replay_order <order_id>
"""

from __future__ import annotations

import logging
import sys
import uuid
from typing import Optional

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext

from kafka_setup import make_avro_deserializer, make_consumer, string_deserializer
from models import TOPIC_EVENTS, OrderState, OrderStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("replay_order")


def apply_event(state: OrderState, event: dict) -> OrderState:
    """Fonction de fold : applique un event sur l'état pour retourner un nouvel état.

    Cf. T2 §4.2 : `state(t+1) = apply(state(t), event)`.
    """
    # TODO 1 : détecter le type d'event (présence de champs discriminants)
    #     - "items"            -> OrderCreated
    #     - "transaction_id"   -> PaymentCompleted
    #     - "reservation_id" et pas "reason" -> StockReserved (ou StockReleased)
    #     - "tracking_number"  -> OrderShipped
    #     - "delivered_at"     -> OrderDelivered
    #     - "reason"           -> PaymentFailed ou OrderCancelled
    #     - "missing_sku"      -> StockOutOfStock
    #
    # Retourner un nouvel OrderState (immutabilité, voir models.OrderState.with_).
    return state


def _end_offsets(consumer: Consumer, topic: str) -> list[TopicPartition]:
    """Retourne la liste des TopicPartition assignés au topic, avec leurs high watermarks."""
    md = consumer.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, p) for p in md.partitions.keys()]
    end = []
    for tp in parts:
        _, high = consumer.get_watermark_offsets(tp, timeout=10)
        end.append(TopicPartition(topic, tp.partition, high))
    return end


def replay_order(order_id: str, *, timeout_seconds: int = 30) -> Optional[OrderState]:
    consumer = make_consumer(group_id=f"replay-{uuid.uuid4().hex[:8]}",
                             auto_offset_reset="earliest")
    avro_deser = make_avro_deserializer()
    key_deser = string_deserializer()

    md = consumer.list_topics(TOPIC_EVENTS, timeout=10).topics[TOPIC_EVENTS]
    parts = [TopicPartition(TOPIC_EVENTS, p, 0) for p in md.partitions.keys()]
    consumer.assign(parts)

    targets = _end_offsets(consumer, TOPIC_EVENTS)
    target_by_part = {tp.partition: tp.offset for tp in targets}
    log.info("Replay until end-offsets: %s", target_by_part)

    state = OrderState(order_id=order_id)
    seen_event_ids: set[str] = set()
    done_partitions: set[int] = set()

    try:
        while len(done_partitions) < len(parts):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            partition = msg.partition()
            if msg.offset() + 1 >= target_by_part.get(partition, 0):
                done_partitions.add(partition)

            ctx_v = SerializationContext(msg.topic(), MessageField.VALUE)
            ctx_k = SerializationContext(msg.topic(), MessageField.KEY)
            value = avro_deser(msg.value(), ctx_v)
            key = key_deser(msg.key(), ctx_k) if msg.key() else None

            if key != order_id:
                continue

            event_id = value.get("event_id")
            if event_id in seen_event_ids:
                continue
            seen_event_ids.add(event_id)

            state = apply_event(state, value)

        return state
    finally:
        consumer.close()


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m replay.replay_order <order_id>")
        sys.exit(1)
    order_id = sys.argv[1]
    state = replay_order(order_id)
    print("Final state:")
    print(state.model_dump_json(indent=2) if state else "<empty>")


if __name__ == "__main__":
    main()
