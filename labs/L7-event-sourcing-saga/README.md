# Lab L7 — Event sourcing + Saga e-commerce

**Durée** : 2h
**Stack** : Python 3.10, confluent-kafka, fastavro, Pydantic

## En bref

Implémenter une saga choreography (commande → paiement → stock → expédition) avec compensation. Reconstruire l'état d'un order par replay.

## Objectifs

- Implémenter une saga choreography avec 4 services
- Tester compensation (OutOfStock → rollback)
- Reconstruire un order par replay des events
- Comprendre l'idempotence des consumers

## Fichiers

| Fichier | Rôle |
|---|---|
| `lab.md` | Étapes 1 à 9 |
| `requirements.txt` | confluent-kafka, fastavro, pydantic |
| `schemas/` | 11 schémas Avro pour les events |
| `models.py` | Modèles Pydantic + OrderState |
| `kafka_setup.py` | Factory producer/consumer Avro |
| `services/order_service.py` | Émet OrderCreated, écoute Payment* |
| `services/payment_service.py` | Écoute OrderCreated, émet PaymentCompleted/Failed |
| `services/stock_service.py` | Écoute PaymentCompleted, gère compensation |
| `services/shipping_service.py` | Écoute StockReserved, émet OrderShipped |
| `replay/replay_order.py` | Reconstruit l'état d'un order |
| `replay/snapshot_writer.py` | Émet snapshots dans topic compacté |
| `setup_topics.sh` | Crée les topics avec compaction |
| `tests/test_saga_happy_path.py` | Test : commande → expédition |
| `tests/test_saga_compensation.py` | Test : OutOfStock → rollback |

## Quick start

```bash
pip install -r requirements.txt
bash setup_topics.sh
# Lancer chaque service dans un terminal séparé :
python -m services.order_service &
python -m services.payment_service &
python -m services.stock_service &
python -m services.shipping_service &
pytest tests/
```

## Étapes complètes

Voir [`lab.md`](lab.md) pour le déroulé pas-à-pas.

## Solutions

Le code complet est dans `solutions/L7-event-sourcing-saga/`.
