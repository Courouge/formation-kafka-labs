# Lab L2 — Producers & Consumers Python avec Avro

**Durée** : 2h
**Stack** : Python 3.10+, confluent-kafka, fastavro, Schema Registry

## En bref

Producer + consumer Python avec sérialisation Avro et Schema Registry, démo rebalance, idempotence, évolution de schéma.

## Objectifs

- Implémenter producer/consumer Avro avec Schema Registry
- Comprendre acks, idempotence, retry
- Démontrer la compatibilité backward sur un schéma

## Fichiers

| Fichier | Rôle |
|---|---|
| `lab.md` | Étapes 1 à 7 |
| `requirements.txt` | Dépendances pip |
| `schemas/order_v1.avsc` | Schéma Avro initial |
| `schemas/order_v2.avsc` | Évolution backward-compatible |
| `producer_simple.py` | Producer plain (sans Avro) |
| `producer_avro.py` | Producer Avro (TODO à compléter) |
| `consumer_avro.py` | Consumer Avro (TODO à compléter) |
| `consumer_group_demo.py` | Démo rebalance (lancer 2 instances) |

## Quick start

```bash
pip install -r requirements.txt
python producer_avro.py    # produit 20 messages
python consumer_avro.py    # consomme et désérialise
```

## Étapes complètes

Voir [`lab.md`](lab.md) pour le déroulé pas-à-pas.

## Solutions

Le code complet est dans `solutions/L2-python-producers-consumers/`.
