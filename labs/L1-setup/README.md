# Lab L1 — Mise en place de l'environnement

**Durée** : 1h
**Stack** : Docker Compose, Kafka KRaft, Schema Registry, Kafka UI

## TL;DR

Démarrer le cluster Kafka 3 brokers avec Schema Registry, vérifier la santé, créer un premier topic, produire et consommer des messages avec les CLI.

## Objectifs

- Démarrer un cluster KRaft 3 brokers en local (sans ZooKeeper)
- Lire et comprendre un `docker-compose.yml` Kafka minimal (compose local au lab)
- Vérifier la résilience (arrêt d'un broker)

À partir de L2, on bascule sur le compose **complet** à la racine (`docker/docker-compose.yml`) qui ajoute Connect, Postgres, MinIO, Spark, Prometheus, Grafana et DuckDB.

## Fichiers

| Fichier | Rôle |
|---|---|
| `lab.md` | Étapes pas-à-pas |
| `docker-compose.yml` | Stack minimale L1 (3 brokers + SR + Kafka UI) |
| `Makefile` | Cibles `up down logs ps health topics-list restart` |

## Quick start

```bash
make up                  # démarre le cluster
make health              # vérifie les endpoints
make topics-list         # liste les topics
```

## Étapes complètes

Voir [`lab.md`](lab.md) pour le déroulé pas-à-pas.

## Solutions

Le code complet est dans `solutions/L1-setup/`.
