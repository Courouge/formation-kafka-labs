# Lab L4 — Kafka Connect, Debezium CDC, sink S3, DLQ

**Durée** : 2h
**Stack** : Kafka Connect distributed, Debezium Postgres, Confluent S3 Sink, MinIO

## En bref

Capturer les changements Postgres en temps réel via Debezium, matérialiser en Parquet sur MinIO (bronze layer), gérer les erreurs avec DLQ.

## Objectifs

- Configurer un connector source Debezium (CDC Postgres)
- Configurer un sink S3 vers MinIO (bronze layer Parquet)
- Mettre en place une DLQ

## Fichiers

| Fichier | Rôle |
|---|---|
| `lab.md` | Étapes 1 à 8 |
| `connectors/debezium-postgres-source.json` | Config source CDC |
| `connectors/s3-sink-bronze.json` | Config sink S3 → bronze |
| `connectors/s3-sink-with-dlq.json` | Variation DLQ |
| `scripts/register-debezium.sh` | POST le source connector |
| `scripts/register-s3-sink.sh` | POST le sink connector |
| `scripts/make-update.sh` | Trigger un UPDATE Postgres |
| `scripts/inspect-topic.sh` | Lit un topic CDC |
| `scripts/read-minio.py` | Lit un Parquet depuis MinIO |

## Quick start

```bash
bash scripts/register-debezium.sh
bash scripts/register-s3-sink.sh
bash scripts/make-update.sh
python scripts/read-minio.py bronze ecommerce.public.customers
```

## Étapes complètes

Voir [`lab.md`](lab.md) pour le déroulé pas-à-pas.

## Solutions

Le code complet est dans `solutions/L4-kafka-connect-cdc/`.
