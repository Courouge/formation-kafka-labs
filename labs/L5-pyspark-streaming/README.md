# Lab L5 — PySpark Structured Streaming → Bronze layer Delta

**Durée** : 2h
**Stack** : PySpark 3.5, Kafka, Delta Lake 3.1, MinIO

## En bref

Pipeline streaming PySpark : Kafka → désérialisation Avro → Delta sur MinIO. Comprendre triggers, checkpoints, watermark.

## Objectifs

- Connecter PySpark à Kafka et désérialiser Avro
- Écrire en streaming dans Delta Lake sur MinIO
- Comprendre triggers, checkpoints, watermark

## Fichiers

| Fichier | Rôle |
|---|---|
| `lab.md` | Étapes 1 à 8 |
| `requirements.txt` | PySpark, Delta, dépendances |
| `schemas/order_v1.avsc` | Schéma Avro CDC plat post-SMT `unwrap` |
| `setup_spark.py` | SparkSession configurée pour Kafka + Delta + MinIO |
| `read_kafka_raw.py` | ReadStream basique en string |
| `read_kafka_avro.py` | ReadStream + désérialisation Avro |
| `write_bronze_delta.py` | Pipeline complet → Delta |
| `read_bronze.py` | Validation : lit le bronze écrit |
| `dedup_cdc.py` | Challenge : dédup CDC avec watermark |

## Quick start

```bash
pip install -r requirements.txt
python read_kafka_raw.py
python write_bronze_delta.py    # tourne 1 min, écrit dans s3a://bronze/
```

## Étapes complètes

Voir [`lab.md`](lab.md) pour le déroulé pas-à-pas.

## Solutions

Le code complet est dans `solutions/L5-pyspark-streaming/`.
