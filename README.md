# Formation Kafka — Labs étudiants

Travaux pratiques de la formation **Kafka — Ingestion, CDC & Streaming Data Pipelines**
(DataScientist.fr). Chaque lab est autonome : énoncé (`lab.md`), code de référence /
squelettes à compléter, données d'exemple et tests.

> Les **corrigés des challenges** ne sont pas dans ce dépôt. Le cours associé (théorie,
> quiz, synthèses) est sur la plateforme DataScientist.fr.

## Prérequis

- Python 3.10+ (labs L0, L2, L5, L7)
- Docker + Docker Compose (labs L1–L8 : cluster Kafka, Connect, Spark, MinIO, Grafana…)
- JDK 17 + sbt (labs Scala L3, L6)

La stack locale (Kafka, Schema Registry, Kafka UI, Connect, Postgres, MinIO,
Prometheus, Grafana) se lance depuis [`docker/`](./docker/).

> Les JARs Spark volumineux ne sont pas versionnés (voir
> [`docker/spark/jars/README.md`](./docker/spark/jars/README.md)) — ils sont récupérés
> au démarrage de la stack Spark.

## Les labs

| Lab | Sujet | Langage |
|---|---|---|
| [L0 — Quickstart ingestion](./labs/L0-quickstart-ingestion/lab.md) | Pipeline JSONL valid/rejected | Python stdlib |
| [L1 — Setup](./labs/L1-setup/lab.md) | Cluster Kafka local | Docker |
| [L2 — Producers/Consumers](./labs/L2-python-producers-consumers/lab.md) | Producer / consumer / Avro | Python |
| [L3 — Producers/Consumers](./labs/L3-scala-producers-consumers/lab.md) | Producer / consumer / Avro | Scala |
| [L4 — Connect & CDC](./labs/L4-kafka-connect-cdc/lab.md) | Connect + Debezium + S3 sink | config |
| [L5 — Spark Streaming](./labs/L5-pyspark-streaming/lab.md) | Streaming → Bronze Delta | PySpark |
| [L6 — Spark Streaming](./labs/L6-scala-spark-streaming/lab.md) | Streaming, windows, joins | Scala |
| [L7 — Event Sourcing & Saga](./labs/L7-event-sourcing-saga/lab.md) | Event sourcing / saga | Python |
| [L8 — Ops & Sécurité](./labs/L8-ops-security/lab.md) | SASL/SCRAM, ACLs, observabilité | config |

> **Paires bilingues** : L2 ≡ L3 (Python/Scala) et L5 ≡ L6 (PySpark/Scala). Choisissez
> une variante selon votre langage cible. L2 et L5 sont les parcours par défaut.

## Outils visuels (une fois la stack lancée)

- Kafka UI : <http://localhost:18080>
- Grafana : <http://localhost:13000>
- Prometheus : <http://localhost:9090>
- MinIO : <http://localhost:9001>

> Les identifiants présents dans les configs (`minioadmin`, `admin-secret`, `postgres`…)
> sont des **valeurs de démo locales** — ne jamais les réutiliser en production.
