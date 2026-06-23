# Labs — guide de navigation

Les labs sont les manipulations exécutables de la formation. Gardez les outils visuels ouverts pendant les exercices :

- Kafka UI : <http://localhost:18080>
- Grafana : <http://localhost:13000>
- Prometheus : <http://localhost:9090>
- MinIO : <http://localhost:9001>

Guide observabilité complet : ../modules/B1-M06/OBSERVABILITE_LABS.md.

## Parcours recommandé

Chaque lab est rattaché à un (ou plusieurs) chapitre(s) du module B1-M06 — voir la colonne **Cours associé**. Les chapitres M1 à M4 utilisent uniquement des exercices intégrés à leur markdown (pas de lab externe).

| Lab | Sujet | Cours associé | Variante langage | Outil visuel principal |
|---|---|---|---|---|
| [L0-quickstart-ingestion](./L0-quickstart-ingestion/lab.md) | Pipeline JSONL valid/rejected (Python stdlib) | M1.4 | — (Python pur, 25 min) | — (aucune dépendance, stdlib uniquement) |
| [L1-setup](./L1-setup/lab.md) | cluster Kafka local | M5.2, M8.1 (2e passe) | — | Kafka UI + Grafana `Kafka Learning Dashboard` |
| [L2-python-producers-consumers](./L2-python-producers-consumers/lab.md) | producer / consumer / Avro | M5.4, M5.6, M6.2, M6.3 | **Python** (défaut, paire avec L3) | Kafka UI topics + Schema Registry + débit Grafana |
| [L3-scala-producers-consumers](./L3-scala-producers-consumers/lab.md) | producer / consumer Scala | idem L2 — parcours JVM | **Scala** (paire avec L2) | Kafka UI topics + consumer groups |
| [L4-kafka-connect-cdc](./L4-kafka-connect-cdc/lab.md) | Connect + Debezium + S3 sink | M6.1, M7.3, M7.4 | — | Kafka UI Connect + Grafana `Kafka Connect` + MinIO |
| [L5-pyspark-streaming](./L5-pyspark-streaming/lab.md) | Spark Streaming vers Bronze | M9.2, M9.3 | **PySpark** (défaut, paire avec L6) | MinIO + Kafka UI + Spark UI |
| [L6-scala-spark-streaming](./L6-scala-spark-streaming/lab.md) | Spark Scala streaming | idem L5 — parcours JVM | **Scala** (paire avec L5) | Spark UI + MinIO |
| [L7-event-sourcing-saga](./L7-event-sourcing-saga/lab.md) | event sourcing / saga | hors B1-M06 (approfondissement event-driven) | — | Kafka UI topics + consumer groups |
| [L8-ops-security](./L8-ops-security/lab.md) | observabilité et sécurité | M6.6 (obs), M8.x (sécurité, 2e passe) | — | Grafana `Kafka Cluster` + Prometheus |

> **Paires bilingues** : L2 ≡ L3 (même contenu, Python/Scala) et L5 ≡ L6 (même contenu, PySpark/Spark Scala). Choisir une seule des deux variantes selon le langage cible de l'apprenant. L2 et L5 sont les défauts pédagogiques (plus accessibles).

## Dashboards Grafana

Les dashboards sont provisionnés depuis [../docker/grafana/dashboards/](../docker/grafana/dashboards/) :

- `Kafka Learning Dashboard` : vue pédagogique avec les panels à observer pendant les labs ;
- `Kafka Cluster` : santé du cluster, débit, ISR, partitions ;
- `Kafka Connect` : état des connecteurs, tasks et débit source/sink.

## Captures attendues

Les screenshots ne sont pas obligatoires, mais ils aident à vérifier que vous avez réellement compris ce que vous manipulez. Les noms recommandés sont listés dans OBSERVABILITE_LABS.md.
