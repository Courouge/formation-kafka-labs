# Spark — JARs requis pour les labs

Le service `spark-master` / `spark-worker-*` utilise l'image `bitnami/spark:3.5`
et monte le volume `./spark/jars:/opt/bitnami/spark/jars-extra` pour ajouter
les connecteurs nécessaires sans rebuild.

## JARs à placer dans `docker/spark/jars/`

| JAR | Version | Usage |
|---|---|---|
| `spark-sql-kafka-0-10_2.12` | 3.5.0 | Source/sink Kafka pour Structured Streaming (Lab L6) |
| `spark-token-provider-kafka-0-10_2.12` | 3.5.0 | Auth tokens Kafka |
| `kafka-clients` | 3.5.1 | Client Kafka bas niveau |
| `commons-pool2` | 2.11.1 | Pooling de connexions Kafka |
| `delta-spark_2.12` | 3.1.0 | Format Delta Lake — couche médaillon |
| `delta-storage` | 3.1.0 | Backend de stockage Delta |
| `hadoop-aws` | 3.3.4 | Filesystem `s3a://` (MinIO) |
| `aws-java-sdk-bundle` | 1.12.262 | SDK AWS pour `s3a://` |

## Téléchargement automatique

```bash
# Depuis docker/spark/
./download-jars.sh
```

Ou utiliser l'image custom `docker/spark/Dockerfile` qui fait `curl` au build :

```bash
docker build -t formation-kafka/spark:3.5 docker/spark/
```

## Soumettre un job

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/bitnami/spark/jars-extra/* \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /jobs/mon_job.py
```
