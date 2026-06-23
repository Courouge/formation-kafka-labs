"""Lab L5 — fabrique de SparkSession pour les pipelines streaming.

Centralise toute la configuration nécessaire à un job qui :
- lit Kafka (connector spark-sql-kafka),
- désérialise Avro (spark-avro),
- écrit en Delta Lake sur MinIO (delta-spark + hadoop-aws).

La fonction ``get_spark`` est volontairement minimale : elle ne fait que
construire la session. Les options métier (topic, chemin bronze...) restent
dans les scripts appelants pour garder une lecture pédagogique.
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession

# Versions alignées avec docker/spark/download-jars.sh
SPARK_VERSION = "3.5.0"
SCALA_VERSION = "2.12"
DELTA_VERSION = "3.1.0"
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"

# Liste des packages Maven résolus à la volée par Ivy au démarrage du driver.
# Au premier run, le téléchargement peut durer 1-2 minutes.
PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}",
    f"org.apache.spark:spark-avro_{SCALA_VERSION}:{SPARK_VERSION}",
    f"io.delta:delta-spark_{SCALA_VERSION}:{DELTA_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
])


def get_spark(app_name: str = "lab-l5-pyspark-streaming") -> SparkSession:
    """Construit (ou récupère) la SparkSession utilisée dans tout le lab.

    Args:
        app_name: nom logique du job (visible dans la Spark UI sur :4040).

    Returns:
        Une SparkSession prête à lire Kafka et à écrire Delta sur MinIO.
    """
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages", PACKAGES)
        # Delta Lake : enregistrement de l'extension SQL et du catalog Delta.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Accès à MinIO via le connecteur S3A (Hadoop AWS).
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Petits volumes en lab : on évite 200 partitions de shuffle par défaut.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Constantes partagées par les scripts du lab.
KAFKA_BOOTSTRAP_HOST = "localhost:9092,localhost:9093,localhost:9094"
KAFKA_BOOTSTRAP_DOCKER = "kafka1:29092,kafka2:29092,kafka3:29092"
DEFAULT_TOPIC = "ecommerce.public.orders"
BRONZE_BUCKET = "s3a://bronze"


if __name__ == "__main__":
    # Petit smoke test : la session doit démarrer sans erreur.
    spark = get_spark("setup-smoke-test")
    print(f"Spark version : {spark.version}")
    print(f"Master         : {spark.sparkContext.master}")
    spark.stop()
