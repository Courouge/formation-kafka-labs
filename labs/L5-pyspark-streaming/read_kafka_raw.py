"""Lab L5 — étape 3 : sanity check Kafka → console.

Lit le topic CDC ``ecommerce.public.orders`` en streaming et affiche les
clés / valeurs en string brute sur la console pendant 30 secondes. Sert
uniquement à vérifier que :

- le connecteur ``spark-sql-kafka`` est bien chargé,
- les brokers sont accessibles depuis l'hôte sur ``localhost:9092-9094``,
- des messages CDC sont présents dans le topic (alimenté par L4).

Aucun parsing Avro ici : on regarde la valeur en bytes castée en string.
"""

from __future__ import annotations

from pyspark.sql.functions import col

from setup_spark import (
    DEFAULT_TOPIC,
    KAFKA_BOOTSTRAP_HOST,
    get_spark,
)


def main() -> None:
    spark = get_spark("l5-read-kafka-raw")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_HOST)
        .option("subscribe", DEFAULT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Spark expose 7 colonnes par message Kafka : key, value (binary),
    # topic, partition, offset, timestamp, timestampType. On en garde
    # un sous-ensemble lisible.
    decoded = raw.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value_preview"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
    )

    query = (
        decoded.writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .outputMode("append")
        .start()
    )

    # Lecture finie au bout de 30 s pour ne pas bloquer le terminal.
    query.awaitTermination(timeout=30)
    query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
