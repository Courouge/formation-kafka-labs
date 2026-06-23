"""Lab L5 — étape 5 : pipeline Kafka → Avro → bronze sink.

C'est le coeur du lab : on assemble lecture Kafka, désérialisation Avro
Confluent puis écriture dans un sink configurable.

Sinks supportés (via env var ``OUTPUT_SINK``) :

- ``kafka`` — republication JSON dans ``bronze.orders``. Pas de
  dépendance MinIO/S3, vérifiable avec ``kafka-console-consumer`` ou la
  Kafka UI. Checkpoint local dans ``/tmp``.
- ``delta`` (défaut) — bronze layer Delta sur S3/MinIO (T4 §3.2). Append-only,
  ``mergeSchema=true``, checkpoint S3.

Conventions communes :

- ``outputMode='append'`` : un message Kafka = une ligne bronze.
- ``checkpointLocation`` obligatoire : sans lui, redémarrer le job perd
  l'offset Kafka et duplique tout le topic.
- ``trigger(processingTime='30 seconds')`` : micro-batch toutes les 30s
  pour ne pas saturer le sink de petits fichiers/messages en lab.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql.functions import col, current_timestamp, expr, lit, to_json, struct, length
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQuery

from setup_spark import (
    BRONZE_BUCKET,
    DEFAULT_TOPIC,
    KAFKA_BOOTSTRAP_HOST,
    get_spark,
)

SCHEMA_PATH = Path(__file__).parent / "schemas" / "order_v1.avsc"
BRONZE_TABLE_PATH = f"{BRONZE_BUCKET}/orders/"
DELTA_CHECKPOINT_PATH = f"{BRONZE_BUCKET}/_checkpoints/orders/"

OUTPUT_SINK = os.environ.get("OUTPUT_SINK", "delta").lower()
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "bronze.orders")
KAFKA_CHECKPOINT_PATH = os.environ.get(
    "KAFKA_CHECKPOINT_PATH", "/tmp/l5-bronze-orders-checkpoint"
)


def build_pipeline(spark) -> StreamingQuery:
    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_HOST)
        .option("subscribe", DEFAULT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Filtre defensif : on ne traite que les messages Confluent-framed Avro
    # (>=5 bytes, magic byte 0x00 en position 1). Cela élimine :
    # - les tombstones (value=null, DELETE Debezium)
    # - les messages texte injectés à la main via kafka-console-producer
    # - tout payload non-Avro qui ferait crasher AvroDataToCatalyst (EOFException).
    framed = raw.filter(
        col("value").isNotNull()
        & (length(col("value")) >= lit(5))
        & (expr("substring(value, 1, 1)") == expr("CAST(X'00' AS BINARY)"))
    )

    avro_payload = expr("substring(value, 6, length(value) - 5)")

    decoded = (
        framed.select(
            col("key").cast("string").alias("kafka_key"),
            # mode=PERMISSIVE : un record cassé devient null au lieu de tuer le job.
            from_avro(avro_payload, schema_str, {"mode": "PERMISSIVE"}).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_ts"),
        )
        # Si from_avro renvoie null (record non parseable), on dégage.
        .filter(col("data").isNotNull())
        # Le schema est plat (post-SMT unwrap cote L4) : on extrait directement
        # les champs Order + les metadonnees CDC ajoutees par la SMT.
        .select(
            "kafka_key",
            "data.*",
            col("__op").alias("op"),
            col("__source_ts_ms").alias("cdc_ts_ms"),
            "topic", "partition", "offset", "kafka_ts",
        )
        # Lineage : on trace quand la ligne a ete ingeree dans le bronze.
        .withColumn("ingested_at", current_timestamp())
        .withColumn("bronze_layer", lit("orders"))
    )

    if OUTPUT_SINK == "delta":
        query = (
            decoded.writeStream
            .format("delta")
            .option("checkpointLocation", DELTA_CHECKPOINT_PATH)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
            .start(BRONZE_TABLE_PATH)
        )
        return query

    # Sink Kafka : on republie en JSON dans ``bronze.orders``.
    kafka_out = decoded.select(
        col("kafka_key").alias("key"),
        to_json(struct([col(c) for c in decoded.columns])).alias("value"),
    )

    query = (
        kafka_out.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_HOST)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", KAFKA_CHECKPOINT_PATH)
        .outputMode("append")
        # micro-batch toutes les 5s : visible dans la Spark UI (onglet
        # Structured Streaming -> Statistics) et permet de mesurer le débit.
        .trigger(processingTime="5 seconds")
        .queryName("l5-bronze-orders")
        .start()
    )
    return query


def main() -> None:
    spark = get_spark("l5-write-bronze-delta")
    query = build_pipeline(spark)
    if OUTPUT_SINK == "delta":
        print(f"Sink Delta : {BRONZE_TABLE_PATH}")
        print(f"Checkpoint : {DELTA_CHECKPOINT_PATH}")
    else:
        print(f"Sink Kafka : topic {OUTPUT_TOPIC}")
        print(f"Checkpoint : {KAFKA_CHECKPOINT_PATH}")
    print("Ctrl-C pour arrêter proprement.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
