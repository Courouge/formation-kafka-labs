"""Lab L5 — étape 8 (challenge starter) : dédup idempotent du CDC.

Démontre comment combiner watermark + ``dropDuplicates`` pour rendre la
pipeline bronze idempotente au redémarrage / replay. C'est utile quand :

- le producer CDC retransmet un message après un crash,
- on relance le job depuis ``startingOffsets=earliest``,
- on se prémunit d'un doublon broker → consumer.

La clé d'unicité retenue est ``(id, cdc_ts_ms)`` : deux versions
distinctes d'une même ligne (UPDATE successifs) doivent rester, mais
deux émissions identiques de la même version sont fusionnées.

Le watermark (10 minutes ici) borne l'état conservé en mémoire pour le dedup.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql.functions import col, expr, to_timestamp
from pyspark.sql.avro.functions import from_avro

from setup_spark import (
    BRONZE_BUCKET,
    DEFAULT_TOPIC,
    KAFKA_BOOTSTRAP_HOST,
    get_spark,
)

SCHEMA_PATH = Path(__file__).parent / "schemas" / "order_v1.avsc"
BRONZE_TABLE_PATH = f"{BRONZE_BUCKET}/orders_dedup/"
CHECKPOINT_PATH = f"{BRONZE_BUCKET}/_checkpoints/orders_dedup/"


def main() -> None:
    spark = get_spark("l5-dedup-cdc")
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

    avro_payload = expr("substring(value, 6, length(value) - 5)")

    decoded = raw.select(
        from_avro(avro_payload, schema_str).alias("order"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
    ).select(
        col("order.__op").alias("op"),
        col("order.__source_ts_ms").alias("cdc_ts_ms"),
        col("order.order_id").alias("order_id"),
        col("order").alias("after"),
        "offset",
        to_timestamp(col("order.__source_ts_ms") / 1000).alias("event_time"),
    )

    # TODO challenge :
    # 1. Ajouter le watermark sur 'event_time' (10 minutes).
    # 2. Appliquer dropDuplicates(['order_id', 'cdc_ts_ms']).
    # 3. Écrire en Delta (append) sur BRONZE_TABLE_PATH avec mergeSchema=true.
    # Indice : voir solutions/L5-pyspark-streaming/dedup_cdc.py
    raise NotImplementedError(
        "Challenge à compléter — voir solutions/L5-pyspark-streaming/dedup_cdc.py"
    )


if __name__ == "__main__":
    main()
