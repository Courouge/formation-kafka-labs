"""Lab L5 — étape 4 : désérialisation Avro depuis Kafka.

Variante du sanity check qui parse la valeur Avro a l'aide de ``from_avro``
et du schema ``schemas/order_v1.avsc`` (Order PLAT, post-SMT unwrap de L4).

Note importante sur la coherence L4 ↔ L5 :
Le connecteur Debezium de L4 applique la SMT ``ExtractNewRecordState`` (unwrap),
qui aplatit le payload Debezium {before, after, op, ts_ms, source} en gardant
uniquement les champs de la ligne courante (after) plus quelques metadonnees
``__deleted``, ``__op``, ``__ts_ms``, ``__source_lsn`` (cf add.fields).
Le schema Avro qu'on charge ici reflete cette structure aplatie.

Subtilites :

- Confluent wire format (cas du Schema Registry par defaut) : 5 premiers octets
  = magic byte + schema id. On les retire via ``substring(value, 6, length(value)-5)``
  AVANT ``from_avro``. C'est le mode utilise par L4 (Debezium + SR).
- Avro nu (pas Confluent) : on passe ``value`` directement a ``from_avro``.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

from setup_spark import (
    DEFAULT_TOPIC,
    KAFKA_BOOTSTRAP_HOST,
    get_spark,
)

SCHEMA_PATH = Path(__file__).parent / "schemas" / "order_v1.avsc"


def main() -> None:
    spark = get_spark("l5-read-kafka-avro")

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

    # Confluent wire format : on coupe les 5 premiers octets (magic + schema id).
    # Voir https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
    avro_payload = expr("substring(value, 6, length(value) - 5)")

    decoded = (
        raw
        .select(
            col("key").cast("string").alias("key"),
            from_avro(avro_payload, schema_str).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_ts"),
        )
        # Le schema est deja plat (post-SMT unwrap cote L4) : on extrait
        # directement les champs Order + les metadonnees CDC ajoutees par
        # la SMT (__op, __deleted, __ts_ms).
        .selectExpr(
            "key",
            "data.*",
            "__op as op",
            "__source_ts_ms as cdc_ts_ms",
            "topic", "partition", "offset", "kafka_ts",
        )
    )

    query = (
        decoded.writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .outputMode("append")
        .start()
    )

    query.awaitTermination(timeout=30)
    query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
