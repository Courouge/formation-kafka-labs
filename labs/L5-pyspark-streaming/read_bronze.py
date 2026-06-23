"""Lab L5 — étape 6 : valider le bronze.

Lit la table Delta ``s3a://bronze/orders/`` en mode batch et affiche :

- un aperçu des 10 dernières lignes ingérées,
- l'historique des commits Delta (``DESCRIBE HISTORY``),
- le nombre total de lignes et la liste des opérations CDC observées.

Sert d'oracle de validation pour l'étape 5 (``write_bronze_delta.py``).
"""

from __future__ import annotations

from setup_spark import BRONZE_BUCKET, get_spark

BRONZE_TABLE_PATH = f"{BRONZE_BUCKET}/orders/"


def main() -> None:
    spark = get_spark("l5-read-bronze")

    df = spark.read.format("delta").load(BRONZE_TABLE_PATH)

    total = df.count()
    print(f"Total lignes bronze : {total}")

    print("\n--- Répartition par opération CDC ---")
    df.groupBy("op").count().orderBy("op").show(truncate=False)

    print("\n--- 10 dernières lignes (par kafka_ts) ---")
    (
        df.orderBy(df["kafka_ts"].desc())
        .select(
            "kafka_key",
            "order_id",
            "customer_id",
            "status",
            "total_amount",
            "op",
            "cdc_ts_ms",
            "kafka_ts",
            "ingested_at",
        )
        .show(10, truncate=False)
    )

    print("\n--- Historique Delta (commits) ---")
    history = spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_TABLE_PATH}`")
    history.select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
