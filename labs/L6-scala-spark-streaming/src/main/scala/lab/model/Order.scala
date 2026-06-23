package lab.model

import java.sql.Timestamp

/** Modèle typé d'une commande tel que produit par Debezium **après**
  * SMT `unwrap` (ExtractNewRecordState) — payload "à plat", sans la
  * structure envelope `{before, after, op, source}`.
  *
  * Important : on tape `total_amount` en `BigDecimal` et non `Double`,
  * sinon on perd en précision sur les montants.
  *
  * Pédago Scala vs Python : avec `Encoders.product[Order]` la moindre
  * incohérence entre ce schéma et l'Avro de Schema Registry échoue à
  * la compilation ou au démarrage de la query — pas à 3h du matin. */
final case class Order(
    order_id: Long,
    customer_id: Long,
    status: String,
    total_amount: BigDecimal,
    currency: String,
    created_at: Timestamp,
    updated_at: Timestamp
)
