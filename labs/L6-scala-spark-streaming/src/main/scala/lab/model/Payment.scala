package lab.model

import java.sql.Timestamp

/** Paiement synthétique produit par un mini-producer (cf. lab étape 8).
  *
  * Ce topic n'existe pas dans la base Postgres du L4 — on le génère
  * uniquement pour exercer le stream-stream join avec un intervalle. */
final case class Payment(
    payment_id: Long,
    order_id: Long,
    amount: BigDecimal,
    currency: String,
    method: String,            // "card" | "paypal" | "transfer"
    paid_at: Timestamp
)
