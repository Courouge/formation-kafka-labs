package lab.model

import com.sksamuel.avro4s.{AvroDoc, AvroNamespace}

/** Commande passée par un client.
  *
  * Schéma partagé avec le L2 Python : même `namespace` Avro, mêmes champs,
  * mêmes types logiques. Ce qui assure l'interop entre langages : un message
  * produit en Python peut être consommé en Scala et inversement.
  *
  * Avro4s génère un Avro Schema à partir des annotations et des types Scala.
  *
  * @param id          identifiant unique (UUID v4 recommandé)
  * @param customer_id identifiant client (clé de partitionnement)
  * @param total       montant total TTC
  * @param currency    code ISO-4217 (EUR, USD, GBP, ...)
  * @param created_at  timestamp epoch millis (côté producer)
  */
@AvroNamespace("fr.formation.kafka.orders")
@AvroDoc("Commande passée par un client. Schéma v1 partagé L2 (Python) / L3 (Scala).")
final case class Order(
    @AvroDoc("Identifiant unique de la commande (UUID v4).") id: String,
    @AvroDoc("Identifiant du client (clé de partitionnement).") customer_id: String,
    @AvroDoc("Montant total TTC de la commande.") total: Double,
    @AvroDoc("Code ISO-4217 (EUR, USD, GBP, ...).") currency: String,
    @AvroDoc("Timestamp epoch millis de création côté producer.") created_at: Long
)
