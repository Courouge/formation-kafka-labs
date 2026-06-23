package lab.model

import java.sql.Timestamp

/** Dimension Customers — lue depuis le bronze layer Delta (matérialisé en L5)
  * pour le stream-static join de l'étape 7. */
final case class Customer(
    customer_id: Long,
    email: String,
    first_name: String,
    last_name: String,
    country: String,
    created_at: Timestamp,
    updated_at: Timestamp
)
