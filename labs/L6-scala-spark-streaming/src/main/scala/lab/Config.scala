package lab

import com.typesafe.config.{Config => TConfig, ConfigFactory}

/** Configuration immuable du job, hydratée depuis `application.conf`
  * (résolution automatique des variables d'environnement).
  *
  * Pédago : on encapsule la lecture Typesafe Config derrière une `case class`
  * — le reste du code n'importe **plus rien** de `com.typesafe.config`. */
final case class Config(
    bootstrapServers: String,
    schemaRegistryUrl: String,
    s3Endpoint: String,
    s3AccessKey: String,
    s3SecretKey: String,
    bronzePath: String,
    silverPath: String,
    checkpointPath: String,
    topicOrders: String,
    topicPayments: String,
    topicCustomers: String
)

object Config {

  def load(): Config = {
    val raw: TConfig = ConfigFactory.load().getConfig("l6")
    Config(
      bootstrapServers   = raw.getString("bootstrap-servers"),
      schemaRegistryUrl  = raw.getString("schema-registry-url"),
      s3Endpoint         = raw.getString("s3-endpoint"),
      s3AccessKey        = raw.getString("s3-access-key"),
      s3SecretKey        = raw.getString("s3-secret-key"),
      bronzePath         = raw.getString("bronze-path"),
      silverPath         = raw.getString("silver-path"),
      checkpointPath     = raw.getString("checkpoint-path"),
      topicOrders        = raw.getString("topic-orders"),
      topicPayments      = raw.getString("topic-payments"),
      topicCustomers     = raw.getString("topic-customers")
    )
  }
}
