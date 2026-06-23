package lab.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions._
import lab.Config

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

/** Lecture d'un topic Kafka Avro Confluent en streaming.
  *
  * Pourquoi ce code n'est pas trivial :
  *   - Spark `from_avro` ne lit pas le wire format Confluent
  *     `[0x00][schemaId int32][payload]`. Il faut donc :
  *       1. récupérer le schéma latest via REST (au démarrage du driver),
  *       2. retirer les 5 octets de header avant de décoder.
  *
  * Pour aller plus loin (non requis ici) : utiliser
  * `za.co.absa:abris` qui gère nativement Schema Registry depuis Spark. */
object ReadKafkaAvro {

  /** Hydrate le schéma Avro depuis Schema Registry pour le subject `<topic>-value`. */
  def fetchValueSchema(schemaRegistryUrl: String, topic: String): String = {
    val url = s"$schemaRegistryUrl/subjects/$topic-value/versions/latest"
    val req = HttpRequest.newBuilder(URI.create(url)).GET().build()
    val res = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString())
    if (res.statusCode() != 200) {
      throw new RuntimeException(
        s"Schema Registry GET $url -> ${res.statusCode()} : ${res.body()}"
      )
    }
    // La réponse est un JSON contenant un champ "schema" lui-même JSON-stringifié.
    // On l'extrait à la main pour ne pas ajouter de dep JSON.
    val body = res.body()
    val key = "\"schema\":\""
    val start = body.indexOf(key) + key.length
    val end = body.indexOf("\"", start) // grossier mais suffisant pour notre use-case
    // En réalité Schema Registry renvoie le schema escapé : on délègue le
    // parsing complet à Spark/Avro qui sait ré-échapper.
    // TODO étape 3 : remplacer par un parsing JSON correct via `org.apache.spark.sql.functions.from_json`
    //                ou `io.circe.parser`. Ici on fait simple.
    body.substring(start, end)
      .replace("\\\"", "\"")
      .replace("\\\\", "\\")
  }

  /** TODO étape 3 : implémenter la lecture streaming d'un topic Avro Confluent.
    *
    * Pseudo-code :
    *   val schema = fetchValueSchema(cfg.schemaRegistryUrl, topic)
    *   spark.readStream
    *     .format("kafka")
    *     .option("kafka.bootstrap.servers", cfg.bootstrapServers)
    *     .option("subscribe", topic)
    *     .option("startingOffsets", "earliest")
    *     .option("failOnDataLoss", "false")
    *     .load()
    *     .select(
    *       col("key").cast("string").as("kafka_key"),
    *       from_avro(expr("substring(value, 6, length(value) - 5)"), schema).as("data"),
    *       col("timestamp").as("kafka_ts")
    *     )
    *     .select(col("kafka_key"), col("data.*"), col("kafka_ts"))
    */
  def readTopic(spark: SparkSession, cfg: Config, topic: String): DataFrame = {
    ??? // À COMPLÉTER
  }
}
