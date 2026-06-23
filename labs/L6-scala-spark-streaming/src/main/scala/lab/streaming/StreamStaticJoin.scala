package lab.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import lab.Config

/** Étape 7 — Stream-static join : enrichir orders avec la dim customers.
  *
  * `customers` est lu en BATCH depuis Delta bronze (matérialisé en L5).
  * → snapshot figé au démarrage de la query.
  *
  * Pour rafraîchir : redémarrer la query, OU lire en streaming
  * (`spark.readStream.format("delta").load(...)`) et faire un
  * stream-stream join avec watermark, plus complexe.
  *
  * Pourquoi `broadcast()` ?
  *   La dim customers a une cardinalité bornée (~10 lignes en seed,
  *   ~10 k en réalité business). Diffuser à tous les executors évite
  *   un shuffle hash join. Pour > 10M lignes, retirer `broadcast()`. */
object StreamStaticJoin {

  def run(spark: SparkSession, cfg: Config): StreamingQuery = {
    import org.apache.spark.sql.functions._

    // TODO 1 : charger customers en batch depuis bronze Delta
    val customers = spark.read.format("delta")
      .load(s"${cfg.bronzePath}customers")
      .select("customer_id", "email", "country", "first_name", "last_name")

    // TODO 2 : lire le stream orders
    val orders = ReadKafkaAvro.readTopic(spark, cfg, cfg.topicOrders)
      .withWatermark("kafka_ts", "10 minutes")

    // TODO 3 : join broadcast
    val enriched = orders /* .join(broadcast(customers), Seq("customer_id"), "left") */

    enriched.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${cfg.checkpointPath}orders_enriched")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start(s"${cfg.silverPath}orders_enriched")
  }
}
