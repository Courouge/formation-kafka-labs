package lab.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import lab.Config

/** Étape 8 — Stream-stream join : orders ⋈ payments.
  *
  * Règles que Spark exige pour un stream-stream join :
  *   1. Watermark défini sur LES DEUX streams (faute de quoi le state
  *      grandit indéfiniment et Spark refuse).
  *   2. Condition de join contenant un INTERVAL temporel (pour borner
  *      la durée de bufferisation).
  *
  * Le state cleanup watermark est `min(WMo, WMp) - intervalMax`. */
object StreamStreamJoin {

  def run(spark: SparkSession, cfg: Config): StreamingQuery = {
    import org.apache.spark.sql.functions._

    val orders = ReadKafkaAvro.readTopic(spark, cfg, cfg.topicOrders)
      .withWatermark("kafka_ts", "10 minutes")
      .alias("o")

    val payments = ReadKafkaAvro.readTopic(spark, cfg, cfg.topicPayments)
      .withWatermark("kafka_ts", "10 minutes")
      .alias("p")

    // TODO étape 8 :
    //   join sur order_id avec interval [0, 15 min] :
    //     o.order_id = p.order_id
    //   AND p.kafka_ts >= o.kafka_ts
    //   AND p.kafka_ts <= o.kafka_ts + interval 15 minutes
    //
    //   inner join → seuls les paiements arrivés dans la fenêtre.
    //   leftOuter → ajoute (order, null) après expiration de l'interval.

    val joined = orders /* .join(payments, expr("..."), "inner") */

    joined.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${cfg.checkpointPath}orders_paid")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start(s"${cfg.silverPath}orders_paid")
  }
}
