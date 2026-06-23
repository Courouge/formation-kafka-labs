package lab.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import lab.Config

/** Étape 5 — Sliding window : fenêtre 5 min, slide 1 min.
  *
  * Coût mémoire : chaque event est dans (durée / slide) = 5 fenêtres
  * ouvertes en simultané. Multiplier la mémoire d'état par 5 par
  * rapport au tumbling équivalent. C'est le compromis : on gagne en
  * granularité de mise à jour, on paie en RAM. */
object SlidingWindow {

  def run(spark: SparkSession, cfg: Config): StreamingQuery = {
    import org.apache.spark.sql.functions._

    val orders = ReadKafkaAvro.readTopic(spark, cfg, cfg.topicOrders)

    // TODO : implémenter
    //   .withWatermark("kafka_ts", "10 minutes")
    //   .groupBy(window(col("kafka_ts"), "5 minutes", "1 minute"), col("status"))
    //   .agg(avg("total_amount").as("avg_basket"), count("*").as("orders_count"))

    val sliding = orders

    sliding.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${cfg.checkpointPath}orders_avg_basket_5m")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start(s"${cfg.silverPath}orders_avg_basket_5m")
  }
}
