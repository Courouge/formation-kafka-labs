package lab.streaming

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import lab.Config

/** Étape 4 — Agrégation tumbling window 1 minute :
  *   COUNT(*) et SUM(total_amount) par status.
  *
  * Sortie en append-only Delta dans s3a://silver/orders_revenue_1m/.
  * Le mode `append` n'est valide qu'en présence d'un watermark, sinon
  * Spark renverra : "Append output mode not supported for streaming
  * aggregations on streaming DataFrames/DataSets without watermark". */
object TumblingWindow {

  def run(spark: SparkSession, cfg: Config): StreamingQuery = {
    import org.apache.spark.sql.functions._

    // TODO 1 : lire orders via ReadKafkaAvro.readTopic
    val orders = ReadKafkaAvro.readTopic(spark, cfg, cfg.topicOrders)

    // TODO 2 : poser un watermark de 10 min sur kafka_ts
    val withWm = orders // .withWatermark(...)

    // TODO 3 : groupby tumbling window 1 min + status, agréger
    val agg = withWm /* .groupBy(window(col("kafka_ts"), "1 minute"), col("status"))
                        .agg(count("*").as("orders_count"),
                             sum("total_amount").as("revenue")) */

    // TODO 4 : flatten window struct en (window_start, window_end, ...)
    val flat = agg

    // TODO 5 : writeStream Delta append
    flat.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${cfg.checkpointPath}orders_revenue_1m")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start(s"${cfg.silverPath}orders_revenue_1m")
  }
}
