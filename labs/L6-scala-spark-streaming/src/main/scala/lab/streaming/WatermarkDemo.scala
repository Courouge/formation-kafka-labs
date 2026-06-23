package lab.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import lab.Config

/** Étape 6 — Démontrer le drop des late events.
  *
  * Le résultat se voit dans la métrique `numRowsDroppedByWatermark`
  * exposée par `StreamingQueryProgress.stateOperators`.
  *
  * Mode opératoire (cf. lab.md) :
  *   1. Lancer cette query.
  *   2. Injecter un event avec un kafka_ts artificiellement reculé
  *      (via kafka-console-producer + un shift d'horloge sur le broker
  *      OU via un producer Python qui contrôle `timestamp_ms`).
  *   3. Observer le compteur `numRowsDroppedByWatermark` augmenter.
  */
object WatermarkDemo {

  def run(spark: SparkSession, cfg: Config): StreamingQuery = {
    import org.apache.spark.sql.functions._

    val orders = ReadKafkaAvro.readTopic(spark, cfg, cfg.topicOrders)

    // TODO étape 6 :
    //   - Poser un watermark COURT (1 minute) pour rendre le drop visible.
    //   - Faire un count par fenêtre 30s.
    //   - Logger `lastProgress.stateOperators` dans foreachBatch.
    //
    // Astuce pour observer :
    //   spark.streams.active.foreach { q =>
    //     println(q.lastProgress.stateOperators.mkString("\n"))
    //   }

    val q = orders
      .withWatermark("kafka_ts", "1 minute")
      .groupBy(window(col("kafka_ts"), "30 seconds"))
      .agg(count("*").as("orders_count"))
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .queryName("watermark-demo")
      .start()

    q
  }
}
