package lab.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import lab.Config

/** Étape 9 — helper d'écriture Delta vers le silver layer.
  *
  * Mutualise les options communes : checkpoint, format, outputMode,
  * mergeSchema, trigger. À utiliser dans Tumbling/Sliding/Joins
  * pour garder le code DRY. */
object WriteSilver {

  def writeAppend(
      df: DataFrame,
      cfg: Config,
      tableName: String,
      trigger: Trigger = Trigger.ProcessingTime("30 seconds")
  ): StreamingQuery = {
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${cfg.checkpointPath}$tableName")
      .option("mergeSchema", "true")
      .trigger(trigger)
      .start(s"${cfg.silverPath}$tableName")
  }
}
