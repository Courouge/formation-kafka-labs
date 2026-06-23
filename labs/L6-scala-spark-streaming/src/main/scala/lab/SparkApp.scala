package lab

import org.apache.spark.sql.SparkSession
import lab.streaming._

/** Point d'entrée du lab : initialise SparkSession (Delta + S3A MinIO),
  * charge la config et orchestre les différentes démos streaming.
  *
  * Stratégie pédago : on enchaîne plusieurs `StreamingQuery` non bloquantes,
  * et on attend qu'une terminaison signale la fin (Ctrl+C, exception fatale).
  * En production on segmenterait en plusieurs jobs dédiés. */
object SparkApp {

  def main(args: Array[String]): Unit = {
    val cfg = Config.load()

    val spark: SparkSession = SparkSession.builder()
      .appName("L6-scala-streaming")
      // Delta SQL extensions — indispensable pour MERGE, OPTIMIZE, time travel.
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // S3A → MinIO. path.style.access est OBLIGATOIRE avec MinIO.
      .config("spark.hadoop.fs.s3a.endpoint", cfg.s3Endpoint)
      .config("spark.hadoop.fs.s3a.access.key", cfg.s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", cfg.s3SecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      // Cluster local 1 worker × 2 cores → 4 partitions sur les shuffles
      // (en prod : 200+ ou laisser AQE décider).
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // TODO Étape 4 : démarrer la query tumbling window
    // val q1 = TumblingWindow.run(spark, cfg)

    // TODO Étape 5 : sliding window
    // val q2 = SlidingWindow.run(spark, cfg)

    // TODO Étape 6 : démo watermark / late data
    // val q3 = WatermarkDemo.run(spark, cfg)

    // TODO Étape 7 : stream-static join
    // val q4 = StreamStaticJoin.run(spark, cfg)

    // TODO Étape 8 : stream-stream join
    // val q5 = StreamStreamJoin.run(spark, cfg)

    // Bloque jusqu'à terminaison de n'importe quelle query.
    spark.streams.awaitAnyTermination()
  }
}
