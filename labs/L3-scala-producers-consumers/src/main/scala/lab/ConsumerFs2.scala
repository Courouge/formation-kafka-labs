package lab

import cats.effect.{IO, IOApp}
import fs2.kafka._

import scala.concurrent.duration._

/** Consumer Kafka style fonctionnel avec FS2-Kafka.
  *
  * Points clés :
  *   - chaque record est `Committable` : on porte l'offset avec la valeur,
  *   - `commitBatchWithin(N, t)` agrège les commits par batch ou fenêtre
  *     temporelle (réduit la pression sur le coordinateur).
  *
  * Lancement : `sbt "runMain lab.ConsumerFs2"` (Ctrl+C pour arrêter).
  */
object ConsumerFs2 extends IOApp.Simple {

  private val Topic     = sys.env.getOrElse("TOPIC", "orders.scala")
  private val GroupId   = sys.env.getOrElse("GROUP_ID", "scala-consumer-fs2")
  private val Bootstrap = sys.env.getOrElse(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
  )

  private val settings: ConsumerSettings[IO, String, String] =
    ConsumerSettings[IO, String, String]
      .withBootstrapServers(Bootstrap)
      .withGroupId(GroupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)

  override def run: IO[Unit] =
    KafkaConsumer
      .stream(settings)
      .subscribeTo(Topic)
      .records
      .evalMap { committable =>
        val r = committable.record
        IO.println(
          s"key=${r.key} value=${r.value} part=${committable.offset.topicPartition.partition()}"
        ).as(committable.offset)
      }
      .through(commitBatchWithin(500, 5.seconds))
      .compile
      .drain
}
