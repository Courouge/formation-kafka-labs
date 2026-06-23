package lab

import cats.effect.{IO, IOApp}
import fs2.Stream
import fs2.kafka._

/** Producer Kafka style fonctionnel avec FS2-Kafka + cats-effect.
  *
  * Comparé à `ProducerSimple` :
  *   - pas de `try / finally` : `KafkaProducer.stream[IO]` est une `Resource`,
  *   - back-pressure : `parEvalMap(10)(identity)` borne le nombre d'envois en
  *     vol à 10 sans configurer `max.in.flight.requests`,
  *   - composabilité : la sortie est un `Stream[IO, *]` qu'on combine avec
  *     d'autres flux (HTTP, fichiers, DB).
  *
  * Lancement : `sbt "runMain lab.ProducerFs2"`.
  */
object ProducerFs2 extends IOApp.Simple {

  private val Topic     = sys.env.getOrElse("TOPIC", "orders.scala")
  private val Bootstrap = sys.env.getOrElse(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
  )

  private val settings: ProducerSettings[IO, String, String] =
    ProducerSettings[IO, String, String]
      .withBootstrapServers(Bootstrap)
      .withAcks(Acks.All)
      .withEnableIdempotence(true)

  /** 20 enregistrements à envoyer. Immutable, indépendant du producer. */
  private val records: Stream[IO, ProducerRecord[String, String]] =
    Stream.emits(
      (1 to 20).map { i =>
        val key   = s"customer-${i % 5}"
        val value = s"""{"id":"$i","customer_id":"$key","total":${i * 9.99}}"""
        ProducerRecord(Topic, key, value)
      }
    )

  override def run: IO[Unit] =
    KafkaProducer
      .stream(settings)
      .flatMap { producer =>
        records
          .map(ProducerRecords.one(_))
          .evalMap(producer.produce)        // IO[IO[ProducerResult[K,V]]]
          .parEvalMap(10)(identity)         // attend max 10 IO d'envoi en parallèle
          .evalTap(r => IO.println(s"sent $r"))
      }
      .compile
      .drain
}
