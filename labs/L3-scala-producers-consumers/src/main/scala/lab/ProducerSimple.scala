package lab

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.util.Using

/** Producer impératif Kafka, style "kafka-clients pur".
  *
  * Objectifs pédagogiques :
  *   - voir l'API Java vue depuis Scala (Properties, classOf[T]),
  *   - utiliser `Using.resource` pour fermer proprement le producer,
  *   - configurer `acks=all` + idempotence (recommandés en prod).
  *
  * Lancement : `sbt "runMain lab.ProducerSimple"`.
  */
object ProducerSimple {

  private val Topic     = sys.env.getOrElse("TOPIC", "orders.scala")
  private val Bootstrap = sys.env.getOrElse(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
  )

  /** Construit la `Properties` Kafka. Fonction pure : facile à tester. */
  private def producerProps(bootstrap: String): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    // En prod on ajoute typiquement : compression.type=zstd, linger.ms=20, batch.size=64*1024.
    p
  }

  def main(args: Array[String]): Unit = {
    println(s"Producing to $Topic via $Bootstrap")

    Using.resource(new KafkaProducer[String, String](producerProps(Bootstrap))) { producer =>
      (1 to 20).foreach { i =>
        val key    = s"customer-${i % 5}"
        val value  = s"""{"id":"$i","customer_id":"$key","total":${i * 9.99}}"""
        val record = new ProducerRecord[String, String](Topic, key, value)

        // .get() rend l'envoi synchrone : utile pour la pédagogie, pas en prod (latence).
        val md = producer.send(record).get()
        println(s"sent key=$key partition=${md.partition()} offset=${md.offset()}")
      }
      producer.flush()
    }

    println("Done.")
  }
}
