package lab

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Consumer impératif Kafka.
  *
  * On commit explicitement après traitement (`enable.auto.commit=false`),
  * ce qui donne de l'at-least-once propre : si le process meurt avant le
  * commit, le message sera reconsommé au redémarrage.
  *
  * Lancement : `sbt "runMain lab.ConsumerSimple"` (Ctrl+C pour arrêter).
  */
object ConsumerSimple {

  private val Topic     = sys.env.getOrElse("TOPIC", "orders.scala")
  private val GroupId   = sys.env.getOrElse("GROUP_ID", "scala-consumer")
  private val Bootstrap = sys.env.getOrElse(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
  )

  private def consumerProps(bootstrap: String, groupId: String): Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    p
  }

  def main(args: Array[String]): Unit = {
    println(s"Consuming from $Topic group=$GroupId")

    // sys.ShutdownHookThread pour Ctrl+C propre.
    @volatile var running = true
    sys.addShutdownHook { running = false }

    Using.resource(new KafkaConsumer[String, String](consumerProps(Bootstrap, GroupId))) { consumer =>
      consumer.subscribe(Collections.singletonList(Topic))

      while (running) {
        val records = consumer.poll(Duration.ofMillis(500))
        for (r <- records.asScala) {
          println(
            s"received key=${r.key()} value=${r.value()} part=${r.partition()} off=${r.offset()}"
          )
          // TODO étudiant : remplacer ce println par un vrai traitement (DB, HTTP, ...).
        }
        if (!records.isEmpty) consumer.commitSync()
      }
    }

    println("Bye.")
  }
}
