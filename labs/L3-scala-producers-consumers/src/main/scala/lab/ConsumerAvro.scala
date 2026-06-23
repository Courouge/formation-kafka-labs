package lab

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import lab.model.Order
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/** Consumer Avro : on lit un `GenericRecord`, puis on le projette vers
  * `case class Order` via Avro4s. Si la projection échoue (schéma incompatible
  * avec le code, valeur null, ...), on log et on continue.
  *
  * Lancement : `sbt "runMain lab.ConsumerAvro"`.
  */
object ConsumerAvro {

  private val Topic        = sys.env.getOrElse("TOPIC_AVRO", "orders.scala.avro")
  private val GroupId      = sys.env.getOrElse("GROUP_ID", "scala-consumer-avro")
  private val Bootstrap    = sys.env.getOrElse(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
  )
  private val SchemaRegUrl = sys.env.getOrElse("SCHEMA_REGISTRY_URL", "http://localhost:8081")

  private def consumerProps(): Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, GroupId)
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SchemaRegUrl)
    // false : on lit en GenericRecord (pas besoin d'avoir la classe Avro générée
    // par maven-avro-plugin sur le classpath). Avro4s s'occupe du mapping ensuite.
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    p
  }

  def main(args: Array[String]): Unit = {
    println(s"Consuming Avro from $Topic group=$GroupId")

    // RecordFormat[Order] derive les implicits Encoder + Decoder de la case class.
    val format = RecordFormat[Order]

    @volatile var running = true
    sys.addShutdownHook { running = false }

    Using.resource(new KafkaConsumer[String, GenericRecord](consumerProps())) { consumer =>
      consumer.subscribe(Collections.singletonList(Topic))

      while (running) {
        val records = consumer.poll(Duration.ofMillis(500))
        for (r <- records.asScala) {
          Try(format.from(r.value())).fold(
            err => println(s"deserialization error at offset=${r.offset()} : $err"),
            order => println(s"received Order(id=${order.id}, total=${order.total} ${order.currency})")
          )
        }
        if (!records.isEmpty) consumer.commitSync()
      }
    }

    println("Bye.")
  }
}
