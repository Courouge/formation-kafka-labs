package lab

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import lab.model.Order
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Using

/** Producer Avro typé : `case class Order` -> `GenericRecord` -> bytes Avro
  * + Schema Registry (wire format Confluent : magic byte 0x00, schema id, payload).
  *
  * Avro4s génère le `Schema` à partir des annotations sur `Order`.
  * `RecordFormat[Order]` fait l'aller-retour `Order <-> GenericRecord`.
  *
  * Lancement : `sbt "runMain lab.ProducerAvro"`.
  */
object ProducerAvro {

  private val Topic         = sys.env.getOrElse("TOPIC_AVRO", "orders.scala.avro")
  private val Bootstrap     = sys.env.getOrElse(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
  )
  private val SchemaRegUrl  = sys.env.getOrElse("SCHEMA_REGISTRY_URL", "http://localhost:8081")

  private def producerProps(): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Bootstrap)
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // Le KafkaAvroSerializer attend des GenericRecord (ou SpecificRecord).
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SchemaRegUrl)
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    p
  }

  def main(args: Array[String]): Unit = {
    println(s"Producing Avro to $Topic via $Bootstrap (SR=$SchemaRegUrl)")

    // Avro4s génère le schéma. Côté Schema Registry, ce sera enregistré
    // sous le subject "${Topic}-value" (TopicNameStrategy par défaut).
    val schema = AvroSchema[Order]
    println(s"Schema généré par Avro4s :\n${schema.toString(true)}")

    // RecordFormat[Order] derive les implicits Encoder + Decoder a partir de la case class.
    // En Avro4s 4.x l'apply ne prend pas de schema runtime -- la SchemaFor est implicite.
    val format = RecordFormat[Order]

    Using.resource(new KafkaProducer[String, GenericRecord](producerProps())) { producer =>
      (1 to 20).foreach { i =>
        val customer = s"customer-${i % 5}"
        val order = Order(
          id          = UUID.randomUUID().toString,
          customer_id = customer,
          total       = i * 9.99,
          currency    = "EUR",
          created_at  = Instant.now().toEpochMilli
        )
        val record = format.to(order)
        val pr     = new ProducerRecord[String, GenericRecord](Topic, customer, record)
        val md     = producer.send(pr).get()
        println(s"sent ${order.id} -> partition=${md.partition()} offset=${md.offset()}")
      }
      producer.flush()
    }

    println("Done.")
  }
}
