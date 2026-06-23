# Lab L6 — Scala Spark Structured Streaming

Voir `lab.md` pour le déroulé pédagogique complet (2h).

## Build

```bash
sbt clean compile
sbt assembly       # produit target/scala-2.12/L6-scala-streaming-assembly-0.1.0.jar
```

## Run en local (sbt, sans cluster)

Nécessite de retirer le `provided` sur Spark dans `build.sbt` :

```bash
BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094 \
S3_ENDPOINT=http://localhost:9000 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
sbt "runMain lab.SparkApp"
```

## Submit sur le cluster Spark Docker

Copier le fat-jar dans le volume monté du master :

```bash
docker cp target/scala-2.12/L6-scala-streaming-assembly-0.1.0.jar spark-master:/opt/jars/

docker exec -it spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.1.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.spark:spark-avro_2.12:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --class lab.SparkApp \
    /opt/jars/L6-scala-streaming-assembly-0.1.0.jar
```

UI Spark : `http://localhost:8082`.

## Structure

```
labs/L6-scala-spark-streaming/
├── build.sbt
├── lab.md
├── README.md
├── project/
│   ├── build.properties
│   └── plugins.sbt
└── src/main/
    ├── resources/
    │   ├── application.conf
    │   └── log4j2.xml
    └── scala/lab/
        ├── Config.scala
        ├── SparkApp.scala
        ├── model/
        │   ├── Order.scala
        │   ├── Payment.scala
        │   └── Customer.scala
        └── streaming/
            ├── ReadKafkaAvro.scala
            ├── TumblingWindow.scala
            ├── SlidingWindow.scala
            ├── WatermarkDemo.scala
            ├── StreamStaticJoin.scala
            ├── StreamStreamJoin.scala
            └── WriteSilver.scala
```
