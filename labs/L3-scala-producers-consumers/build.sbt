// Lab L3 — Producers & Consumers Scala
// Stack : Scala 2.13 + sbt 1.10 + kafka-clients + FS2-Kafka + Avro4s + Confluent SR

ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "fr.formation.kafka"
ThisBuild / version      := "0.1.0"

// Confluent publie le KafkaAvroSerializer/Deserializer hors Maven Central.
ThisBuild / resolvers += "Confluent" at "https://packages.confluent.io/maven/"

lazy val kafkaClientsV   = "3.7.0"
lazy val fs2KafkaV       = "3.5.1"
lazy val avro4sV         = "4.1.2"
lazy val confluentV      = "7.6.0"
lazy val logbackV        = "1.5.6"
lazy val scalaTestV      = "3.2.18"
lazy val catsEffectV     = "3.5.4"

lazy val root = (project in file("."))
  .settings(
    name := "lab-l3-scala-producers-consumers",

    // Une JVM par runMain : évite les fuites de classpath et les fuites mémoire metaspace.
    fork := true,

    // Logback en dépendance "runtime" suffit, mais on l'expose en compile pour le voir.
    libraryDependencies ++= Seq(
      // Client Kafka officiel — API impérative.
      "org.apache.kafka"           % "kafka-clients"          % kafkaClientsV,

      // FS2-Kafka — wrapper streams / cats-effect autour de kafka-clients.
      "com.github.fd4s"           %% "fs2-kafka"              % fs2KafkaV,

      // Avro4s — case class <-> Avro Schema/GenericRecord via macros.
      "com.sksamuel.avro4s"       %% "avro4s-core"            % avro4sV,

      // Serializer/Deserializer Confluent qui parle au Schema Registry.
      "io.confluent"               % "kafka-avro-serializer"  % confluentV,
      "io.confluent"               % "kafka-schema-registry-client" % confluentV,

      // Cats-effect (déjà tiré par fs2-kafka mais explicite ici pour clarté).
      "org.typelevel"             %% "cats-effect"            % catsEffectV,

      // Logging.
      "ch.qos.logback"             % "logback-classic"        % logbackV,

      // Tests.
      "org.scalatest"             %% "scalatest"              % scalaTestV % Test
    ),

    // Avro4s 4.x utilise des macros : -Ymacro-annotations est requis pour les annotations.
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:imports",
      "-Ymacro-annotations"
    ),

    // Évite les conflits SLF4J entre kafka-clients et logback.
    excludeDependencies ++= Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
    )
  )
