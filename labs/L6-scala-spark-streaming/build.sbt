// Lab L6 — Scala Spark Structured Streaming
// Build pour Spark 3.5 / Scala 2.12 / Delta 3.1 / MinIO via S3A.

ThisBuild / organization := "fr.formation.kafka"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

// Spark 3.5 publie Scala 2.12 et 2.13. On reste sur 2.12, écosystème big data
// le plus aligné (Hudi, Iceberg, certains connectors n'ont pas encore tous leurs
// artefacts en 2.13).

lazy val sparkVersion = "3.5.0"
lazy val deltaVersion = "3.1.0"

resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Confluent"     at "https://packages.confluent.io/maven/"
)

// Spark est marqué `provided` : fourni par le cluster (image spark:3.5.0).
// Pour `sbt run` en local, retirer `provided` ou utiliser `Compile / run / fork := true`.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"                  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10"       % sparkVersion,
  "org.apache.spark" %% "spark-avro"                 % sparkVersion,
  "io.delta"         %% "delta-spark"                % deltaVersion,
  "org.apache.hadoop" % "hadoop-aws"                 % "3.3.4",
  "com.typesafe"      % "config"                     % "1.4.3",
  "org.scalatest"    %% "scalatest"                  % "3.2.18" % Test
)

// sbt-assembly : fat-jar pour spark-submit.
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _ @ _*)       => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF")            => MergeStrategy.discard
  case PathList("META-INF", _ @ _*)                   => MergeStrategy.discard
  case "module-info.class"                            => MergeStrategy.discard
  case PathList("reference.conf")                     => MergeStrategy.concat
  case _                                              => MergeStrategy.first
}

// fork := true : isole la JVM, évite les fuites de classloader sbt entre runs.
Compile / run / fork := true
Test    / fork := true

// Évite le warning Spark "Reflective access of class X is deprecated".
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-encoding", "UTF-8"
)
