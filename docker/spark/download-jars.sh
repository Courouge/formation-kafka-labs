#!/usr/bin/env bash
# Télécharge les JARs Spark / Kafka / Delta / S3 nécessaires aux labs L5-L6.
# Usage : bash docker/spark/download-jars.sh

set -euo pipefail

DEST="$(cd "$(dirname "$0")" && pwd)/jars"
mkdir -p "$DEST"

SPARK_VERSION="3.5.0"
SCALA_VERSION="2.12"
KAFKA_CLIENTS_VERSION="3.5.1"
DELTA_VERSION="3.1.0"
HADOOP_AWS_VERSION="3.3.4"
AWS_SDK_VERSION="1.12.262"
COMMONS_POOL_VERSION="2.11.1"

declare -A JARS=(
  [spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar]="https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
  [spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar]="https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
  [kafka-clients-${KAFKA_CLIENTS_VERSION}.jar]="https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
  [commons-pool2-${COMMONS_POOL_VERSION}.jar]="https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar"
  [delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar]="https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar"
  [delta-storage-${DELTA_VERSION}.jar]="https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"
  [hadoop-aws-${HADOOP_AWS_VERSION}.jar]="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
  [aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar]="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"
)

for jar in "${!JARS[@]}"; do
  url="${JARS[$jar]}"
  out="$DEST/$jar"
  if [[ -f "$out" ]]; then
    echo "[skip] $jar"
  else
    echo "[get ] $jar"
    curl -fL -o "$out" "$url"
  fi
done

echo "Tous les JARs sont dans $DEST"
