# Image Spark 3.5 avec JARs supplémentaires pour les labs :
# - Spark SQL Kafka connector (Structured Streaming -> Kafka)  : Lab L6
# - Delta Lake (writer médaillon Bronze/Silver/Gold)            : Lab L6
# - Hadoop AWS + AWS SDK (lecture/écriture S3 via MinIO)        : Lab L5/L6
#
# Note : ce Dockerfile n'est pas utilisé par défaut dans docker-compose.yml,
# l'image bitnami/spark:3.5 est utilisée directement et les JARs sont
# montés via le volume ./spark/jars:/opt/bitnami/spark/jars-extra.
# Les apprenants peuvent soit lancer le script ./spark/download-jars.sh
# soit construire cette image custom (build context : ./spark).

FROM bitnami/spark:3.5

USER root

ARG SPARK_VERSION=3.5.0
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.5.1
ARG DELTA_VERSION=3.1.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

WORKDIR /opt/bitnami/spark/jars

RUN set -eux; \
    curl -fL -o spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar; \
    curl -fL -o spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar; \
    curl -fL -o kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar; \
    curl -fL -o commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
    curl -fL -o delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar; \
    curl -fL -o delta-storage-${DELTA_VERSION}.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar; \
    curl -fL -o hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fL -o aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

USER 1001
