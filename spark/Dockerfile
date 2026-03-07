FROM apache/spark:3.5.0

USER root

# ─── Delta Lake + AWS/S3A + Kafka JARs ──────────────────────
RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
      -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
      -o /opt/spark/jars/delta-storage-3.2.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
      -o /opt/spark/jars/kafka-clients-3.5.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      -o /opt/spark/jars/commons-pool2-2.11.1.jar && \
    curl -fsSL https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
      -o /opt/spark/jars/mysql-connector-java-8.0.28.jar

USER spark
