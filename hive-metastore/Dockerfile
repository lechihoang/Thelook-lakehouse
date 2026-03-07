FROM bitsondatadev/hive-metastore:latest

USER root

# Install AWS SDK for S3A support
RUN curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
      -o /opt/apache-hive-metastore-3.0.0-bin/lib/aws-java-sdk-bundle-1.11.375.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar \
      -o /opt/apache-hive-metastore-3.0.0-bin/lib/hadoop-aws-3.2.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
      -o /opt/apache-hive-metastore-3.0.0-bin/lib/mysql-connector-java-8.0.28.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9083
ENTRYPOINT ["/entrypoint.sh"]
