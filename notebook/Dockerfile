FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    kafka-python==2.0.2 \
    minio==7.2.0 \
    python-dotenv==1.0.0

# ─── JARs required by the driver (Delta Lake + S3A) ──────────
RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
      -o /usr/local/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
      -o /usr/local/spark/jars/delta-storage-3.2.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /usr/local/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
      -o /usr/local/spark/jars/aws-java-sdk-bundle-1.12.367.jar

USER ${NB_UID}
