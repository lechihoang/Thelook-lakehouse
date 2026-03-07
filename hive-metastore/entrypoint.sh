#!/bin/bash
set -e

export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar

# Wait for MariaDB to be ready
echo "Waiting for MariaDB..."
for i in $(seq 1 10); do
  if (echo > /dev/tcp/mariadb/3306) 2>/dev/null; then
    echo "MariaDB is ready."
    break
  fi
  echo "Attempt $i: MariaDB not ready yet, waiting 5s..."
  sleep 5
done

# Init schema if not already initialized
/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType mysql -info 2>/dev/null \
  || /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType mysql -initSchema

# Start Hive Metastore
exec /opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
