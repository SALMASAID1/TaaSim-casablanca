#!/bin/sh
set -eu

# Kafka Connector
if [ ! -s "/jars/flink-sql-connector-kafka-3.1.0-1.18.jar" ]; then
  curl -fLSL --retry 3 -o "/jars/flink-sql-connector-kafka-3.1.0-1.18.jar" \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"
fi

# Cassandra Connector
if [ ! -s "/jars/flink-connector-cassandra_2.12-3.2.0-1.18.jar" ]; then
  curl -fLSL --retry 3 -o "/jars/flink-connector-cassandra_2.12-3.2.0-1.18.jar" \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/3.2.0-1.18/flink-connector-cassandra_2.12-3.2.0-1.18.jar"
fi

# Hadoop/S3 Jars
if [ ! -s "/jars/hadoop-aws-3.3.4.jar" ]; then
  curl -fLSL --retry 3 -o "/jars/hadoop-aws-3.3.4.jar" \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
fi

if [ ! -s "/jars/aws-java-sdk-bundle-1.12.262.jar" ]; then
  curl -fLSL --retry 3 -o "/jars/aws-java-sdk-bundle-1.12.262.jar" \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
fi

mkdir -p /jars/s3-fs-hadoop
cp -f "/jars/hadoop-aws-3.3.4.jar" /jars/s3-fs-hadoop/
cp -f "/jars/aws-java-sdk-bundle-1.12.262.jar" /jars/s3-fs-hadoop/

# Flink filesystem plugins are loaded from per-plugin subfolders under /opt/flink/plugins.
# The official Flink image enables built-in plugins by linking the JAR into a directory named
# after the plugin JAR (without the .jar suffix). We mirror the S3A dependencies into that
# expected plugin directory so the plugin classloader can resolve Hadoop's S3A classes.
mkdir -p /jars/flink-s3-fs-hadoop-1.18.1
cp -f "/jars/hadoop-aws-3.3.4.jar" /jars/flink-s3-fs-hadoop-1.18.1/
cp -f "/jars/aws-java-sdk-bundle-1.12.262.jar" /jars/flink-s3-fs-hadoop-1.18.1/
chmod -R a+rX /jars

echo "--- All JARs Downloaded Successfully ---"
