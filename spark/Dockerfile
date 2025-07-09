FROM bitnami/spark:3.3.2

USER root

RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    pip3 install --upgrade pip && \
    pip3 install notebook jupyter pyspark pandas matplotlib great_expectations[pyspark]

# Hadoop AWS JAR
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

# Iceberg & Nessie JARs for Spark 3.3
RUN curl -L -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.3_2.12.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar && \
    curl -L -o /opt/bitnami/spark/jars/nessie-spark-extensions-3.3_2.12.jar https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions/3.3_2.12-0.70.0/nessie-spark-extensions-3.3_2.12-0.70.0.jar
