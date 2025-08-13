# Use Bitnami's Spark 3.3.2 image as the base image
FROM bitnami/spark:3.3.2

# Switch to root user to install system packages
USER root

# Update package lists and install Python 3 pip and curl
# Then upgrade pip and install Python libraries: Jupyter, PySpark, pandas, matplotlib, Great Expectations (with PySpark support)
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    pip3 install --upgrade pip && \
    pip3 install notebook jupyter pyspark pandas matplotlib great_expectations[pyspark]

# Hadoop AWS JARs for Spark S3 access
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.901.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

# Iceberg & Nessie JARs for Spark 3.3 - Iceberg runtime JAR: allows Spark to read/write Iceberg tables
RUN curl -L -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.3_2.12.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar && \
    curl -L -o /opt/bitnami/spark/jars/nessie-spark-extensions-3.3_2.12.jar https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions/3.3_2.12-0.70.0/nessie-spark-extensions-3.3_2.12-0.70.0.jar

    
# PostgreSQL JDBC driver - Required for Spark to connect to PostgreSQL databases
RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
