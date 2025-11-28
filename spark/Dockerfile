# Image officielle Spark 3.5.0 avec Python et Java 11
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

# Utilisateur root pour les installations
USER root

# Set environment variables for PySpark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Increase pip timeout and install Python packages
# Note: PySpark is already included in the base image at $SPARK_HOME/python
RUN pip install --upgrade pip && \
    psycopg2-binary boto3 \
    numpy \
    pandas>=2.0.0 \
    spark-nlp==5.1.4

# Répertoire de travail
WORKDIR /opt/spark

# Télécharger les JARs pour Hive, S3 et PostgreSQL
RUN mkdir -p /opt/spark/jars/extra && \
    curl -fsSL -o /opt/spark/jars/extra/hive-standalone-metastore-3.0.0.jar https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/3.0.0/hive-standalone-metastore-3.0.0.jar && \
    curl -fsSL -o /opt/spark/jars/extra/aws-java-sdk-s3-1.12.565.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.565/aws-java-sdk-s3-1.12.565.jar && \
    curl -fsSL -o /opt/spark/jars/extra/aws-java-sdk-core-1.12.565.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.565/aws-java-sdk-core-1.12.565.jar && \
    curl -fsSL -o /opt/spark/jars/extra/hadoop-aws-3.3.5.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.5/hadoop-aws-3.3.5.jar && \
    curl -fsSL -o /opt/spark/jars/extra/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Par défaut, ouvrir un shell (override dans docker-compose)
CMD ["/bin/bash"]
