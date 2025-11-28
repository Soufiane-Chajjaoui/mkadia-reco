# Image officielle Spark 3.5.0 avec Python et Java 11
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

# Utilisateur root pour les installations
USER root

# Set environment variables for PySpark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages with Spark-NLP (using JAR-only approach)
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir psycopg2-binary boto3 numpy pandas && \
    python3 -m pip install --no-cache-dir spark-nlp==5.1.4

# Répertoire de travail
WORKDIR /opt/spark

# Download JARs for Hive, S3, PostgreSQL and Spark-NLP
RUN mkdir -p /opt/spark/jars/extra && \
    curl -fsSL -o /opt/spark/jars/extra/hadoop-aws-3.3.5.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.5/hadoop-aws-3.3.5.jar && \
    curl -fsSL -o /opt/spark/jars/extra/aws-java-sdk-s3-1.12.565.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.565/aws-java-sdk-s3-1.12.565.jar && \
    curl -fsSL -o /opt/spark/jars/extra/aws-java-sdk-core-1.12.565.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.565/aws-java-sdk-core-1.12.565.jar && \
    curl -fsSL -o /opt/spark/jars/extra/aws-java-sdk-sts-1.12.565.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-sts/1.12.565/aws-java-sdk-sts-1.12.565.jar && \
    curl -fsSL -o /opt/spark/jars/extra/joda-time-2.12.5.jar https://repo1.maven.org/maven2/joda-time/joda-time/2.12.5/joda-time-2.12.5.jar && \
    curl -fsSL -o /opt/spark/jars/extra/postgresql-42.6.0.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar && \
    curl -fsSL -o /opt/spark/jars/extra/hive-metastore-4.0.0.jar https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/4.0.0/hive-metastore-4.0.0.jar

# Ensure JARs are on classpath
ENV SPARK_CLASSPATH="/opt/spark/jars/extra/*"

# Copy BERT sentiment model from local directory
COPY --chown=185:185 models/bert_sequence_classifier_multilingual_sentiment /opt/spark/models/sentiment_model

# Par défaut, ouvrir un shell (override dans docker-compose)
CMD ["/bin/bash"]
