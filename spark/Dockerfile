FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

# Versions
ENV SCALA_VERSION=2.12
ENV ICEBERG_VERSION=1.5.2
ENV NESSIE_VERSION=0.78.0
ENV SPARK_COMPILATION_VERSION=3.5

# Créer dossier pour jars supplémentaires
RUN mkdir -p /opt/spark/jars/lakehouse /opt/spark/jars/extra

# --------------------------
# Iceberg Spark runtime service
# --------------------------
RUN curl -fSL --retry 5 --retry-delay 5 --retry-max-time 300 \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_COMPILATION_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_COMPILATION_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
  -o /opt/spark/jars/lakehouse/iceberg-spark-runtime.jar

# --------------------------
# Nessie Spark Extensions
# --------------------------
RUN curl -fSL --retry 5 --retry-delay 5 --retry-max-time 300 \
  "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-${SPARK_COMPILATION_VERSION}_2.12/${NESSIE_VERSION}/nessie-spark-extensions-${SPARK_COMPILATION_VERSION}_2.12-${NESSIE_VERSION}.jar" \
  -o /opt/spark/jars/lakehouse/nessie-spark-extensions.jar

# --------------------------
# Hadoop AWS S3 libs
# --------------------------
RUN curl -fSL --retry 5 --retry-delay 5 --retry-max-time 300 \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
  -o /opt/spark/jars/lakehouse/hadoop-aws.jar

RUN curl -fSL --retry 5 --retry-delay 5 --retry-max-time 300 \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.686/aws-java-sdk-bundle-1.12.686.jar" \
  -o /opt/spark/jars/lakehouse/aws-java-sdk-bundle.jar

# --------------------------
# JDBC Postgres
# --------------------------
RUN curl -fSL --retry 5 --retry-delay 5 --retry-max-time 300 \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar" \
  -o /opt/spark/jars/extra/postgresql-42.6.0.jar

# --------------------------
# Python libs
# --------------------------
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir \
    psycopg2-binary boto3 numpy pandas pyarrow kafka-python

# --------------------------
# Copier jars lakehouse dans le dossier jars principal pour classpath
# --------------------------
RUN cp /opt/spark/jars/lakehouse/*.jar /opt/spark/jars/

# --------------------------
# Classpath Spark
# --------------------------
ENV SPARK_CLASSPATH="/opt/spark/jars/*:/opt/spark/jars/extra/*"

WORKDIR /opt/spark

# --------------------------
# Commande par défaut : Spark Master
# --------------------------
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "spark-master"]
