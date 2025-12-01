# Image officielle Spark 3.5.0 avec Python & Java 11
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

# Install Python libs
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir psycopg2-binary boto3 numpy pandas pyarrow kafka-python

# Workdir
WORKDIR /opt/spark

# PostgreSQL JDBC
RUN mkdir -p /opt/spark/jars/extra && \
    curl -fsSL -o /opt/spark/jars/extra/postgresql-42.6.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

ENV SPARK_CLASSPATH="/opt/spark/jars/extra/*"

CMD ["/bin/bash"]
