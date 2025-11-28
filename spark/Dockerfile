# Image officielle Spark 3.5.0 avec Python et Java 11
# This image already includes PySpark 3.5.0!
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
    pip install --default-timeout=1000 \
    numpy \
    pandas>=2.0.0 \
    spark-nlp==5.1.4

# Répertoire de travail
WORKDIR /opt/spark

# Par défaut, ouvrir un shell (override dans docker-compose)
CMD ["/bin/bash"]
