# Image officielle Spark 3.5.0 avec Python et Java 11
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

# Utilisateur root pour les installations et copier les JARs
USER root

# Installer PySpark et autres dépendances Python
RUN pip install --upgrade pip
RUN pip install pyspark==3.5.0

# Répertoire de travail
WORKDIR /opt/spark

# Par défaut, ouvrir un shell (override dans docker-compose)
CMD ["/bin/bash"]
