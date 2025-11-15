FROM python:3.8-bullseye

# Installer Java et utilitaires
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl git && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Installer PySpark 3.5.0 et JupyterLab
RUN pip install --upgrade pip
RUN pip install pyspark==3.5.0 jupyterlab

# Créer un utilisateur jovyan
RUN useradd -ms /bin/bash jovyan
USER jovyan
WORKDIR /home/jovyan

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
