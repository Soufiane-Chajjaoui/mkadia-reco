FROM python:3.8-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl git && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN mkdir -p /opt/spark/jars/

RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

RUN curl -L -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

RUN pip install --upgrade pip
RUN pip install pyspark==3.5.0 jupyterlab

RUN useradd -ms /bin/bash jovyan
USER jovyan
WORKDIR /home/jovyan

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
