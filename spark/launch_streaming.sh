#!/bin/bash

/opt/spark/bin/spark-submit \
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.2 \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=false \
  --driver-memory 4g \
  --executor-memory 4g \
  --num-executors 2 \
  --executor-cores 2 \
  --master spark://spark-master:7077 \
  /opt/spark/apps/streaming_with_sentiment.py
