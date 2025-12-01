#!/bin/bash

/opt/spark/bin/spark-submit \
  --conf spark.kryoserializer.buffer.max=128m \
  --conf spark.driver.maxResultSize=512m \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.shuffle.service.enabled=false \
  --conf spark.sql.streaming.failOnDataLoss=false \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --driver-memory 1g \
  --executor-memory 1g \
  --num-executors 1 \
  --executor-cores 1 \
  --master spark://spark-master:7077 \
  /opt/spark/apps/streaming.py
