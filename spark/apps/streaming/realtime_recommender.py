"""Real-time streaming recommendation system

Features:
- Multi-signal recommendations (reviews + ratings + favorites + cart + orders)
- Real-time model retraining
- Interactions stored in Iceberg lakehouse (every 100 interactions)
- Recommendations stored in Iceberg lakehouse & PostgreSQL (every 30 seconds)
"""

import time
from utils.spark_session import get_spark
from utils.logger import get_logger
from utils.kafka_utils import create_kafka_topics, safe_read_topic
from utils.postgres_utils import init_postgres
from utils.buffer_manager import InteractionBuffer
from models.schemas import TOPICS_SCHEMAS
from models.recommender import Recommender

log = get_logger(__name__)
spark = get_spark("realtime_reco")

interaction_buffer = InteractionBuffer(threshold=5)  # Buffer manager for interactions


def initialize_system():
    """Initialize system components: Kafka, PostgreSQL, Iceberg"""
    log.info("Creating Kafka topics...")
    create_kafka_topics()

    log.info("Initializing PostgreSQL...")
    init_postgres()

    log.info("Creating Iceberg tables...")
    global recommender
    recommender.create_namespace_if_not_exists(namespace="reco")
    recommender.create_iceberg_interactions_table()
    recommender.create_iceberg_recommendations_table()


def process_batch(batch_df, batch_id):
    """Process each micro-batch of interactions"""
    if batch_df is None or batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    log.info(f"Processing batch {batch_id} - {count} interactions")

    global recommender, interaction_buffer

    buffer_ready = interaction_buffer.add(batch_df, spark)

    if buffer_ready:
        buffer_df, buffer_count = interaction_buffer.flush_and_get()
        log.info(f"Flushing {buffer_count} interactions to Iceberg...")
        recommender.store_interactions_iceberg(buffer_df)

    if recommender.all_interactions is None:
        recommender.all_interactions = batch_df
    else:
        recommender.all_interactions = recommender.all_interactions.unionByName(
            batch_df,
            allowMissingColumns=True
        )
        recommender.all_interactions.cache()

    if recommender.should_retrain():
        recommender.train_model(recommender.all_interactions)

        active_users = batch_df.select("userId").distinct().collect()

        if recommender.model is not None:
            log.info(f"🎯 Generating recommendations for {len(active_users)} active users")

            recommendations_list = []
            for row in active_users:
                if row["userId"] is not None:
                    uid = row["userId"]
                    try:
                        recommendations = recommender.generate_recommendations(uid, 5)
                        if recommendations:
                            recommendations_list.append((uid, recommendations))
                            recommender.store_recommendations_postgres(uid, recommendations)
                    except Exception as e:
                        log.error(f"Error for user {uid}: {e}")

            if recommendations_list:
                log.info(f"💾 Storing {len(recommendations_list)} users recommendations to Iceberg...")
                recommender.store_recommendations_iceberg(recommendations_list, batch_id)


def start_streaming():
    """Start streaming queries for all topics"""
    global recommender

    recommender = Recommender(spark)

    initialize_system()

    queries = []

    for topic, schema in TOPICS_SCHEMAS.items():
        df = safe_read_topic(spark, topic, schema)
        if df is not None:
            normalized_df = recommender.normalize_interaction(df, topic)

            q = (
                normalized_df.writeStream
                .foreachBatch(process_batch)
                .outputMode("append")
                .option("checkpointLocation", f"/opt/spark/checkpoints/reco-{topic}")
                .trigger(processingTime="30 seconds")
                .start()
            )
            queries.append(q)

    if len(queries) == 0:
        log.error("No streams could start (no topics found)")
        return

    log.info(f"{len(queries)} streams started")
    log.info("Processing batches every 30 seconds")
    log.info("Generating recommendations in real-time")

    try:
        while True:
            active_queries = spark.streams.active
            for q in active_queries:
                if not q.isActive:
                    log.warning(f"Stream '{q.name}' ended")

            if len(active_queries) == 0:
                log.error("All streams have terminated")
                break

            time.sleep(10)
    except KeyboardInterrupt:
        log.info("Shutting down...")
        for q in spark.streams.active:
            q.stop()


if __name__ == "__main__":
    start_streaming()
