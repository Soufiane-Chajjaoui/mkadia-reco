"""Recommendation engine using ALS model"""

import time
import psycopg2
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from psycopg2.extras import execute_values
from pyspark.sql.functions import col, lit, explode as F_explode
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from utils.logger import get_logger
from config.storage_config import POSTGRES_CONFIG, MINIO_CONFIG

log = get_logger(__name__)


class Recommender:
    """ALS-based collaborative filtering recommender"""

    def __init__(self, spark_session, training_interval=300):
        self.spark = spark_session
        self.model = None
        self.all_interactions = None
        self.last_training_time = 0
        self.training_interval = training_interval

        self.als = ALS(
            maxIter=10,
            regParam=0.1,
            userCol="userId",
            itemCol="itemId",
            ratingCol="rating",
            coldStartStrategy="drop",
            implicitPrefs=True,
            rank=10
        )

    def normalize_interaction(self, df, interaction_type):
        """Normalize interactions to common format (userId, itemId, rating, timestamp, interaction_type)"""
        if interaction_type == "review":
            return df.select(
                col("userId").cast("int").alias("userId"),
                col("itemId").cast("int").alias("itemId"),
                col("rating").cast("double").alias("rating"),
                col("timestamp"),
                lit(interaction_type).alias("interaction_type")
            )
        elif interaction_type == "favorite":
            return df.select(
                col("userId").cast("int").alias("userId"),
                col("productId").cast("int").alias("itemId"),
                lit(4.0).alias("rating"),
                col("timestamp"),
                lit(interaction_type).alias("interaction_type")
            )
        elif interaction_type == "cart":
            return df.select(
                col("userId").cast("int").alias("userId"),
                col("productId").cast("int").alias("itemId"),
                lit(3.0).alias("rating"),
                col("timestamp"),
                lit(interaction_type).alias("interaction_type")
            )
        elif interaction_type == "order":
            return df.select(
                col("userId").cast("int").alias("userId"),
                F_explode(col("productIds").cast("array<int>")).alias("itemId"),
                lit(5.0).alias("rating"),
                col("timestamp"),
                lit(interaction_type).alias("interaction_type")
            )
        else:
            return df.limit(0)

    def should_retrain(self):
        """Check if model should be retrained based on interval"""
        current_time = time.time()
        return (current_time - self.last_training_time) >= self.training_interval

    def train_model(self, interactions_df):
        """Train ALS model with interactions"""
        if interactions_df is None or interactions_df.rdd.isEmpty():
            log.warning("No interactions for training")
            return

        count = interactions_df.count()
        log.info(f"Training model with {count} interactions...")

        try:
            interactions_df = interactions_df.cache()
            training, test = interactions_df.randomSplit([0.8, 0.2], seed=42)

            self.model = self.als.fit(training)

            if not test.rdd.isEmpty():
                predictions = self.model.transform(test)
                evaluator = RegressionEvaluator(
                    metricName="rmse",
                    labelCol="rating",
                    predictionCol="prediction"
                )
                rmse = evaluator.evaluate(predictions.na.drop(subset=['prediction']))
                log.info(f"RMSE (test set) = {rmse:.2f}")

            self.last_training_time = time.time()
            log.info("Model trained successfully")

        except Exception as e:
            log.error(f"Training error: {e}")

    def generate_recommendations(self, user_id, num_recs=5):
        """Generate ALS recommendations for a user"""
        if self.model is None:
            return []

        try:
            user_df = self.spark.createDataFrame([(int(user_id),)], ["userId"])
            recs = self.model.recommendForUserSubset(user_df, num_recs).collect()

            if not recs:
                return []

            user_recs = recs[0]["recommendations"]
            return [
                (int(r["itemId"]), float(r["rating"]))
                for r in user_recs
            ]

        except Exception as e:
            log.error(f"Error generating recommendations for user {user_id}: {e}")
            return []

    def store_recommendations_postgres(self, user_id, recommendations):
        """Store recommendations in PostgreSQL"""
        if not recommendations:
            return

        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()

            cursor.execute("DELETE FROM recommendations WHERE user_id = %s", (user_id,))

            data = [(user_id, int(item_id), float(score)) for item_id, score in recommendations]

            insert_query = """
                INSERT INTO recommendations (user_id, prod_id, score)
                VALUES %s
            """
            execute_values(cursor, insert_query, data)

            conn.commit()
            cursor.close()
            conn.close()
            log.info(f"Stored {len(recommendations)} recommendations for user {user_id}")
        except Exception as e:
            log.error(f"Error storing recommendations for user {user_id}: {e}")

    def create_namespace_if_not_exists(self, namespace="reco"):
        """Create namespace in Nessie catalog if it does not exist"""
        try:
            # Vérifie si le namespace existe
            namespaces = [row.namespace for row in self.spark.sql("SHOW NAMESPACES IN lakehouse_catalog").collect()]
            if namespace not in namespaces:
                self.spark.sql(f"CREATE NAMESPACE lakehouse_catalog.{namespace}")
                log.info(f"Namespace '{namespace}' created in lakehouse_catalog")
            else:
                log.info(f"Namespace '{namespace}' already exists")
        except Exception as e:
            log.warning(f"Could not create namespace '{namespace}': {e}")

    def create_iceberg_interactions_table(self):
        """Create Iceberg table for interactions in lakehouse"""
        try:
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS lakehouse_catalog.reco.interactions (
                    user_id INT,
                    item_id INT,
                    rating DOUBLE,
                    interaction_type STRING,
                    timestamp LONG,
                    created_at TIMESTAMP
                )
                USING ICEBERG
            """)
            log.info("Iceberg interactions table created")
        except Exception as e:
            log.warning(f"Could not create Iceberg interactions table: {e}")

    def create_iceberg_recommendations_table(self):
        """Create Iceberg table for recommendations in lakehouse"""
        try:
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS lakehouse_catalog.reco.recommendations (
                    user_id INT,
                    item_id INT,
                    score DOUBLE,
                    rank INT,
                    batch_id LONG,
                    created_at TIMESTAMP
                )
                USING ICEBERG
            """)
            log.info("Iceberg recommendations table created")
        except Exception as e:
            log.warning(f"Could not create Iceberg recommendations table: {e}")

    def store_interactions_iceberg(self, interactions_df):
        """Store interactions in Iceberg lakehouse"""
        if interactions_df is None or interactions_df.rdd.isEmpty():
            return

        try:
            df_with_timestamp = interactions_df.withColumn(
                "created_at",
                lit(datetime.now()).cast("timestamp")
            )

            df_with_timestamp.write \
                .mode("append") \
                .option("iceberg.write.update.mode", "merge") \
                .option("iceberg.write.target.data-file-format", "parquet") \
                .saveAsTable("lakehouse_catalog.reco.interactions")

            count = interactions_df.count()
            log.info(f"Stored {count} interactions in Iceberg lakehouse")
        except Exception as e:
            log.error(f"Error storing interactions in Iceberg: {e}")

    def store_recommendations_iceberg(self, recommendations_list, batch_id):
        """Store recommendations in Iceberg lakehouse"""
        if not recommendations_list:
            return

        try:
            data = []
            for user_id, recs in recommendations_list:
                for rank, (item_id, score) in enumerate(recs, 1):
                    data.append({
                        "user_id": int(user_id),
                        "item_id": int(item_id),
                        "score": float(score),
                        "rank": rank,
                        "batch_id": batch_id,
                        "created_at": datetime.now()
                    })

            if data:
                df_recs = self.spark.createDataFrame(data)
                df_recs.write \
                    .mode("append") \
                    .option("iceberg.write.update.mode", "merge") \
                    .option("iceberg.write.target.data-file-format", "parquet") \
                    .saveAsTable("lakehouse_catalog.reco.recommendations")

                log.info(f"Stored {len(data)} recommendations in Iceberg lakehouse")
        except Exception as e:
            log.error(f"Error storing recommendations in Iceberg: {e}")
