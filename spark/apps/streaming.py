"""Streaming Recommendation System
==================================
Real-time collaborative filtering with ALS

Features:
- Multi-signal recommendations (reviews + ratings + favorites + cart + orders)
- Real-time model retraining
- Recommendations stored in PostgreSQL and MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, explode
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import socket
import subprocess
import boto3
import json
import pandas as pd
from io import BytesIO

bootstrap = "kafka:29092"
kafka_host = "kafka"
kafka_port = "9092"
postgres_config = {
    "host": "10.126.145.208",
    "port": 5432,
    "user": "postgres",
    "password": "soufianch",
    "database": "mkadia-db"
}
minio_config = {
    "endpoint": "10.126.145.208:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "mkadia-objects"
}

# =====================================================
# Wait for Hive MetaStore to be ready
# =====================================================
def wait_for_hive_metastore(host="hive-metastore", port=9083, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.close()
            print(f"✅ Hive MetaStore is reachable at {host}:{port}")
            return True
        except (socket.timeout, socket.error) as e:
            elapsed = time.time() - start_time
            print(f"⏳ Waiting for Hive MetaStore ({elapsed:.1f}s)...")
            time.sleep(3)
    print(f"❌ Hive MetaStore did not become available within {timeout}s")
    return False

print("🔄 Starting up, waiting for Hive MetaStore...")
wait_for_hive_metastore()

# =====================================================
# Create Kafka topics if they don't exist
# =====================================================
def create_kafka_topics():
    topics = ["review", "favorite", "cart", "order"]
    
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
        
        for topic in topics:
            try:
                new_topics = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
                admin_client.create_topics(new_topics=new_topics, validate_only=False)
                print(f"✅ Topic '{topic}' → created")
            except TopicAlreadyExistsError:
                print(f"✅ Topic '{topic}' → exists")
            except Exception as e:
                print(f"⚠️ Topic '{topic}' → {str(e)[:50]}")
        
        admin_client.close()
    except ImportError:
        print("⚠️ kafka-python not available, topics must be created manually")
    except Exception as e:
        print(f"⚠️ Failed to create topics: {str(e)[:60]}")

print("📋 Creating Kafka topics...")
create_kafka_topics()

# =====================================================
# Spark session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Recommendations")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# Safe read from Kafka topic
# =====================================================
def safe_read_topic(topic, schema):
    try:
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribePattern", f"^{topic}$")
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) AS json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
        )
        print(f"✅ Topic '{topic}' → Stream started")
        return df
    except Exception as e:
        print(f"⚠️ Topic '{topic}' → {str(e)[:80].replace(chr(10), ' ')}")
        return None


# =====================================================
# Schemas
# =====================================================
review_schema = StructType([
    StructField("userId", StringType()),
    StructField("itemId", StringType()),
    StructField("rating", FloatType()),
    StructField("timestamp", LongType())
])

favorite_schema = StructType([
    StructField("userId", StringType()),
    StructField("productId", IntegerType()),
    StructField("timestamp", LongType())
])

cart_schema = StructType([
    StructField("userId", StringType()),
    StructField("productId", IntegerType()),
    StructField("timestamp", LongType())
])

order_schema = StructType([
    StructField("userId", StringType()),
    StructField("productIds", ArrayType(IntegerType())),
    StructField("timestamp", LongType())
])

# =====================================================
# Topics map
# =====================================================
topics_and_schemas = {
    "review": review_schema,
    "favorite": favorite_schema,
    "cart": cart_schema,
    "order": order_schema
}


# =====================================================
# Recommendation Engine
# =====================================================
class Recommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.model = None
        self.all_interactions = None
        self.last_training_time = 0
        self.training_interval = 300

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
        """Normalize interactions to common format"""
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
                F.explode(col("productIds").cast("array<int>")).alias("itemId"),
                lit(5.0).alias("rating"),
                col("timestamp"),
                lit(interaction_type).alias("interaction_type")
            )
        else:
            return df.limit(0)

    def should_retrain(self):
        current_time = time.time()
        return (current_time - self.last_training_time) >= self.training_interval

    def train_model(self, interactions_df):
        """Train ALS model with interactions"""
        if interactions_df is None or interactions_df.rdd.isEmpty():
            print("⚠️ No interactions for training")
            return

        count = interactions_df.count()
        print(f"🔄 Training model with {count} interactions...")

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
                # Filter out NaN predictions that result from cold-start strategies
                rmse = evaluator.evaluate(predictions.na.drop(subset=['prediction'])) 
                print(f"📉 RMSE (test set) = {rmse:.2f}")

            self.last_training_time = time.time()
            print("✅ Model trained successfully")

        except Exception as e:
            print(f"❌ Training error: {e}")

    def generate_recommendations(self, user_id, num_recs=5):
        """
        Generate ALS recommendations
        """
        if self.model is None:
            return None

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
            print(f"❌ Error generating recommendations for user {user_id}: {e}")
            return []

    def store_recommendations_postgres(self, user_id, recommendations):
        """Store recommendations in PostgreSQL"""
        if not recommendations:
            return
        
        try:
            conn = psycopg2.connect(**postgres_config)
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
            print(f"💾 Stored {len(recommendations)} recommendations for user {user_id}")
        except Exception as e:
            print(f"❌ Error storing recommendations for user {user_id}: {e}")

    def store_recommendations_parquet_minio(self, user_id, recommendations, batch_id):
        """Stocker les recommandations Parquet dans MinIO"""
        if not recommendations:
            return
        
        try:
            data = {
                "user_id": [user_id] * len(recommendations),
                "item_id": [int(item_id) for item_id, score in recommendations],
                "score": [float(score) for item_id, score in recommendations],
                "rank": list(range(1, len(recommendations) + 1)),
                "batch_id": [batch_id] * len(recommendations),
                "timestamp": [datetime.now().isoformat()] * len(recommendations)
            }
            
            df_pandas = pd.DataFrame(data)
            
            buffer = BytesIO()
            df_pandas.to_parquet(buffer, index=False, compression='snappy')
            buffer.seek(0)
            
            s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{minio_config['endpoint']}",
                aws_access_key_id=minio_config['access_key'],
                aws_secret_access_key=minio_config['secret_key'],
                region_name='us-east-1'
            )
            
            bucket = minio_config['bucket']
            try:
                s3_client.head_bucket(Bucket=bucket)
            except Exception:
                s3_client.create_bucket(Bucket=bucket)
            
            key = f"recommendations/batch_{batch_id}/user_{user_id}.parquet"
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue()
            )
            
            print(f"📦 MinIO Parquet: {key}")
        except Exception as e:
            print(f"❌ Erreur MinIO user {user_id}: {str(e)}")

    def create_hive_table_if_not_exists(self):
        """Create Hive table for recommendations"""
        try:
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS recommendations_hive (
                    user_id INT,
                    item_id INT,
                    score DOUBLE,
                    rank INT,
                    batch_id LONG,
                    timestamp TIMESTAMP
                )
                USING PARQUET
                LOCATION 's3a://mkadia-objects/recommendations/'
            """)
            print("✅ Hive table created")
        except Exception as e:
            print(f"⚠️ Could not create Hive table: {e}")

    def display_recommendations(self, user_id, recommendations):
        """Display recommendations"""
        if not recommendations:
            print(f"📭 User {user_id}: No recommendations available")
            return

        print(f"🎯 Recommendations for User {user_id}:")
        for i, (item_id, score) in enumerate(recommendations, 1):
            print(f"{i}. Item {item_id} — score {score:.3f}")
        print()


# =====================================================
# Stream Processing
# =====================================================
# PostgreSQL initialization
# =====================================================
def init_postgres(retries=5, delay=5):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**postgres_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    rec_id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    prod_id INT NOT NULL,
                    score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_recommendations_user_id ON recommendations(user_id);
            """)
            conn.commit()
            cursor.close()
            conn.close()
            print("✅ PostgreSQL table initialized successfully")
            return True
        except Exception as e:
            if attempt < retries - 1:
                print(f"⚠️ PostgreSQL connection failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(delay)
            else:
                print(f"❌ PostgreSQL connection failed after {retries} attempts: {e}")
                return False

init_postgres()

# =====================================================
# Initialize Recommender
# =====================================================
recommender = Recommender(spark)


def process_batch(batch_df, batch_id):
    """
    Process each micro-batch
    """
    if batch_df is None or batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    print(f"\n📦 Processing batch {batch_id} - {count} interactions")

    global recommender

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
            print(f"🎯 Generating recommendations for {len(active_users)} active users:")

            for row in active_users:
                if row["userId"] is not None:
                    uid = row["userId"]
                    try:
                        recommendations = recommender.generate_recommendations(uid, 5)
                        recommender.display_recommendations(uid, recommendations)
                        recommender.store_recommendations_postgres(uid, recommendations)
                        recommender.store_recommendations_parquet_minio(uid, recommendations, batch_id)
                    except Exception as e:
                        print(f"❌ Erreur pour l'utilisateur {uid}: {e}")


# =====================================================
# Start streaming queries
# =====================================================
queries = []

for topic, schema in topics_and_schemas.items():
    df = safe_read_topic(topic, schema)
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
    print("❌ No streams could start (no topics found)")
else:
    print(f"🔥 {len(queries)} streams started...")
    print("⏰ Processing batches every 30 seconds")
    print("🎯 Generating recommendations in real-time")

try:
    while True:
        active_queries = spark.streams.active
        for q in active_queries:
            if q.isActive == False:
                print(f"⚠️ Stream '{q.name}' ended")
        
        if len(active_queries) == 0:
            print("❌ All streams have terminated")
            break
        
        time.sleep(10)
except KeyboardInterrupt:
    print("Shutting down...")
    for q in spark.streams.active:
        q.stop()