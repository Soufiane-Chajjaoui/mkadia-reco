from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, LongType, IntegerType,
    ArrayType, DoubleType, TimestampType
)
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
import time
import psycopg2
from psycopg2.extras import execute_values
import boto3
import json
from datetime import datetime

bootstrap = "kafka:29092"
postgres_config = {
    "host": "postgres",
    "port": 5432,
    "user": "postgres",
    "password": "soufianch",
    "database": "mkadia-db"
}
minio_config = {
    "endpoint": "minio-reco:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "mkadia-warehouse"
}

# =====================================================
# Spark session with HiveMetaStore & S3 configurations
# =====================================================
spark = (
    SparkSession.builder
    .appName("Kafka-Realtime-Events")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "s3a://mkadia-warehouse/hive-warehouse")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-reco:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# PostgreSQL initialization with retry
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_recommendations_user_id ON recommendations(user_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_recommendations_created_at ON recommendations(created_at);
            """)
            conn.commit()
            cursor.close()
            conn.close()
            print("✅ PostgreSQL table initialized successfully")
            return True
        except Exception as e:
            if attempt < retries - 1:
                print(f"⚠️ PostgreSQL connection failed (attempt {attempt + 1}/{retries}): {e}")
                print(f"⏳ Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"❌ PostgreSQL connection failed after {retries} attempts: {e}")
                print("⚠️ Continuing without PostgreSQL - recommendations will only be stored in MinIO")
                return False

# =====================================================
# MinIO/S3 initialization
# =====================================================
def init_minio():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http://{minio_config['endpoint']}",
            aws_access_key_id=minio_config['access_key'],
            aws_secret_access_key=minio_config['secret_key'],
            use_ssl=False
        )
        
        try:
            s3_client.head_bucket(Bucket=minio_config['bucket'])
        except:
            s3_client.create_bucket(Bucket=minio_config['bucket'])
            print(f"✅ MinIO bucket '{minio_config['bucket']}' created")
        
        print("✅ MinIO initialized successfully")
    except Exception as e:
        print(f"❌ Error initializing MinIO: {e}")

postgres_available = init_postgres()
init_minio()

# =====================================================
# Vérifier si un topic existe
# =====================================================
def topic_exists(topic_name):
    try:
        sc = spark.sparkContext
        props = sc._jvm.java.util.Properties()
        props.put("bootstrap.servers", bootstrap)

        AdminClient = sc._jvm.org.apache.kafka.clients.admin.AdminClient
        client = AdminClient.create(props)

        kafka_topics = client.listTopics().names().get()
        client.close()

        return topic_name in kafka_topics
    except Exception as e:
        print(f"❌ Erreur lors de la vérification du topic {topic_name}: {e}")
        return False


# =====================================================
# Fonction robuste pour Spark (lire un topic Kafka en JSON)
# =====================================================
def safe_read_topic(topic, schema):
    if not topic_exists(topic):
        print(f"⚠️ WARNING: Topic '{topic}' n'existe pas → Stream ignoré.")
        return None

    print(f"✅ Topic '{topic}' existe → Stream démarré.")
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )
    return df


# =====================================================
# Schémas
# =====================================================
review_schema = StructType([
    StructField("userId", StringType()),
    StructField("itemId", StringType()),
    StructField("comment", StringType()),
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
# Moteur de recommandation en temps réel (ALS)
# =====================================================
class RealTimeRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.model = None
        self.all_interactions = None
        self.last_training_time = 0
        self.training_interval = 300  # 5 minutes

        # ALS configuration (données implicites possible)
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
        """Normalise les interactions selon leur type en colonnes (userId:int, itemId:int, rating:float, timestamp)"""
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
            # explode productIds -> une ligne par produit
            return df.select(
                col("userId").cast("int").alias("userId"),
                F.explode(col("productIds")).alias("itemId"),
                lit(5.0).alias("rating"),
                col("timestamp"),
                lit(interaction_type).alias("interaction_type")
            ).select(
                col("userId"),
                col("itemId").cast("int"),
                col("rating"),
                col("timestamp"),
                col("interaction_type")
            )
        else:
            # si type inconnu, renvoyer schéma vide compatible
            return df.limit(0).select(
                lit(None).cast("int").alias("userId"),
                lit(None).cast("int").alias("itemId"),
                lit(None).cast("double").alias("rating"),
                lit(None).cast("long").alias("timestamp"),
                lit(interaction_type).alias("interaction_type")
            )

    def should_retrain(self):
        current_time = time.time()
        return (current_time - self.last_training_time) >= self.training_interval

    def train_model(self, interactions_df):
        """Entraîne le modèle ALS avec toutes les interactions disponibles"""
        if interactions_df is None:
            print("⚠️ Aucune interaction fournie pour l'entraînement")
            return

        # si dataset vide -> skip
        if interactions_df.rdd.isEmpty():
            print("⚠️ Aucune interaction disponible pour l'entraînement (dataset vide)")
            return

        count = interactions_df.count()
        print(f"🔄 Entraînement du modèle avec {count} interactions...")

        try:
            # optimisation : cache temporaire pour éviter recomptes coûteux
            interactions_df = interactions_df.cache()

            # split train/test
            training, test = interactions_df.randomSplit([0.8, 0.2], seed=42)

            # fit
            self.model = self.als.fit(training)

            # évaluation si test non vide
            if not test.rdd.isEmpty():
                predictions = self.model.transform(test)
                evaluator = RegressionEvaluator(
                    metricName="rmse",
                    labelCol="rating",
                    predictionCol="prediction"
                )
                rmse = evaluator.evaluate(predictions.na.drop())
                print(f"📉 RMSE (test set) = {rmse:.2f}")

            # mettre à jour le timestamp d'entraînement
            self.last_training_time = time.time()
            print("✅ Modèle entraîné avec succès")

        except Exception as e:
            print(f"❌ Erreur entraînement modèle: {e}")

    def generate_recommendations(self, user_id, num_recs=5):
        """Génère des recommandations pour un user_id (entier)"""
        if self.model is None:
            return None

        try:
            user_df = self.spark.createDataFrame([(int(user_id),)], ["userId"])
            recs = self.model.recommendForUserSubset(user_df, num_recs).collect()

            if recs:
                user_recs = recs[0]["recommendations"]
                return [(int(r["itemId"]), float(r["rating"])) for r in user_recs]
            return []

        except Exception as e:
            print(f"❌ Erreur génération recommandations pour user {user_id}: {e}")
            return []

    def store_recommendations_postgres(self, user_id, recommendations, batch_id):
        """Stocke les recommandations dans PostgreSQL (rec_id, user_id, prod_id, created_at, updated_at)"""
        if not recommendations or not postgres_available:
            return
        
        try:
            conn = psycopg2.connect(**postgres_config)
            cursor = conn.cursor()
            
            delete_query = "DELETE FROM recommendations WHERE user_id = %s"
            cursor.execute(delete_query, (user_id,))
            
            data = []
            for item_id, score in recommendations:
                data.append((user_id, int(item_id)))
            
            insert_query = """
                INSERT INTO recommendations (user_id, prod_id)
                VALUES %s
            """
            execute_values(cursor, insert_query, data)
            
            conn.commit()
            cursor.close()
            conn.close()
            print(f"💾 Stored {len(recommendations)} recommendations for user {user_id} in PostgreSQL")
        except Exception as e:
            print(f"❌ Error storing recommendations in PostgreSQL for user {user_id}: {e}")

    def store_recommendations_parquet(self, user_id, recommendations, batch_id):
        """Stocke les recommandations dans MinIO/S3 au format Parquet"""
        if not recommendations:
            return
        
        try:
            data = [
                (user_id, int(item_id), float(score), int(rank), batch_id, datetime.now())
                for rank, (item_id, score) in enumerate(recommendations, 1)
            ]
            
            df = self.spark.createDataFrame(
                data,
                schema=StructType([
                    StructField("user_id", IntegerType()),
                    StructField("item_id", IntegerType()),
                    StructField("score", DoubleType()),
                    StructField("rank", IntegerType()),
                    StructField("batch_id", LongType()),
                    StructField("timestamp", TimestampType())
                ])
            )
            
            parquet_path = f"s3a://{minio_config['bucket']}/recommendations/batch_{batch_id}/user_{user_id}/"
            df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
            
            print(f"📦 Stored recommendations for user {user_id} in Parquet format (s3a://{minio_config['bucket']}/recommendations/batch_{batch_id}/user_{user_id}/)")
        except Exception as e:
            print(f"❌ Error storing recommendations in Parquet for user {user_id}: {e}")
    
    def create_hive_table_if_not_exists(self):
        """Crée la table Hive pour les recommandations"""
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
                LOCATION 's3a://mkadia-warehouse/recommendations/'
            """)
            print("✅ Hive table 'recommendations_hive' created/verified")
        except Exception as e:
            print(f"❌ Error creating Hive table: {e}")
    
    def register_recommendations_in_hive(self, batch_id):
        """Enregistre les recommandations d'un batch dans la table Hive"""
        try:
            recommendations_path = f"s3a://{minio_config['bucket']}/recommendations/batch_{batch_id}/"
            
            df = self.spark.read.parquet(recommendations_path)
            
            df.write.mode("append").insertInto("recommendations_hive")
            
            print(f"✅ Registered recommendations for batch {batch_id} in Hive MetaStore")
        except Exception as e:
            print(f"⚠️ Could not register batch {batch_id} in Hive (may be first batch): {e}")

    def display_recommendations(self, user_id, recommendations):
        """Affiche joliment les recommandations sur la console"""
        if not recommendations:
            print(f"📭 User {user_id}: Aucune recommandation disponible")
            return

        print(f"🎯 Recommandations pour User {user_id}:")
        for i, (item_id, score) in enumerate(recommendations, 1):
            print(f"{i}. Item {item_id} — score {score:.3f}")
        print()


# =====================================================
# Traitement des batches en streaming
# =====================================================
recommender = RealTimeRecommender(spark)
recommender.create_hive_table_if_not_exists()

def process_batch(batch_df, batch_id):
    """Traite chaque micro-batch reçu par foreachBatch."""
    # batch_df est un DataFrame Spark
    if batch_df is None:
        return
    # vérifier si vide
    if batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    print(f"\n📦 Traitement du batch {batch_id} - {count} interactions")

    print("📊 Interactions reçues (preview):")
    batch_df.show(truncate=False, n=10)

    global recommender

    # concatène aux interactions historiques (si existantes)
    if recommender.all_interactions is None:
        recommender.all_interactions = batch_df
    else:
        # union en s'assurant des mêmes colonnes
        recommender.all_interactions = recommender.all_interactions.unionByName(batch_df, allowMissingColumns=True)

    # réentraîner si nécessaire
    if recommender.should_retrain():
        recommender.train_model(recommender.all_interactions)

        # générer pour utilisateurs actifs du batch
        active_users = batch_df.select("userId").distinct().collect()
        print(f"🎯 Génération de recommandations pour {len(active_users)} utilisateurs actifs:")

        for row in active_users:
            uid = row["userId"]
            try:
                recommendations = recommender.generate_recommendations(uid, 5)
                if recommendations:
                    recommender.display_recommendations(uid, recommendations)
                    recommender.store_recommendations_postgres(uid, recommendations, batch_id)
                    recommender.store_recommendations_parquet(uid, recommendations, batch_id)
                else:
                    print(f"📭 User {uid}: Aucune recommandation disponible")
            except Exception as e:
                print(f"❌ Erreur pour user {uid}: {e}")
        
        recommender.register_recommendations_in_hive(batch_id)


# =====================================================
# Créer et lancer les streams normalisés (foreachBatch)
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
            .option("checkpointLocation", f"/opt/spark/checkpoints/realtime-reco-{topic}")
            .trigger(processingTime="30 seconds")
            .start()
        )
        queries.append(q)

if len(queries) == 0:
    print("❌ Aucun stream n'a pu démarrer (aucun topic trouvé)")
else:
    print(f"🔥 {len(queries)} streams de recommandation démarrés…")
    print("⏰ Traitement des batches toutes les 30 secondes")
    print("🎯 Recommandations affichées en temps réel sur la console")

spark.streams.awaitAnyTermination()
