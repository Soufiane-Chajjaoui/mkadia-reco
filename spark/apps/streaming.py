from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
import time

bootstrap = "kafka:29092"

# =====================================================
# Spark session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Kafka-Realtime-Events")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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
                F.explode(col("productIds")).cast("int").alias("itemId"),
                lit(5.0).alias("rating"),
                col("timestamp"),
                lit(interaction_type).alias("interaction_type")
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
                recommender.display_recommendations(uid, recommendations)
            except Exception as e:
                print(f"❌ Erreur pour user {uid}: {e}")


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
