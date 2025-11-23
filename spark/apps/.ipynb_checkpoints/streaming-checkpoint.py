from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

# ------------------------------
# 1️⃣ Créer la session Spark
# ------------------------------
spark = (
    SparkSession.builder
    .appName("KafkaConsumerStreamingRobust")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------------
# 2️⃣ Définir le schéma des données
# ------------------------------
schema = StructType([
    StructField("userId", StringType(), True),
    StructField("itemId", StringType(), True),
    StructField("action", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", LongType(), True)
])

# ------------------------------
# 3️⃣ Lire le flux Kafka
# ------------------------------
kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "user-interactions"

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir la valeur binaire en chaîne JSON
raw_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

# ------------------------------
# 4️⃣ Parser le JSON avec gestion des erreurs
# ------------------------------
from pyspark.sql.functions import from_json, col, lit

parsed_df = raw_df.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

# Optionnel : filtrer les lignes malformées
parsed_df = parsed_df.na.drop(subset=["userId", "itemId", "action"])

# ------------------------------
# 5️⃣ Écriture robuste avec checkpointing
# ------------------------------
query = (
    parsed_df.writeStream
    .format("console")                  # Affichage console pour debug
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "/opt/spark/checkpoints/user_interactions")  # 🔑 checkpoint
    .trigger(processingTime="5 seconds")  # micro-batch toutes les 5 sec
    .start()
)

query.awaitTermination()
