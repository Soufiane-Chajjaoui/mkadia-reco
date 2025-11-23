from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

bootstrap = "kafka:29092"

# =====================================================
# Vérifier si un topic existe
# =====================================================
def topic_exists(topic_name):
    try:
        # Access the JVM behind PySpark
        sc = spark.sparkContext
        
        # Create Java Properties for Kafka connection
        props = sc._jvm.java.util.Properties()
        props.put("bootstrap.servers", bootstrap)
        
        # Use the Java AdminClient (included in the JARs you loaded)
        AdminClient = sc._jvm.org.apache.kafka.clients.admin.AdminClient
        client = AdminClient.create(props)
        
        # List topics
        kafka_topics = client.listTopics().names().get()
        client.close()
        
        return topic_name in kafka_topics
    except Exception as e:
        print(f"❌ Erreur lors de la vérification du topic {topic_name}: {e}")
        return False


# =====================================================
# Fonction robuste pour Spark
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
# Spark session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Kafka-Realtime-Events")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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
# LIRE CHAQUE TOPIC EN TOUTE SÉCURITÉ
# =====================================================
topics_and_schemas = {
    "review": review_schema,
    "favorite": favorite_schema,
    "cart": cart_schema,
    "order": order_schema
}

queries = []

for topic, schema in topics_and_schemas.items():
    df = safe_read_topic(topic, schema)

    if df is not None:
        q = (
            df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .option("checkpointLocation", f"/opt/spark/checkpoints/{topic}")
            .start()
        )
        queries.append(q)

if len(queries) == 0:
    print("❌ Aucun stream n’a pu démarrer (aucun topic trouvé)")
else:
    print(f"🔥 {len(queries)} streams sont en cours d'exécution…")

spark.streams.awaitAnyTermination()
