"""Enhanced Streaming Recommendation System with Sentiment Analysis
=================================================================
Integrates NLP review classification with real-time recommendations

Features:
- Real-time review sentiment classification
- Sentiment-aware product scoring
- Filtered recommendations (removes poorly-reviewed products)
- Multi-signal recommendations (reviews + ratings + favorites + cart + orders)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, avg, count, udf, explode
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
import time

# --- NEW CONFIGURATION ---
# IMPORTANT: This path must point to your converted Hugging Face model 
# saved in the Spark NLP format.
HUGGINGFACE_SPARK_NLP_MODEL_PATH = "./model/final_bert_review_model" 
# --- END NEW CONFIGURATION ---

bootstrap = "kafka:29092"

# =====================================================
# Spark session with Spark-NLP support
# =====================================================
spark = (
    SparkSession.builder
    .appName("Sentiment-Enhanced-Recommendations")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# Check if topic exists
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
        print(f"❌ Error checking topic {topic_name}: {e}")
        return False


# =====================================================
# Safe read from Kafka topic
# =====================================================
def safe_read_topic(topic, schema):
    if not topic_exists(topic):
        print(f"⚠️ WARNING: Topic '{topic}' does not exist → Stream ignored.")
        return None

    print(f"✅ Topic '{topic}' exists → Stream started.")
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
# Schemas
# =====================================================
# NOTE: userId and itemId (productId) are treated as Strings in the schema 
# but cast to Int inside normalize_interaction for ALS. This is common.
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
# Sentiment-Enhanced Recommendation Engine
# =====================================================
class SentimentEnhancedRecommender:
    def __init__(self, spark_session, sentiment_model_path=None):
        self.spark = spark_session
        self.model = None
        self.sentiment_pipeline = None # Renamed for clarity
        self.all_interactions = None
        self.product_sentiments = None
        self.last_training_time = 0
        self.training_interval = 300  # 5 minutes

        # --- FIX: Load converted Spark NLP pipeline ---
        if sentiment_model_path:
            try:
                # Assuming the converted HF model is wrapped in a minimal Spark NLP Pipeline 
                # for text preprocessing (DocumentAssembler, Tokenizer, etc.) and classification.
                self.sentiment_pipeline = PipelineModel.load(sentiment_model_path)
                print("✅ Converted Spark NLP Sentiment pipeline loaded successfully.")
            except Exception as e:
                print(f"⚠️ Could not load Spark NLP sentiment model from {sentiment_model_path}: {e}")
                print("📝 Continuing without sentiment analysis...")

        # ALS configuration
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
    
    # REMOVED: classify_review_sentiment (No longer needed, UDF or native pipeline handles it)

    def update_product_sentiments(self, reviews_df):
        """
        Update product sentiment scores based on new reviews using the converted Spark NLP model.
        """
        if reviews_df is None or reviews_df.rdd.isEmpty():
            return

        print("🔍 Analyzing review sentiments with native Spark NLP...")

        # Add sentiment classification 
        if self.sentiment_pipeline:
            # Classify all reviews in batch using the Spark NLP Pipeline
            try:
                # The input column must be named 'comment' as per the schema
                classified = self.sentiment_pipeline.transform(
                    reviews_df.select("itemId", "comment")
                )
                
                # --- NEW LOGIC: Extract the final prediction and map to score ---
                # We assume the last stage of the pipeline produces a prediction column, e.g., 'prediction'
                # The prediction contains an array of results. We take the first element's result string.
                # The result string will be the class label: '0', '1', '2', '3', '4'
                
                # Extract the prediction label (e.g., '4' for 5 stars)
                extracted = classified.withColumn(
                    "predicted_label", 
                    col("prediction").getItem(0).getItem("result").cast("int") 
                )
                
                # Convert the predicted label (0-4) to a 1.0-5.0 score (star rating)
                scored = extracted.withColumn(
                    # predicted_label (0-4) + 1 = Star Rating (1-5)
                    "sentiment_score",
                    col("predicted_label") + 1.0
                ).select("itemId", "comment", "sentiment_score") # Select necessary columns
                
                # Check for empty predictions
                if scored.rdd.isEmpty():
                     raise Exception("Sentiment pipeline returned no predictions.")
            
            except Exception as e:
                print(f"⚠️ Native sentiment classification failed: {e}")
                # Fallback: neutral sentiment
                scored = reviews_df.withColumn("sentiment_score", lit(3.0)) # 3.0 represents 3 stars/Neutral

        else:
            # No sentiment model: use rating from stream as proxy
            # This is the original logic, using 1-5 rating directly
            scored = reviews_df.withColumn("sentiment_score", col("rating"))

        # Aggregate by product
        new_sentiments = scored.groupBy("itemId").agg(
            avg("sentiment_score").alias("avg_sentiment"),
            count("*").alias("review_count"),
            
            # Use 4.0 and 2.0 as thresholds (4+5 stars, 1+2 stars)
            count(when(col("sentiment_score") >= 4.0, 1)).alias("positive_count"),
            count(when(col("sentiment_score") <= 2.0, 1)).alias("negative_count")
        )

        # Merge with existing sentiments
        if self.product_sentiments is None:
            self.product_sentiments = new_sentiments
        else:
            # Union and re-aggregate
            combined = self.product_sentiments.unionByName(
                new_sentiments,
                allowMissingColumns=True
            )
            self.product_sentiments = combined.groupBy("itemId").agg(
                avg("avg_sentiment").alias("avg_sentiment"),
                F.sum("review_count").alias("review_count"),
                F.sum("positive_count").alias("positive_count"),
                F.sum("negative_count").alias("negative_count")
            )

        print(f"✅ Updated sentiments for {new_sentiments.count()} products")

    def normalize_interaction(self, df, interaction_type):
        """Normalize interactions to common format"""
        # Ensure userId and itemId are cast to Int for ALS compatibility
        if interaction_type == "review":
            return df.select(
                col("userId").cast("int").alias("userId"),
                col("itemId").cast("int").alias("itemId"),
                col("rating").cast("double").alias("rating"),
                col("timestamp"),
                col("comment"),  # Keep comment for sentiment analysis
                lit(interaction_type).alias("interaction_type")
            )
        elif interaction_type == "favorite":
            return df.select(
                col("userId").cast("int").alias("userId"),
                col("productId").cast("int").alias("itemId"),
                lit(4.0).alias("rating"),
                col("timestamp"),
                lit(None).cast("string").alias("comment"),
                lit(interaction_type).alias("interaction_type")
            )
        elif interaction_type == "cart":
            return df.select(
                col("userId").cast("int").alias("userId"),
                col("productId").cast("int").alias("itemId"),
                lit(3.0).alias("rating"),
                col("timestamp"),
                lit(None).cast("string").alias("comment"),
                lit(interaction_type).alias("interaction_type")
            )
        elif interaction_type == "order":
            return df.select(
                col("userId").cast("int").alias("userId"),
                F.explode(col("productIds")).cast("int").alias("itemId"),
                lit(5.0).alias("rating"),
                col("timestamp"),
                lit(None).cast("string").alias("comment"),
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
        Generate sentiment-filtered recommendations
        """
        if self.model is None:
            return None

        try:
            # Get ALS recommendations (more than needed for filtering)
            user_df = self.spark.createDataFrame([(int(user_id),)], ["userId"])
            recs = self.model.recommendForUserSubset(user_df, num_recs * 3).collect()

            if not recs:
                return []

            user_recs = recs[0]["recommendations"]
            
            # Convert to DataFrame for sentiment filtering
            recs_df = self.spark.createDataFrame([
                (int(r["itemId"]), float(r["rating"]))
                for r in user_recs
            ], ["itemId", "als_rating"])

            # Join with sentiment scores
            if self.product_sentiments is not None:
                enriched = recs_df.join(
                    self.product_sentiments,
                    "itemId",
                    "left"
                ).fillna({"avg_sentiment": 3.0, "review_count": 0}) # Default neutral score is 3.0 (3 stars)

                # Calculate final score (weighted combination)
                # Note: Sentiment score (avg_sentiment) is now 1.0-5.0 scale
                final_recs = enriched.withColumn(
                    "final_score",
                    col("als_rating") * 0.5 +       # ALS importance (reduced slightly)
                    (col("avg_sentiment") / 5.0) * 0.4 * 5.0 + # Normalize sentiment (1-5 to 0-1) then boost
                    (col("review_count") / 100) * 0.1     # Popularity boost
                ).withColumn(
                    "final_score",
                    col("als_rating") * 0.6 +          # ALS importance
                    (col("avg_sentiment") / 5.0) * 0.3 * 5.0 +  # Sentiment contribution (normalized and re-scaled for weight)
                    F.least(col("review_count") / 50.0, lit(1.0)) * 0.1 # Cap popularity boost at 0.1
                )


                # Filter: remove poorly reviewed products (average sentiment below 2.5 stars)
                filtered = final_recs.filter(
                    (col("avg_sentiment") >= 2.5) | (col("review_count") < 5)
                )

                # Sort and limit
                top_recs = filtered.orderBy(
                    col("final_score").desc()
                ).limit(num_recs).collect()

                return [
                    (
                        int(r["itemId"]),
                        float(r["final_score"]),
                        float(r["avg_sentiment"]),
                        int(r["review_count"])
                    )
                    for r in top_recs
                ]
            else:
                # No sentiment data yet, use ALS only
                return [
                    (int(r["itemId"]), float(r["rating"]), 3.0, 0)
                    for r in user_recs[:num_recs]
                ]

        except Exception as e:
            print(f"❌ Error generating recommendations for user {user_id}: {e}")
            return []

    def display_recommendations(self, user_id, recommendations):
        """Display recommendations with sentiment info"""
        if not recommendations:
            print(f"📭 User {user_id}: No recommendations available")
            return

        print(f"🎯 Sentiment-Enhanced Recommendations for User {user_id}:")
        for i, (item_id, score, sentiment, review_count) in enumerate(recommendations, 1):
            # Use star rating for display
            sentiment_emoji = "⭐⭐⭐⭐⭐" if sentiment >= 4.5 else "⭐⭐⭐⭐" if sentiment >= 3.5 else "⭐⭐⭐" if sentiment >= 2.5 else "⭐⭐" if sentiment >= 1.5 else "⭐"
            print(f"{i}. Item {item_id} — score {score:.3f} {sentiment_emoji} "
                  f"(Avg Rating: {sentiment:.2f} based on {review_count} reviews)")
        print()


# =====================================================
# Stream Processing with Sentiment
# =====================================================
recommender = SentimentEnhancedRecommender(
    spark,
    sentiment_model_path=HUGGINGFACE_SPARK_NLP_MODEL_PATH 
)

def process_batch(batch_df, batch_id):
    """
    Process each micro-batch with sentiment analysis
    """
    if batch_df is None or batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    print(f"\n📦 Processing batch {batch_id} - {count} interactions")

    global recommender

    # Update sentiment scores for reviews
    reviews_in_batch = batch_df.filter(
        (col("interaction_type") == "review") & 
        (col("comment").isNotNull())
    )
    
    if not reviews_in_batch.rdd.isEmpty():
        recommender.update_product_sentiments(reviews_in_batch)

    # Append to all interactions
    if recommender.all_interactions is None:
        recommender.all_interactions = batch_df
    else:
        # Use unionByName for safe merging of schemas (e.g., reviews have 'comment', others do not)
        recommender.all_interactions = recommender.all_interactions.unionByName(
            batch_df,
            allowMissingColumns=True
        )
        # Force cache refresh if memory allows, for efficient retraining
        recommender.all_interactions.cache()

    # Retrain if needed
    if recommender.should_retrain():
        recommender.train_model(recommender.all_interactions)

        # Generate recommendations for active users
        # Get distinct user IDs from the current batch
        active_users = batch_df.select("userId").distinct().collect()
        
        # NOTE: Only generating recs if training happened to prevent excessive output
        if recommender.model is not None:
             print(f"🎯 Generating recommendations for {len(active_users)} active users:")

             for row in active_users:
                 # Check if userId is not None before processing
                 if row["userId"] is not None:
                     uid = row["userId"]
                     try:
                         # Ensure the UID is cast to int for the recommender function
                         recommendations = recommender.generate_recommendations(uid, 5)
                         recommender.display_recommendations(uid, recommendations)
                     except Exception as e:
                         print(f"❌ Error for user {uid}: {e}")


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
            .option("checkpointLocation", f"/opt/spark/checkpoints/sentiment-reco-{topic}")
            .trigger(processingTime="30 seconds")
            .start()
        )
        queries.append(q)

if len(queries) == 0:
    print("❌ No streams could start (no topics found)")
else:
    print(f"🔥 {len(queries)} sentiment-enhanced streams started...")
    print("⏰ Processing batches every 30 seconds")
    print("🎯 Recommendations with sentiment filtering displayed in real-time")
    print("💡 Products with poor reviews are filtered out automatically")

spark.streams.awaitAnyTermination()