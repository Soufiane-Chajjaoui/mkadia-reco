import sys
import sparknlp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline

def create_spark_session():
    """Initializes Spark Session with Spark-NLP support."""
    try:
        spark = sparknlp.start()
        spark.sparkContext.setLogLevel("WARN")
        print(f"✅ Spark Session initialized with Spark NLP {sparknlp.version()}")
        return spark
    except Exception as e:
        print(f"❌ Failed to initialize Spark session: {e}")
        sys.exit(1)

class SentimentClassifier:
    """Loads and runs the pre-trained Multilingual BERT model for sentiment classification."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.pipeline = None
        
        # Model name: bert_sequence_classifier_multilingual_sentiment
        # Language: xx (Multilingual)
        MODEL_NAME = "bert_sequence_classifier_multilingual_sentiment"
        LANGUAGE = "xx"
        
        print(f"📦 Loading pre-trained model: {MODEL_NAME} ({LANGUAGE})...")
        print("   (This will download the model on first run - approx 650MB)")
        
        try:
            from sparknlp.base import DocumentAssembler
            from sparknlp.annotator import Tokenizer, BertForSequenceClassification
            
            # Build pipeline
            document_assembler = DocumentAssembler() \
                .setInputCol("comment") \
                .setOutputCol("document")
            
            tokenizer = Tokenizer() \
                .setInputCols(["document"]) \
                .setOutputCol("token")
            
            # Load pre-trained model from local path
            # The user downloaded it to spark/apps/model/bert_sequence_classification_multilingual_sentiment
            # mapped to /opt/spark-apps/model/bert_sequence_classification_multilingual_sentiment in Docker
            LOCAL_MODEL_PATH = "/opt/spark-apps/model/bert_sequence_classification_multilingual_sentiment"
            
            print(f"📂 Loading model from local path: {LOCAL_MODEL_PATH}")
            
            bert_classifier = BertForSequenceClassification \
                .load(LOCAL_MODEL_PATH) \
                .setInputCols(["token", "document"]) \
                .setOutputCol("class") \
                .setCaseSensitive(False) \
                .setMaxSentenceLength(512)
            
            self.pipeline = Pipeline(stages=[
                document_assembler,
                tokenizer,
                bert_classifier
            ])
            
            # Initialize pipeline
            empty_df = self.spark.createDataFrame([["test"]]).toDF("comment")
            self.pipeline = self.pipeline.fit(empty_df)
            
            print("✅ Pre-trained BERT model loaded successfully!")
            
        except Exception as e:
            print(f"❌ ERROR loading model: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def classify_reviews(self, reviews_df):
        """Classifies reviews and extracts sentiment scores."""
        print("\n🔍 Analyzing reviews with Multilingual BERT model...")
        
        # Run classification
        classified = self.pipeline.transform(reviews_df.select("itemId", "comment"))
        
        # Extract predictions
        # The output is in 'class.result' as an array
        extracted = classified.withColumn(
            "predicted_label",
            col("class.result").getItem(0)
        )
        
        # Map labels to scores
        # The model outputs: "1 star", "2 stars", "3 stars", "4 stars", "5 stars"
        scored = extracted.withColumn(
            "sentiment_score",
            when(col("predicted_label").contains("5 star"), lit(5.0))
            .when(col("predicted_label").contains("4 star"), lit(4.0))
            .when(col("predicted_label").contains("3 star"), lit(3.0))
            .when(col("predicted_label").contains("2 star"), lit(2.0))
            .when(col("predicted_label").contains("1 star"), lit(1.0))
            .otherwise(lit(3.0))  # Default neutral
        ).select("itemId", "comment", "predicted_label", "sentiment_score")
        
        return scored

def run_test():
    """Main test function."""
    spark = create_spark_session()
    classifier = SentimentClassifier(spark)
    
    # Test data - Multilingual mix to test capabilities
    test_reviews_data = [
        # Produit : Café Gourmet
        (101, "Ce café est un chef-d'œuvre absolu ! Un arôme riche et une saveur incroyablement douce. Cinq étoiles.", 5.0), 
        
        # Produit : Éponge de cuisine standard
        (102, "C'est correct, rien de spécial. Elle fait le travail, mais elle s'use rapidement.", 3.0),
        
        # Produit : Mixeur de marque économique
        (103, "Je suis extrêmement déçu, ce mixeur est tombé en panne en moins d'une journée d'utilisation. À éviter.", 1.0),
        
        # Produit : Shampooing Bio
        (104, "Fonctionne très bien, a rendu mes cheveux doux et brillants. A dépassé mes attentes. Très contente.", 4.0),
        
        # Produit : Fromage artisanal
        (105, "C'est magnifique ! Une texture parfaite et un goût incroyable. J'adore ce produit, je le rachèterai.", 5.0), 
        
        # Produit : Nettoyant multi-usages
        (106, "C'est horrible. Laisse des traces partout et l'odeur est insupportable. Un gaspillage d'argent.", 1.0), 
        
        # Produit : Barre de céréales
        (107, "Très bonne barre, le goût est excellent et c'est très nourrissant. Je l'aime beaucoup.", 4.0) 
    ]
    
    # Create DataFrame
    test_schema = StructType([
        StructField("itemId", StringType(), False),
        StructField("comment", StringType(), False),
        StructField("expected_rating", StringType(), False),
    ])
    
    test_df = spark.createDataFrame([
        (str(item_id), comment, str(expected_rating))
        for item_id, comment, expected_rating in test_reviews_data
    ], test_schema)
    
    # Run classification
    results_df = classifier.classify_reviews(test_df)
    
    # Display results
    final_results = results_df.join(test_df, ["itemId", "comment"], "inner")
    
    print("\n" + "="*80)
    print("🌟 MULTILINGUAL SENTIMENT CLASSIFICATION RESULTS 🌟")
    print("="*80)
    
    final_results.select(
        col("itemId").alias("ID"),
        col("comment").alias("Review Text"),
        col("predicted_label").alias("Model Output"),
        col("sentiment_score").alias("Score"),
        col("expected_rating").alias("Expected")
    ).withColumn(
        "Match",
        when(col("Score") == col("Expected").cast("float"), lit("✅"))
        .otherwise("❌")
    ).show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    run_test()
