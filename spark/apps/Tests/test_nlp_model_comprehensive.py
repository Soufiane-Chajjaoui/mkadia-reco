"""
Comprehensive NLP Sentiment Model Testing Script
=================================================
Tests sentiment classification models (ViveknSentiment or ClassifierDL)

Usage:
    python test_nlp_model_comprehensive.py [model_path]
    
If no model_path provided, will use pre-trained models from Spark-NLP
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Spark-NLP imports
import sparknlp
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import (
    Tokenizer,
    ViveknSentimentModel,
    ClassifierDLModel,
    UniversalSentenceEncoder
)


class SentimentModelTester:
    """
    Comprehensive tester for NLP sentiment classification models
    """
    
    def __init__(self, model_path=None, use_pretrained=True):
        """
        Initialize tester
        
        Args:
            model_path: Path to saved model (optional)
            use_pretrained: Use Spark-NLP pre-trained model if model_path is None
        """
        self.spark = None
        self.model = None
        self.model_path = model_path
        self.use_pretrained = use_pretrained
        
    def start_spark(self):
        """Start Spark session with Spark-NLP"""
        print("🚀 Starting Spark session with Spark-NLP...")
        
        self.spark = sparknlp.start()
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"✅ Spark {self.spark.version} initialized")
        print(f"✅ Spark-NLP {sparknlp.version()} loaded")
        
        return self.spark
    
    def load_model(self):
        """Load sentiment classification model"""
        
        if self.model_path:
            # Load saved model
            print(f"📦 Loading model from: {self.model_path}")
            try:
                self.model = PipelineModel.load(self.model_path)
                print("✅ Custom model loaded successfully")
            except Exception as e:
                print(f"❌ Failed to load model: {e}")
                sys.exit(1)
                
        elif self.use_pretrained:
            # Build pipeline with pre-trained ViveknSentiment
            print("📦 Building pipeline with pre-trained ViveknSentiment model...")
            
            document = DocumentAssembler() \
                .setInputCol("review") \
                .setOutputCol("document")
            
            tokenizer = Tokenizer() \
                .setInputCols(["document"]) \
                .setOutputCol("token")
            
            sentiment = ViveknSentimentModel.pretrained() \
                .setInputCols(["document", "token"]) \
                .setOutputCol("sentiment")
            
            finisher = Finisher() \
                .setInputCols(["sentiment"]) \
                .setOutputCols(["sentiment_result"]) \
                .setOutputAsArray(False)
            
            pipeline = Pipeline(stages=[document, tokenizer, sentiment, finisher])
            
            # Create dummy data to fit the pipeline
            dummy = self.spark.createDataFrame([("dummy",)], ["review"])
            self.model = pipeline.fit(dummy)
            
            print("✅ Pre-trained model pipeline built successfully")
        else:
            print("❌ No model specified and use_pretrained=False")
            sys.exit(1)
    
    def create_test_data(self):
        """Create comprehensive test dataset"""
        print("\n📝 Creating test dataset...")
        
        test_data = [
            # POSITIVE reviews
            ("This product is absolutely amazing! Best purchase ever!", "positive"),
            ("Excellent quality! Highly recommend to everyone.", "positive"),
            ("Love it! Exceeded all my expectations.", "positive"),
            ("Perfect! Exactly what I needed.", "positive"),
            ("Outstanding product! Worth every penny.", "positive"),
            ("Great value for money. Very satisfied.", "positive"),
            ("Superb quality and fast delivery!", "positive"),
            ("Fantastic! Will definitely buy again.", "positive"),
            ("Very happy with this purchase. Top notch!", "positive"),
            ("Brilliant product! No complaints at all.", "positive"),
            
            # NEGATIVE reviews
            ("Terrible quality. Complete waste of money.", "negative"),
            ("Very disappointed. Broke after one day.", "negative"),
            ("Worst product ever. Don't buy!", "negative"),
            ("Poor quality. Not worth the price.", "negative"),
            ("Horrible experience. Would not recommend.", "negative"),
            ("Cheaply made. Fell apart immediately.", "negative"),
            ("Awful product. Total disaster.", "negative"),
            ("Extremely dissatisfied. Returning it.", "negative"),
            ("Bad quality control. Very frustrating.", "negative"),
            ("Useless product. Save your money.", "negative"),
            
            # NEUTRAL reviews
            ("It's okay. Nothing special but works.", "neutral"),
            ("Average product. Does the job.", "neutral"),
            ("It's fine for the price. No complaints.", "neutral"),
            ("Decent quality. Not amazing but acceptable.", "neutral"),
            ("Works as described. Nothing more.", "neutral"),
            ("Fair product. Could be better.", "neutral"),
            ("Acceptable quality. Met expectations.", "neutral"),
            ("It's alright. Not great, not terrible.", "neutral"),
            ("Standard product. No surprises.", "neutral"),
            ("Okay for the price. Nothing exceptional.", "neutral"),
            
            # EDGE CASES
            ("Love the color but quality is poor.", "neutral"),  # Mixed
            ("Great price! But shipping took forever.", "neutral"),  # Mixed
            ("", "neutral"),  # Empty review
            ("Good", "positive"),  # Single word
            ("Bad", "negative"),  # Single word
            ("NOT GOOD AT ALL!", "negative"),  # Caps with negation
            ("This is NOT terrible, actually quite nice.", "positive"),  # Double negative
            ("😊👍", "positive"),  # Emojis only
            ("Meh", "neutral"),  # Slang
            ("10/10 would recommend!", "positive"),  # Rating format
        ]
        
        schema = StructType([
            StructField("review", StringType(), True),
            StructField("expected_sentiment", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema)
        
        print(f"✅ Created {df.count()} test reviews")
        
        return df
    
    def predict(self, df):
        """Apply model to test data"""
        print("\n🔮 Running predictions...")
        
        predictions = self.model.transform(df)
        
        print("✅ Predictions completed")
        
        return predictions
    
    def evaluate_predictions(self, predictions):
        """Evaluate model performance"""
        print("\n" + "="*80)
        print("📊 EVALUATION RESULTS")
        print("="*80)
        
        # Show sample predictions
        print("\n📋 Sample Predictions:")
        predictions.select(
            "review",
            "expected_sentiment",
            "sentiment_result"
        ).show(10, truncate=50)
        
        # Calculate accuracy
        correct = predictions.filter(
            col("expected_sentiment") == col("sentiment_result")
        ).count()
        
        total = predictions.count()
        accuracy = (correct / total) * 100 if total > 0 else 0
        
        print(f"\n🎯 Overall Accuracy: {accuracy:.2f}% ({correct}/{total})")
        
        # Confusion matrix
        print("\n📈 Confusion Matrix:")
        confusion = predictions.groupBy("expected_sentiment", "sentiment_result") \
            .count() \
            .orderBy("expected_sentiment", "sentiment_result")
        
        confusion.show()
        
        # Per-class metrics
        print("\n📊 Per-Class Performance:")
        for sentiment in ["positive", "negative", "neutral"]:
            class_total = predictions.filter(
                col("expected_sentiment") == sentiment
            ).count()
            
            class_correct = predictions.filter(
                (col("expected_sentiment") == sentiment) &
                (col("sentiment_result") == sentiment)
            ).count()
            
            class_accuracy = (class_correct / class_total * 100) if class_total > 0 else 0
            
            print(f"  {sentiment.capitalize():8s}: {class_accuracy:5.2f}% ({class_correct}/{class_total})")
        
        # Detailed metrics using Spark ML evaluator
        print("\n📈 Detailed Metrics:")
        
        # Convert labels to numeric for evaluation
        label_indexer = StringIndexer(
            inputCol="expected_sentiment",
            outputCol="label_indexed"
        )
        pred_indexer = StringIndexer(
            inputCol="sentiment_result",
            outputCol="prediction_indexed"
        )
        
        indexed = label_indexer.fit(predictions).transform(predictions)
        indexed = pred_indexer.fit(indexed).transform(indexed)
        
        # Calculate metrics
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="label_indexed",
            predictionCol="prediction_indexed",
            metricName="f1"
        )
        
        evaluator_precision = MulticlassClassificationEvaluator(
            labelCol="label_indexed",
            predictionCol="prediction_indexed",
            metricName="weightedPrecision"
        )
        
        evaluator_recall = MulticlassClassificationEvaluator(
            labelCol="label_indexed",
            predictionCol="prediction_indexed",
            metricName="weightedRecall"
        )
        
        f1 = evaluator_f1.evaluate(indexed)
        precision = evaluator_precision.evaluate(indexed)
        recall = evaluator_recall.evaluate(indexed)
        
        print(f"  F1 Score:  {f1:.4f}")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall:    {recall:.4f}")
        
        return {
            "accuracy": accuracy,
            "f1_score": f1,
            "precision": precision,
            "recall": recall
        }
    
    def test_edge_cases(self):
        """Test specific edge cases"""
        print("\n" + "="*80)
        print("🧪 EDGE CASE TESTING")
        print("="*80)
        
        edge_cases = [
            ("", "How does it handle empty reviews?"),
            ("a", "Single character"),
            ("Good good good good good", "Repeated words"),
            ("NOT bad NOT terrible NOT awful", "Multiple negations"),
            ("This is... okay? Maybe? I don't know.", "Uncertainty"),
            ("AMAZING!!! LOVE IT!!!", "Excessive punctuation"),
            ("terrible terrible TERRIBLE", "Mixed case repetition"),
            ("The best worst product ever", "Contradictory terms"),
        ]
        
        df = self.spark.createDataFormat(
            [(text, desc) for text, desc in edge_cases],
            ["review", "description"]
        )
        
        results = self.model.transform(df)
        
        print("\n📋 Edge Case Results:")
        results.select("description", "review", "sentiment_result").show(
            truncate=50
        )
    
    def test_custom_reviews(self, reviews):
        """Test custom reviews provided by user"""
        print("\n" + "="*80)
        print("🎯 CUSTOM REVIEW TESTING")
        print("="*80)
        
        df = self.spark.createDataFrame(
            [(review,) for review in reviews],
            ["review"]
        )
        
        results = self.model.transform(df)
        
        print("\n📋 Custom Review Results:")
        results.select("review", "sentiment_result").show(truncate=100)
        
        return results
    
    def run_full_test(self):
        """Run complete test suite"""
        print("\n" + "="*80)
        print("🧪 COMPREHENSIVE SENTIMENT MODEL TESTING")
        print("="*80)
        
        # 1. Initialize
        self.start_spark()
        
        # 2. Load model
        self.load_model()
        
        # 3. Create test data
        test_df = self.create_test_data()
        
        # 4. Predict
        predictions = self.predict(test_df)
        
        # 5. Evaluate
        metrics = self.evaluate_predictions(predictions)
        
        # 6. Test edge cases
        # self.test_edge_cases()  # Uncomment if needed
        
        # 7. Summary
        print("\n" + "="*80)
        print("✅ TESTING COMPLETED")
        print("="*80)
        print(f"✅ Accuracy: {metrics['accuracy']:.2f}%")
        print(f"✅ F1 Score: {metrics['f1_score']:.4f}")
        
        if metrics['accuracy'] >= 80:
            print("\n🎉 Model performance is EXCELLENT!")
        elif metrics['accuracy'] >= 60:
            print("\n👍 Model performance is GOOD")
        else:
            print("\n⚠️ Model performance needs improvement")
        
        return metrics


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Test NLP sentiment classification model"
    )
    parser.add_argument(
        "--model-path",
        type=str,
        default=None,
        help="Path to saved model (optional, uses pre-trained if not provided)"
    )
    parser.add_argument(
        "--custom-reviews",
        nargs="+",
        help="Test specific custom reviews"
    )
    
    args = parser.parse_args()
    
    # Create tester
    tester = SentimentModelTester(
        model_path=args.model_path,
        use_pretrained=(args.model_path is None)
    )
    
    # Run tests
    if args.custom_reviews:
        # Test custom reviews only
        tester.start_spark()
        tester.load_model()
        tester.test_custom_reviews(args.custom_reviews)
    else:
        # Run full test suite
        tester.run_full_test()
    
    # Stop Spark
    tester.spark.stop()
    print("\n✅ Testing session ended")


if __name__ == "__main__":
    main()
