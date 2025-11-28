# NLP Model Testing Guide

## 📋 Overview

This directory contains comprehensive testing scripts for your sentiment classification models.

## 🧪 Test Files

### **1. test_nlp_model_comprehensive.py**
Full-featured testing script for NLP sentiment models.

**Features:**
- ✅ Tests pre-trained models OR your custom models
- ✅ 40+ test cases (positive, negative, neutral, edge cases)
- ✅ Detailed evaluation metrics (accuracy, F1, precision, recall)
- ✅ Confusion matrix analysis
- ✅ Per-class performance breakdown
- ✅ Custom review testing

### **2. test_sentiment_model.py**
Your existing test for BERT-based models.

---

## 🚀 Usage

### **Option 1: Test Pre-trained Model (No Training Required)**

```bash
# Use Spark-NLP's pre-trained ViveknSentiment model
python test_nlp_model_comprehensive.py
```

This will:
1. Download Spark-NLP pre-trained model automatically
2. Run 40+ test cases
3. Show accuracy, F1 score, confusion matrix
4. Display sample predictions

### **Option 2: Test Your Custom Trained Model**

```bash
# Test your saved model
python test_nlp_model_comprehensive.py --model-path /path/to/your/model
```

Example:
```bash
python test_nlp_model_comprehensive.py \
  --model-path /opt/spark-data/models/sentiment_classifier
```

### **Option 3: Test Specific Reviews**

```bash
# Test custom reviews only
python test_nlp_model_comprehensive.py \
  --custom-reviews \
  "This product is amazing!" \
  "Terrible quality" \
  "It's okay, nothing special"
```

### **Option 4: Docker Spark Cluster**

```bash
# Submit to cluster
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 \
  /opt/spark-apps/Tests/test_nlp_model_comprehensive.py
```

---

## 📊 Output Example

```
🧪 COMPREHENSIVE SENTIMENT MODEL TESTING
================================================================================
🚀 Starting Spark session with Spark-NLP...
✅ Spark 3.5.0 initialized
✅ Spark-NLP 5.1.4 loaded

📦 Building pipeline with pre-trained ViveknSentiment model...
✅ Pre-trained model pipeline built successfully

📝 Creating test dataset...
✅ Created 40 test reviews

🔮 Running predictions...
✅ Predictions completed

================================================================================
📊 EVALUATION RESULTS
================================================================================

📋 Sample Predictions:
+--------------------------------------------------+-------------------+----------------+
|review                                             |expected_sentiment|sentiment_result|
+--------------------------------------------------+-------------------+----------------+
|This product is absolutely amazing! Best purc...  |positive          |positive        |
|Excellent quality! Highly recommend to everyone.  |positive          |positive        |
|Terrible quality. Complete waste of money.        |negative          |negative        |
|It's okay. Nothing special but works.             |neutral           |neutral         |
+--------------------------------------------------+-------------------+----------------+

🎯 Overall Accuracy: 85.00% (34/40)

📈 Confusion Matrix:
+-------------------+----------------+-----+
|expected_sentiment|sentiment_result|count|
+-------------------+----------------+-----+
|negative          |negative        |   9 |
|negative          |neutral         |   1 |
|neutral           |neutral         |   8 |
|neutral           |positive        |   2 |
|positive          |positive        |  10 |
+-------------------+----------------+-----+

📊 Per-Class Performance:
  Positive: 100.00% (10/10)
  Negative:  90.00% (9/10)
  Neutral :  80.00% (8/10)

📈 Detailed Metrics:
  F1 Score:  0.8456
  Precision: 0.8521
  Recall:    0.8500

================================================================================
✅ TESTING COMPLETED
================================================================================
✅ Accuracy: 85.00%
✅ F1 Score: 0.8456

🎉 Model performance is EXCELLENT!
```

---

## 📝 Test Cases Included

### **Positive Reviews (10 cases)**
- Strong positive language
- Recommendations
- Satisfaction expressions
- Value statements

### **Negative Reviews (10 cases)**
- Quality complaints
- Disappointment
- Warnings
- Refund requests

### **Neutral Reviews (10 cases)**
- Mixed opinions
- "Okay" statements
- Fair assessments
- Average ratings

### **Edge Cases (10 cases)**
- Empty reviews
- Single words
- Emojis
- Multiple negations
- Contradictory terms
- Excessive punctuation
- Mixed case

---

## 🔧 Customization

### **Modify Test Data**

Edit `create_test_data()` method:

```python
def create_test_data(self):
    test_data = [
        ("Your custom review here", "expected_sentiment"),
        # Add more test cases
    ]
    # ...
```

### **Change Evaluation Metrics**

Add custom evaluators:

```python
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"  # or "f1", "precision", "recall"
)
```

### **Test Different Models**

```python
# Test ViveknSentiment
tester = SentimentModelTester(use_pretrained=True)

# Test ClassifierDL (requires more setup)
# Build ClassifierDL pipeline and pass model_path

# Test your custom model
tester = SentimentModelTester(model_path="/your/model/path")
```

---

## 📈 Performance Benchmarks

### **Expected Performance:**

| Model | Accuracy | F1 Score | Speed |
|-------|----------|----------|-------|
| ViveknSentiment | 75-85% | 0.75-0.85 | Fast |
| ClassifierDL | 85-92% | 0.85-0.92 | Medium |
| Custom BERT | 90-95% | 0.90-0.95 | Slow |

### **Interpreting Results:**

- **Accuracy > 80%**: Excellent performance
- **Accuracy 60-80%**: Good performance
- **Accuracy < 60%**: Needs improvement

- **F1 Score > 0.80**: Well-balanced model
- **F1 Score 0.60-0.80**: Acceptable balance
- **F1 Score < 0.60**: Imbalanced predictions

---

## 🐛 Troubleshooting

### **Error: Model not found**
```bash
# Check model path
ls -la /opt/spark-data/models/sentiment_classifier

# Ensure model was saved correctly
# Re-train and save model if needed
```

### **Error: Spark-NLP package not found**
```bash
# Ensure package is specified
spark-submit \
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 \
  test_nlp_model_comprehensive.py
```

### **Low Accuracy**
- Check if test data matches training data domain
- Verify model was trained correctly
- Try different pre-trained model
- Increase training data size

### **Memory Issues**
```python
# Reduce test data size
test_data = test_data[:20]  # Use fewer test cases

# Or increase Spark memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

---

## 📚 Additional Resources

- **Spark-NLP Testing**: https://nlp.johnsnowlabs.com/docs/en/quickstart
- **Model Evaluation**: https://spark.apache.org/docs/latest/ml-tuning.html
- **Metrics Guide**: https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html

---

## ✅ Quick Test Checklist

- [ ] Test pre-trained model works
- [ ] Test with sample positive reviews
- [ ] Test with sample negative reviews
- [ ] Test with neutral reviews
- [ ] Test edge cases (empty, special chars)
- [ ] Check accuracy > 75%
- [ ] Check F1 score > 0.70
- [ ] Test with your custom model (if trained)
- [ ] Test integration with streaming pipeline
- [ ] Verify predictions match expectations

---

**Happy Testing!** 🧪🎯
