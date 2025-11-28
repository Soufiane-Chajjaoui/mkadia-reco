# Model Optimization Strategies for Production

## 1. Use Smaller/Faster Models

### Current Setup
- **Model**: `bert_sequence_classifier_multilingual_sentiment`
- **Size**: ~670MB
- **Inference Time**: ~2-3 seconds per batch

### Recommended Alternatives

#### Option A: Use DistilBERT (Recommended for Production)
- **Size**: ~260MB (60% reduction)
- **Speed**: 2x faster than BERT
- **Accuracy**: ~97% of BERT's accuracy

```python
# Not all DistilBERT models are available pre-trained in Spark NLP
# You may need to use sentiment_dl (universal sentence encoder based)
```

#### Option B: Use Lightweight Sentiment Models
```python
# ViveknSentiment - Only 5MB! Very fast
from sparknlp.annotator import ViveknSentimentModel

sentiment = ViveknSentimentModel.pretrained() \
    .setInputCols(["document", "token"]) \
    .setOutputCol("sentiment")
```

#### Option C: SentimentDL (Universal Sentence Encoder)
- **Size**: ~250MB
- **Speed**: Faster than BERT
- **Good for English**

---

## 2. Batch Processing Optimization

### Current Issue
Processing one review at a time is inefficient.

### Solution: Process in Batches
```python
# In your streaming app, collect micro-batches
df.write \
  .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df)) \
  .start()

def process_batch(df):
    # Process 100+ reviews at once
    results = sentiment_pipeline.transform(df)
    return results
```

---

## 3. Spark Configuration Tuning

### Update spark-submit Command
```bash
docker exec -it spark-master spark-submit \
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.2 \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=8 \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 2 \
  --master spark://spark-master:7077 \
  /opt/spark-apps/streaming_with_sentiment.py
```

---

## 4. Model Loading Optimization

### Cache the Model (Avoid Reloading)
```python
# Load model ONCE and reuse
class SentimentClassifier:
    _model_cache = None
    
    @classmethod
    def get_model(cls, spark):
        if cls._model_cache is None:
            cls._model_cache = BertForSequenceClassification.load(MODEL_PATH)
        return cls._model_cache
```

---

## 5. Use ONNX Runtime (Advanced)

Spark NLP 5.3+ supports ONNX models which are faster:

```python
# Convert to ONNX format (in Colab)
from optimum.onnxruntime import ORTModelForSequenceClassification

model = ORTModelForSequenceClassification.from_pretrained(
    "your-model",
    from_transformers=True,
    export=True
)
model.save_pretrained("model_onnx")
```

---

## 6. Reduce Max Sentence Length

```python
bert_classifier = BertForSequenceClassification \
    .load(LOCAL_MODEL_PATH) \
    .setInputCols(["token", "document"]) \
    .setOutputCol("class") \
    .setMaxSentenceLength(128)  # Reduced from 512 → 4x faster!
```

**Trade-off**: Longer reviews will be truncated.

---

## Performance Comparison

| Strategy | Size Reduction | Speed Improvement | Accuracy Impact |
|----------|----------------|-------------------|-----------------|
| DistilBERT | 60% | 2x | -3% |
| Max Length 128 | 0% | 4x | -1% (for short texts) |
| ViveknSentiment | 99% | 10x+ | -15% |
| Batch Size 32 | 0% | 3x | 0% |
| ONNX Runtime | 0% | 1.5x | 0% |

---

## Recommended Setup for Production

### Best Balance: Speed + Accuracy
1. **Use current BERT** but with optimizations:
   - `setMaxSentenceLength(128)` 
   - Batch processing (32-64 reviews)
   - Cache model in memory

### Best Speed: Sacrifice Some Accuracy
1. **Switch to ViveknSentiment** (5MB)
2. Very fast inference
3. Good enough for basic sentiment

### I can help you implement any of these! Which optimization would you like to try first?
