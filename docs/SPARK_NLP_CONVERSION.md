# Convert Hugging Face BERT Model to Spark NLP Format

This script converts your PyTorch/TensorFlow BERT model to Spark NLP's specific format.

## Run in Google Colab

```python
!pip install spark-nlp transformers torch

import sparknlp
from sparknlp.annotator import *
from sparknlp.base import *
import tensorflow as tf
from transformers import TFBertForSequenceClassification, BertTokenizer

# Start Spark NLP
spark = sparknlp.start()

# Paths
PYTORCH_MODEL_PATH = "/content/final_bert_review_model_pytorch"  # Your uploaded PyTorch model
OUTPUT_PATH = "/content/spark_nlp_model"  # Output for Spark NLP format

print("🔄 Step 1: Loading PyTorch model...")
# Load the PyTorch model
tokenizer = BertTokenizer.from_pretrained(PYTORCH_MODEL_PATH)
model = TFBertForSequenceClassification.from_pretrained(PYTORCH_MODEL_PATH, from_pt=True)

print("💾 Step 2: Saving in TensorFlow SavedModel format...")
# Save as TensorFlow SavedModel
tf_model_path = "/content/tf_model"
model.save_pretrained(tf_model_path, saved_model=True)

print("🔧 Step 3: Converting to Spark NLP format...")
# Use Spark NLP's import function
try:
    # Import the model using Spark NLP's tools
    from sparknlp.annotator import BertForSequenceClassification
    
    # This is the correct way to import a Hugging Face model into Spark NLP
    bert_model = BertForSequenceClassification.loadSavedModel(
        folder=f"{tf_model_path}/saved_model/1",
        spark_session=spark
    )
    
    # Save in Spark NLP format
    bert_model.write().overwrite().save(OUTPUT_PATH)
    
    print(f"✅ Model saved in Spark NLP format at: {OUTPUT_PATH}")
    
    # Zip for download
    import shutil
    shutil.make_archive("/content/spark_nlp_bert_model", 'zip', OUTPUT_PATH)
    print("📦 Created spark_nlp_bert_model.zip for download")
    
except Exception as e:
    print(f"❌ Spark NLP import failed: {e}")
    print("\n💡 Alternative: Use Spark NLP's pretrained model import")
    print("   This requires the model to be in a specific format")
```

## Alternative: Direct Spark NLP Training

If the above doesn't work, the most reliable approach is to **train/fine-tune directly with Spark NLP**:

```python
# This ensures perfect compatibility
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.training import CoNLL

# Load your training data
# Fine-tune a BERT model using Spark NLP's training pipeline
bert = BertForSequenceClassification.pretrained("bert_base_sequence_classifier_imdb") \
    .setInputCols(["document", "token"]) \
    .setOutputCol("class")

# Fine-tune on your data
# ... training code ...

# Save
bert.write().overwrite().save("/content/my_bert_model")
```

## What to Download

After running the script, download `spark_nlp_bert_model.zip` and extract to:
`spark/apps/model/spark_nlp_bert/`

Then update the test script to use this path.
