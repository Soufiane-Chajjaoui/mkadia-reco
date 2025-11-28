# Hugging Face BERT to Spark NLP Conversion (Correct Method)

## Step-by-Step Conversion in Google Colab

```python
# Install dependencies
!pip install spark-nlp==5.1.4 pyspark==3.5.0 transformers torch tensorflow

import sparknlp
from sparknlp.annotator import BertForSequenceClassification
from sparknlp.base import DocumentAssembler
from transformers import TFBertForSequenceClassification, BertTokenizer
import tensorflow as tf
import os
import shutil

# Start Spark NLP
spark = sparknlp.start()
print(f"✅ Spark NLP {sparknlp.version()} started")

# ============================================================================
# STEP 1: Ensure you have all Hugging Face files
# ============================================================================
# Upload your PyTorch model zip and unzip it to this path
PYTORCH_MODEL_PATH = "/content/final_bert_review_model_pytorch" 
HF_TF_PATH = "/content/hf_tf_model"

print("\n📦 Step 1: Loading and saving complete Hugging Face model...")

# Load from PyTorch
tokenizer = BertTokenizer.from_pretrained(PYTORCH_MODEL_PATH)
model = TFBertForSequenceClassification.from_pretrained(PYTORCH_MODEL_PATH, from_pt=True)

# Save with ALL files (config.json, vocab.txt, tokenizer.json, model)
model.save_pretrained(HF_TF_PATH, saved_model=True)
tokenizer.save_pretrained(HF_TF_PATH)

print(f"✅ Saved complete HF model to {HF_TF_PATH}")
print(f"   Files: {os.listdir(HF_TF_PATH)}")

# ============================================================================
# STEP 2: Create labels.txt (REQUIRED by Spark NLP)
# ============================================================================
print("\n📝 Step 2: Creating labels.txt...")

labels_path = os.path.join(HF_TF_PATH, "saved_model", "1", "assets")
os.makedirs(labels_path, exist_ok=True)

# Your model's labels (1-5 star ratings)
# IMPORTANT: Ensure these match your model's actual output classes
labels = ["1", "2", "3", "4", "5"]
with open(os.path.join(labels_path, "labels.txt"), "w") as f:
    f.write("\n".join(labels))

# Also copy vocab.txt to assets
vocab_src = os.path.join(HF_TF_PATH, "vocab.txt")
vocab_dst = os.path.join(labels_path, "vocab.txt")
if os.path.exists(vocab_src):
    shutil.copy(vocab_src, vocab_dst)
    print(f"✅ Copied vocab.txt to assets")

print(f"✅ Created labels.txt with {len(labels)} labels")

# ============================================================================
# STEP 3: Load into Spark NLP and save in Spark NLP format
# ============================================================================
print("\n🔧 Step 3: Converting to Spark NLP format...")

SPARK_NLP_OUTPUT = "/content/spark_nlp_bert_model"
saved_model_path = os.path.join(HF_TF_PATH, "saved_model", "1")

try:
    # Load the TensorFlow SavedModel into Spark NLP
    # This imports the TF graph and converts it to Spark NLP's internal format
    bert_classifier = BertForSequenceClassification.loadSavedModel(
        folder=saved_model_path,
        spark_session=spark
    ) \
    .setInputCols(["document", "token"]) \
    .setOutputCol("class") \
    .setMaxSentenceLength(512) \
    .setCaseSensitive(True)
    
    print("✅ Model loaded into Spark NLP successfully!")
    
    # Save in Spark NLP's native format
    bert_classifier.write().overwrite().save(SPARK_NLP_OUTPUT)
    print(f"✅ Saved in Spark NLP format to: {SPARK_NLP_OUTPUT}")
    
    # ========================================================================
    # STEP 4: Package for download
    # ========================================================================
    print("\n📦 Step 4: Creating zip file...")
    shutil.make_archive("/content/spark_nlp_bert_final", 'zip', SPARK_NLP_OUTPUT)
    print("✅ Created spark_nlp_bert_final.zip")
    print("\n🎉 SUCCESS! Download spark_nlp_bert_final.zip")
    
except Exception as e:
    print(f"\n❌ ERROR during Spark NLP conversion: {e}")
    print("\n💡 Troubleshooting:")
    print("   1. Check that saved_model.pb exists")
    print("   2. Verify labels.txt is in assets folder")
    print("   3. Ensure vocab.txt is in assets folder")
    
    import traceback
    traceback.print_exc()
    
    # Show what files we have
    print(f"\n📂 Files in {saved_model_path}:")
    for root, dirs, files in os.walk(saved_model_path):
        level = root.replace(saved_model_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f'{indent}{os.path.basename(root)}/')
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            print(f'{subindent}{file}')
```

## After Running in Colab

1. **Download** `spark_nlp_bert_final.zip`
2. **Extract** the contents. You should see folders like `metadata`, `fields`, etc.
3. **Place** these extracted contents into your Docker volume:
   `spark/apps/model/bert_spark_nlp/`
   *(Delete the old contents first)*

## Verify

The directory `spark/apps/model/bert_spark_nlp/` should now contain:
- `metadata/` folder
- `fields/` folder
- ...and other Spark NLP files (NOT `saved_model.pb`)
