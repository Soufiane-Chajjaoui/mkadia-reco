# Correct Model Conversion for Spark NLP

Your previous conversion saved the model in Hugging Face format (`.safetensors`), but Spark NLP requires **TensorFlow SavedModel format** (`.pb` file).

## Updated Colab Script

Use this corrected script in Google Colab:

```python
!pip install transformers tensorflow torch

import os
import shutil
from transformers import TFBertForSequenceClassification, BertTokenizer

# Path to your uploaded PyTorch model
INPUT_MODEL_PATH = "/content/final_bert_review_model_pytorch"
OUTPUT_DIR = "/content/bert_tf_spark_nlp"

def convert():
    print(f"🔄 Loading PyTorch model from {INPUT_MODEL_PATH}...")
    
    try:
        # Load and convert from PyTorch to TensorFlow
        model = TFBertForSequenceClassification.from_pretrained(INPUT_MODEL_PATH, from_pt=True)
        tokenizer = BertTokenizer.from_pretrained(INPUT_MODEL_PATH)
        
        print(f"💾 Saving TensorFlow SavedModel format...")
        
        # IMPORTANT: Use saved_model=True to create the .pb format
        model.save_pretrained(OUTPUT_DIR, saved_model=True)
        tokenizer.save_pretrained(OUTPUT_DIR)
        
        # Prepare assets for Spark NLP
        saved_model_dir = os.path.join(OUTPUT_DIR, "saved_model", "1")
        assets_dir = os.path.join(saved_model_dir, "assets")
        os.makedirs(assets_dir, exist_ok=True)
        
        # Copy vocab to assets
        vocab_src = os.path.join(OUTPUT_DIR, "vocab.txt")
        vocab_dst = os.path.join(assets_dir, "vocab.txt")
        if os.path.exists(vocab_src):
            shutil.copy(vocab_src, vocab_dst)
            print(f"✅ Copied vocab.txt to assets")
        
        print("✅ Conversion successful!")
        print(f"📂 Model saved to: {saved_model_dir}")
        
        # Verify the saved_model.pb exists
        pb_file = os.path.join(saved_model_dir, "saved_model.pb")
        if os.path.exists(pb_file):
            print(f"✅ Verified: saved_model.pb exists ({os.path.getsize(pb_file)} bytes)")
        else:
            print("❌ ERROR: saved_model.pb not found!")
        
        # Zip the saved_model/1 directory for download
        shutil.make_archive("/content/bert_spark_nlp", 'zip', saved_model_dir)
        print("📦 Created bert_spark_nlp.zip for download")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

convert()
```

## After Running

1. Download `bert_spark_nlp.zip`
2. Extract to: `spark\apps\model\final_bert_review_model_tf\saved_model\1\`
3. Verify `saved_model.pb` exists in that folder
4. Run the test!
