import os
import shutil
import tensorflow as tf
from transformers import TFBertForSequenceClassification, BertTokenizer

# Paths inside Docker
MODEL_PATH = "/opt/spark-apps/model/final_bert_review_model"
EXPORT_DIR = "/opt/spark-apps/model/bert_tf_spark_nlp"

def convert():
    print(f"🔄 Loading PyTorch model from {MODEL_PATH}...")
    
    # Load model converting from PyTorch
    # This requires 'torch' and 'safetensors' to be installed
    try:
        model = TFBertForSequenceClassification.from_pretrained(MODEL_PATH, from_pt=True)
        tokenizer = BertTokenizer.from_pretrained(MODEL_PATH)
        
        print(f"💾 Saving TensorFlow SavedModel to {EXPORT_DIR}...")
        
        # Clean output dir if exists
        if os.path.exists(EXPORT_DIR):
            shutil.rmtree(EXPORT_DIR)
            
        # Save model in TF SavedModel format
        # This creates a 'saved_model.pb' in the directory
        model.save_pretrained(EXPORT_DIR, saved_model=True)
        
        # Save tokenizer assets (vocab.txt)
        tokenizer.save_pretrained(EXPORT_DIR)
        
        # Move vocab to assets folder for Spark NLP
        # Expected structure: .../saved_model/1/assets/vocab.txt
        saved_model_dir = os.path.join(EXPORT_DIR, "saved_model", "1")
        assets_dir = os.path.join(saved_model_dir, "assets")
        os.makedirs(assets_dir, exist_ok=True)
        
        vocab_src = os.path.join(EXPORT_DIR, "vocab.txt")
        vocab_dst = os.path.join(assets_dir, "vocab.txt")
        
        if os.path.exists(vocab_src):
            shutil.copy(vocab_src, vocab_dst)
            print(f"✅ Copied vocab.txt to {vocab_dst}")
        
        print("✅ Conversion successful!")
        print(f"📂 Output directory: {EXPORT_DIR}")
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    convert()
