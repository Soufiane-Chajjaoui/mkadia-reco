# How to Convert PyTorch BERT Model to Spark NLP (TensorFlow)

Since your local environment/Docker container lacks the heavy dependencies (`torch`, `tensorflow`) to convert the model, it's recommended to use **Google Colab**.

## Steps

1.  **Zip your model folder**:
    *   Go to `spark/apps/model/`
    *   Zip the `final_bert_review_model` folder.

2.  **Open Google Colab**: [https://colab.research.google.com/](https://colab.research.google.com/)

3.  **Upload & Unzip**:
    *   Upload your zip file.
    *   Run: `!unzip final_bert_review_model.zip`

4.  **Run Conversion Script**:
    Copy and paste this code into a cell:

    ```python
    !pip install transformers tensorflow torch

    import os
    import shutil
    from transformers import TFBertForSequenceClassification, BertTokenizer

    # --- CONFIGURATION ---
    INPUT_MODEL_PATH = "/content/final_bert_review_model" 
    OUTPUT_DIR = "/content/bert_tf_spark_nlp"
    # ---------------------

    def convert():
        print(f"🔄 Loading PyTorch model from {INPUT_MODEL_PATH}...")
        try:
            model = TFBertForSequenceClassification.from_pretrained(INPUT_MODEL_PATH, from_pt=True)
            tokenizer = BertTokenizer.from_pretrained(INPUT_MODEL_PATH)
            
            print(f"💾 Saving TensorFlow SavedModel to {OUTPUT_DIR}...")
            model.save_pretrained(OUTPUT_DIR, saved_model=True)
            tokenizer.save_pretrained(OUTPUT_DIR)
            
            # Prepare assets for Spark NLP
            saved_model_dir = os.path.join(OUTPUT_DIR, "saved_model", "1")
            assets_dir = os.path.join(saved_model_dir, "assets")
            os.makedirs(assets_dir, exist_ok=True)
            
            vocab_src = os.path.join(OUTPUT_DIR, "vocab.txt")
            vocab_dst = os.path.join(assets_dir, "vocab.txt")
            if os.path.exists(vocab_src):
                shutil.copy(vocab_src, vocab_dst)
            
            print("✅ Conversion successful!")
            
            # Zip output
            shutil.make_archive("/content/bert_tf_model", 'zip', saved_model_dir)
            print("📦 Download 'bert_tf_model.zip' from the files pane!")
            
        except Exception as e:
            print(f"❌ Error: {e}")

    convert()
    ```

5.  **Download & Install**:
    *   Download `bert_tf_model.zip`.
    *   Extract it on your machine to:
        `C:\Users\SAMSUNG\Documents\GitHub\mkadia-reco\spark\apps\model\bert_tf_spark_nlp\saved_model\1`
    *   Ensure `saved_model.pb` is inside that folder.

6.  **Run Test**:
    ```powershell
    docker exec -it spark-master spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 --master spark://spark-master:7077 /opt/spark-apps/Tests/test_sentiment_model.py
    ```
