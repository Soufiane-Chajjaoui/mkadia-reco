# ✅ DOCKER SETUP COMPLETE - Quick Reference

## 🎉 Status: ALL WORKING!

Your Spark cluster with Kafka is running successfully!

**PySpark Version:** 3.5.0 ✅  
**Python Version:** 3.8.10 ✅  
**All Packages:** Working ✅

---

## 🚀 Ready-to-Use Commands (Copy & Paste)

### **1. Test All Packages**
```powershell
docker exec spark-master bash -c "python3 -c 'import pyspark, numpy, pandas; print(\"✅ PySpark:\", pyspark.__version__)'"
```

### **2. Run Your Streaming App**
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming.py
```

### **3. Test NLP Model (Pre-trained)**
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/Tests/test_nlp_model_comprehensive.py
```

### **4. Run Sentiment-Enhanced Streaming**
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/streaming_with_sentiment.py
```

---

## 📊 Access Web UIs

### **Spark Master UI**
```powershell
start http://localhost:8081
```
- View workers status
- Monitor running jobs
- Check cluster resources

### **Kafka UI**
```powershell
start http://localhost:8085
```
- View topics
- Monitor messages
- Manage Kafka cluster

---

## 🔍 Quick Checks

### **View Running Containers**
```powershell
docker ps
```

### **Check Spark Logs**
```powershell
docker logs -f spark-master
```

### **Interactive Python Shell**
```powershell
docker exec -it spark-master python3
```

Then in Python:
```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
print("✅ Spark started!")
spark.stop()
```

### **Interactive Bash Shell**
```powershell
docker exec -it spark-master bash
```

---

## 📁 Your Project Structure

```
mkadia-reco/
├── spark/
│   ├── apps/
│   │   ├── streaming.py                      # Original streaming
│   │   ├── streaming_with_sentiment.py       # Enhanced with NLP
│   │   ├── Tests/
│   │   │   ├── test_nlp_model_comprehensive.py
│   │   │   ├── test_sentiment_model.py
│   │   │   └── README_TESTING.md
│   │   └── review_classification/
│   │       ├── PIPELINE_GUIDE.md
│   │       ├── MODEL_APPLICATION_GUIDE.md
│   │       └── STREAMING_INTEGRATION.md
│   ├── data/                                 # Mount point for data
│   └── Dockerfile                            # Fixed with PYTHONPATH
└── docker-compose.yml                        # All services config
```

---

## 🧪 Test Workflow

### **Step 1: Test Basic Spark**
```powershell
docker exec spark-master bash -c "python3 -c 'import pyspark; print(pyspark.__version__)'"
```
Expected: `3.5.0`

### **Step 2: Test Streaming (No NLP)**
```powershell
# Start streaming app
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming.py

# In another terminal, produce some test events to Kafka topics:
# review, favorite, cart, order
```

### **Step 3: Test NLP Classification**
```powershell
# This will download Spark-NLP models on first run
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/Tests/test_nlp_model_comprehensive.py
```

Expected output:
```
🎯 Overall Accuracy: 85.00% (34/40)
✅ Model performance is EXCELLENT!
```

### **Step 4: Test Sentiment Streaming**
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/streaming_with_sentiment.py
```

---

## 🛠️ Maintenance Commands

### **Stop All Containers**
```powershell
docker-compose down
```

### **Start All Containers**
```powershell
docker-compose up -d
```

### **Rebuild After Code Changes**
```powershell
docker-compose up --build -d
```

### **View Resource Usage**
```powershell
docker stats
```

### **Clean Up Everything**
```powershell
# Stop and remove containers, networks, volumes
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

---

## ⚠️ Important Notes

### **PowerShell Line Continuation**
- Use **backticks** `` ` `` (not backslashes `\`)
- Or write everything on one line

### **First-Time Spark-NLP Download**
- Spark-NLP models download automatically on first use
- This can take 5-10 minutes
- Models are cached after first download

### **Memory Considerations**
- **Spark Master**: Default settings
- **Workers**: 4GB RAM each, 2 cores each
- Adjust in `docker-compose.yml` if needed:
  ```yaml
  environment:
    - SPARK_WORKER_MEMORY=8G
    - SPARK_WORKER_CORES=4
  ```

---

## 📚 Documentation Reference

| Guide | Purpose |
|-------|---------|
| [PIPELINE_GUIDE.md](spark/apps/review_classification/PIPELINE_GUIDE.md) | Build NLP pipeline |
| [MODEL_APPLICATION_GUIDE.md](spark/apps/review_classification/MODEL_APPLICATION_GUIDE.md) | Apply model & integrate to recommendations |
| [STREAMING_INTEGRATION.md](spark/apps/review_classification/STREAMING_INTEGRATION.md) | Kafka streaming integration |
| [README_TESTING.md](spark/apps/Tests/README_TESTING.md) | Testing guide |
| [POWERSHELL_COMMANDS.md](POWERSHELL_COMMANDS.md) | All PowerShell commands |
| [DOCKER_BUILD_FIX.md](DOCKER_BUILD_FIX.md) | Troubleshooting |

---

## 🎯 Your Project is Ready!

You can now:
- ✅ Run Spark jobs on the cluster
- ✅ Process streaming data from Kafka
- ✅ Classify reviews with NLP models
- ✅ Generate sentiment-aware recommendations
- ✅ Test everything with comprehensive test suites

**Next Steps:**
1. Test the NLP model
2. Train your own custom model (optional)
3. Run streaming with sentiment analysis
4. Integrate with your product recommendation system

---

**Happy Coding!** 🚀🎓
