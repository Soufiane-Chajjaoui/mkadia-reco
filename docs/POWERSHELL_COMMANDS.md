# PowerShell Commands for Docker Spark
**Windows PowerShell Compatible Commands**

## 🔧 Issue: PowerShell Line Continuation

PowerShell uses **backticks** `` ` `` (not backslashes `\`) for line continuation.

## ✅ Correct PowerShell Commands

### **Test Python Packages**
```powershell
# Single line (recommended for PowerShell)
docker exec -it spark-master python3 -c "import pyspark, numpy, pandas; print('✅ All packages work!')"

# Or use environment
docker exec -it spark-master bash -c "python3 -c 'import pyspark, numpy, pandas; print(\"✅ Works!\")'"
```

### **Run Streaming App**
```powershell
# Single line (EASIEST)
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming.py

# Multi-line with PowerShell backticks
docker exec -it spark-master spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/streaming.py
```

### **Run with Spark-NLP**
```powershell
# Single line
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/Tests/test_nlp_model_comprehensive.py

# Multi-line with backticks
docker exec -it spark-master spark-submit `
  --master spark://spark-master:7077 `
  --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 `
  /opt/spark-apps/Tests/test_nlp_model_comprehensive.py
```

### **Run Sentiment-Enhanced Streaming**
```powershell
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/streaming_with_sentiment.py
```

## 🔄 Rebuild After Dockerfile Fix

```powershell
# Stop containers
docker-compose down

# Rebuild with new Dockerfile
docker-compose up --build -d

# Check status
docker ps
```

## 🧪 Quick Tests

### **1. Check PySpark is Available**
```powershell
docker exec -it spark-master python3 -c "import pyspark; print(pyspark.__version__)"
```

Expected output: `3.5.0`

### **2. Check NumPy and Pandas**
```powershell
docker exec -it spark-master python3 -c "import numpy as np, pandas as pd; print(f'NumPy: {np.__version__}, Pandas: {pd.__version__}')"
```

### **3. Verify Spark Submit**
```powershell
docker exec -it spark-master spark-submit --version
```

### **4. Check Workers Connected**
```powershell
docker exec -it spark-master bash -c "curl -s http://localhost:8080 | grep -i worker"
```

## 📊 Monitor Spark Jobs

### **Spark Master UI**
```powershell
# Open in browser
start http://localhost:8081
```

### **Check Logs**
```powershell
# Master logs
docker logs spark-master

# Worker 1 logs
docker logs spark-worker-1

# Follow logs in real-time
docker logs -f spark-master
```

## 🎯 Common Commands

### **Interactive Python Shell**
```powershell
docker exec -it spark-master python3
```

Then in Python:
```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame([(1, "test")], ["id", "value"])
df.show()
spark.stop()
```

### **Interactive Bash Shell**
```powershell
docker exec -it spark-master bash
```

Then you can run commands normally:
```bash
spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming.py
```

### **Check Container Resource Usage**
```powershell
docker stats
```

### **Restart Specific Container**
```powershell
docker restart spark-master
docker restart spark-worker-1
docker restart spark-worker-2
```

## 🐛 Troubleshooting

### **PYTHONPATH Issues**
```powershell
# Check PYTHONPATH in container
docker exec -it spark-master bash -c 'echo $PYTHONPATH'
```

Should output:
```
/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:
```

### **PySpark Not Found**
If you get `ModuleNotFoundError: No module named 'pyspark'`:

1. Rebuild with fixed Dockerfile:
```powershell
docker-compose down
docker-compose up --build -d
```

2. Verify environment:
```powershell
docker exec -it spark-master python3 -c "import sys; print('\n'.join(sys.path))"
```

### **Spark Submit Fails**
```powershell
# Check if master is accessible
docker exec -it spark-worker-1 bash -c "nc -zv spark-master 7077"

# Should output: Connection to spark-master 7077 port [tcp/*] succeeded!
```

## 📚 Useful Aliases (Optional)

Add to your PowerShell profile (`$PROFILE`):

```powershell
# Spark shortcuts
function spark-shell { docker exec -it spark-master bash }
function spark-python { docker exec -it spark-master python3 }
function spark-logs { docker logs -f spark-master }
function spark-submit-local { docker exec -it spark-master spark-submit $args }
function spark-ui { start http://localhost:8081 }
function kafka-ui { start http://localhost:8085 }
```

Then use:
```powershell
spark-shell
spark-python
spark-ui
```

## 🚀 Ready-to-Use Commands

Copy-paste these single-line commands (no escaping needed):

```powershell
# Test everything works
docker exec -it spark-master python3 -c "import pyspark, numpy, pandas; print('✅ Ready!')"

# Run basic streaming
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/streaming.py

# Run NLP test
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/Tests/test_nlp_model_comprehensive.py

# Run sentiment streaming
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 /opt/spark-apps/streaming_with_sentiment.py
```

---

**Remember:** In PowerShell, use backticks `` ` `` (not `\`) or write everything on one line! 🎯
