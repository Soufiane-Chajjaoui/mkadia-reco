# Docker Build Fix Guide

## 🔧 Issue Fixed

**Problem:** Docker build timing out when downloading PySpark (316.9 MB)

**Root Cause:** 
- The base image `apache/spark:3.5.0-scala2.12-java11-python3-ubuntu` already includes PySpark 3.5.0
- Attempting to reinstall PySpark was redundant and caused timeouts

## ✅ Changes Made

### 1. **Dockerfile Optimization**
- ❌ **Removed**: `pip install pyspark==3.5.0` (already in base image)
- ✅ **Added**: Increased pip timeout to 1000 seconds
- ✅ **Optimized**: Combined RUN commands to reduce layers
- ✅ **Updated**: Pandas version to >=2.0.0 (better compatibility)

### 2. **docker-compose.yml Cleanup**
- ✅ **Removed**: Obsolete `version: '3.9'` field (no longer needed)

## 🚀 How to Rebuild

### **Step 1: Clean Previous Build**
```powershell
# Stop and remove old containers
docker-compose down

# Remove old images (optional but recommended)
docker-compose down --rmi all

# Or just remove the custom Spark image
docker rmi mkadia-reco-spark-master mkadia-reco-spark-worker-1 mkadia-reco-spark-worker-2
```

### **Step 2: Rebuild and Start**
```powershell
# Build and start all services
docker-compose up --build

# Or run in background
docker-compose up --build -d
```

### **Step 3: Verify**
```powershell
# Check running containers
docker ps

# Check Spark Master logs
docker logs spark-master

# Access Spark Master UI
# Open browser: http://localhost:8081

# Access Kafka UI
# Open browser: http://localhost:8085
```

## 📦 What's Included in the Image

The base image already includes:
- ✅ Apache Spark 3.5.0
- ✅ PySpark 3.5.0
- ✅ Java 11
- ✅ Scala 2.12
- ✅ Python 3.8
- ✅ Ubuntu base

We only add:
- NumPy (for numerical operations)
- Pandas (for data manipulation)

## 🔍 Troubleshooting

### **Build still times out?**

If you still get timeout errors, try:

```dockerfile
# In Dockerfile, increase timeout even more:
RUN pip install --upgrade pip && \
    pip install --default-timeout=2000 --retries 5 \
    numpy>=1.24.0 \
    pandas>=2.0.0
```

### **Network issues?**

```powershell
# Use pip cache
docker-compose build --build-arg PIP_NO_CACHE_DIR=0

# Or build with no cache
docker-compose build --no-cache
```

### **Want to verify PySpark is installed?**

```powershell
# Run Python in container
docker run --rm -it apache/spark:3.5.0-scala2.12-java11-python3-ubuntu python3

# In Python shell:
>>> import pyspark
>>> print(pyspark.__version__)
3.5.0
>>> exit()
```

## 🎯 Expected Build Time

- **Without rebuilding**: ~10-30 seconds (if images cached)
- **With rebuild**: ~2-5 minutes (downloading dependencies)
- **First time**: ~5-10 minutes (downloading base images)

## ✅ Success Indicators

After successful build, you should see:

```
✔ Container zookeeper        Started
✔ Container kafka            Started  
✔ Container kafka-ui         Started
✔ Container spark-master     Started
✔ Container spark-worker-1   Started
✔ Container spark-worker-2   Started
```

## 🧪 Test Installation

### **Test 1: Spark is Running**
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --version
```

Expected output:
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/
```

### **Test 2: Python Packages Work**
```powershell
docker exec -it spark-master python3 -c "import pyspark, numpy, pandas; print('✅ All packages imported successfully')"
```

### **Test 3: Run Sample Script**
```powershell
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/streaming.py
```

## 📚 Additional Resources

- **Dockerfile Best Practices**: https://docs.docker.com/develop/develop-images/dockerfile_best-practices/
- **Spark Docker Images**: https://hub.docker.com/r/apache/spark
- **Docker Compose Docs**: https://docs.docker.com/compose/

## 🎉 Next Steps

Once your containers are running:

1. ✅ Access Spark Master UI: http://localhost:8081
2. ✅ Access Kafka UI: http://localhost:8085
3. ✅ Test sentiment model: `python test_nlp_model_comprehensive.py`
4. ✅ Run streaming pipeline: `docker exec -it spark-master spark-submit /opt/spark-apps/streaming.py`

---

**Build fixed! Try running `docker-compose up --build` now.** 🚀
