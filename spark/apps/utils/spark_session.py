from pyspark.sql import SparkSession


def get_spark(app_name="mkadia-app"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.catalog.lakehouse_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse_catalog.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.lakehouse_catalog.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.lakehouse_catalog.ref", "main") \
        .config("spark.sql.catalog.lakehouse_catalog.warehouse", "s3a://mkadia-lakehouse/iceberg/") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
