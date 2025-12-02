"""Kafka topic schemas for streaming data"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, LongType

REVIEW_SCHEMA = StructType([
    StructField("userId", StringType()),
    StructField("itemId", StringType()),
    StructField("rating", FloatType()),
    StructField("timestamp", LongType())
])

FAVORITE_SCHEMA = StructType([
    StructField("userId", StringType()),
    StructField("productId", IntegerType()),
    StructField("timestamp", LongType())
])

CART_SCHEMA = StructType([
    StructField("userId", StringType()),
    StructField("productId", IntegerType()),
    StructField("timestamp", LongType())
])

ORDER_SCHEMA = StructType([
    StructField("userId", StringType()),
    StructField("productIds", ArrayType(IntegerType())),
    StructField("timestamp", LongType())
])

TOPICS_SCHEMAS = {
    "review": REVIEW_SCHEMA,
    "favorite": FAVORITE_SCHEMA,
    "cart": CART_SCHEMA,
    "order": ORDER_SCHEMA
}
