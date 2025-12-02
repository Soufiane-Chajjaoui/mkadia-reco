"""Kafka utilities for topic management and stream reading"""

from pyspark.sql.functions import from_json, col
from config.storage_config import KAFKA_CONFIG
from utils.logger import get_logger

log = get_logger(__name__)

def create_kafka_topics():
    """Create Kafka topics if they don't exist"""
    topics = ["review", "favorite", "cart", "order"]

    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            request_timeout_ms=5000
        )

        for topic in topics:
            try:
                new_topics = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
                admin_client.create_topics(new_topics=new_topics, validate_only=False)
                log.info(f"Topic '{topic}' → created")
            except TopicAlreadyExistsError:
                log.info(f"Topic '{topic}' → exists")
            except Exception as e:
                log.warning(f"Topic '{topic}' → {str(e)[:50]}")

        admin_client.close()
    except ImportError:
        log.warning("kafka-python not available, topics must be created manually")
    except Exception as e:
        log.warning(f"Failed to create topics: {str(e)[:60]}")


def safe_read_topic(spark, topic, schema):
    """Safely read from Kafka topic with schema"""
    try:
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"])
            .option("subscribePattern", f"^{topic}$")
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) AS json_str")
            .select(from_json(col("json_str"), schema).alias("data"))
            .select("data.*")
        )
        log.info(f"Topic '{topic}' → Stream started")
        return df
    except Exception as e:
        log.error(f"Topic '{topic}' → {str(e)[:80].replace(chr(10), ' ')}")
        return None
