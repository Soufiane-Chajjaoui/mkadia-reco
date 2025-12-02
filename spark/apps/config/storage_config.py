"""Storage configurations for PostgreSQL and MinIO"""

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "user": "postgres",
    "password": "soufianch",
    "database": "mkadia-db"
}

MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "bucket": "mkadia-objects"
}

KAFKA_CONFIG = {
    "bootstrap_servers": "kafka:29092",
    "host": "kafka",
    "port": "9092"
}
