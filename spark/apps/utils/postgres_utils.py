"""PostgreSQL utilities for database initialization"""

import time
import psycopg2
from config.storage_config import POSTGRES_CONFIG
from utils.logger import get_logger

log = get_logger(__name__)


def init_postgres(retries=5, delay=5):
    """Initialize PostgreSQL database and create recommendations and related_items tables"""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    rec_id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    prod_id INT NOT NULL,
                    score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_recommendations_user_id ON recommendations(user_id);
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS related_items (
                    related_item_id BIGSERIAL PRIMARY KEY,
                    product_id BIGINT NOT NULL,
                    related_product_id BIGINT NOT NULL,
                    confidence FLOAT NOT NULL,
                    support FLOAT,
                    lift FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_related_items_product
                        FOREIGN KEY (product_id) REFERENCES products (product_id)
                            ON DELETE CASCADE,
                    CONSTRAINT fk_related_items_related_product
                        FOREIGN KEY (related_product_id) REFERENCES products (product_id)
                            ON DELETE CASCADE
                );
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_related_items_product_id ON related_items(product_id);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_related_items_confidence ON related_items(confidence DESC);
            """)
            conn.commit()
            cursor.close()
            conn.close()
            log.info("PostgreSQL tables initialized successfully")
            return True
        except Exception as e:
            if attempt < retries - 1:
                log.warning(f"PostgreSQL connection failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(delay)
            else:
                log.error(f"PostgreSQL connection failed after {retries} attempts: {e}")
                return False
