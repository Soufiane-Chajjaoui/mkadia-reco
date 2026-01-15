"""Related items discovery using FP-Growth algorithm"""

import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values
from pyspark.sql.functions import col, collect_list, row_number, desc
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.window import Window
from utils.logger import get_logger
from config.storage_config import POSTGRES_CONFIG

log = get_logger(__name__)


class RelatedItemsFinder:
    """FP-Growth based related items discovery"""

    def __init__(self, spark_session, min_support=0.01, min_confidence=0.1):
        self.spark = spark_session
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.fp_growth_model = None

    def read_order_items(self):
        """Read order items from PostgreSQL"""
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}") \
                .option("dbtable", "order_items") \
                .option("user", POSTGRES_CONFIG['user']) \
                .option("password", POSTGRES_CONFIG['password']) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            count = df.count()
            log.info(f"Read {count} order items from PostgreSQL")
            return df
        except Exception as e:
            log.error(f"Error reading order items from PostgreSQL: {e}")
            return None

    def prepare_transactions(self, order_items_df):
        """Transform order items into transactions (order_id -> list of product_ids)"""
        try:
            if order_items_df is None or order_items_df.rdd.isEmpty():
                log.warning("No order items for transaction preparation")
                return None

            transactions = order_items_df \
                .groupBy("order_id") \
                .agg(collect_list(col("product_id")).alias("items")) \
                .select(col("items"))

            count = transactions.count()
            log.info(f"Prepared {count} transactions from order items")
            return transactions
        except Exception as e:
            log.error(f"Error preparing transactions: {e}")
            return None

    def mine_frequent_itemsets(self, transactions_df):
        """Mine frequent itemsets using FP-Growth"""
        try:
            if transactions_df is None or transactions_df.rdd.isEmpty():
                log.warning("No transactions for FP-Growth mining")
                return None, None

            fp_growth = FPGrowth(
                minSupport=self.min_support,
                minConfidence=self.min_confidence,
                itemsCol="items",
                predictionCol="prediction"
            )

            self.fp_growth_model = fp_growth.fit(transactions_df)

            frequent_itemsets = self.fp_growth_model.freqItemsets
            association_rules = self.fp_growth_model.associationRules

            itemsets_count = frequent_itemsets.count()
            rules_count = association_rules.count()

            log.info(f"Mined {itemsets_count} frequent itemsets and {rules_count} association rules")

            return frequent_itemsets, association_rules
        except Exception as e:
            log.error(f"Error mining frequent itemsets: {e}")
            return None, None

    def extract_related_items(self, association_rules_df):
        """Extract product relationships from association rules"""
        try:
            if association_rules_df is None or association_rules_df.rdd.isEmpty():
                log.warning("No association rules for extraction")
                return None

            related_items = association_rules_df \
                .select(
                    col("antecedents")[0].alias("product_id"),
                    col("consequents")[0].alias("related_product_id"),
                    col("confidence"),
                    col("support"),
                    col("lift")
                ) \
                .filter((col("antecedents").getItem(0).isNotNull()) &
                       (col("consequents").getItem(0).isNotNull()))

            count = related_items.count()
            log.info(f"Extracted {count} related item pairs")
            return related_items
        except Exception as e:
            log.error(f"Error extracting related items: {e}")
            return None

    def create_iceberg_related_items_table(self):
        """Create Iceberg table for related items in lakehouse"""
        try:
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS lakehouse_catalog.reco.related_items (
                    product_id BIGINT,
                    related_product_id BIGINT,
                    confidence DOUBLE,
                    support DOUBLE,
                    lift DOUBLE,
                    created_at TIMESTAMP
                )
                USING ICEBERG
            """)
            log.info("Iceberg related_items table created")
        except Exception as e:
            log.warning(f"Could not create Iceberg related_items table: {e}")

    def store_related_items_iceberg(self, related_items_df):
        """Store related items in Iceberg lakehouse"""
        if related_items_df is None or related_items_df.rdd.isEmpty():
            return

        try:
            from pyspark.sql.functions import lit
            
            df_with_timestamp = related_items_df.withColumn(
                "created_at",
                lit(datetime.now()).cast("timestamp")
            )

            df_with_timestamp.write \
                .mode("append") \
                .option("iceberg.write.update.mode", "merge") \
                .option("iceberg.write.target.data-file-format", "parquet") \
                .saveAsTable("lakehouse_catalog.reco.related_items")

            count = related_items_df.count()
            log.info(f"Stored {count} related items in Iceberg lakehouse")
        except Exception as e:
            log.error(f"Error storing related items in Iceberg: {e}")

    def store_related_items_postgres(self, related_items_df):
        """Store related items in PostgreSQL"""
        if related_items_df is None or related_items_df.rdd.isEmpty():
            return

        try:
            rows = related_items_df.collect()
            
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            cursor = conn.cursor()

            data = [
                (
                    int(row.product_id),
                    int(row.related_product_id),
                    float(row.confidence),
                    float(row.support) if row.support is not None else None,
                    float(row.lift) if row.lift is not None else None
                )
                for row in rows
            ]

            insert_query = """
                INSERT INTO related_items 
                (product_id, related_product_id, confidence, support, lift)
                VALUES %s
                ON CONFLICT (product_id, related_product_id) 
                DO UPDATE SET 
                    confidence = EXCLUDED.confidence,
                    support = EXCLUDED.support,
                    lift = EXCLUDED.lift,
                    updated_at = CURRENT_TIMESTAMP
            """

            execute_values(cursor, insert_query, data)

            conn.commit()
            cursor.close()
            conn.close()
            log.info(f"Stored {len(data)} related items in PostgreSQL")
        except Exception as e:
            log.error(f"Error storing related items in PostgreSQL: {e}")

    def run(self):
        """Run the complete FP-Growth pipeline"""
        try:
            log.info("Starting FP-Growth related items discovery...")

            order_items_df = self.read_order_items()
            if order_items_df is None:
                return False

            transactions_df = self.prepare_transactions(order_items_df)
            if transactions_df is None:
                return False

            frequent_itemsets, association_rules = self.mine_frequent_itemsets(transactions_df)
            if association_rules is None:
                return False

            related_items_df = self.extract_related_items(association_rules)
            if related_items_df is None:
                return False

            self.create_iceberg_related_items_table()
            self.store_related_items_iceberg(related_items_df)

            self.store_related_items_postgres(related_items_df)

            log.info("FP-Growth related items discovery completed successfully")
            return True
        except Exception as e:
            log.error(f"Error in FP-Growth pipeline: {e}")
            return False
