"""Batch job for FP-Growth related items discovery"""

import sys
import time
from utils.spark_session import get_spark
from utils.postgres_utils import init_postgres
from models.related_items import RelatedItemsFinder
from utils.logger import get_logger

log = get_logger(__name__)


def main():
    """Run FP-Growth batch job"""
    try:
        log.info("Initializing FP-Growth batch job...")

        init_postgres()

        spark = get_spark("mkadia-fpgrowth")

        min_support = float(sys.argv[1]) if len(sys.argv) > 1 else 0.01
        min_confidence = float(sys.argv[2]) if len(sys.argv) > 2 else 0.1

        log.info(f"FP-Growth parameters: min_support={min_support}, min_confidence={min_confidence}")

        finder = RelatedItemsFinder(spark, min_support=min_support, min_confidence=min_confidence)

        start_time = time.time()
        success = finder.run()
        elapsed_time = time.time() - start_time

        if success:
            log.info(f"FP-Growth job completed successfully in {elapsed_time:.2f}s")
            return 0
        else:
            log.error("FP-Growth job failed")
            return 1

    except Exception as e:
        log.error(f"Unexpected error in FP-Growth job: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
