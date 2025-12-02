"""Buffer manager for accumulating interactions before Iceberg storage"""

from utils.logger import get_logger

log = get_logger(__name__)


class InteractionBuffer:
    """Manages buffering of interactions before storage threshold is reached"""

    def __init__(self, threshold=100):
        self.threshold = threshold
        self.buffer_df = None
        self.count = 0

    def add(self, df, spark):
        """Add dataframe to buffer"""
        if df is None or df.rdd.isEmpty():
            return False

        batch_count = df.count()

        if self.buffer_df is None:
            self.buffer_df = df
            self.count = batch_count
        else:
            self.buffer_df = self.buffer_df.unionByName(df, allowMissingColumns=True)
            self.count += batch_count

        log.info(f"Buffer: {self.count}/{self.threshold} interactions")
        return self.should_flush()

    def should_flush(self):
        """Check if buffer should be flushed"""
        return self.count >= self.threshold

    def get_buffer(self):
        """Get current buffer"""
        return self.buffer_df

    def get_count(self):
        """Get buffer count"""
        return self.count

    def clear(self):
        """Clear buffer"""
        self.buffer_df = None
        self.count = 0
        log.info("Buffer cleared")

    def flush_and_get(self):
        """Get buffer and clear it"""
        df = self.buffer_df
        count = self.count
        self.clear()
        return df, count
