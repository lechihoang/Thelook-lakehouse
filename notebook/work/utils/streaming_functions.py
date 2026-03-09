from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def parse_debezium_event(df: DataFrame, schema: StructType) -> DataFrame:
    """Extract 'after' payload from Debezium CDC JSON events, keeping only inserts/updates/reads."""
    return (
        df.select(
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
            F.col("timestamp"),
        )
        .select(
            F.col("data.op").alias("operation"),
            F.col("data.ts_ms").alias("event_ts_ms"),
            F.col("data.after.*"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .filter(F.col("operation").isin("c", "u", "r"))
    )
