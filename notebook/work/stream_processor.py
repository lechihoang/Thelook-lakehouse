import sys
sys.path.append("/opt/bitnami/spark/work")

from operators.streaming import SparkStreaming
from utils.config import CHECKPOINT_BASE, DELTA_BASE
from utils.schemas import TOPIC_CONFIG
from utils.streaming_functions import parse_debezium_event

spark = SparkStreaming.get_instance()

for topic, (schema, table_name) in TOPIC_CONFIG.items():
    raw = SparkStreaming.create_kafka_read_stream(spark, topic)
    parsed = parse_debezium_event(raw, schema)
    SparkStreaming.create_delta_write_stream(parsed, table_name, CHECKPOINT_BASE, DELTA_BASE).start()
    print(f"Stream started: {topic} -> {DELTA_BASE}/{table_name}")

SparkStreaming.register_delta_tables(spark, TOPIC_CONFIG, DELTA_BASE)

spark.streams.awaitAnyTermination()
