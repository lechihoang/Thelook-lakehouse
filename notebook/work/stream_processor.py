"""
TPC-DS Lakehouse — Spark Structured Streaming Processor
Reads CDC events from Kafka (Debezium format) and writes to Delta Lake on MinIO.

Run from notebook or submit to Spark cluster:
  spark-submit --master spark://spark-master:7077 stream_processor.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType, IntegerType
)

# ─── Config ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_KEY       = os.getenv("MINIO_KEY",        "minio")
MINIO_SECRET    = os.getenv("MINIO_SECRET",     "minio123")
CHECKPOINT_BASE = "s3a://lakehouse/checkpoints"
DELTA_BASE      = "s3a://lakehouse/bronze"

# ─── Spark Session ───────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("TPC-DS Lakehouse Streaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",                MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",              MINIO_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",              MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access",       "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled",  "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.streaming.backpressure.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("✅ SparkSession created")


# ─── Debezium CDC event parser ────────────────────────────────────
def parse_debezium_event(df, value_schema: StructType):
    """
    Extract 'after' payload from Debezium CDC events.
    For INSERT/UPDATE ops (op=c,u,r) we use the 'after' state.
    """
    parsed = df.select(
        F.from_json(F.col("value").cast("string"), value_schema).alias("data"),
        F.col("timestamp"),
        F.col("topic"),
    )
    return parsed.select(
        F.col("data.op").alias("operation"),
        F.col("data.ts_ms").alias("event_ts_ms"),
        F.col("data.after.*"),
        F.col("topic"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).filter(F.col("operation").isin("c", "u", "r"))  # create, update, snapshot


# ─── Schemas ─────────────────────────────────────────────────────
store_sales_payload = StructType([
    StructField("op",  StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", StructType([
        StructField("ss_sold_date_sk",      IntegerType()),
        StructField("ss_sold_time_sk",      IntegerType()),
        StructField("ss_item_sk",           IntegerType()),
        StructField("ss_customer_sk",       IntegerType()),
        StructField("ss_store_sk",          IntegerType()),
        StructField("ss_promo_sk",          IntegerType()),
        StructField("ss_ticket_number",     LongType()),
        StructField("ss_quantity",          IntegerType()),
        StructField("ss_list_price",        DoubleType()),
        StructField("ss_coupon_amt",        DoubleType()),
        StructField("ss_net_paid",          DoubleType()),
        StructField("ss_net_paid_inc_tax",  DoubleType()),
        StructField("ss_net_profit",        DoubleType()),
    ])),
])

web_sales_payload = StructType([
    StructField("op",  StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", StructType([
        StructField("ws_sold_date_sk",        IntegerType()),
        StructField("ws_sold_time_sk",        IntegerType()),
        StructField("ws_item_sk",             IntegerType()),
        StructField("ws_bill_customer_sk",    IntegerType()),
        StructField("ws_ship_customer_sk",    IntegerType()),
        StructField("ws_web_site_sk",         IntegerType()),
        StructField("ws_promo_sk",            IntegerType()),
        StructField("ws_order_number",        LongType()),
        StructField("ws_quantity",            IntegerType()),
        StructField("ws_list_price",          DoubleType()),
        StructField("ws_coupon_amt",          DoubleType()),
        StructField("ws_net_paid",            DoubleType()),
        StructField("ws_net_paid_inc_tax",    DoubleType()),
        StructField("ws_net_profit",          DoubleType()),
    ])),
])

catalog_sales_payload = StructType([
    StructField("op",  StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", StructType([
        StructField("cs_sold_date_sk",        IntegerType()),
        StructField("cs_sold_time_sk",        IntegerType()),
        StructField("cs_item_sk",             IntegerType()),
        StructField("cs_bill_customer_sk",    IntegerType()),
        StructField("cs_ship_customer_sk",    IntegerType()),
        StructField("cs_warehouse_sk",        IntegerType()),
        StructField("cs_promo_sk",            IntegerType()),
        StructField("cs_order_number",        LongType()),
        StructField("cs_quantity",            IntegerType()),
        StructField("cs_list_price",          DoubleType()),
        StructField("cs_coupon_amt",          DoubleType()),
        StructField("cs_net_paid",            DoubleType()),
        StructField("cs_net_paid_inc_tax",    DoubleType()),
        StructField("cs_net_profit",          DoubleType()),
    ])),
])

inventory_payload = StructType([
    StructField("op",  StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", StructType([
        StructField("inv_date_sk",          IntegerType()),
        StructField("inv_item_sk",          IntegerType()),
        StructField("inv_warehouse_sk",     IntegerType()),
        StructField("inv_quantity_on_hand", IntegerType()),
    ])),
])

store_returns_payload = StructType([
    StructField("op",  StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", StructType([
        StructField("sr_returned_date_sk",   IntegerType()),
        StructField("sr_return_time_sk",     IntegerType()),
        StructField("sr_item_sk",            IntegerType()),
        StructField("sr_customer_sk",        IntegerType()),
        StructField("sr_store_sk",           IntegerType()),
        StructField("sr_reason_sk",          IntegerType()),
        StructField("sr_ticket_number",      LongType()),
        StructField("sr_return_quantity",    IntegerType()),
        StructField("sr_return_amt",         DoubleType()),
        StructField("sr_return_tax",         DoubleType()),
        StructField("sr_return_amt_inc_tax", DoubleType()),
        StructField("sr_fee",                DoubleType()),
        StructField("sr_return_ship_cost",   DoubleType()),
        StructField("sr_refunded_cash",      DoubleType()),
        StructField("sr_reversed_charge",    DoubleType()),
        StructField("sr_store_credit",       DoubleType()),
        StructField("sr_net_loss",           DoubleType()),
    ])),
])

web_returns_payload = StructType([
    StructField("op",  StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", StructType([
        StructField("wr_returned_date_sk",    IntegerType()),
        StructField("wr_returned_time_sk",    IntegerType()),
        StructField("wr_item_sk",             IntegerType()),
        StructField("wr_refunded_customer_sk",IntegerType()),
        StructField("wr_returning_customer_sk",IntegerType()),
        StructField("wr_web_page_sk",         IntegerType()),
        StructField("wr_reason_sk",           IntegerType()),
        StructField("wr_order_number",        LongType()),
        StructField("wr_return_quantity",     IntegerType()),
        StructField("wr_return_amt",          DoubleType()),
        StructField("wr_return_tax",          DoubleType()),
        StructField("wr_return_amt_inc_tax",  DoubleType()),
        StructField("wr_fee",                 DoubleType()),
        StructField("wr_return_ship_cost",    DoubleType()),
        StructField("wr_refunded_cash",       DoubleType()),
        StructField("wr_reversed_charge",     DoubleType()),
        StructField("wr_account_credit",      DoubleType()),
        StructField("wr_net_loss",            DoubleType()),
    ])),
])

catalog_returns_payload = StructType([
    StructField("op",  StringType()),
    StructField("ts_ms", LongType()),
    StructField("after", StructType([
        StructField("cr_returned_date_sk",    IntegerType()),
        StructField("cr_returned_time_sk",    IntegerType()),
        StructField("cr_item_sk",             IntegerType()),
        StructField("cr_refunded_customer_sk",IntegerType()),
        StructField("cr_returning_customer_sk",IntegerType()),
        StructField("cr_warehouse_sk",        IntegerType()),
        StructField("cr_call_center_sk",      IntegerType()),
        StructField("cr_ship_mode_sk",        IntegerType()),
        StructField("cr_reason_sk",           IntegerType()),
        StructField("cr_order_number",        LongType()),
        StructField("cr_return_quantity",     IntegerType()),
        StructField("cr_return_amt",          DoubleType()),
        StructField("cr_return_tax",          DoubleType()),
        StructField("cr_return_amt_inc_tax",  DoubleType()),
        StructField("cr_fee",                 DoubleType()),
        StructField("cr_return_ship_cost",    DoubleType()),
        StructField("cr_refunded_cash",       DoubleType()),
        StructField("cr_reversed_charge",     DoubleType()),
        StructField("cr_store_credit",        DoubleType()),
        StructField("cr_net_loss",            DoubleType()),
    ])),
])

TOPIC_CONFIG = {
    "tpcds.public.store_sales":    (store_sales_payload,    "store_sales"),
    "tpcds.public.web_sales":      (web_sales_payload,      "web_sales"),
    "tpcds.public.catalog_sales":  (catalog_sales_payload,  "catalog_sales"),
    "tpcds.public.inventory":      (inventory_payload,       "inventory"),
    "tpcds.public.store_returns":  (store_returns_payload,  "store_returns"),
    "tpcds.public.web_returns":    (web_returns_payload,    "web_returns"),
    "tpcds.public.catalog_returns":(catalog_returns_payload,"catalog_returns"),
}


# ─── Streaming query per topic ────────────────────────────────────
queries = []

for topic, (schema, table_name) in TOPIC_CONFIG.items():
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = parse_debezium_event(raw_df, schema)

    query = (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table_name}")
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .start(f"{DELTA_BASE}/{table_name}")
    )
    queries.append(query)
    print(f"✅ Stream started: {topic} → {DELTA_BASE}/{table_name}")


# ─── Register Delta tables in Hive Metastore ─────────────────────
def register_tables():
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    for _, (_, table_name) in TOPIC_CONFIG.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze.{table_name}
            USING DELTA
            LOCATION '{DELTA_BASE}/{table_name}'
        """)
        print(f"✅ Registered: bronze.{table_name}")

register_tables()

# Wait for all streams
for q in queries:
    q.awaitTermination()
