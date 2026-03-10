from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class SparkStreaming:

    @staticmethod
    def get_instance(app_name: str = "TheLookStreaming") -> SparkSession:
        from utils.config import KAFKA_BOOTSTRAP, MINIO_ENDPOINT, MINIO_KEY, MINIO_SECRET
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("spark://spark-master:7077")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key",             MINIO_KEY)
            .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET)
            .config("spark.hadoop.fs.s3a.path.style.access",      "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled",   "true")
            .enableHiveSupport()
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark

    @staticmethod
    def create_kafka_read_stream(spark: SparkSession, topic: str, starting_offset: str = "earliest"):
        from utils.config import KAFKA_BOOTSTRAP
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offset)
            .option("failOnDataLoss", "false")
            .load()
        )

    @staticmethod
    def create_delta_write_stream(stream, table_name: str, checkpoint_base: str, delta_base: str,
                                   trigger: str = "30 seconds"):
        return (
            stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{checkpoint_base}/{table_name}")
            .option("mergeSchema", "true")
            .trigger(processingTime=trigger)
            .start(f"{delta_base}/{table_name}")
        )

    @staticmethod
    def register_delta_tables(spark: SparkSession, topic_config: dict, delta_base: str):
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        for _, (_, table_name) in topic_config.items():
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS bronze.{table_name}
                USING DELTA
                LOCATION '{delta_base}/{table_name}'
            """)
            print(f"Registered: bronze.{table_name}")
