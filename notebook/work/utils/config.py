import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_KEY       = os.getenv("MINIO_KEY",        "minio")
MINIO_SECRET    = os.getenv("MINIO_SECRET",     "minio123")

CHECKPOINT_BASE = "s3a://lakehouse/checkpoints"
DELTA_BASE      = "s3a://lakehouse/bronze"
