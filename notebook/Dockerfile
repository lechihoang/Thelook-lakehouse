FROM jupyter/pyspark-notebook:spark-3.3.2

USER root

RUN pip install --no-cache-dir \
    delta-spark==2.3.0 \
    kafka-python==2.0.2 \
    minio==7.2.0 \
    python-dotenv==1.0.0

USER ${NB_UID}
