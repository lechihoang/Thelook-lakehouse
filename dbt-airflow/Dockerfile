FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install dbt in isolated venv to avoid dependency conflicts
RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir \
      dbt-core==1.7.0 \
      dbt-trino==1.7.0

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["airflow standalone"]
