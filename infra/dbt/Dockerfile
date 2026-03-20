FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core==1.7.0 dbt-trino==1.7.0 protobuf==4.25.3

WORKDIR /dbt
COPY . .

ENTRYPOINT ["dbt"]
