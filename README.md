# TPC-DS Lakehouse Pipeline

A streaming data lakehouse built on the **TPC-DS** retail benchmark dataset, following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Architecture

```
PostgreSQL (TPC-DS)  ←──  Python Simulator
        │ CDC via Debezium (pgoutput)
        ▼
    Kafka Broker
        │ Spark Structured Streaming
        ▼
    MinIO (Delta Lake)
    ├── bronze/   raw CDC events
    ├── silver/   enriched + dimension-joined
    └── gold/     aggregated business metrics
        │
   ┌────┴────┐
   Trino    Airflow + dbt
   Superset
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Source DB | PostgreSQL | 15 |
| CDC | Debezium | 2.5 |
| Message Broker | Kafka (Confluent) | 7.5.0 |
| Stream Processing | Spark Structured Streaming | 3.5.0 |
| Table Format | Delta Lake | 3.2.0 |
| Object Storage | MinIO | latest |
| Metastore | Hive Metastore + MariaDB | 3.0 / 10.5 |
| Query Engine | Trino | 400 |
| Visualization | Apache Superset | latest |
| Orchestration | Airflow + dbt-trino | 2.9.2 / 1.7.0 |

---

## Prerequisites

- Docker Desktop ≥ 24.x — allocate at least **8 GB RAM**
- Docker Compose v2
- `make` and `gcc` (for compiling tpcds-kit in Step 1)

> All credentials and ports are pre-configured in `.env`. No manual network setup required.

---

## Setup

### Step 1 — Start core stack

```bash
docker compose up -d
```

Starts: PostgreSQL, MinIO, MariaDB, Hive Metastore, Spark cluster, Jupyter Notebook, Simulator.

Once healthy, load the TPC-DS dataset into PostgreSQL (run once):

```bash
bash postgres/scripts/setup-tpcds.sh
```

This compiles `dsdgen`, generates ~1 GB of retail data (24 tables, scale factor 1), loads it into PostgreSQL, and creates the Debezium publication.

---

### Step 2 — Start Kafka + Debezium

```bash
docker compose -f kafka/docker-compose.yaml up -d
```

Wait ~30 seconds for Debezium to be ready, then register the connector:

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @kafka/conf/register-tpcds-connector.json
```

Verify:
```bash
curl -s http://localhost:8083/connectors/tpcds-connector/status | python3 -m json.tool
```

---

### Step 3 — Start the Spark Streaming job

Open Jupyter at **http://localhost:8888** and run `work/stream_processor.py`.

Or submit directly:
```bash
docker exec tpcds-notebook \
  spark-submit \
    --master spark://spark-master:7077 \
    /home/jovyan/work/stream_processor.py
```

CDC events from Kafka flow into Delta tables at `s3a://lakehouse/bronze/`.

---

### Step 4 — Start Trino + Superset

```bash
docker compose -f trino-superset/docker-compose.yaml up -d
```

Open Superset at **http://localhost:8088** (admin / admin123):
1. **Settings → Database Connections → + Database**
2. Choose **Trino**, enter connection string: `trino://trino:8080/delta`

---

### Step 5 — Start Airflow + dbt

```bash
docker compose -f dbt-airflow/docker-compose.yaml up -d
```

Open Airflow at **http://localhost:8081** (admin / admin123).

The DAG `tpcds_dbt_pipeline` runs daily at 23:00 and executes Bronze → Silver → Gold transformations. To trigger manually:

```bash
docker exec tpcds-airflow airflow dags trigger tpcds_dbt_pipeline
```

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8082 | — |
| Jupyter Notebook | http://localhost:8888 | — |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| Kafka Control Center | http://localhost:9021 | — |
| Debezium REST API | http://localhost:8083 | — |
| Trino UI | http://localhost:8080 | — |
| Apache Superset | http://localhost:8088 | admin / admin123 |
| Apache Airflow | http://localhost:8081 | admin / admin123 |

---

## dbt Models

### Bronze — views on raw Delta tables
`bronze_store_sales`, `bronze_web_sales`, `bronze_catalog_sales`, `bronze_inventory`,
`bronze_store_returns`, `bronze_web_returns`, `bronze_catalog_returns`

### Silver — cleaned + dimension-enriched tables
`silver_store_sales`, `silver_web_sales`, `silver_catalog_sales`, `silver_inventory`,
`silver_store_returns`, `silver_web_returns`, `silver_catalog_returns`

### Gold — business metrics
| Model | Description |
|---|---|
| `gold_sales_by_channel` | Daily revenue & transactions by store / web / catalog |
| `gold_product_performance` | Revenue, profit, margin per product |
| `gold_customer_segments` | Customer LTV with VIP / High / Regular / Low segmentation |
| `gold_inventory_status` | Stock levels & out-of-stock rates by category & warehouse |
| `gold_return_rate` | Return rate % by channel, category, and brand |
| `gold_shipping_analysis` | Orders & returns by shipping mode and carrier |
| `gold_promotion_effectiveness` | Promo ROI, discount rate, profit margin |
| `gold_geographic_analysis` | Revenue & return rate by country / state / city |
| `gold_call_center_analysis` | Call center performance + top return reasons |

---

## Useful Commands

```bash
# Check all running containers
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Open Trino CLI
docker exec -it tpcds-trino trino --catalog delta --schema bronze

# Run dbt manually
docker exec tpcds-airflow \
  /opt/dbt_venv/bin/dbt run \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt

# View simulator logs
docker logs -f tpcds-simulator

# Stop all stacks
docker compose down
docker compose -f kafka/docker-compose.yaml down
docker compose -f trino-superset/docker-compose.yaml down
docker compose -f dbt-airflow/docker-compose.yaml down
```

---

## Troubleshooting

**Debezium connector fails**
- Check PostgreSQL: `docker exec tpcds-postgres psql -U admin -d tpcds -c "SHOW wal_level;"`
- Should return `logical`. If not, restart PostgreSQL with the correct `wal_level` setting.

**Spark cannot write to MinIO**
- Verify MinIO is running: http://localhost:9001
- Check `MINIO_ENDPOINT` in `.env` uses `http://` not `https://`

**Hive Metastore fails to start**
- MariaDB may need ~30s on first boot. Check: `docker logs tpcds-hive-metastore`

**Trino cannot read Delta tables**
- Run `stream_processor.py` first — it registers tables in the metastore.
- Verify: `SHOW TABLES IN delta.bronze` in Trino UI.

---

## License

Educational and portfolio use. TPC-DS benchmark specification © [TPC](https://www.tpc.org/).
