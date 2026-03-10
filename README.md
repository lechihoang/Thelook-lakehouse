# TheLook E-commerce Lakehouse

A streaming data lakehouse built on the **TheLook E-commerce** dataset, following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Architecture

```
TheLook Simulator (Python)
        │ INSERT / UPDATE via psycopg2
        ▼
   PostgreSQL 15  ─── Debezium CDC (pgoutput) ───▶  Kafka
                                                        │ Spark Structured Streaming
                                                        ▼
                                                   MinIO (Delta Lake)
                                                   ├── bronze/   raw CDC events
                                                   ├── silver/   enriched tables
                                                   └── gold/     business metrics
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
| Stream Processing | Spark Structured Streaming | 3.5.3 |
| Table Format | Delta Lake | 3.2.0 |
| Object Storage | MinIO | latest |
| Metastore | Hive Metastore + MariaDB | 3.0 / 10.5 |
| Query Engine | Trino | 400 |
| Visualization | Apache Superset | latest |
| Orchestration | Airflow + dbt-trino | 2.9.2 / 1.7.0 |

---

## Dataset — TheLook E-commerce

Synthetic e-commerce dataset modelled after the [Google BigQuery public dataset](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce).

**6 tables:**

| Table | Role | Description |
|---|---|---|
| `users` | Dimension | Customer profiles (name, age, gender, location, traffic source) |
| `products` | Dimension | Product catalog — 29,120 items loaded from CSV |
| `dist_centers` | Dimension | 10 distribution center locations |
| `orders` | Fact | Order transactions with status lifecycle |
| `order_items` | Fact | Line items per order with sale price |
| `events` | Fact | Web/app behavioral events (browse, cart, purchase, return) |

**Simulator behavior:**
- Initializes 1,000 users + full product catalog on first run
- Continuously generates purchases at ~2 events/second
- Order status follows a state machine: `Processing → Shipped → Delivered → Returned/Cancelled`
- 5% chance to create a new user per iteration
- 40% chance to advance a random order's status per iteration
- 20% chance to generate anonymous "ghost" browsing events

---

## Prerequisites

- Docker Desktop ≥ 24.x — allocate at least **12 GB RAM**
- Docker Compose v2
- Python 3.12 (for the simulator)

> Credentials and ports are pre-configured in `.env`.

---

## Part 1 — Start Services

### Step 1 — Core stack

```bash
make up-core
```

Starts: PostgreSQL, MinIO, MariaDB, Hive Metastore, Spark cluster, Jupyter Notebook.

---

### Step 2 — Kafka + Debezium

```bash
make up-kafka
```

The Debezium connector is registered automatically on startup. To verify:

```bash
curl -s http://localhost:8083/connectors/thelook-connector/status | python3 -m json.tool
```

---

### Step 3 — Trino + Superset

```bash
make up-trino
make up-superset
```

Superset is available at **http://localhost:8088** (admin / admin123). The Trino database connection is registered automatically on startup.

---

### Step 4 — Airflow + dbt

```bash
make up-airflow
```

Open Airflow at **http://localhost:8085** (admin / admin123).

The DAG `thelook_dbt_pipeline` runs daily at 23:00 and executes Bronze → Silver → Gold transformations via Astronomer Cosmos. To trigger manually:

```bash
docker exec thelook-airflow airflow dags trigger thelook_dbt_pipeline
```

---

## Part 2 — Start Stream Data

Open Jupyter at **http://localhost:8888** (password: `admin123`), navigate to `work/stream_processor.ipynb` and run all cells.

| Cell | Description |
|---|---|
| **1 — Health Checks** | Wait for all services to be ready (PostgreSQL, MinIO, Kafka, Spark) |
| **2 — Simulator** | Start the simulator in background, wait for Debezium to create Kafka topics |
| **3 — Spark Streaming** | Start 6 streams (one per table), register Delta tables in Hive Metastore |

> Interrupt the kernel (■) to stop everything — the simulator will be terminated automatically.

Once data is flowing into the bronze layer, trigger the dbt pipeline to build silver and gold tables:

```bash
docker exec thelook-airflow airflow dags trigger thelook_dbt_pipeline
```

Or let it run automatically on its daily schedule (23:00).

To run individually via Makefile:

```bash
make simulator-start   # chạy simulator trên host (localhost:5432)
make simulator-stop

make stream-start      # submit spark-submit
make stream-status
make stream-logs
make stream-stop
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
| Apache Airflow | http://localhost:8085 | admin / admin123 |

---

## dbt Models

### Bronze — ephemeral CTEs on raw Delta tables
`bronze_orders`, `bronze_order_items`, `bronze_events`

Deduplicates CDC UPDATE events using `ROW_NUMBER()` to keep only the latest state per record.

### Silver — enriched tables (incremental merge)

| Model | Description |
|---|---|
| `silver_order_items` | Order items joined with products, orders, users, and distribution centers |
| `silver_orders` | Orders enriched with user info |
| `silver_events` | Web events enriched with user profile |

### Gold — business metrics (incremental delete+insert)

| Model | Description |
|---|---|
| `gold_sales_by_category` | Revenue and margin by product category, department, and brand |
| `gold_order_funnel` | Order volume by status with average fulfillment time |
| `gold_customer_segments` | Purchase behavior by country, gender, age group, and traffic source |
| `gold_product_performance` | Revenue, return rate, and cancel rate per product |

---

## Troubleshooting

**Debezium connector fails to start**
- Ensure the simulator has run at least once (creates the tables Debezium needs to track)
- Check WAL level: `docker exec thelook-postgres psql -U admin -d thelook -c "SHOW wal_level;"` — should return `logical`

**Spark cannot write to MinIO**
- Verify MinIO is running: http://localhost:9001
- Check `MINIO_ENDPOINT` in `.env` uses `http://` not `https://`

**Hive Metastore fails to start**
- MariaDB may need ~30s on first boot. Check: `docker logs thelook-hive-metastore`

**Trino cannot read Delta tables**
- Run stream processor first — it registers tables in the metastore
- Verify: `SHOW TABLES IN delta.bronze` in Trino CLI

**Trino runs out of memory on large queries**
- Trino is configured with 4 GB heap (`trino/conf/jvm.config`)
- Reduce concurrent queries or add `LIMIT` when exploring large tables interactively

**Airflow webserver not ready**
- `airflow-init` must complete before webserver and scheduler start
- Check: `docker logs thelook-airflow-init`
- If init fails: `make down-airflow && make up-airflow`

---

## License

Educational and portfolio use. TheLook dataset © Google LLC (Apache 2.0).
