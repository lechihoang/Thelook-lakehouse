# TheLook E-commerce Lakehouse

A streaming data lakehouse built on the **TheLook E-commerce** dataset, following the **Medallion Architecture** (Staging → Intermediate → Mart).

---

## Architecture

```
PostgreSQL  ── CDC (pgoutput) ──▶ Kafka ──▶ Schema Registry (Avro)
     │                                     │
     │                              Spark Structured Streaming
     │                                     │
     └─────────────────────────────────────▶ MinIO (Delta Lake)
                                               │
                              ┌────────────────┼────────────────┐
                              │                │                │
                          staging/          intermediate/       mart/
                          raw CDC           enriched           metrics
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
| CDC | Debezium | 3.0 |
| Message Broker | Kafka (KRaft) | 3.9 |
| Schema Registry | Confluent | 8.0 |
| Stream Processing | Spark Structured Streaming | 3.5.6 |
| Table Format | Delta Lake | 3.0 |
| Object Storage | MinIO | latest |
| Metastore | Hive Metastore + MariaDB | 4.1 / 10.5 |
| Query Engine | Trino | latest |
| Visualization | Apache Superset | 3.1 |
| Orchestration | Airflow (LocalExecutor) + dbt | 3.0 / 1.7 |
| Data Generator | Python + Confluent Kafka | — |

---

## Dataset — TheLook E-commerce

Synthetic e-commerce dataset modelled after the [Google BigQuery public dataset](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce).

**6 source tables:**

| Table | Role | Description |
|---|---|---|
| `users` | Dimension | Customer profiles (name, age, gender, location, traffic source) |
| `products` | Dimension | Product catalog |
| `dist_centers` | Dimension | Distribution center locations |
| `orders` | Fact | Order transactions with status lifecycle |
| `order_items` | Fact | Line items per order with sale price |
| `events` | Fact | Web/app behavioral events |

---

## Quick Start

Start all core services:

```bash
# Start everything except Airflow
docker compose --profile core --profile datagen up -d

# Or start with Airflow
docker compose --profile core --profile datagen --profile airflow up -d
```

Start JupyterLab (explore profile):

```bash
docker compose --profile explore up -d
```

Stop everything:

```bash
docker compose down
```

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8088 | — |
| JupyterLab | http://localhost:8888 | — |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| Trino UI | http://localhost:8080 | — |
| Apache Superset | http://localhost:8089 | admin / admin123 |
| Schema Registry | http://localhost:8081 | — |
| Debezium REST API | http://localhost:8083 | — |
| Apache Airflow | http://localhost:8085 | admin / admin123 |

---

## Docker Compose Profiles

| Profile | Services |
|---|---|
| `core` | postgres, minio, kafka, schema-registry, debezium, hive-metastore, spark, trino, superset |
| `explore` | jupyter-lab |
| `datagen` | data-generator |
| `airflow` | airflow-db, airflow-init, airflow-scheduler, airflow-webserver |

---

## dbt Models (Medallion Layers)

### Staging — raw source tables
Ephemeral CTEs on raw Delta tables. Deduplicates CDC events using `ROW_NUMBER()`.

### Intermediate — enriched tables (incremental merge)

| Model | Description |
|---|---|
| `int_order_items` | Order items joined with products, orders, users |
| `int_orders` | Orders enriched with user info |
| `int_events` | Web events enriched with user profile |

### Mart — business metrics (incremental)

| Model | Description |
|---|---|
| `mart_sales` | Revenue and margin by category, department |
| `mart_orders` | Order volume by status with fulfillment time |
| `mart_customers` | Purchase behavior by demographics |

---

## Environment Configuration

All configuration via `.env` at project root. Key variables:

```env
# PostgreSQL (CDC source)
POSTGRES_DB=thelook
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_CDC_USER=cdc_reader
POSTGRES_CDC_PASSWORD=cdc_reader_pwd

# MinIO
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=lakehouse

# MariaDB (Hive Metastore backend)
MARIADB_ROOT_PASSWORD=root123
MARIADB_DATABASE=metastore_db
MARIADB_USER=admin
MARIADB_PASSWORD=admin123

# Superset
SUPERSET_SECRET_KEY=<generate-a-secret>
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_EMAIL=admin@thelook.com
SUPERSET_ADMIN_PASSWORD=admin123
```

---

## Troubleshooting

**Debezium connector fails to start**
- Ensure data-generator has run (creates the tables Debezium needs to track)
- Check WAL level: `docker exec thelook-postgres psql -U admin -d thelook -c "SHOW wal_level;"` — should return `logical`

**Spark cannot write to MinIO**
- Verify MinIO is running: http://localhost:9001
- Check `MINIO_ENDPOINT` in `.env` uses `http://` not `https://`

**Hive Metastore fails to start**
- MariaDB may need ~30s on first boot. Check: `docker logs thelook-hive-metastore`

**Trino cannot read Delta tables**
- Run data-generator first to produce CDC events
- Verify: `SHOW TABLES IN delta.staging` in Trino CLI

**Airflow webserver not ready**
- `airflow-init` must complete before webserver and scheduler start
- Check: `docker logs thelook-airflow-init`

---

## License

Educational and portfolio use. TheLook dataset © Google LLC (Apache 2.0).
