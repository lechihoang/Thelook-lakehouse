include .env
export

CORE    := docker compose -f docker-compose.yaml
KAFKA   := docker compose -f ./kafka/docker-compose.yaml
TRINO   := docker compose -f ./trino/docker-compose.yaml
SUPERSET := docker compose -f ./superset/docker-compose.yaml
AIRFLOW := docker compose -f ./dbt-airflow/docker-compose.yaml

.PHONY: help \
        up down logs \
        up-core up-kafka up-trino up-superset up-airflow \
        down-core down-kafka down-trino down-superset down-airflow \
        setup ps

# ─── Default ──────────────────────────────────────────────────
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  setup          Load TPC-DS data into PostgreSQL (run once)"
	@echo ""
	@echo "  up             Start all stacks"
	@echo "  down           Stop all stacks"
	@echo "  ps             Show running containers"
	@echo ""
	@echo "  up-core        Start core stack (Postgres, MinIO, Spark, ...)"
	@echo "  up-kafka       Start Kafka + Debezium"
	@echo "  up-trino       Start Trino"
	@echo "  up-superset    Start Superset"
	@echo "  up-airflow     Start Airflow + dbt"
	@echo ""
	@echo "  down-core      Stop core stack"
	@echo "  down-kafka     Stop Kafka"
	@echo "  down-trino     Stop Trino"
	@echo "  down-superset  Stop Superset"
	@echo "  down-airflow   Stop Airflow"
	@echo ""
	@echo "  logs-core      Follow core stack logs"
	@echo "  logs-kafka     Follow Kafka logs"
	@echo "  logs-superset  Follow Superset logs"
	@echo "  logs-airflow   Follow Airflow logs"

# ─── Setup ────────────────────────────────────────────────────
setup:
	bash postgres/scripts/setup-tpcds.sh

# ─── Up ───────────────────────────────────────────────────────
up-core:
	$(CORE) up -d

up-kafka:
	$(KAFKA) up -d

up-trino:
	$(TRINO) up -d

up-superset:
	$(SUPERSET) up -d --build

up-airflow:
	$(AIRFLOW) up -d

up: up-core up-kafka up-trino up-superset up-airflow

# ─── Down ─────────────────────────────────────────────────────
down-core:
	$(CORE) down

down-kafka:
	$(KAFKA) down

down-trino:
	$(TRINO) down

down-superset:
	$(SUPERSET) down

down-airflow:
	$(AIRFLOW) down

down: down-airflow down-superset down-trino down-kafka down-core

# ─── Logs ─────────────────────────────────────────────────────
logs-core:
	$(CORE) logs -f

logs-kafka:
	$(KAFKA) logs -f

logs-superset:
	$(SUPERSET) logs -f

logs-airflow:
	$(AIRFLOW) logs -f

# ─── Status ───────────────────────────────────────────────────
ps:
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
