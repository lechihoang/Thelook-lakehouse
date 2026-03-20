include .env
export

DBT_IMAGE := thelook-dbt:latest

.PHONY: help build-dbt up down ps

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  up-core       Start core services (postgres, minio, kafka, spark, trino, etc.)"
	@echo "  up-explore   Start JupyterLab for exploration"
	@echo "  up-datagen   Start data generator"
	@echo "  up-airflow   Start Airflow + dbt"
	@echo "  up-all       Start everything"
	@echo "  down         Stop all containers"
	@echo "  ps            Show running containers"
	@echo ""
	@echo "  build-dbt     Build the dbt Docker image"
	@echo ""
	@echo "Examples:"
	@echo "  make up-core          # Core services only"
	@echo "  make up-all          # Everything"
	@echo "  docker compose --profile core up -d"
	@echo "  docker compose --profile core --profile datagen --profile explore --profile airflow up -d"

# ─── Build ────────────────────────────────────────────────────
build-dbt:
	docker build -t $(DBT_IMAGE) ./infra/dbt

# ─── Up ─────────────────────────────────────────────────────
up-core:
	docker compose --profile core up -d

up-explore:
	docker compose --profile explore up -d

up-datagen:
	docker compose --profile datagen up -d

up-airflow:
	docker compose --profile airflow up -d

up-all:
	docker compose --profile core --profile datagen up -d

up:
	docker compose --profile core up -d

# ─── Down ─────────────────────────────────────────────────────
down:
	docker compose down

# ─── Status ───────────────────────────────────────────────────
ps:
	@docker compose ps
