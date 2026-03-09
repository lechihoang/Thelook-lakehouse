include .env
export

DBT_IMAGE := tpcds-dbt:latest

CORE     := docker compose -f docker-compose.yaml
KAFKA    := docker compose -f ./kafka/docker-compose.yaml
TRINO    := docker compose -f ./trino/docker-compose.yaml
SUPERSET := docker compose -f ./superset/docker-compose.yaml
AIRFLOW  := docker compose -f ./airflow/docker-compose.yaml

.PHONY: help \
        up down \
        up-core up-kafka up-trino up-superset up-airflow \
        down-core down-kafka down-trino down-superset down-airflow \
        simulator-start simulator-stop \
        build-dbt setup ps \
        stream-start stream-stop stream-logs stream-status

# ─── Default ──────────────────────────────────────────────────
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  setup          Show setup instructions for TheLook"
	@echo "  build-dbt      Build the dbt Docker image"
	@echo ""
	@echo "  up             Start all stacks"
	@echo "  down           Stop all stacks"
	@echo "  ps             Show running containers"
	@echo ""
	@echo "  up-core        Start core stack (Postgres, MinIO, Spark, ...)"
	@echo "  up-kafka       Start Kafka + Debezium"
	@echo "  up-trino       Start Trino"
	@echo "  up-superset    Start Superset"
	@echo "  up-airflow     Build dbt image + start Airflow"
	@echo "  simulator-start  Run TheLook simulator locally (requires Python)"
	@echo "  simulator-stop   Stop the simulator"
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
	@echo ""
	@echo "  stream-start   Submit Spark Streaming job manually"
	@echo "  stream-stop    Kill the running Streaming job"
	@echo "  stream-logs    Tail stream processor logs"
	@echo "  stream-status  Check if stream processor is running"

# ─── Setup ────────────────────────────────────────────────────
# No manual setup needed — TheLook simulator creates tables automatically on first run
setup:
	@echo "TheLook simulator creates tables automatically."
	@echo "Just run: make up-core && make up-kafka"
	@echo "Then register Debezium connector and run: make stream-start"

# ─── Build ────────────────────────────────────────────────────
build-dbt:
	docker build -t $(DBT_IMAGE) ./dbt

# ─── Up ───────────────────────────────────────────────────────
up-core:
	$(CORE) up -d

up-kafka:
	$(KAFKA) up -d

up-trino:
	$(TRINO) up -d

up-superset:
	$(SUPERSET) up -d --build

up-airflow: build-dbt
	$(AIRFLOW) up -d --build

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
	$(AIRFLOW) down -v --remove-orphans

simulator-start:
	cd notebook/work/simulator && python3.12 -m pip install -q --break-system-packages faker psycopg2-binary SQLAlchemy && \
	python3.12 generator.py \
		--db-host localhost \
		--db-user ${POSTGRES_USER} \
		--db-password ${POSTGRES_PASSWORD} \
		--db-name ${POSTGRES_DB} \
		--db-schema public \
		--init-num-users 1000 \
		--avg-qps 2 &
	@echo "Simulator started (PID: $$!). Stop with: make simulator-stop"

simulator-stop:
	@pkill -f "generator.py" 2>/dev/null && echo "Simulator stopped" || echo "Simulator not running"

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

# ─── Stream Processor ─────────────────────────────────────────
stream-start:
	docker cp notebook/work/stream_processor.py tpcds-spark-master:/tmp/stream_processor.py
	docker exec -d tpcds-spark-master bash -c \
		"/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--total-executor-cores 2 \
		--executor-memory 1G \
		--driver-memory 1G \
		--conf spark.app.name=TheLookStreaming \
		/tmp/stream_processor.py > /tmp/stream.log 2>&1"
	@echo "Stream started. Logs: make stream-logs"

stream-stop:
	@APP_ID=$$(curl -s http://localhost:8082/api/v1/applications \
		| python3 -c "import sys,json; apps=json.load(sys.stdin); \
		  [print(a['id']) for a in apps if 'TheLookStreaming' in a.get('name','')]" \
		2>/dev/null); \
	if [ -n "$$APP_ID" ]; then \
		curl -s -X POST http://localhost:8082/api/v1/applications/$$APP_ID/kill > /dev/null; \
		echo "Stream stopped (app: $$APP_ID)"; \
	else \
		echo "No running stream found"; \
	fi

stream-logs:
	docker exec tpcds-spark-master tail -f /tmp/stream.log

stream-status:
	@curl -s http://localhost:8082/api/v1/applications \
		| python3 -c "import sys,json; apps=json.load(sys.stdin); \
		  found=[a for a in apps if 'TheLookStreaming' in a.get('name','')]; \
		  print('Running: ' + found[0]['id'] if found else 'Not running')" \
		2>/dev/null || echo "Not running"

# ─── Delta Lake Maintenance ────────────────────────────────────
BRONZE_TABLES := users orders order_items events products dist_centers

delta-optimize:
	@echo "Running OPTIMIZE on all Bronze Delta tables..."
	@for table in $(BRONZE_TABLES); do \
		echo "  Optimizing bronze.$$table ..."; \
		docker exec tpcds-trino trino \
			--catalog delta --schema bronze \
			--execute "ALTER TABLE $$table EXECUTE optimize(file_size_threshold => '128MB')" 2>&1; \
	done
	@echo "✅ OPTIMIZE complete"

delta-vacuum:
	@echo "Running VACUUM on all Bronze Delta tables (retain 7 days)..."
	@for table in $(BRONZE_TABLES); do \
		echo "  Vacuuming bronze.$$table ..."; \
		docker exec tpcds-trino trino \
			--catalog delta --schema bronze \
			--execute "ALTER TABLE $$table EXECUTE vacuum(retention_threshold => '7d')" 2>&1; \
	done
	@echo "✅ VACUUM complete"
