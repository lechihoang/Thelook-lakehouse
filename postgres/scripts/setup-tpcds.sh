#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# TPC-DS Data Setup Script
# Run this ONCE after `docker compose up postgres` is healthy
# ─────────────────────────────────────────────────────────────────

set -e

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_USER=${POSTGRES_USER:-admin}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-admin123}
POSTGRES_DB=${POSTGRES_DB:-tpcds}
SCALE=${SCALE:-1}           # Scale factor in GB (1 = ~1GB data)
DATA_DIR="./tpcds-data"

export PGPASSWORD=$POSTGRES_PASSWORD

echo "=== Step 1: Clone tpcds-kit ==="
if [ ! -d "tpcds-kit" ]; then
  git clone https://github.com/gregrahn/tpcds-kit.git
fi

echo "=== Step 2: Compile dsdgen ==="
cd tpcds-kit/tools
# macOS:
make OS=MACOS 2>/dev/null || make OS=LINUX
cd ../..

echo "=== Step 3: Generate data (scale=${SCALE}GB) ==="
mkdir -p "$DATA_DIR"
cd tpcds-kit/tools
./dsdgen -SCALE $SCALE -DIR "../../$DATA_DIR" -FORCE YES
cd ../..

echo "=== Step 4: Load TPC-DS schema ==="
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB \
  -f tpcds-kit/tools/tpcds.sql

echo "=== Step 5: Load data into PostgreSQL ==="
for table in customer customer_address customer_demographics date_dim time_dim \
             item store warehouse promotion ship_mode income_band reason \
             household_demographics call_center catalog_page web_site web_page \
             store_sales store_returns web_sales web_returns \
             catalog_sales catalog_returns inventory; do
  FILE="$DATA_DIR/${table}.dat"
  if [ -f "$FILE" ]; then
    echo "  Loading $table..."
    psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB \
      -c "\COPY $table FROM '$FILE' WITH (FORMAT CSV, DELIMITER '|', NULL '')"
  fi
done

echo "=== Step 6: Create Debezium publication ==="
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB << 'SQL'
  CREATE PUBLICATION debezium_tpcds_pub FOR TABLE
    store_sales, store_returns,
    web_sales, web_returns,
    catalog_sales, catalog_returns,
    inventory;
SQL

echo ""
echo "✅ TPC-DS setup complete!"
echo "   Register Debezium connector:"
echo "   curl -X POST http://localhost:8083/connectors \\"
echo "     -H 'Content-Type: application/json' \\"
echo "     -d @kafka/conf/register-tpcds-connector.json"
