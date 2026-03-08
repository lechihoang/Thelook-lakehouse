#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# TPC-DS Data Setup Script
# - If tpcds-data/ already exists, skips compile + generate steps
# - Run this ONCE after `docker compose up -d` is healthy
# ─────────────────────────────────────────────────────────────────

set -e

CONTAINER="tpcds-postgres"
POSTGRES_USER="admin"
POSTGRES_PASSWORD="admin123"
POSTGRES_DB="tpcds"
SCALE=${SCALE:-1}
DATA_DIR="$(pwd)/tpcds-data"

# ─── Step 1+2+3: Compile & generate (skip if data already exists) ─
if [ -f "$DATA_DIR/store_sales.dat" ]; then
  echo "=== Data already exists, skipping compile + generate ==="
else
  echo "=== Step 1: Clone tpcds-kit ==="
  if [ ! -d "tpcds-kit" ]; then
    git clone --depth 1 https://github.com/gregrahn/tpcds-kit.git
  fi

  echo "=== Step 2: Compile dsdgen ==="
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS: create values.h stub (removed in newer Xcode) and suppress legacy C warnings
    cat > tpcds-kit/tools/values.h << 'EOF'
#ifndef VALUES_H
#define VALUES_H
#include <limits.h>
#include <float.h>
#define MAXDOUBLE DBL_MAX
#define MAXFLOAT  FLT_MAX
#define MINDOUBLE DBL_MIN
#define MINFLOAT  FLT_MIN
#endif
EOF
    cd tpcds-kit/tools && make OS=MACOS dsdgen \
      CFLAGS="-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -DYYDEBUG -DMACOS -g \
              -Wno-implicit-int -Wno-deprecated-non-prototype -Wno-implicit-function-declaration"
  else
    # Linux: install build deps if missing (supports apt-based distros)
    if ! command -v gcc &>/dev/null || ! command -v make &>/dev/null; then
      echo "  Installing build tools..."
      sudo apt-get update -qq && sudo apt-get install -y -qq git make gcc
    fi
    cd tpcds-kit/tools && make OS=LINUX dsdgen
  fi
  cd ../..

  echo "=== Step 3: Generate data (scale=${SCALE}GB) ==="
  mkdir -p "$DATA_DIR"
  cd tpcds-kit/tools
  ./dsdgen -SCALE $SCALE -DIR "$DATA_DIR" -FORCE YES
  cd ../..
fi

# ─── Step 4: Load schema ──────────────────────────────────────────
echo "=== Step 4: Load TPC-DS schema ==="
docker exec -i "$CONTAINER" bash -c \
  "PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d $POSTGRES_DB" \
  < tpcds-kit/tools/tpcds.sql

# ─── Step 5: Load data ───────────────────────────────────────────
echo "=== Step 5: Load data into PostgreSQL ==="
for table in customer customer_address customer_demographics date_dim time_dim \
             item store warehouse promotion ship_mode income_band reason \
             household_demographics call_center catalog_page web_site web_page \
             store_sales store_returns web_sales web_returns \
             catalog_sales catalog_returns inventory; do
  FILE="$DATA_DIR/${table}.dat"
  if [ -f "$FILE" ]; then
    echo "  Loading $table..."
    sed 's/|$//' "$FILE" | docker exec -i "$CONTAINER" bash -c \
      "PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d $POSTGRES_DB \
       -c \"\COPY ${table} FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '')\""
  fi
done

# ─── Step 6: Create Debezium publication ─────────────────────────
echo "=== Step 6: Create Debezium publication ==="
docker exec -i "$CONTAINER" bash -c \
  "PGPASSWORD=$POSTGRES_PASSWORD psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"
    CREATE PUBLICATION debezium_tpcds_pub FOR TABLE
      store_sales, store_returns,
      web_sales, web_returns,
      catalog_sales, catalog_returns,
      inventory;
  \"" 2>/dev/null || echo "  (publication already exists, skipping)"

echo ""
echo "✅ TPC-DS setup complete!"
