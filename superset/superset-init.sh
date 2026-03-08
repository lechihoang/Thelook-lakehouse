#!/bin/bash
set -e

superset db upgrade
superset fab create-admin \
  --username "$SUPERSET_ADMIN_USERNAME" \
  --firstname Admin \
  --lastname User \
  --email "$SUPERSET_ADMIN_EMAIL" \
  --password "$SUPERSET_ADMIN_PASSWORD" 2>/dev/null || true
superset init
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
