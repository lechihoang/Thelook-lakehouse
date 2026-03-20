#!/bin/bash
set -e

SCHEMA_REGISTRY="http://schema-registry:8081"
SCHEMA_DIR="/schemas"
RETRIES=30

echo "Waiting for Schema Registry at $SCHEMA_REGISTRY..."
for i in $(seq 1 $RETRIES); do
    if curl -sf "$SCHEMA_REGISTRY/subjects" > /dev/null 2>&1; then
        echo "Schema Registry is ready."
        break
    fi
    echo "Attempt $i/$RETRIES: Schema Registry not ready, waiting 3s..."
    sleep 3
done

# Register all schemas
register_schema() {
    local topic=$1
    local schema_type=$2   # KEY or VALUE
    local schema_file=$3

    echo "Registering $schema_type schema for topic: $topic"

    # Check if already registered
    existing=$(curl -sf -s "$SCHEMA_REGISTRY/subjects/thelook.$topic-$schema_type/versions/latest" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null || echo "")
    if [ -n "$existing" ]; then
        echo "  Schema already registered (id=$existing), skipping."
        return 0
    fi

    local payload
    payload=$(python3 -c "
import json, sys
with open('$schema_file') as f:
    schema = json.load(f)
print(json.dumps({'schemaType': 'AVRO', 'schema': json.dumps(schema)}))
")

    local response
    response=$(curl -sf -s -X POST \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "$SCHEMA_REGISTRY/subjects/thelook.$topic-$schema_type/versions")

    if [ $? -eq 0 ]; then
        local schema_id
        schema_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
        echo "  Registered successfully (id=$schema_id)"
    else
        echo "  Failed to register schema: $response"
        return 1
    fi
}

# Register key schemas
for table in users orders order_items events products dist_centers; do
    register_schema "public.$table" "KEY" "$SCHEMA_DIR/thelook-$table-key.avsc"
done

# Register value schemas
for table in users orders order_items events products dist_centers; do
    register_schema "public.$table" "VALUE" "$SCHEMA_DIR/thelook-$table-value.avsc"
done

echo ""
echo "All schemas registered successfully."
echo "Registered schemas:"
curl -sf -s "$SCHEMA_REGISTRY/subjects" | python3 -m json.tool
