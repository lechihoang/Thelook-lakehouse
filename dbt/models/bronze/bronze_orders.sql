{{ config(materialized='ephemeral') }}

-- Latest state of each order (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM delta.bronze.orders
    WHERE operation != 'd'
)
WHERE rn = 1
