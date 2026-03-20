-- Latest state of each product (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM {{ source('staging', 'products') }}
    WHERE operation != 'd'
)
WHERE rn = 1
