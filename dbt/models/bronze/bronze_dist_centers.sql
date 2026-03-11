-- Latest state of each distribution center (deduplicate in case of stream re-ingestion)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM {{ source('bronze', 'dist_centers') }}
    WHERE operation != 'd'
)
WHERE rn = 1
