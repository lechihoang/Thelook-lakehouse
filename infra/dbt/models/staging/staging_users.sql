-- Latest state of each user (deduplicate CDC updates, e.g. address changes)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM {{ source('staging', 'users') }}
    WHERE operation != 'd'
)
WHERE rn = 1
