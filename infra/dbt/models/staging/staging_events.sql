-- Web/app events (append-only, no dedup needed)
SELECT *
FROM {{ source('staging', 'events') }}
WHERE operation != 'd'
