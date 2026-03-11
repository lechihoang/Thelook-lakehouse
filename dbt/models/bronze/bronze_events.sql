-- Web/app events (append-only, no dedup needed)
SELECT *
FROM {{ source('bronze', 'events') }}
WHERE operation != 'd'
