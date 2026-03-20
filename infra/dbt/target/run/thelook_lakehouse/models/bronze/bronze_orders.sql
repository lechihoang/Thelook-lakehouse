
  create or replace view
    "delta"."bronze"."bronze_orders"
  security definer
  as
    -- Latest state of each order (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."orders"
    WHERE operation != 'd'
)
WHERE rn = 1
  ;
