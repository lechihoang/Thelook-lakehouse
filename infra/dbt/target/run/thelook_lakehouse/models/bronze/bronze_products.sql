
  create or replace view
    "delta"."bronze"."bronze_products"
  security definer
  as
    -- Latest state of each product (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."products"
    WHERE operation != 'd'
)
WHERE rn = 1
  ;
