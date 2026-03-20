
  create or replace view
    "delta"."bronze"."bronze_order_items"
  security definer
  as
    -- Latest state of each order item (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."order_items"
    WHERE operation != 'd'
)
WHERE rn = 1
  ;
