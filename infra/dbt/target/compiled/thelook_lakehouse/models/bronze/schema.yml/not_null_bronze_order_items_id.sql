
    
    



with __dbt__cte__bronze_order_items as (
-- Latest state of each order item (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."order_items"
    WHERE operation != 'd'
)
WHERE rn = 1
) select id
from __dbt__cte__bronze_order_items
where id is null


