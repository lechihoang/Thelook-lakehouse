
    
    



with __dbt__cte__bronze_products as (
-- Latest state of each product (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."products"
    WHERE operation != 'd'
)
WHERE rn = 1
) select cost
from __dbt__cte__bronze_products
where cost is null


