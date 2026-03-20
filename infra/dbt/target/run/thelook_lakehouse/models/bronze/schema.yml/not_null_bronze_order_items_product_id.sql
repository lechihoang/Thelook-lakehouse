select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



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
) select product_id
from __dbt__cte__bronze_order_items
where product_id is null



      
    ) dbt_internal_test