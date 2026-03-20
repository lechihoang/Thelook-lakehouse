
    
    

with  __dbt__cte__bronze_orders as (
-- Latest state of each order (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."orders"
    WHERE operation != 'd'
)
WHERE rn = 1
), all_values as (

    select
        status as value_field,
        count(*) as n_records

    from __dbt__cte__bronze_orders
    group by status

)

select *
from all_values
where value_field not in (
    'Processing','Shipped','Delivered','Cancelled','Returned'
)


