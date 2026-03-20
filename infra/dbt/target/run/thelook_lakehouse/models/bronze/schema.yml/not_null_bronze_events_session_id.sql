select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



with __dbt__cte__bronze_events as (
-- Web/app events (append-only, no dedup needed)
SELECT *
FROM "delta"."bronze"."events"
WHERE operation != 'd'
) select session_id
from __dbt__cte__bronze_events
where session_id is null



      
    ) dbt_internal_test