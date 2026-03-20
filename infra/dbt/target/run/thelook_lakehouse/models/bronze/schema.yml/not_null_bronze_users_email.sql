select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



with __dbt__cte__bronze_users as (
-- Latest state of each user (deduplicate CDC updates, e.g. address changes)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."users"
    WHERE operation != 'd'
)
WHERE rn = 1
) select email
from __dbt__cte__bronze_users
where email is null



      
    ) dbt_internal_test