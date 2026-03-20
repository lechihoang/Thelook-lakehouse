select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



with __dbt__cte__bronze_dist_centers as (
-- Latest state of each distribution center (deduplicate in case of stream re-ingestion)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."dist_centers"
    WHERE operation != 'd'
)
WHERE rn = 1
) select id
from __dbt__cte__bronze_dist_centers
where id is null



      
    ) dbt_internal_test