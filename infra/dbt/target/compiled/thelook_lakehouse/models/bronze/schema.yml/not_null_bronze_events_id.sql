
    
    



with __dbt__cte__bronze_events as (
-- Web/app events (append-only, no dedup needed)
SELECT *
FROM "delta"."bronze"."events"
WHERE operation != 'd'
) select id
from __dbt__cte__bronze_events
where id is null


