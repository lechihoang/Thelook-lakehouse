






with  __dbt__cte__bronze_events as (


-- Web/app events (append-only, no dedup needed)
SELECT *
FROM delta.bronze.events
WHERE operation != 'd'
), recency as (

    select 

      
      
        max(created_at) as most_recent

    from __dbt__cte__bronze_events

    

)

select

    
    most_recent,
    cast(date_add('hour', -4, current_timestamp) as timestamp) as threshold

from recency
where most_recent < cast(date_add('hour', -4, current_timestamp) as timestamp)

