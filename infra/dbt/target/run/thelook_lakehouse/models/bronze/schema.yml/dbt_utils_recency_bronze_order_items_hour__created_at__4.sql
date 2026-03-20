select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      






with  __dbt__cte__bronze_order_items as (


-- Latest state of each order item (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM delta.bronze.order_items
    WHERE operation != 'd'
)
WHERE rn = 1
), recency as (

    select 

      
      
        max(created_at) as most_recent

    from __dbt__cte__bronze_order_items

    

)

select

    
    most_recent,
    cast(date_add('hour', -4, current_timestamp) as timestamp) as threshold

from recency
where most_recent < cast(date_add('hour', -4, current_timestamp) as timestamp)


      
    ) dbt_internal_test