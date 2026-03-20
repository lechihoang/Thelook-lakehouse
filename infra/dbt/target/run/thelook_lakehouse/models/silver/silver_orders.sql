
  
    

    create table "delta"."silver"."silver_orders"
      
      
    as (
      

with __dbt__cte__bronze_orders as (
-- Latest state of each order (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."orders"
    WHERE operation != 'd'
)
WHERE rn = 1
),  __dbt__cte__bronze_users as (
-- Latest state of each user (deduplicate CDC updates, e.g. address changes)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."users"
    WHERE operation != 'd'
)
WHERE rn = 1
) SELECT
    o.id                                AS order_id,
    o.status                            AS order_status,
    o.num_of_items,
    o.created_at,
    o.updated_at,
    o.shipped_at,
    o.delivered_at,
    o.returned_at,
    o.cancelled_at,
    -- User
    o.user_id,
    u.first_name || ' ' || u.last_name  AS customer_name,
    u.gender                            AS customer_gender,
    u.age                               AS customer_age,
    u.country                           AS customer_country,
    u.state                             AS customer_state,
    u.city                              AS customer_city,
    u.latitude                          AS customer_lat,
    u.longitude                         AS customer_lon,
    CAST(u.created_at AS varchar)       AS user_registered_at,
    u.traffic_source,
    -- Metadata
    o.event_ts_ms

FROM __dbt__cte__bronze_orders o
LEFT JOIN __dbt__cte__bronze_users u ON o.user_id = u.id

    );

  