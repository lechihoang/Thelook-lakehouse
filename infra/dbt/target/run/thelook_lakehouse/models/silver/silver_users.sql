
  
    

    create table "delta"."silver"."silver_users"
      
      
    as (
      

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
) -- User dimension: latest profile state, enriched with purchase summary
SELECT
    u.id                            AS user_id,
    u.first_name,
    u.last_name,
    u.first_name || ' ' || u.last_name AS full_name,
    u.email,
    u.age,
    u.gender,
    u.street_address,
    u.postal_code,
    u.city,
    u.state,
    u.country,
    u.latitude,
    u.longitude,
    u.traffic_source,
    u.created_at                    AS registered_at,
    u.updated_at,
    u.event_ts_ms

FROM __dbt__cte__bronze_users u


    );

  