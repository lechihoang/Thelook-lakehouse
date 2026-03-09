{{ config(materialized='table') }}

SELECT
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
    u.traffic_source,
    -- Metadata
    o.event_ts_ms

FROM {{ ref('bronze_orders') }} o
LEFT JOIN postgresql.public.users u ON o.user_id = u.id
