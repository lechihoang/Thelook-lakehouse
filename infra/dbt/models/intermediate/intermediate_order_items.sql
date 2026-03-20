{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

SELECT
    oi.id                               AS order_item_id,
    oi.order_id,
    -- Order info
    o.status                            AS order_status,
    o.num_of_items                      AS order_num_items,
    o.created_at                        AS order_created_at,
    o.shipped_at                        AS order_shipped_at,
    o.delivered_at                      AS order_delivered_at,
    o.returned_at                       AS order_returned_at,
    o.cancelled_at                      AS order_cancelled_at,
    -- Item status
    oi.status                           AS item_status,
    oi.quantity,
    oi.sale_price,
    oi.quantity * oi.sale_price         AS revenue,
    CAST(oi.created_at AS varchar)        AS item_created_at,
    CAST(oi.shipped_at AS varchar)        AS item_shipped_at,
    CAST(oi.delivered_at AS varchar)      AS item_delivered_at,
    CAST(oi.returned_at AS varchar)       AS item_returned_at,
    CAST(oi.cancelled_at AS varchar)      AS item_cancelled_at,
    -- Product
    oi.product_id,
    p.name                              AS product_name,
    p.category                          AS product_category,
    p.brand                             AS product_brand,
    p.department                        AS product_department,
    p.sku                               AS product_sku,
    p.cost                              AS product_cost,
    p.retail_price                      AS product_retail_price,
    oi.sale_price - p.cost              AS gross_margin,
    -- Distribution center
    p.distribution_center_id,
    dc.name                             AS distribution_center,
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
    oi.event_ts_ms

FROM {{ ref('staging_order_items') }} oi
LEFT JOIN {{ ref('staging_orders') }}       o   ON oi.order_id               = o.id
LEFT JOIN {{ ref('staging_products') }}     p   ON oi.product_id             = p.id
LEFT JOIN {{ ref('staging_dist_centers') }} dc  ON p.distribution_center_id = dc.id
LEFT JOIN {{ ref('staging_users') }}        u   ON o.user_id                 = u.id

WHERE oi.order_id IS NOT NULL
{% if is_incremental() %}
    AND oi.event_ts_ms > (SELECT MAX(event_ts_ms) FROM {{ this }})
{% endif %}
