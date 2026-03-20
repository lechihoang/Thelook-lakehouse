
  
    

    create table "delta"."silver"."silver_order_items"
      
      
    as (
      

with __dbt__cte__bronze_order_items as (


-- Latest state of each order item (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM delta.bronze.order_items
    WHERE operation != 'd'
)
WHERE rn = 1
),  __dbt__cte__bronze_orders as (


-- Latest state of each order (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM delta.bronze.orders
    WHERE operation != 'd'
)
WHERE rn = 1
) SELECT
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
    -- Product
    oi.product_id,
    p.name                              AS product_name,
    p.category                          AS product_category,
    p.brand                             AS product_brand,
    p.department                        AS product_department,
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
    u.traffic_source,
    -- Metadata
    oi.event_ts_ms

FROM __dbt__cte__bronze_order_items oi
LEFT JOIN __dbt__cte__bronze_orders       o   ON oi.order_id      = o.id
LEFT JOIN postgresql.public.products       p   ON oi.product_id    = p.id
LEFT JOIN postgresql.public.dist_centers   dc  ON p.distribution_center_id = dc.id
LEFT JOIN postgresql.public.users          u   ON o.user_id        = u.id

WHERE oi.order_id IS NOT NULL
    );

  