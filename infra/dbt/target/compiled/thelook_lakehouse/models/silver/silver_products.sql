

with __dbt__cte__bronze_products as (
-- Latest state of each product (deduplicate CDC updates)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."products"
    WHERE operation != 'd'
)
WHERE rn = 1
),  __dbt__cte__bronze_dist_centers as (
-- Latest state of each distribution center (deduplicate in case of stream re-ingestion)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."dist_centers"
    WHERE operation != 'd'
)
WHERE rn = 1
) -- Product dimension: latest catalog state with distribution center info
SELECT
    p.id                            AS product_id,
    p.name                          AS product_name,
    p.category                      AS product_category,
    p.department                    AS product_department,
    p.brand                         AS product_brand,
    p.sku,
    p.cost,
    p.retail_price,
    p.retail_price - p.cost         AS list_margin,
    p.distribution_center_id,
    dc.name                         AS distribution_center,
    dc.latitude                     AS dc_latitude,
    dc.longitude                    AS dc_longitude,
    p.event_ts_ms

FROM __dbt__cte__bronze_products p
LEFT JOIN __dbt__cte__bronze_dist_centers dc ON p.distribution_center_id = dc.id

