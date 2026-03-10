{{ config(materialized='table') }}

WITH sales AS (
    SELECT
        product_id,
        product_name,
        product_category,
        product_brand,
        distribution_center,
        COUNT(order_item_id)                            AS total_sold,
        ROUND(SUM(revenue), 2)                          AS total_revenue,
        ROUND(AVG(sale_price), 2)                       AS avg_sale_price,
        ROUND(AVG(gross_margin), 2)                     AS avg_margin,
        COUNT(CASE WHEN item_status = 'Returned'  THEN 1 END) AS return_count,
        COUNT(CASE WHEN item_status = 'Cancelled' THEN 1 END) AS cancel_count
    FROM {{ ref('silver_order_items') }}
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    *,
    ROUND(return_count  * 100.0 / NULLIF(total_sold, 0), 2) AS return_rate_pct,
    ROUND(cancel_count  * 100.0 / NULLIF(total_sold, 0), 2) AS cancel_rate_pct
FROM sales
