{{ config(materialized='table', schema='gold') }}

WITH all_sales AS (
    SELECT item_sk, item_category, item_class, item_brand, product_name,
           quantity, net_paid, net_profit, sale_date, channel
    FROM {{ ref('silver_store_sales') }}
    UNION ALL
    SELECT item_sk, item_category, item_class, item_brand, product_name,
           quantity, net_paid, net_profit, sale_date, channel
    FROM {{ ref('silver_web_sales') }}
    UNION ALL
    SELECT item_sk, item_category, item_class, item_brand, product_name,
           quantity, net_paid, net_profit, sale_date, channel
    FROM {{ ref('silver_catalog_sales') }}
)

SELECT
    item_sk,
    product_name,
    item_category,
    item_class,
    item_brand,
    COUNT(*)                    AS total_orders,
    SUM(quantity)               AS total_units_sold,
    ROUND(SUM(net_paid), 2)     AS total_revenue,
    ROUND(SUM(net_profit), 2)   AS total_profit,
    ROUND(AVG(net_paid), 2)     AS avg_order_value,
    ROUND(
        SUM(net_profit) / NULLIF(SUM(net_paid), 0) * 100, 2
    )                           AS profit_margin_pct,
    COUNT(DISTINCT sale_date)   AS active_days,
    MIN(sale_date)              AS first_sale_date,
    MAX(sale_date)              AS last_sale_date

FROM all_sales
GROUP BY item_sk, product_name, item_category, item_class, item_brand
ORDER BY total_revenue DESC
