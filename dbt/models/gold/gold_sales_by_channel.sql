{{ config(materialized='table', schema='gold') }}

WITH store_daily AS (
    SELECT
        sale_date,
        sale_year,
        sale_month,
        'store'         AS channel,
        COUNT(*)        AS total_transactions,
        SUM(quantity)   AS total_units_sold,
        SUM(net_paid)   AS total_revenue,
        SUM(net_profit) AS total_profit,
        AVG(net_paid)   AS avg_order_value
    FROM {{ ref('silver_store_sales') }}
    GROUP BY sale_date, sale_year, sale_month
),
web_daily AS (
    SELECT
        sale_date,
        sale_year,
        sale_month,
        'web'           AS channel,
        COUNT(*)        AS total_transactions,
        SUM(quantity)   AS total_units_sold,
        SUM(net_paid)   AS total_revenue,
        SUM(net_profit) AS total_profit,
        AVG(net_paid)   AS avg_order_value
    FROM {{ ref('silver_web_sales') }}
    GROUP BY sale_date, sale_year, sale_month
),
catalog_daily AS (
    SELECT
        sale_date,
        sale_year,
        sale_month,
        'catalog'       AS channel,
        COUNT(*)        AS total_transactions,
        SUM(quantity)   AS total_units_sold,
        SUM(net_paid)   AS total_revenue,
        SUM(net_profit) AS total_profit,
        AVG(net_paid)   AS avg_order_value
    FROM {{ ref('silver_catalog_sales') }}
    GROUP BY sale_date, sale_year, sale_month
)

SELECT * FROM store_daily
UNION ALL
SELECT * FROM web_daily
UNION ALL
SELECT * FROM catalog_daily
ORDER BY sale_date DESC, channel
