{{ config(materialized='table', schema='gold') }}

WITH all_sales AS (
    SELECT customer_sk, customer_name, customer_gender,
           education_status, marital_status,
           net_paid, net_profit, quantity, sale_date, channel
    FROM {{ ref('silver_store_sales') }}
    UNION ALL
    SELECT customer_sk, customer_name, customer_gender,
           NULL AS education_status, NULL AS marital_status,
           net_paid, net_profit, quantity, sale_date, channel
    FROM {{ ref('silver_web_sales') }}
    UNION ALL
    SELECT customer_sk, customer_name, customer_gender,
           NULL AS education_status, NULL AS marital_status,
           net_paid, net_profit, quantity, sale_date, channel
    FROM {{ ref('silver_catalog_sales') }}
)

SELECT
    customer_sk,
    customer_name,
    customer_gender,
    education_status,
    marital_status,
    COUNT(*)                        AS total_orders,
    SUM(quantity)                   AS total_units_bought,
    ROUND(SUM(net_paid), 2)         AS total_spend,
    ROUND(AVG(net_paid), 2)         AS avg_order_value,
    COUNT(DISTINCT sale_date)       AS active_days,
    COUNT(DISTINCT channel)         AS channels_used,
    MIN(sale_date)                  AS first_purchase_date,
    MAX(sale_date)                  AS last_purchase_date,
    CASE
        WHEN SUM(net_paid) >= 10000 THEN 'VIP'
        WHEN SUM(net_paid) >= 3000  THEN 'High Value'
        WHEN SUM(net_paid) >= 500   THEN 'Regular'
        ELSE 'Low Value'
    END                             AS customer_segment

FROM all_sales
WHERE customer_sk IS NOT NULL
GROUP BY customer_sk, customer_name, customer_gender, education_status, marital_status
ORDER BY total_spend DESC
