{{ config(materialized='table', schema='gold') }}

WITH promo_sales AS (
    SELECT
        promo_sk,
        promo_name,
        promo_channel_email,
        promo_channel_tv,
        channel,
        COUNT(*)        AS total_orders,
        SUM(quantity)   AS total_units,
        SUM(net_paid)   AS total_revenue,
        SUM(coupon_discount) AS total_discount_given,
        SUM(net_profit) AS total_profit
    FROM {{ ref('silver_store_sales') }}
    WHERE promo_sk IS NOT NULL
    GROUP BY promo_sk, promo_name, promo_channel_email, promo_channel_tv, channel

    UNION ALL

    SELECT
        promo_sk, promo_name, promo_channel_email, promo_channel_tv, channel,
        COUNT(*), SUM(quantity), SUM(net_paid), SUM(coupon_discount), SUM(net_profit)
    FROM {{ ref('silver_web_sales') }}
    WHERE promo_sk IS NOT NULL
    GROUP BY promo_sk, promo_name, promo_channel_email, promo_channel_tv, channel

    UNION ALL

    SELECT
        promo_sk, promo_name, promo_channel_email, promo_channel_tv, channel,
        COUNT(*), SUM(quantity), SUM(net_paid), SUM(coupon_discount), SUM(net_profit)
    FROM {{ ref('silver_catalog_sales') }}
    WHERE promo_sk IS NOT NULL
    GROUP BY promo_sk, promo_name, promo_channel_email, promo_channel_tv, channel
),
catalog_page_sales AS (
    SELECT
        catalog_department,
        catalog_number,
        catalog_type,
        COUNT(*)        AS total_orders,
        SUM(quantity)   AS total_units,
        SUM(net_paid)   AS total_revenue
    FROM {{ ref('silver_catalog_sales') }}
    WHERE catalog_department IS NOT NULL
    GROUP BY catalog_department, catalog_number, catalog_type
),
web_page_sales AS (
    SELECT
        web_page_type,
        web_page_url,
        COUNT(*)        AS total_orders,
        SUM(quantity)   AS total_units,
        SUM(net_paid)   AS total_revenue
    FROM {{ ref('silver_web_sales') }}
    WHERE web_page_type IS NOT NULL
    GROUP BY web_page_type, web_page_url
)

SELECT
    'promotion'                             AS analysis_type,
    CAST(promo_sk AS VARCHAR)               AS entity_id,
    promo_name                              AS entity_name,
    channel,
    total_orders,
    total_units,
    ROUND(total_revenue, 2)                 AS total_revenue,
    ROUND(total_discount_given, 2)          AS total_discount,
    ROUND(total_profit, 2)                  AS total_profit,
    ROUND(total_profit / NULLIF(total_revenue, 0) * 100, 2) AS profit_margin_pct,
    ROUND(total_discount_given / NULLIF(total_revenue + total_discount_given, 0) * 100, 2) AS discount_rate_pct

FROM promo_sales

ORDER BY total_revenue DESC
