{{ config(materialized='table', schema='gold') }}

WITH all_sales AS (
    SELECT customer_state, customer_city, customer_country, customer_zip,
           channel, net_paid, net_profit, quantity, sale_date
    FROM {{ ref('silver_store_sales') }}
    UNION ALL
    SELECT customer_state, customer_city, customer_country, customer_zip,
           channel, net_paid, net_profit, quantity, sale_date
    FROM {{ ref('silver_web_sales') }}
    UNION ALL
    SELECT customer_state, customer_city, customer_country, customer_zip,
           channel, net_paid, net_profit, quantity, sale_date
    FROM {{ ref('silver_catalog_sales') }}
),
all_returns AS (
    SELECT customer_state, customer_city, customer_country,
           channel, return_amt, net_loss, return_quantity
    FROM {{ ref('silver_store_returns') }}
    UNION ALL
    SELECT customer_state, customer_city, customer_country,
           channel, return_amt, net_loss, return_quantity
    FROM {{ ref('silver_web_returns') }}
    UNION ALL
    SELECT customer_state, customer_city, customer_country,
           channel, return_amt, net_loss, return_quantity
    FROM {{ ref('silver_catalog_returns') }}
),
geo_sales AS (
    SELECT
        customer_country,
        customer_state,
        customer_city,
        channel,
        COUNT(*)                    AS total_orders,
        SUM(quantity)               AS total_units,
        ROUND(SUM(net_paid), 2)     AS total_revenue,
        ROUND(SUM(net_profit), 2)   AS total_profit,
        ROUND(AVG(net_paid), 2)     AS avg_order_value,
        COUNT(DISTINCT sale_date)   AS active_days
    FROM all_sales
    WHERE customer_state IS NOT NULL
    GROUP BY customer_country, customer_state, customer_city, channel
),
geo_returns AS (
    SELECT
        customer_country,
        customer_state,
        customer_city,
        channel,
        SUM(return_quantity)        AS total_returned_units,
        ROUND(SUM(return_amt), 2)   AS total_return_amt,
        ROUND(SUM(net_loss), 2)     AS total_loss
    FROM all_returns
    WHERE customer_state IS NOT NULL
    GROUP BY customer_country, customer_state, customer_city, channel
)

SELECT
    gs.customer_country,
    gs.customer_state,
    gs.customer_city,
    gs.channel,
    gs.total_orders,
    gs.total_units,
    gs.total_revenue,
    gs.total_profit,
    gs.avg_order_value,
    gs.active_days,
    COALESCE(gr.total_returned_units, 0)    AS total_returned_units,
    COALESCE(gr.total_return_amt, 0)        AS total_return_amt,
    COALESCE(gr.total_loss, 0)              AS total_loss,
    ROUND(
        COALESCE(gr.total_returned_units, 0) * 100.0
        / NULLIF(gs.total_units, 0), 2
    )                                       AS return_rate_pct

FROM geo_sales gs
LEFT JOIN geo_returns gr
    ON  gs.customer_country = gr.customer_country
    AND gs.customer_state   = gr.customer_state
    AND gs.customer_city    = gr.customer_city
    AND gs.channel          = gr.channel

ORDER BY total_revenue DESC
