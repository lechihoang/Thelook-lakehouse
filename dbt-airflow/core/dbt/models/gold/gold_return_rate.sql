{{ config(materialized='table', schema='gold') }}

WITH sales AS (
    SELECT sale_date, item_category, item_brand, channel,
           SUM(quantity) AS units_sold, SUM(net_paid) AS revenue
    FROM {{ ref('silver_store_sales') }}
    GROUP BY sale_date, item_category, item_brand, channel
    UNION ALL
    SELECT sale_date, item_category, item_brand, channel,
           SUM(quantity), SUM(net_paid)
    FROM {{ ref('silver_web_sales') }}
    GROUP BY sale_date, item_category, item_brand, channel
    UNION ALL
    SELECT sale_date, item_category, item_brand, channel,
           SUM(quantity), SUM(net_paid)
    FROM {{ ref('silver_catalog_sales') }}
    GROUP BY sale_date, item_category, item_brand, channel
),
returns AS (
    SELECT return_date, item_category, item_brand, channel,
           SUM(return_quantity) AS units_returned,
           SUM(return_amt)      AS return_amt,
           SUM(net_loss)        AS total_loss
    FROM {{ ref('silver_store_returns') }}
    GROUP BY return_date, item_category, item_brand, channel
    UNION ALL
    SELECT return_date, item_category, item_brand, channel,
           SUM(return_quantity), SUM(return_amt), SUM(net_loss)
    FROM {{ ref('silver_web_returns') }}
    GROUP BY return_date, item_category, item_brand, channel
    UNION ALL
    SELECT return_date, item_category, item_brand, channel,
           SUM(return_quantity), SUM(return_amt), SUM(net_loss)
    FROM {{ ref('silver_catalog_returns') }}
    GROUP BY return_date, item_category, item_brand, channel
)

SELECT
    COALESCE(s.sale_date, r.return_date)    AS date,
    COALESCE(s.item_category, r.item_category) AS item_category,
    COALESCE(s.item_brand, r.item_brand)    AS item_brand,
    COALESCE(s.channel, r.channel)          AS channel,
    COALESCE(s.units_sold, 0)               AS units_sold,
    COALESCE(s.revenue, 0)                  AS revenue,
    COALESCE(r.units_returned, 0)           AS units_returned,
    COALESCE(r.return_amt, 0)               AS return_amt,
    COALESCE(r.total_loss, 0)               AS total_loss,
    ROUND(
        COALESCE(r.units_returned, 0) * 100.0
        / NULLIF(COALESCE(s.units_sold, 0), 0), 2
    )                                       AS return_rate_pct

FROM sales s
FULL OUTER JOIN returns r
    ON  s.sale_date    = r.return_date
    AND s.item_category = r.item_category
    AND s.item_brand    = r.item_brand
    AND s.channel       = r.channel

ORDER BY date DESC, return_rate_pct DESC
