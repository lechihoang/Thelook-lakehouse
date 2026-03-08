{{ config(materialized='table', schema='gold') }}

WITH center_sales AS (
    SELECT
        call_center_sk,
        call_center_name,
        call_center_manager,
        call_center_hours,
        COUNT(*)                    AS total_orders,
        SUM(quantity)               AS total_units_sold,
        ROUND(SUM(net_paid), 2)     AS total_revenue,
        ROUND(SUM(net_profit), 2)   AS total_profit,
        ROUND(AVG(net_paid), 2)     AS avg_order_value
    FROM {{ ref('silver_catalog_sales') }}
    WHERE call_center_sk IS NOT NULL
    GROUP BY call_center_sk, call_center_name, call_center_manager, call_center_hours
),
center_returns AS (
    SELECT
        call_center_sk,
        call_center_name,
        return_reason,
        COUNT(*)                        AS total_returns,
        SUM(return_quantity)            AS total_units_returned,
        ROUND(SUM(return_amt), 2)       AS total_return_amt,
        ROUND(SUM(net_loss), 2)         AS total_loss
    FROM {{ ref('silver_catalog_returns') }}
    WHERE call_center_sk IS NOT NULL
    GROUP BY call_center_sk, call_center_name, return_reason
),
top_return_reasons AS (
    SELECT
        call_center_sk,
        return_reason,
        total_returns,
        ROW_NUMBER() OVER (PARTITION BY call_center_sk ORDER BY total_returns DESC) AS rn
    FROM center_returns
)

SELECT
    cs.call_center_sk,
    cs.call_center_name,
    cs.call_center_manager,
    cs.call_center_hours,
    cs.total_orders,
    cs.total_units_sold,
    cs.total_revenue,
    cs.total_profit,
    cs.avg_order_value,
    COALESCE(SUM(cr.total_returns), 0)          AS total_returns,
    COALESCE(SUM(cr.total_units_returned), 0)   AS total_units_returned,
    COALESCE(SUM(cr.total_return_amt), 0)       AS total_return_amt,
    COALESCE(SUM(cr.total_loss), 0)             AS total_loss,
    ROUND(
        COALESCE(SUM(cr.total_units_returned), 0) * 100.0
        / NULLIF(cs.total_units_sold, 0), 2
    )                                           AS return_rate_pct,
    MAX(CASE WHEN tr.rn = 1 THEN tr.return_reason END) AS top_return_reason

FROM center_sales cs
LEFT JOIN center_returns cr ON cs.call_center_sk = cr.call_center_sk
LEFT JOIN top_return_reasons tr
    ON  cs.call_center_sk = tr.call_center_sk AND tr.rn = 1

GROUP BY
    cs.call_center_sk, cs.call_center_name, cs.call_center_manager,
    cs.call_center_hours, cs.total_orders, cs.total_units_sold,
    cs.total_revenue, cs.total_profit, cs.avg_order_value

ORDER BY cs.total_revenue DESC
