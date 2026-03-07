{{ config(materialized='table', schema='gold') }}

WITH web_ship AS (
    SELECT
        sale_date,
        sale_year,
        sale_month,
        ship_mode_type,
        ship_carrier,
        'web' AS channel,
        COUNT(*)        AS total_orders,
        SUM(quantity)   AS total_units,
        SUM(net_paid)   AS total_revenue
    FROM {{ ref('silver_web_sales') }}
    WHERE ship_mode_type IS NOT NULL
    GROUP BY sale_date, sale_year, sale_month, ship_mode_type, ship_carrier
),
catalog_ship AS (
    SELECT
        sale_date,
        sale_year,
        sale_month,
        ship_mode_type,
        ship_carrier,
        'catalog' AS channel,
        COUNT(*)        AS total_orders,
        SUM(quantity)   AS total_units,
        SUM(net_paid)   AS total_revenue
    FROM {{ ref('silver_catalog_sales') }}
    WHERE ship_mode_type IS NOT NULL
    GROUP BY sale_date, sale_year, sale_month, ship_mode_type, ship_carrier
),
return_ship AS (
    SELECT
        sm.sm_type                          AS ship_mode_type,
        sm.sm_carrier                       AS ship_carrier,
        COUNT(*)                            AS total_returns,
        SUM(cr.return_amt)                  AS total_return_amt,
        SUM(cr.ship_cost)                   AS total_ship_cost
    FROM {{ ref('silver_catalog_returns') }} cr
    LEFT JOIN delta.bronze.ship_mode sm ON cr.ship_mode_sk = sm.sm_ship_mode_sk
    WHERE cr.ship_mode_sk IS NOT NULL
    GROUP BY sm.sm_type, sm.sm_carrier
)

SELECT
    COALESCE(w.sale_date, c.sale_date)          AS sale_date,
    COALESCE(w.sale_year, c.sale_year)          AS sale_year,
    COALESCE(w.sale_month, c.sale_month)        AS sale_month,
    COALESCE(w.ship_mode_type, c.ship_mode_type) AS ship_mode_type,
    COALESCE(w.ship_carrier, c.ship_carrier)    AS ship_carrier,
    COALESCE(w.total_orders, 0) + COALESCE(c.total_orders, 0)   AS total_orders,
    COALESCE(w.total_units, 0) + COALESCE(c.total_units, 0)     AS total_units,
    COALESCE(w.total_revenue, 0) + COALESCE(c.total_revenue, 0) AS total_revenue,
    COALESCE(rs.total_returns, 0)               AS total_returns,
    COALESCE(rs.total_ship_cost, 0)             AS total_ship_cost

FROM web_ship w
FULL OUTER JOIN catalog_ship c
    ON  w.sale_date      = c.sale_date
    AND w.ship_mode_type = c.ship_mode_type
    AND w.ship_carrier   = c.ship_carrier
LEFT JOIN return_ship rs
    ON  COALESCE(w.ship_mode_type, c.ship_mode_type) = rs.ship_mode_type
    AND COALESCE(w.ship_carrier, c.ship_carrier)     = rs.ship_carrier

ORDER BY sale_date DESC, total_orders DESC
