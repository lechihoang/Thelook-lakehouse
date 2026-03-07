{{ config(materialized='table', schema='gold') }}

SELECT
    snapshot_date,
    item_category,
    item_brand,
    warehouse_name,
    warehouse_state,
    COUNT(*)                                    AS total_sku_count,
    SUM(quantity_on_hand)                       AS total_units,
    SUM(CASE WHEN stock_status = 'Out of Stock' THEN 1 ELSE 0 END) AS out_of_stock_count,
    SUM(CASE WHEN stock_status = 'Low Stock'    THEN 1 ELSE 0 END) AS low_stock_count,
    SUM(CASE WHEN stock_status = 'Normal'       THEN 1 ELSE 0 END) AS normal_stock_count,
    SUM(CASE WHEN stock_status = 'High Stock'   THEN 1 ELSE 0 END) AS high_stock_count,
    ROUND(
        SUM(CASE WHEN stock_status = 'Out of Stock' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                           AS out_of_stock_pct

FROM {{ ref('silver_inventory') }}
GROUP BY snapshot_date, item_category, item_brand, warehouse_name, warehouse_state
ORDER BY snapshot_date DESC, out_of_stock_pct DESC
