{{ config(materialized='table', schema='silver') }}

SELECT
    inv.inv_date_sk,
    d.d_date                        AS snapshot_date,
    d.d_year                        AS snapshot_year,
    d.d_moy                         AS snapshot_month,
    inv.inv_item_sk,
    i.i_product_name                AS product_name,
    i.i_category                    AS item_category,
    i.i_brand                       AS item_brand,
    inv.inv_warehouse_sk,
    w.w_warehouse_name              AS warehouse_name,
    w.w_city                        AS warehouse_city,
    w.w_state                       AS warehouse_state,
    inv.inv_quantity_on_hand        AS quantity_on_hand,
    CASE
        WHEN inv.inv_quantity_on_hand = 0   THEN 'Out of Stock'
        WHEN inv.inv_quantity_on_hand < 50  THEN 'Low Stock'
        WHEN inv.inv_quantity_on_hand < 200 THEN 'Normal'
        ELSE 'High Stock'
    END                             AS stock_status

FROM {{ ref('bronze_inventory') }} inv

LEFT JOIN delta.bronze.date_dim  d ON inv.inv_date_sk      = d.d_date_sk
LEFT JOIN delta.bronze.item      i ON inv.inv_item_sk       = i.i_item_sk
LEFT JOIN delta.bronze.warehouse w ON inv.inv_warehouse_sk  = w.w_warehouse_sk
