{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per order item — most granular fact, FK to all dims
SELECT
    -- Keys
    oi.order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    CAST(date_format(date_trunc('day', from_unixtime(oi.event_ts_ms / 1000)), '%Y%m%d') AS INTEGER) AS date_key,
    -- Status
    oi.order_status,
    oi.item_status,
    -- Measures
    oi.quantity,
    oi.sale_price,
    oi.revenue,
    oi.gross_margin,
    oi.product_cost,
    -- Timestamps
    oi.order_created_at,
    oi.item_created_at,
    oi.item_shipped_at,
    oi.item_delivered_at,
    oi.item_returned_at,
    oi.item_cancelled_at,
    -- Metadata
    oi.event_ts_ms

FROM {{ ref('silver_order_items') }} oi

{% if is_incremental() %}
WHERE oi.event_ts_ms > (SELECT MAX(event_ts_ms) FROM {{ this }})
{% endif %}
