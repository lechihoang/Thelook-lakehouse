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
    -- Dates and timestamps (converted from epoch ms)
    TRY(date(from_unixtime(CAST(oi.order_created_at  AS BIGINT) / 1000))) AS order_date,
    TRY(date(from_unixtime(CAST(oi.item_created_at   AS BIGINT) / 1000))) AS item_date,
    TRY(from_unixtime(CAST(oi.order_created_at       AS BIGINT) / 1000))  AS order_created_at,
    TRY(from_unixtime(CAST(oi.item_created_at        AS BIGINT) / 1000))  AS item_created_at,
    TRY(from_unixtime(CAST(oi.item_shipped_at        AS BIGINT) / 1000))  AS item_shipped_at,
    TRY(from_unixtime(CAST(oi.item_delivered_at      AS BIGINT) / 1000))  AS item_delivered_at,
    TRY(from_unixtime(CAST(oi.item_returned_at       AS BIGINT) / 1000))  AS item_returned_at,
    TRY(from_unixtime(CAST(oi.item_cancelled_at      AS BIGINT) / 1000))  AS item_cancelled_at,
    -- Metadata
    oi.event_ts_ms

FROM {{ ref('intermediate_order_items') }} oi

{% if is_incremental() %}
WHERE oi.event_ts_ms > (SELECT MAX(event_ts_ms) FROM {{ this }})
{% endif %}
