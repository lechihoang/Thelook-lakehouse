{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per order
SELECT
    -- Keys
    o.order_id,
    o.user_id,
    CAST(date_format(date_trunc('day', from_unixtime(o.event_ts_ms / 1000)), '%Y%m%d') AS INTEGER) AS date_key,
    -- Attributes
    o.order_status,
    o.num_of_items,
    o.traffic_source,
    -- Timing
    o.created_at            AS order_created_at,
    o.shipped_at            AS order_shipped_at,
    o.delivered_at          AS order_delivered_at,
    o.returned_at           AS order_returned_at,
    o.cancelled_at          AS order_cancelled_at,
    -- Metadata
    o.event_ts_ms

FROM {{ ref('silver_orders') }} o

{% if is_incremental() %}
WHERE o.event_ts_ms > (SELECT MAX(event_ts_ms) FROM {{ this }})
{% endif %}
