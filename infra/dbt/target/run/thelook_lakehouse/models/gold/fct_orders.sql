
  
    

    create table "delta"."gold"."fct_orders"
      
      
    as (
      

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
    -- Dates and timestamps (converted from epoch ms)
    TRY(date(from_unixtime(CAST(o.created_at   AS BIGINT) / 1000))) AS order_date,
    TRY(from_unixtime(CAST(o.created_at        AS BIGINT) / 1000))  AS order_created_at,
    TRY(from_unixtime(CAST(o.shipped_at        AS BIGINT) / 1000))  AS shipped_at,
    TRY(from_unixtime(CAST(o.delivered_at      AS BIGINT) / 1000))  AS delivered_at,
    TRY(from_unixtime(CAST(o.returned_at       AS BIGINT) / 1000))  AS returned_at,
    TRY(from_unixtime(CAST(o.cancelled_at      AS BIGINT) / 1000))  AS cancelled_at,
    -- Metadata
    o.event_ts_ms

FROM "delta"."silver"."silver_orders" o


    );

  