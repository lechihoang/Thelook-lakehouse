
  
    

    create table "delta"."gold"."fct_events"
      
      
    as (
      

-- Grain: 1 row per event
SELECT
    -- Keys
    e.event_id,
    e.user_id,
    e.session_id,
    CAST(date_format(date_trunc('day', from_unixtime(e.event_ts_ms / 1000)), '%Y%m%d') AS INTEGER) AS date_key,
    -- Attributes
    e.sequence_number,
    e.event_type,
    e.uri,
    e.traffic_source,
    e.browser,
    e.ip_address,
    -- Location
    e.city,
    e.state,
    e.postal_code,
    -- Flags
    e.is_ghost,
    -- Timestamps
    date(from_unixtime(e.event_ts_ms / 1000))        AS event_date,
    from_unixtime(e.event_ts_ms / 1000)              AS event_time,
    e.event_ts_ms

FROM "delta"."silver"."silver_events" e


    );

  