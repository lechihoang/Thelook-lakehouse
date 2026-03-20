

-- Grain: 1 row per session
-- Safety window: reprocess sessions with events in last 2h to handle sessions spanning batch boundaries



SELECT
    session_id,
    MAX(user_id)                                                        AS user_id,
    CAST(date_format(date_trunc('day', from_unixtime(MIN(event_ts_ms) / 1000)), '%Y%m%d') AS INTEGER) AS date_key,
    MAX(traffic_source)                                                 AS traffic_source,
    MAX(browser)                                                        AS browser,
    MAX(city)                                                           AS city,
    MAX(state)                                                          AS state,
    MAX(customer_country)                                               AS customer_country,
    bool_or(is_ghost)                                                   AS is_ghost,
    -- Funnel stages
    MAX(CASE WHEN event_type = 'home'                      THEN 1 ELSE 0 END) AS hit_home,
    MAX(CASE WHEN event_type IN ('department', 'category') THEN 1 ELSE 0 END) AS hit_browse,
    MAX(CASE WHEN event_type = 'product'                   THEN 1 ELSE 0 END) AS hit_product,
    MAX(CASE WHEN event_type = 'cart'                      THEN 1 ELSE 0 END) AS hit_cart,
    MAX(CASE WHEN event_type = 'purchase'                  THEN 1 ELSE 0 END) AS hit_purchase,
    MAX(CASE WHEN event_type = 'cancel'                    THEN 1 ELSE 0 END) AS hit_cancel,
    MAX(CASE WHEN event_type = 'return'                    THEN 1 ELSE 0 END) AS hit_return,
    -- Date
    date(from_unixtime(MIN(event_ts_ms) / 1000))                       AS session_date,
    -- Measures
    COUNT(event_id)                                                     AS total_events,
    MIN(from_unixtime(event_ts_ms / 1000))                             AS session_start_at,
    MAX(from_unixtime(event_ts_ms / 1000))                             AS session_end_at,
    date_diff('second',
        MIN(from_unixtime(event_ts_ms / 1000)),
        MAX(from_unixtime(event_ts_ms / 1000)))                        AS session_duration_seconds,
    -- Watermark for incremental runs
    MAX(event_ts_ms)                                                    AS last_event_ts_ms

FROM "delta"."silver"."silver_events"



GROUP BY 1