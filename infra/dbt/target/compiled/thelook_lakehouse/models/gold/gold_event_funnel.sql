

WITH session_stages AS (
    SELECT
        session_id,
        traffic_source,
        MAX(CASE WHEN event_type = 'home'       THEN 1 ELSE 0 END) AS hit_home,
        MAX(CASE WHEN event_type IN ('department', 'category')
                                                THEN 1 ELSE 0 END) AS hit_browse,
        MAX(CASE WHEN event_type = 'product'    THEN 1 ELSE 0 END) AS hit_product,
        MAX(CASE WHEN event_type = 'cart'       THEN 1 ELSE 0 END) AS hit_cart,
        MAX(CASE WHEN event_type = 'purchase'   THEN 1 ELSE 0 END) AS hit_purchase,
        MAX(CASE WHEN event_type = 'cancel'     THEN 1 ELSE 0 END) AS hit_cancel,
        MAX(CASE WHEN event_type = 'return'     THEN 1 ELSE 0 END) AS hit_return,
        COUNT(event_id)                                             AS total_events
    FROM "delta"."silver"."silver_events"
    GROUP BY 1, 2
)

SELECT
    traffic_source,
    COUNT(DISTINCT session_id)                                                      AS total_sessions,
    SUM(hit_home)                                                                   AS sessions_home,
    SUM(hit_browse)                                                                 AS sessions_browse,
    SUM(hit_product)                                                                AS sessions_product,
    SUM(hit_cart)                                                                   AS sessions_cart,
    SUM(hit_purchase)                                                               AS sessions_purchase,
    SUM(hit_cancel)                                                                 AS sessions_cancel,
    SUM(hit_return)                                                                 AS sessions_return,
    -- Conversion rates between stages
    ROUND(SUM(hit_browse)   * 100.0 / NULLIF(SUM(hit_home), 0), 2)                AS home_to_browse_pct,
    ROUND(SUM(hit_product)  * 100.0 / NULLIF(SUM(hit_browse), 0), 2)              AS browse_to_product_pct,
    ROUND(SUM(hit_cart)     * 100.0 / NULLIF(SUM(hit_product), 0), 2)             AS product_to_cart_pct,
    ROUND(SUM(hit_purchase) * 100.0 / NULLIF(SUM(hit_cart), 0), 2)               AS cart_to_purchase_pct,
    -- Overall session conversion
    ROUND(SUM(hit_purchase) * 100.0 / NULLIF(COUNT(DISTINCT session_id), 0), 2)   AS overall_conversion_pct,
    ROUND(SUM(total_events) * 1.0    / NULLIF(COUNT(DISTINCT session_id), 0), 2)  AS avg_events_per_session
FROM session_stages
GROUP BY 1