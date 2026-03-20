

with __dbt__cte__bronze_events as (
-- Web/app events (append-only, no dedup needed)
SELECT *
FROM "delta"."bronze"."events"
WHERE operation != 'd'
),  __dbt__cte__bronze_users as (
-- Latest state of each user (deduplicate CDC updates, e.g. address changes)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."users"
    WHERE operation != 'd'
)
WHERE rn = 1
) SELECT
    e.id                                AS event_id,
    e.session_id,
    e.sequence_number,
    e.event_type,
    e.uri,
    e.created_at                        AS event_time,
    -- Location
    e.city,
    e.state,
    e.postal_code,
    -- Tech
    e.browser,
    e.traffic_source,
    e.ip_address,
    -- User (nullable — ghost events have no user)
    e.user_id,
    u.first_name || ' ' || u.last_name  AS customer_name,
    u.gender                            AS customer_gender,
    u.age                               AS customer_age,
    u.country                           AS customer_country,
    u.latitude                          AS customer_lat,
    u.longitude                         AS customer_lon,
    CAST(u.created_at AS varchar)       AS user_registered_at,
    u.traffic_source                    AS user_traffic_source,
    CASE WHEN e.user_id IS NULL THEN TRUE ELSE FALSE END AS is_ghost,
    -- Metadata
    e.event_ts_ms

FROM __dbt__cte__bronze_events e
LEFT JOIN __dbt__cte__bronze_users u ON e.user_id = u.id
