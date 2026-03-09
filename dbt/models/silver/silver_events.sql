{{ config(materialized='table') }}

SELECT
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
    u.traffic_source                    AS user_traffic_source,
    CASE WHEN e.user_id IS NULL THEN TRUE ELSE FALSE END AS is_ghost,
    -- Metadata
    e.event_ts_ms

FROM {{ ref('bronze_events') }} e
LEFT JOIN postgresql.public.users u ON e.user_id = u.id
