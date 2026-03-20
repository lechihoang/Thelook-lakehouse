
  create or replace view
    "delta"."bronze"."bronze_users"
  security definer
  as
    -- Latest state of each user (deduplicate CDC updates, e.g. address changes)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."bronze"."users"
    WHERE operation != 'd'
)
WHERE rn = 1
  ;
