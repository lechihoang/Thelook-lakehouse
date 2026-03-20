
  create or replace view
    "delta"."bronze"."bronze_events"
  security definer
  as
    -- Web/app events (append-only, no dedup needed)
SELECT *
FROM "delta"."bronze"."events"
WHERE operation != 'd'
  ;
