-- Web/app events (append-only, no dedup needed)
SELECT *
FROM "delta"."bronze"."events"
WHERE operation != 'd'