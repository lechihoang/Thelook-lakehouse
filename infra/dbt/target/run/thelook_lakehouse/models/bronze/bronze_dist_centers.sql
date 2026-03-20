
  create or replace view
    "delta"."bronze"."bronze_dist_centers"
  security definer
  as
    -- Distribution centers are static reference data (no updates expected)
SELECT *
FROM "delta"."bronze"."dist_centers"
WHERE operation != 'd'
  ;
