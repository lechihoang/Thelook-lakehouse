
  
    

    create table "delta"."gold"."gold_customer_segments"
      
      
    as (
      

-- Customer segments by demographics and purchase behavior
SELECT
    customer_country,
    customer_gender,
    CASE
        WHEN customer_age < 25 THEN '18-24'
        WHEN customer_age < 35 THEN '25-34'
        WHEN customer_age < 45 THEN '35-44'
        WHEN customer_age < 55 THEN '45-54'
        ELSE '55+'
    END                                             AS age_group,
    traffic_source,
    COUNT(DISTINCT user_id)                         AS customer_count,
    COUNT(DISTINCT order_id)                        AS total_orders,
    ROUND(SUM(revenue), 2)                          AS total_revenue,
    ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS revenue_per_customer,
    ROUND(AVG(sale_price), 2)                       AS avg_order_value
FROM "delta"."silver"."silver_order_items"
WHERE item_status NOT IN ('Cancelled')
GROUP BY 1, 2, 3, 4
    );

  