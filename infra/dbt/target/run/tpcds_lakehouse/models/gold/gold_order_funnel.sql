
  
    

    create table "delta"."gold"."gold_order_funnel"
      
      
    as (
      

-- Order status funnel: how many orders reach each stage
SELECT
    order_status,
    COUNT(order_id)                         AS order_count,
    ROUND(AVG(num_of_items), 2)             AS avg_items_per_order,
    ROUND(AVG(CASE WHEN shipped_at IS NOT NULL
        THEN date_diff('hour', from_unixtime(CAST(created_at AS BIGINT)/1000),
                               from_unixtime(CAST(shipped_at AS BIGINT)/1000))
    END), 2)                                AS avg_hours_to_ship,
    ROUND(AVG(CASE WHEN delivered_at IS NOT NULL AND shipped_at IS NOT NULL
        THEN date_diff('hour', from_unixtime(CAST(shipped_at AS BIGINT)/1000),
                               from_unixtime(CAST(delivered_at AS BIGINT)/1000))
    END), 2)                                AS avg_hours_to_deliver,
    COUNT(DISTINCT user_id)                 AS unique_customers,
    -- Traffic source breakdown
    COUNT(CASE WHEN traffic_source = 'Organic' THEN 1 END)  AS organic,
    COUNT(CASE WHEN traffic_source = 'Search'  THEN 1 END)  AS search,
    COUNT(CASE WHEN traffic_source = 'Facebook' THEN 1 END) AS facebook,
    COUNT(CASE WHEN traffic_source = 'Email'   THEN 1 END)  AS email,
    COUNT(CASE WHEN traffic_source = 'Display' THEN 1 END)  AS display
FROM "delta"."silver"."silver_orders"
GROUP BY 1
    );

  