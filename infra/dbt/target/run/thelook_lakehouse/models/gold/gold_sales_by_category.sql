
  
    

    create table "delta"."gold"."gold_sales_by_category"
      
      
    as (
      

SELECT
    product_category,
    product_department,
    product_brand,
    COUNT(DISTINCT order_id)                        AS total_orders,
    COUNT(order_item_id)                            AS total_items_sold,
    ROUND(SUM(revenue), 2)                          AS total_revenue,
    ROUND(SUM(gross_margin), 2)                     AS total_gross_margin,
    ROUND(AVG(sale_price), 2)                       AS avg_sale_price,
    ROUND(SUM(gross_margin) / NULLIF(SUM(revenue), 0) * 100, 2) AS margin_pct,
    COUNT(DISTINCT user_id)                         AS unique_customers
FROM "delta"."silver"."silver_order_items"
WHERE item_status NOT IN ('Cancelled', 'Returned')
GROUP BY 1, 2, 3
    );

  