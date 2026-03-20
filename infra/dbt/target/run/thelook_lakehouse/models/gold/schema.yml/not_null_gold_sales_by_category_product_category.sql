select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select product_category
from "delta"."gold"."gold_sales_by_category"
where product_category is null



      
    ) dbt_internal_test