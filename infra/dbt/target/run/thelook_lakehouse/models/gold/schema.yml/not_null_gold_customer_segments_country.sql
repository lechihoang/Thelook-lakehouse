select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select country
from "delta"."gold"."gold_customer_segments"
where country is null



      
    ) dbt_internal_test