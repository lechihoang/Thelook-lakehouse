select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select browser
from "delta"."silver"."silver_events"
where browser is null



      
    ) dbt_internal_test