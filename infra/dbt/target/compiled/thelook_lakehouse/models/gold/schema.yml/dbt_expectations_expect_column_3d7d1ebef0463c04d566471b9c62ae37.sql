






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and return_rate_pct >= 0 and return_rate_pct <= 100
)
 as expression


    from "delta"."gold"."gold_product_performance"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







