






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and margin_pct >= 0 and margin_pct <= 100
)
 as expression


    from "delta"."gold"."gold_sales_by_category"
    

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







