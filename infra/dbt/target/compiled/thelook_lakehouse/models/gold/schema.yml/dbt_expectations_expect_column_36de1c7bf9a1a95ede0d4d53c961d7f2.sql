






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and total_revenue >= 0 and total_revenue <= 100000000
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







