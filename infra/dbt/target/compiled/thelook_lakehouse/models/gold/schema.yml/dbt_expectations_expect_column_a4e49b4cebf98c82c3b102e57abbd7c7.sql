






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and cancel_rate >= 0 and cancel_rate <= 100
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







