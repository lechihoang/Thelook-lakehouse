






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and customer_count >= 0 and customer_count <= 1000000
)
 as expression


    from "delta"."gold"."gold_customer_segments"
    

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







