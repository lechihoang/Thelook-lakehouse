






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and total_customers >= 0 and total_customers <= 1000000
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







