






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and total_sessions >= 0 and total_sessions <= 10000000
)
 as expression


    from "delta"."gold"."gold_event_funnel"
    

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







