
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from "delta"."gold"."gold_order_funnel"
    group by order_status

)

select *
from all_values
where value_field not in (
    'Processing','Shipped','Delivered','Cancelled','Returned'
)


