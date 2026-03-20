
    
    

with all_values as (

    select
        customer_gender as value_field,
        count(*) as n_records

    from "delta"."gold"."gold_customer_segments"
    group by customer_gender

)

select *
from all_values
where value_field not in (
    'M','F','Other'
)


