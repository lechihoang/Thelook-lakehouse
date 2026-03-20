
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from "delta"."gold"."gold_customer_segments"
    group by gender

)

select *
from all_values
where value_field not in (
    'M','F','Other'
)


