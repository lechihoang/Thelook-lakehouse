
    
    

with all_values as (

    select
        channel as value_field,
        count(*) as n_records

    from "delta"."silver"."silver_store_sales"
    group by channel

)

select *
from all_values
where value_field not in (
    'store'
)


