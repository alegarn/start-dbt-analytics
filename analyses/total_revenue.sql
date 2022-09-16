with payments as (
    select *
    from
    {{ ref('stg_payments')}}
    where
        status = 'success'
)

select 
    count(*)
from
    payments