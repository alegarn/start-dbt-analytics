with payments as (
    select *
    from {{ ref('stg_payments')}}
    where
        STATUS = 'success'
)

select 
    order_id,
    CASE WHEN count(distinct order_id)= count(order_id) then 'order_id uniq' ELSE 'column values are NOT unique' end as order_uniq
from
    payments
group by
    order_id