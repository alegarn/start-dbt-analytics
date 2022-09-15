with payments as (
    select *
    from {{ ref('stg_payments')}}
    where
        STATUS = 'success' AND
        amount > 0
),

status_success as (
    select 
        --payment_id,
        order_id,
        count(distinct order_id),
        count(order_id)
    from
        payments
    group by
        order_id
)

select 
    *
    --CASE WHEN count(distinct order_id)= count(order_id) then 'order_id uniq' ELSE 'column values are NOT unique' end as order_uniq
from
    status_success
