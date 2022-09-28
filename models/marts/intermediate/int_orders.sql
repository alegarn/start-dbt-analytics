with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

 orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

paid_orders as (

    select 
        orders.*,
        P.total_amount_paid,
        P.payment_finalized_date,
        C.customer_first_name,
        C.customer_last_name
    FROM orders
    
    left join payments as P
        ON orders.order_id = P.order_id
    left join customers as C 
        on orders.customer_id = C.customer_id
    
),

total_amount_paid_per_cust as (

    select
        p.order_id,
        sum(p_o.total_amount_paid) as cust_lifetime_val

    from 
        paid_orders as p

    left join paid_orders as p_o 
        on p.customer_id = p_o.customer_id 
        and p.order_id >= p_o.order_id

    group by 1
    order by p.order_id

),

final as (
    select
        paid_orders.*,
        total_amount_paid_per_cust.cust_lifetime_val
    from
        paid_orders
    left join total_amount_paid_per_cust
    on paid_orders.order_id = total_amount_paid_per_cust.order_id
)

select * from final
