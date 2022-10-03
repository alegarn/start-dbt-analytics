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
        paid_orders.order_id,
        sum(paid_orders.total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
        ) as customer_lifetime_value

    from 
        paid_orders

),

final as (
    select
        paid_orders.*,
        total_amount_paid_per_cust.customer_lifetime_value
    from
        paid_orders
    left join total_amount_paid_per_cust
    on paid_orders.order_id = total_amount_paid_per_cust.order_id
)

select * from final


