with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

 orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

customer_orders as (

    select 

        C.customer_id,
        min(order_placed_at)        as first_order_date,
        max(order_placed_at)        as most_recent_order_date, 
        count(order_id)             as number_of_orders

    from 
         customers C 
    left join 
         orders
    on 
        orders.customer_id = C.customer_id
    group by 1
)

select * from customer_orders