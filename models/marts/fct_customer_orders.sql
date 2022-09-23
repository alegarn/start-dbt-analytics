-- with statement

with customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

 orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),


-- import CTEs

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),


-- logical CTEs
paid_orders as (

    select 
        orders.ID          as order_id,
        orders.USER_ID	    as customer_id,
        orders.ORDER_DATE  AS order_placed_at,
        orders.STATUS      AS order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        C.FIRST_NAME            as customer_first_name,
        C.LAST_NAME             as customer_last_name

    FROM orders
    
    left join payments
        ON orders.ID = payments.order_id

    left join customers C 
        on orders.USER_ID = C.ID 
    
),

total_amount_paid_per_cust as (

    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from 
        paid_orders p
    left join 
        paid_orders t2 
        on p.customer_id = t2.customer_id 
        and p.order_id >= t2.order_id
    group by 1
    order by p.order_id

),

customer_orders as (

    select 

        C.ID as customer_id
        , min(ORDER_DATE) as first_order_date
        , max(ORDER_DATE) as most_recent_order_date
        , count(orders.ID) AS number_of_orders

    from 
         customers C 
    left join 
         orders
    on 
        orders.USER_ID = C.ID 
    group by 1
),

final as (

    select

        p.*,

        ROW_NUMBER() OVER (
            ORDER BY p.order_id
            ) as transaction_seq,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY p.order_id
            ) as customer_sales_seq,

        CASE 
            WHEN c.first_order_date = p.order_placed_at
            THEN 'new'
            ELSE 'return' END 
            as nvsr,

        total_amount_paid_per_cust.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos

    FROM 
        paid_orders p

    left join
        customer_orders as c USING (customer_id)

    LEFT OUTER JOIN total_amount_paid_per_cust
        on total_amount_paid_per_cust.order_id = p.order_id
        
    ORDER BY order_id

)

select * from final
