-- with statement

with paid_orders as (

    select * from {{ ref('int_orders') }}
   
),

-- int
customer_orders as (

    select * from {{ ref('int_customer_orders') }}
    
),

-- Final CTE
final as (

    select

        paid_orders.*,

        ROW_NUMBER() OVER (ORDER BY paid_orders.order_id) as transaction_seq,

        ROW_NUMBER() OVER (
            PARTITION BY paid_orders.customer_id 
            ORDER BY paid_orders.order_id
            ) as customer_sales_seq,

    -- new vs old returning customers
        CASE 
            WHEN customer_orders.first_order_date = paid_orders.order_placed_at
            THEN 'new'
            ELSE 'return' END 
            as nvsr,

    -- first day of sale
        customer_orders.first_order_date as fdos


    FROM 
        paid_orders

    left join
        customer_orders on paid_orders.customer_id = customer_orders.customer_id
        
    ORDER BY order_id

)

select * from final
