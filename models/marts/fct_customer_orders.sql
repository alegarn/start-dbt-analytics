-- with statement

with paid_orders as (

    select * from {{ ref('int_orders') }}
   
),
-- int p..d_ord.rs
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
-- int
customer_orders as (

    select * from {{ ref('int_customer_orders') }}
    
),

-- F.n.l CTE
final as (

    select

        p_o.*,
-- ch.que ord.r m..s n.n f..l
        ROW_NUMBER() OVER (
            ORDER BY p_o.order_id
            ) as transaction_seq,

-- c.st: Xe f..s c.mm.nde
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY p_o.order_id
            ) as customer_sales_seq,

-- t.ble is_n.w?
        CASE 
            WHEN c.first_order_date = p_o.order_placed_at
            THEN 'new'
            ELSE 'return' END 
            as new_order?,

        cus_tot_paid.cust_lifetime_val as customer_lifetime_value,
        c.first_order_date as cst_frst_ord_dt

    FROM 
        paid_orders as p_o

    left join
        customer_orders as c USING (customer_id)

    LEFT OUTER JOIN total_amount_paid_per_cust as cus_tot_paid
        on cus_tot_paid.order_id = p_o.order_id
        
    ORDER BY order_id

)

select * from final
