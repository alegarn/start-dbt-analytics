with payments as (

    select * from {{ ref('stg_payments')}}

), 

final as (
    select
        order_id,
        {{ dbt_utils.pivot(
            'PAYMENT_METHOD',
            dbt_utils.get_column_values(ref('stg_payments'), 'PAYMENT_METHOD')
        ) }}
    from payments
    group by order_id

)

select * from final

