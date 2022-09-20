--Create a stg_payments.sql model in models/staging/stripe

with payments as (
    select
        ID as payment_id,
        ORDERID as order_id,
        PAYMENTMETHOD as payment_method,
        STATUS,
        -- amount is stored in cents, convert it to dollars
        {{ cents_to_dollars('amount') }} as amount,
        
        CREATED as created_at

    from {{ source('stripe', 'payment')}}
)

select * from payments