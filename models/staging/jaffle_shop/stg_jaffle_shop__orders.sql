WITH 

    source as (

        select * from {{ source('jaffle_shop', 'orders')}}
    )

select * from source