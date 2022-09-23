with 

    source as (

        select * from {{ source('jaffle_shop', 'customers') }}
    
    )

select * from source

