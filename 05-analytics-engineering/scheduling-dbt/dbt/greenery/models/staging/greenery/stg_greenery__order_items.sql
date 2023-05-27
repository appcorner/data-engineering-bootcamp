with source as (

    select * from {{ source('greenery', 'order-items') }}

),

renamed_recasted as (

    select
        `order` as order_guid
        , quantity
        , product as product_guid

    from source

)

select * from renamed_recasted