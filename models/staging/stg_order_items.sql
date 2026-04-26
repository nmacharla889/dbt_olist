with source as (

    select * from {{ source('olist_raw', 'olist_order_items_dataset') }}

),

renamed as (

    select
        order_id,
        order_item_id as item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value

    from source

)

select * from renamed