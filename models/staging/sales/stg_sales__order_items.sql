with source as (

    select * from {{ source('sales', 'olist_order_items_dataset') }}

),

renamed as (

    select
        order_id,
        order_item_id as item_id,
        product_id,
        seller_id,
        date(convert_timezone('America/Sao_Paulo', 'UTC', shipping_limit_date)) as shipping_limit_date,
        price as item_price_in_BRL,
        freight_value as item_freight_value_in_BRL

    from source

)

select * from renamed