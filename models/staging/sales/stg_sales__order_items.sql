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
        cast(round(price, 2) as decimal(10, 2)) as item_price_in_BRL,
        cast(round(freight_value, 2) as decimal(10, 2)) as item_freight_value_in_BRL

    from source

)

select * from renamed