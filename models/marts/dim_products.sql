with products as (

    select * from {{ ref('stg_products') }}

),

product_categrory_name_translation as (

    select * from {{ ref('stg_product_category_name_translation') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        p.product_id,
        pcnt.product_category_name,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight, 
        p.product_length,
        p.product_height,
        p.product_width
    from products p
    join product_categrory_name_translation pcnt on p.product_category_name = pcnt.product_category_name

)

select * from final