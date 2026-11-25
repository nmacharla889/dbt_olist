with products as (

    select * from {{ ref('stg_sales__products') }}

),

product_category_name_translation as (

    select * from {{ ref('stg_sales__product_category_name_translation') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_key,
        p.product_id,
        coalesce(p.product_category_name, 'unknown') as product_category_name,
        coalesce(pcnt.product_category_name_english, p.product_category_name, 'unknown') as product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight,
        p.product_length,
        p.product_height,
        p.product_width
    from products p
    left join product_category_name_translation pcnt
        on p.product_category_name = pcnt.product_category_name

)

select * from final
