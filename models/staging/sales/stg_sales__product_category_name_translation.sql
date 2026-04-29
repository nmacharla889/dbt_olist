with source as (

    select * from {{ source('sales', 'product_category_name_translation') }}

),

renamed as (

    select
        product_category_name,
        product_category_name_english

    from source

)

select * from renamed