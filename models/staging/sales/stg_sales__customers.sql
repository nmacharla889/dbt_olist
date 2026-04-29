with source as (

    select * from {{ source('sales', 'olist_customers_dataset') }}

),

renamed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix as zip_code,
        customer_city,
        customer_state

    from source

)

select * from renamed