with source as (

    select *  from {{ source('sales', 'olist_geolocation_dataset') }}
    
),

final as (
    select
        geolocation_zip_code_prefix as zip_code,
        geolocation_city,
        geolocation_state
    from source
)

select * from final