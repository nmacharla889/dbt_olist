-- One row per seller. City and state come from the seller record itself (not the geolocation
-- table, which is missing some zip codes and turned those sellers into 'unknown').

with sellers as (

    select * from {{ ref('stg_sales__sellers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        seller_id,
        zip_code,
        coalesce(seller_city, 'unknown')         as seller_city,
        coalesce(upper(seller_state), 'unknown') as seller_state
    from sellers

)

select * from final
