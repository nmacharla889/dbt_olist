with sellers as (

    select * from {{ ref('stg_sales__sellers') }}

),

geolocation as (

    select * from {{ ref('stg_sales__geolocation') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        seller_id,
        g.zip_code,
        g.geolocation_city as seller_city,
        g.geolocation_state as seller_state
    from sellers s
    left join geolocation g on s.zip_code = g.zip_code

)

select * from final