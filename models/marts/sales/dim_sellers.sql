with sellers as (

    select * from {{ ref('stg_sales__sellers') }}

),

geolocation as (

    select
        zip_code,
        geolocation_city as seller_city,
        geolocation_state as seller_state,
        row_number() over (partition by zip_code order by zip_code) as rn
    from {{ ref('stg_sales__geolocation') }}
    qualify rn = 1

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        seller_id,
        g.zip_code,
        g.seller_city,
        g.seller_state
    from sellers s
    left join geolocation g on s.zip_code = g.zip_code

)

select * from final