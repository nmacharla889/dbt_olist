with customers as (

    select * from {{ ref('stg_sales__customers') }}

),

orders as (

    select * from {{ ref('stg_sales__orders') }}

),

geolocation as (

    select
        zip_code,
        geolocation_city,
        geolocation_state,
        row_number() over (partition by zip_code order by zip_code) as rn
    from {{ ref('stg_sales__geolocation') }}
    qualify rn = 1

),

latest_order as (

    select
        c.customer_unique_id,
        c.zip_code,
        g.geolocation_city                          as customer_city,
        g.geolocation_state                         as customer_state,
        row_number() over (
            partition by c.customer_unique_id
            order by o.order_purchase_date desc
        )                                           as row_num
    from customers c
    left join orders o
        on c.customer_id = o.customer_id
    left join geolocation g
        on c.zip_code = g.zip_code

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }}  as customer_key,
        customer_unique_id,
        zip_code,
        customer_city,
        customer_state
    from latest_order
    where row_num = 1

)

select * from final