-- One row per real customer (customer_unique_id).
-- City and state come from the customer record itself. They used to come from the geolocation table,
-- which is missing some zip codes, so those customers showed up as state 'unknown' on the dashboard.
-- A customer who moved keeps the address from their latest order.

with customers as (

    select * from {{ ref('stg_sales__customers') }}

),

orders as (

    select * from {{ ref('stg_sales__orders') }}

),

latest_order as (

    select
        c.customer_unique_id,
        c.zip_code,
        c.customer_city,
        c.customer_state,
        row_number() over (
            partition by c.customer_unique_id
            order by o.order_purchase_date desc, c.customer_id
        )                                           as row_num
    from customers c
    left join orders o
        on c.customer_id = o.customer_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }}  as customer_key,
        customer_unique_id,
        zip_code,
        coalesce(customer_city, 'unknown')  as customer_city,
        coalesce(upper(customer_state), 'unknown') as customer_state
    from latest_order
    where row_num = 1

)

select * from final
