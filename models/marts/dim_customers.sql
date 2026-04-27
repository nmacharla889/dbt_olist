with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

latest_order as (

    select 
        c.customer_unique_id,
        c.zip_code,
        c.customer_city,
        c.customer_state,
        row_number() over (partition by c.customer_unique_id order by o.order_purchase_date desc) as row_num
    from customers c
    left join orders o
    on c.customer_id = o.customer_id

),

final as (

    select 
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_key,
        customer_unique_id,
        zip_code,
        customer_city,
        customer_state
    from latest_order
    where row_num = 1

)
select * from final