with source as (

    select * from {{ source('sales', 'olist_orders_dataset') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,
        to_date(order_purchase_timestamp) as order_purchase_date,
        to_date(order_approved_at) as order_approved_date,
        to_date(order_delivered_carrier_date) as order_delivered_carrier_date,
        to_date(order_delivered_customer_date) as order_delivered_customer_date,
        to_date(order_estimated_delivery_date) as order_estimated_delivery_date

    from source

)

select * from renamed