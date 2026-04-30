with source as (

    select * from {{ source('sales', 'olist_order_payments_dataset') }}

),

renamed as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        cast(round(payment_value, 2) as decimal(10, 2)) as payment_value_in_BRL

    from source

)

select * from renamed