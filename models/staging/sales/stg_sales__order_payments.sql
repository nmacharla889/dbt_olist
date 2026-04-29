with source as (

    select * from {{ source('sales', 'olist_order_payments_dataset') }}

),

renamed as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value as payment_value_in_BRL

    from source

)

select * from renamed