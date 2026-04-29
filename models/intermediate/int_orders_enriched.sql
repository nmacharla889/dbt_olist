with orders as (

    select * from {{ref('stg_sales__orders')}}

),

order_items as (

    select * from {{ref('stg_sales__order_items')}}

),

payments as (

    select
        order_id,
        sum(payment_value_in_BRL) as total_payment_value_in_BRL,
        max(payment_installments)   as max_payment_installments,
        count(distinct payment_type) as payment_method_count,
        max(case when payment_type = 'credit_card' then 1 else 0 end)  as paid_by_credit_card,
        max(case when payment_type = 'boleto'      then 1 else 0 end)  as paid_by_boleto,
        max(case when payment_type = 'voucher'     then 1 else 0 end)  as paid_by_voucher,
        max(case when payment_type = 'debit_card'  then 1 else 0 end)  as paid_by_debit_card
    from {{ ref('stg_sales__order_payments') }}
    group by order_id

),

reviews as (

    select
        order_id,
        avg(review_score) as avg_review_score
    from {{ ref('stg_sales__order_reviews') }}
    group by order_id

),

final as (

    select
        oi.order_id,
        oi.item_id,
        o.customer_id,
        oi.product_id,
        oi.seller_id,
        o.order_status,
        o.order_purchase_date,
        o.order_approved_date,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date) as delivery_delay_days,
        oi.item_price_in_BRL,
        oi.item_freight_value_in_BRL,
        oi.item_price_in_BRL + oi.item_freight_value_in_BRL as total_item_value_in_BRL,
        p.total_payment_value_in_BRL,
        p.max_payment_installments,
        p.payment_method_count,
        p.paid_by_credit_card,
        p.paid_by_boleto,
        p.paid_by_voucher,
        p.paid_by_debit_card,
        r.avg_review_score
    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join payments p
        on oi.order_id = p.order_id
    left join reviews r
        on oi.order_id = r.order_id

)

select * from final