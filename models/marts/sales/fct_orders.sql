-- Grain: one row per order (order_id). Covers every order, including the ~775 orders that have no items
-- (most of them canceled or unavailable), which fct_order_items cannot see.
-- Use this table for order-level questions: order counts by status, cancellation rate, delivery
-- (on time %, delay) and review score. Use fct_order_items for revenue, products and sellers.

with orders as (

    select * from {{ ref('stg_sales__orders') }}

),

items as (

    select
        order_id,
        count(*)                                                as item_count,
        count(distinct seller_id)                               as seller_count,
        cast(sum(item_price_in_BRL) as decimal(12, 2))          as order_items_value_in_brl,
        cast(sum(item_freight_value_in_BRL) as decimal(12, 2))  as order_freight_value_in_brl
    from {{ ref('stg_sales__order_items') }}
    group by order_id

),

payments as (

    select
        order_id,
        cast(sum(payment_value_in_BRL) as decimal(12, 2))       as order_payment_value_in_brl
    from {{ ref('stg_sales__order_payments') }}
    group by order_id

),

reviews as (

    select
        order_id,
        avg(review_score)                                       as review_score
    from {{ ref('stg_sales__order_reviews') }}
    group by order_id

),

stg_customers as (

    select customer_id, customer_unique_id
    from {{ ref('stg_sales__customers') }}

),

dim_customers as (

    select customer_key, customer_unique_id
    from {{ ref('dim_customers') }}

),

dates as (

    select date_key, date_day
    from {{ ref('dim_date') }}

),

joined as (

    select
        o.order_id,
        dc.customer_key,
        d.date_key                                              as order_purchase_date_key,
        o.order_status,
        o.order_purchase_date,
        o.order_approved_date,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date) as delivery_delay_days,
        coalesce(i.item_count, 0)                               as item_count,
        coalesce(i.seller_count, 0)                             as seller_count,
        coalesce(i.order_items_value_in_brl, 0)                 as order_items_value_in_brl,
        coalesce(i.order_freight_value_in_brl, 0)               as order_freight_value_in_brl,
        coalesce(p.order_payment_value_in_brl, 0)               as order_payment_value_in_brl,
        r.review_score
    from orders o
    left join items i          on i.order_id = o.order_id
    left join payments p       on p.order_id = o.order_id
    left join reviews r        on r.order_id = o.order_id
    left join stg_customers sc on sc.customer_id = o.customer_id
    left join dim_customers dc on dc.customer_unique_id = sc.customer_unique_id
    left join dates d          on d.date_day = o.order_purchase_date

),

final as (

    select
        *,
        order_delivered_customer_date is not null               as is_delivered,
        case
            when order_delivered_customer_date is null then null
            when delivery_delay_days <= 0 then true
            else false
        end                                                     as is_on_time,
        case
            when order_delivered_customer_date is null then 'Not delivered'
            when delivery_delay_days <= 0 then 'On time'
            else 'Late'
        end                                                     as delivery_status,
        case
            when order_delivered_customer_date is null then 'Not delivered'
            when delivery_delay_days <= -15 then '15+ days early'
            when delivery_delay_days <= -8  then '8-14 days early'
            when delivery_delay_days <= -1  then '1-7 days early'
            when delivery_delay_days = 0    then 'On the day'
            when delivery_delay_days <= 3   then '1-3 days late'
            when delivery_delay_days <= 7   then '4-7 days late'
            else '8+ days late'
        end                                                     as delivery_delay_bucket,
        case
            when order_delivered_customer_date is null then 8
            when delivery_delay_days <= -15 then 1
            when delivery_delay_days <= -8  then 2
            when delivery_delay_days <= -1  then 3
            when delivery_delay_days = 0    then 4
            when delivery_delay_days <= 3   then 5
            when delivery_delay_days <= 7   then 6
            else 7
        end                                                     as delivery_delay_bucket_sort,
        order_status in ('canceled', 'unavailable')             as is_canceled_or_unavailable,
        review_score is not null                                as has_review
    from joined

)

select * from final
