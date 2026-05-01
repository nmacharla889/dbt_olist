{{config(
    materialized='incremental',
    unique_key='order_item_key',
    incremental_strategy='merge'
)}}


with orders_enriched as (

    select * from {{ ref('int_orders_enriched') }}
    {{incremental_filter ('order_purchase_date')}}

),

stg_customers as (
   select
   customer_id, 
    customer_unique_id
    from {{ ref('stg_sales__customers') }}
),


sellers as (
    select
    seller_key,
    seller_id 
     from {{ ref('dim_sellers') }}
),

products as (
    select 
    product_key,
    product_id
    from {{ ref('dim_products') }}
),

dim_customers as (
    select 
    customer_key,
    customer_unique_id
    from  {{ ref('dim_customers') }}
),

dates as (
    select 
    date_key,
    date_day
    from {{ ref('dim_date') }}
),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['oe.order_id', 'oe.item_id']) }} as order_item_key,
        p.product_key,
        s.seller_key,
        dc.customer_key,
        d.date_key as order_purchase_date_key,
        {{ dbt_utils.star(from=ref('int_orders_enriched'), except=['product_id', 'seller_id', 'customer_id']) }}
    from orders_enriched oe
    left join stg_customers sc on sc.customer_id = oe.customer_id
    left join sellers s on s.seller_id = oe.seller_id
    left join products p on p.product_id = oe.product_id
    left join dim_customers dc on dc.customer_unique_id = sc.customer_unique_id
    left join dates d on d.date_day = oe.order_purchase_date

)
select * from final
