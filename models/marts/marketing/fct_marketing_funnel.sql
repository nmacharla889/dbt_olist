{{
  config(
    materialized='incremental',
    unique_key='marketing_funnel_key',
    incremental_strategy='merge'
  )
}}


with qualified_leads as (

    select * from  {{ ref('stg_marketing__marketing_qualified_leads') }}
    {{ incremental_filter('lead_original_date', days=3) }}

),

closed_deals as (

    select * from  {{ ref('stg_marketing__closed_deals') }}

),

sellers as (

    select * from  {{ ref('dim_sellers') }}

),

dates as (

    select * from  {{ ref('dim_date') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['ql.mql_id']) }} as marketing_funnel_key,
        s.seller_key,
        d2.date_key as deal_close_date_key,
        d.date_key as lead_original_date_key,
        ql.mql_id,
        ql.lead_source,
        cd.sales_rep_id,
        cd.sales_manager_id,
        cd.business_segment,
        cd.lead_type,
        cd.lead_behaviour_profile,
        cd.has_company,
        cd.has_gtin,
        cd.average_stock,
        cd.business_type,
        cd.declared_product_catalog_size,
        cd.declared_monthly_revenue,
        case when cd.deal_close_date is not null then true else false end as is_deal_closed,
        case when cd.deal_close_date is not null then datediff(cd.deal_close_date, ql.lead_original_date) else null end as days_to_close_deal
    from qualified_leads ql
    left join closed_deals cd on cd.mql_id = ql.mql_id
    left join sellers s on s.seller_id = cd.seller_id
    left join dates d on d.date_day = ql.lead_original_date
    left join dates d2 on d2.date_day = cd.deal_close_date

)

select * from final