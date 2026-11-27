{{
  config(
    materialized='incremental',
    unique_key='marketing_funnel_key',
    incremental_strategy='merge'
  )
}}

-- Grain: one row per marketing qualified lead (mql_id).
-- Incremental runs pick up (a) new leads and (b) older leads whose deal closed after the last load,
-- so a lead that converts later is updated in place instead of staying "not closed" forever.

with watermark as (
    {% if is_incremental() %}
    select
        max(lead_original_date)  as max_lead_date,
        max(deal_close_date_key) as max_close_key
    from {{ this }}
    {% else %}
    select cast('1900-01-01' as date) as max_lead_date, 0 as max_close_key
    {% endif %}
),

closed_deals as (
    select * from {{ ref('stg_marketing__closed_deals') }}
),

qualified_leads as (
    select * from {{ ref('stg_marketing__marketing_qualified_leads') }}
    {% if is_incremental() %}
    where lead_original_date > (select max_lead_date from watermark)
       or mql_id in (
            select mql_id
            from closed_deals
            where cast(date_format(deal_close_date, 'yyyyMMdd') as int)
                  >= (select coalesce(max_close_key, 0) from watermark)
       )
    {% endif %}
),

sellers as (
    select * from {{ ref('dim_sellers') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ql.mql_id']) }} as marketing_funnel_key,
        s.seller_key,
        d2.date_key as deal_close_date_key,
        d.date_key as lead_original_date_key,
        ql.mql_id,
        ql.lead_source,
        ql.lead_original_date,
        cd.sales_rep_id,
        cd.sales_manager_id,
        -- Deal attributes only exist for closed deals. Closed deals with a blank value get 'unknown';
        -- leads that never closed get 'not closed', so no chart ever shows a (Blank) bar.
        case when cd.mql_id is null then 'not closed' else coalesce(cd.business_segment, 'unknown') end       as business_segment,
        case when cd.mql_id is null then 'not closed' else coalesce(cd.lead_type, 'unknown') end              as lead_type,
        case when cd.mql_id is null then 'not closed' else coalesce(cd.lead_behaviour_profile, 'unknown') end as lead_behaviour_profile,
        cd.has_company,
        cd.has_gtin,
        cd.average_stock,
        case when cd.mql_id is null then 'not closed' else coalesce(cd.business_type, 'unknown') end          as business_type,
        cd.declared_product_catalog_size,
        cd.declared_monthly_revenue,
        case when cd.deal_close_date is not null then true else false end as is_deal_closed,
        case when cd.deal_close_date is not null
            then datediff(cd.deal_close_date, ql.lead_original_date)
            else null end as days_to_close_deal
    from qualified_leads ql
    left join closed_deals cd on cd.mql_id = ql.mql_id
    left join sellers s on s.seller_id = cd.seller_id
    left join dates d on d.date_day = ql.lead_original_date
    left join dates d2 on d2.date_day = cd.deal_close_date
)

select * from final
