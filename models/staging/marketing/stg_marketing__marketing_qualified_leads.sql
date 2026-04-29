with source as (

    select * from {{ source('marketing', 'olist_marketing_qualified_leads_dataset') }}

),

final as (

    select
        mql_id,
        first_contact_date as lead_original_date,
        landing_page_id as lead_source_id,
        origin as lead_source
    from source

)

select * from final