with source as (

    select * from {{ source('marketing', 'olist_closed_deals_dataset') }}

),

renamed as (

    select
        mql_id,
        seller_id,
        sdr_id as sales_rep_id,
        sr_id as sales_manager_id,
        date(convert_timezone('America/Sao_Paulo', 'UTC', won_date)) as deal_close_date,
        business_segment,
        lead_type,
        lead_behaviour_profile,
        has_company,
        has_gtin,
        average_stock,
        business_type,
        declared_product_catalog_size,
        cast(round(declared_monthly_revenue, 2) as decimal(10, 2)) as declared_monthly_revenue
    from source

)

select * from renamed