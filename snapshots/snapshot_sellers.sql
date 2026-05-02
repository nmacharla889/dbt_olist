{% snapshot snapshot_sellers %}
    {{
        config(
            target_schema='olist_dev_raw',
            strategy= 'check',
            unique_key= 'seller_id',
            check_cols= ['seller_zip_code_prefix', 'seller_city', 'seller_state']
        )
    }}

    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM {{ source('sales', 'olist_sellers_dataset') }}

{% endsnapshot %}
