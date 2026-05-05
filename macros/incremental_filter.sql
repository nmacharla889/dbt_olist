with max_date as (
    {% if is_incremental() %}
    select max(lead_original_date) as max_val from {{ this }}
    {% else %}
    select cast('1900-01-01' as date) as max_val
    {% endif %}
),

qualified_leads as (
    select * from {{ ref('stg_marketing__marketing_qualified_leads') }}
    {% if is_incremental() %}
    where lead_original_date > (select max_val from max_date)
    {% endif %}
),