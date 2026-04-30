select *
from {{ ref('fct_marketing_funnel') }} f
left join {{ ref('dim_sellers') }} d using (seller_key)
where d.seller_key is null and f.seller_key is not null