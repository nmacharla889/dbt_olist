select * 
from  {{ref('int_orders_enriched')}}
where avg_review_score not between 1 and 5