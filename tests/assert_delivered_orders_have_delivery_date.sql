select *
from {{ ref('stg_sales__orders') }}
where order_estimated_delivery_date is null
and order_status = 'delivered'