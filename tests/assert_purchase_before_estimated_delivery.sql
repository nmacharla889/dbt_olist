select * 
from  {{ ref('stg_sales__orders') }}
where order_purchase_date >= order_estimated_delivery_date