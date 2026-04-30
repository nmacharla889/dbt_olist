select *
from {{ ref('stg_sales__order_items') }}
where item_price_in_BRL <= 0