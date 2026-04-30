select * 
from {{ ref('int_orders_enriched')}} 
where total_item_value_in_BRL != (item_price_in_BRL + item_freight_value_in_BRL)