-- Olist dashboard reconciliation, v3. Run in the Databricks SQL editor AFTER the dbt full refresh.
-- Dev schemas below. For prod, replace olist_dev_ with olist_prod_.
-- "EXPECT" values were calculated from the seed CSVs with the fixed dbt logic. If a result differs,
-- stop and find out why before you touch Power BI. The Power BI cards (no filters) must match too.

-- 1. Sales totals. Cards: Revenue, Freight, Items Sold, Orders
-- EXPECT items_sold 112650 | orders 98666 | revenue 13591643.70 | freight 2251909.54
select count(*) as items_sold,
       count(distinct order_id) as orders,
       round(sum(item_price_in_brl), 2) as revenue,
       round(sum(item_freight_value_in_brl), 2) as freight
from workspace.olist_dev_marts.fct_order_items;

-- 2. Customers. Card: Customers
-- EXPECT 95420
select count(distinct customer_key) as customers
from workspace.olist_dev_marts.fct_order_items;

-- 3. Order grain. Cards: Total Orders, Delivered Orders, On Time Orders, On Time %, Avg Delivery Delay Days
-- EXPECT total_orders 99441 | delivered 96476 | on_time 89941 | on_time_pct 0.9323 | avg_delay -11.88
-- (Before the fix this was 89687 / 0.9296: the UTC conversion pushed evening deliveries to the next day.)
select count(*) as total_orders,
       sum(case when is_delivered then 1 else 0 end) as delivered_orders,
       sum(case when is_on_time then 1 else 0 end) as on_time_orders,
       round(sum(case when is_on_time then 1 else 0 end) / sum(case when is_delivered then 1 else 0 end), 4) as on_time_pct,
       round(avg(delivery_delay_days), 2) as avg_delay_days
from workspace.olist_dev_marts.fct_orders;

-- 4. Reviews. Cards: Avg Review Score (per order), Reviewed Orders
-- EXPECT avg_review 4.0868 | reviewed_orders 98673  (was 4.1069 / 98123 before the review dedupe fix)
select round(avg(review_score), 4) as avg_review_per_order,
       sum(case when has_review then 1 else 0 end) as reviewed_orders
from workspace.olist_dev_marts.fct_orders;

-- 5. Cancellation. Card: Cancelled or Unavailable Orders %
-- EXPECT 1234 | 0.0124   (the old dashboard showed 0.47% because orders with no items were missing)
select sum(case when is_canceled_or_unavailable then 1 else 0 end) as canceled_or_unavailable,
       round(avg(case when is_canceled_or_unavailable then 1.0 else 0.0 end), 4) as pct
from workspace.olist_dev_marts.fct_orders;

-- 6. Order status counts (all orders)
-- EXPECT delivered 96478, shipped 1107, canceled 625, unavailable 609, invoiced 314, processing 301, created 5, approved 2
select order_status, count(*) as orders
from workspace.olist_dev_marts.fct_orders
group by order_status
order by orders desc;

-- 7. Lateness vs reviews. Chart: Avg Review Score by delivery_delay_bucket
-- EXPECT  15+ days early 4.32 | 8-14 days early 4.31 | 1-7 days early 4.20 | On the day 4.04
--         1-3 days late 3.29 | 4-7 days late 2.11 | 8+ days late 1.70 | Not delivered 1.75
select delivery_delay_bucket, delivery_delay_bucket_sort,
       count(*) as orders,
       round(avg(review_score), 2) as avg_review
from workspace.olist_dev_marts.fct_orders
group by 1, 2
order by 2;

-- 8. Marketing funnel. Cards: Leads, Closed Deals, Lead to Deal %, Avg Days to Close
-- EXPECT leads 8000 | closed 842 | pct 0.1053 | avg_days 48.44
select count(*) as leads,
       sum(case when is_deal_closed then 1 else 0 end) as closed_deals,
       round(sum(case when is_deal_closed then 1 else 0 end) / count(*), 4) as lead_to_deal_pct,
       round(avg(days_to_close_deal), 2) as avg_days_to_close
from workspace.olist_dev_mkt_marts.fct_marketing_funnel;

-- 9. Lead sources. Chart: Leads and Closed Deals by lead_source. No null row any more.
-- EXPECT organic_search 2296/271, paid_search 1586/195, social 1350/75, unknown 1159/193, direct_traffic 499/56,
--        email 493/15, referral 284/24, other 150/4, display 118/6, other_publicities 65/3
select lead_source, count(*) as leads, sum(case when is_deal_closed then 1 else 0 end) as closed_deals
from workspace.olist_dev_mkt_marts.fct_marketing_funnel
group by lead_source
order by leads desc;

-- 10. Blank checks. EVERY count must be 0.
select 'customers with unknown state' as check_name, count(*) as n
from workspace.olist_dev_marts.dim_customers where customer_state = 'unknown'
union all
select 'sellers with unknown state', count(*)
from workspace.olist_dev_marts.dim_sellers where seller_state = 'unknown'
union all
select 'items with null product_key', count(*)
from workspace.olist_dev_marts.fct_order_items where product_key is null
union all
select 'items with null date key', count(*)
from workspace.olist_dev_marts.fct_order_items where order_purchase_date_key is null
union all
select 'orders with null customer_key', count(*)
from workspace.olist_dev_marts.fct_orders where customer_key is null
union all
select 'orders with null date key', count(*)
from workspace.olist_dev_marts.fct_orders where order_purchase_date_key is null
union all
select 'leads with null lead_source', count(*)
from workspace.olist_dev_mkt_marts.fct_marketing_funnel where lead_source is null
union all
select 'leads with null lead date key', count(*)
from workspace.olist_dev_mkt_marts.fct_marketing_funnel where lead_original_date_key is null
union all
select 'leads with null business_segment', count(*)
from workspace.olist_dev_mkt_marts.fct_marketing_funnel where business_segment is null;

-- 11. Revenue by year and like-for-like growth. Cards: Revenue YoY % (year = 2018)
-- EXPECT by year: 2016 49785.92 | 2017 6155806.98 | 2018 7386050.80
-- (2017 and 2018 moved slightly from 6155449.08 / 7386408.70 because purchase dates are no longer shifted to UTC)
select year(order_purchase_date) as year, round(sum(item_price_in_brl), 2) as revenue
from workspace.olist_dev_marts.fct_order_items
group by 1 order by 1;

-- EXPECT jan_aug_2017 3113000.32 | jan_aug_2018 7385905.80 | yoy 1.3726 (+137.3%)
select round(sum(case when year(order_purchase_date) = 2017 then item_price_in_brl end), 2) as jan_aug_2017,
       round(sum(case when year(order_purchase_date) = 2018 then item_price_in_brl end), 2) as jan_aug_2018,
       round(sum(case when year(order_purchase_date) = 2018 then item_price_in_brl end)
           / sum(case when year(order_purchase_date) = 2017 then item_price_in_brl end) - 1, 4) as yoy
from workspace.olist_dev_marts.fct_order_items
where month(order_purchase_date) <= 8;

-- 12. Top 10 categories. Chart: Revenue by product_category_name_english
-- EXPECT health_beauty 1258681.34, watches_gifts 1205005.68, bed_bath_table 1036988.68, sports_leisure 988048.97,
--        computers_accessories 911954.32, furniture_decor 729762.49, cool_stuff 635290.85, housewares 632248.66,
--        auto 592720.11, garden_tools 485256.46
select product_category_name_english, round(sum(f.item_price_in_brl), 2) as revenue
from workspace.olist_dev_marts.fct_order_items f
join workspace.olist_dev_marts.dim_products p on p.product_key = f.product_key
group by product_category_name_english
order by revenue desc
limit 10;
