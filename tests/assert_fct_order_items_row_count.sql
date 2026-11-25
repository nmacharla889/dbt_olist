-- fct_order_items must hold exactly one row per order item that has an order.
-- A mismatch means rows were lost or duplicated, for example by an incremental filter that
-- skips late-arriving rows. Compare counts, because no_nulls and relationship tests cannot see missing rows.
with expected as (
    select count(*) as expected_rows
    from {{ ref('stg_sales__order_items') }} oi
    inner join {{ ref('stg_sales__orders') }} o
        on oi.order_id = o.order_id
),

actual as (
    select count(*) as actual_rows
    from {{ ref('fct_order_items') }}
)

select expected_rows, actual_rows
from expected
cross join actual
where expected_rows <> actual_rows
