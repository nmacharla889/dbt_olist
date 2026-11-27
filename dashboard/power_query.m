// =====================================================================
// Olist Power BI: Power Query (M), v3
// 14 queries. In Power BI Desktop: Home > Transform data.
//   Existing query: select it > Home > Advanced Editor > select all > paste the new code > Done.
//   New query: Home > New Source > Blank Query > Advanced Editor > paste > Done > rename it to the NAME below.
// Query names must match exactly (lowercase, underscores). Measures and the model script depend on them.
//
// Every table lowercases its column names first (Text.Lower), so it does not matter whether Databricks
// returns item_price_in_BRL or item_price_in_brl. Columns are then picked by name: if dbt drops or
// renames a column, the refresh fails with a clear error instead of silently loading less data.
//
// Connection: Databricks connector, sign in with a personal access token. Never save a token in a file.
// =====================================================================


// ---------------------------------------------------------------------
// QUERY 1 / 14   NAME: ServerHostname   (parameter, unchanged)
// ---------------------------------------------------------------------
"dbc-8b1e00c5-2ddc.cloud.databricks.com" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]


// ---------------------------------------------------------------------
// QUERY 2 / 14   NAME: HttpPath   (parameter, unchanged)
// Same value as http_path in your dbt profiles.yml.
// ---------------------------------------------------------------------
"/sql/1.0/warehouses/a3a4e1283a4b50a3" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]


// ---------------------------------------------------------------------
// QUERY 3 / 14   NAME: Catalog   (parameter, unchanged)
// ---------------------------------------------------------------------
"workspace" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]


// ---------------------------------------------------------------------
// QUERY 4 / 14   NAME: SalesSchema   (parameter, unchanged)
// olist_dev_marts while you build. Switch to olist_prod_marts after prod is rebuilt with a full refresh.
// ---------------------------------------------------------------------
"olist_dev_marts" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]


// ---------------------------------------------------------------------
// QUERY 5 / 14   NAME: MktSchema   (parameter, unchanged)
// olist_dev_mkt_marts now, olist_prod_mkt_marts later.
// ---------------------------------------------------------------------
"olist_dev_mkt_marts" meta [IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]


// ---------------------------------------------------------------------
// QUERY 6 / 14   NAME: fnDatabricksTable   (function, CHANGED: lowercases column names)
// ---------------------------------------------------------------------
(schemaName as text, tableName as text) as table =>
let
    Source = Databricks.Catalogs(ServerHostname, HttpPath, [Catalog = null, Database = null, EnableAutomaticProxyDiscovery = null]),
    CatalogNav = Source{[Name = Catalog, Kind = "Database"]}[Data],
    SchemaNav = CatalogNav{[Name = schemaName, Kind = "Schema"]}[Data],
    TableNav = SchemaNav{[Name = tableName, Kind = "Table"]}[Data],
    Lowered = Table.TransformColumnNames(TableNav, Text.Lower)
in
    Lowered


// ---------------------------------------------------------------------
// QUERY 7 / 14   NAME: dim_date   (CHANGED: adds month_start and is_full_data_month)
// ---------------------------------------------------------------------
let
    Source = fnDatabricksTable(SalesSchema, "dim_date"),
    Kept = Table.SelectColumns(Source, {
        "date_day", "date_key", "year", "month_num", "month_name", "month_short",
        "quarter_num", "quarter_label", "day_of_week", "day_name", "day_short",
        "day_of_month", "day_of_year", "week_of_year", "is_weekend", "year_month",
        "month_start", "is_full_data_month"
    }),
    Typed = Table.TransformColumnTypes(Kept, {
        {"date_day", type date}, {"date_key", Int64.Type}, {"year", Int64.Type},
        {"month_num", Int64.Type}, {"month_name", type text}, {"month_short", type text},
        {"quarter_num", Int64.Type}, {"quarter_label", type text},
        {"day_of_week", Int64.Type}, {"day_name", type text}, {"day_short", type text},
        {"day_of_month", Int64.Type}, {"day_of_year", Int64.Type}, {"week_of_year", Int64.Type},
        {"is_weekend", type logical}, {"year_month", type text},
        {"month_start", type date}, {"is_full_data_month", type logical}
    })
in
    Typed


// ---------------------------------------------------------------------
// QUERY 8 / 14   NAME: dim_customers   (same columns; the data is fixed in dbt, no more 'unknown' states)
// ---------------------------------------------------------------------
let
    Source = fnDatabricksTable(SalesSchema, "dim_customers"),
    Kept = Table.SelectColumns(Source, {"customer_key", "zip_code", "customer_city", "customer_state"}),
    Typed = Table.TransformColumnTypes(Kept, {
        {"customer_key", type text}, {"zip_code", type text},
        {"customer_city", type text}, {"customer_state", type text}
    })
in
    Typed


// ---------------------------------------------------------------------
// QUERY 9 / 14   NAME: dim_products   (unchanged apart from the lowercase step in the function)
// ---------------------------------------------------------------------
let
    Source = fnDatabricksTable(SalesSchema, "dim_products"),
    Kept = Table.SelectColumns(Source, {
        "product_key", "product_id", "product_category_name", "product_category_name_english",
        "product_photos_qty", "product_weight"
    }),
    Typed = Table.TransformColumnTypes(Kept, {
        {"product_key", type text}, {"product_id", type text},
        {"product_category_name", type text}, {"product_category_name_english", type text},
        {"product_photos_qty", Int64.Type}, {"product_weight", Int64.Type}
    })
in
    Typed


// ---------------------------------------------------------------------
// QUERY 10 / 14   NAME: dim_sellers   (same columns)
// ---------------------------------------------------------------------
let
    Source = fnDatabricksTable(SalesSchema, "dim_sellers"),
    Kept = Table.SelectColumns(Source, {"seller_key", "seller_id", "zip_code", "seller_city", "seller_state"}),
    Typed = Table.TransformColumnTypes(Kept, {
        {"seller_key", type text}, {"seller_id", type text}, {"zip_code", type text},
        {"seller_city", type text}, {"seller_state", type text}
    })
in
    Typed


// ---------------------------------------------------------------------
// QUERY 11 / 14   NAME: fct_order_items   (CHANGED: sales only)
// Grain: one row per order item. Used for revenue, freight, items, products, sellers.
// Delivery and review columns were removed: they now come from fct_orders (one row per order),
// so nothing order-level is repeated per item. Payment columns stay out for the same reason.
// ---------------------------------------------------------------------
let
    Source = fnDatabricksTable(SalesSchema, "fct_order_items"),
    Kept = Table.SelectColumns(Source, {
        "product_key", "seller_key", "customer_key", "order_purchase_date_key",
        "order_id", "order_status", "order_purchase_date",
        "item_price_in_brl", "item_freight_value_in_brl", "total_item_value_in_brl"
    }),
    Typed = Table.TransformColumnTypes(Kept, {
        {"product_key", type text}, {"seller_key", type text}, {"customer_key", type text},
        {"order_purchase_date_key", Int64.Type},
        {"order_id", type text}, {"order_status", type text},
        {"order_purchase_date", type date},
        {"item_price_in_brl", Currency.Type}, {"item_freight_value_in_brl", Currency.Type},
        {"total_item_value_in_brl", Currency.Type}
    })
in
    Typed


// ---------------------------------------------------------------------
// QUERY 12 / 14   NAME: fct_orders   (NEW)
// Grain: one row per order, all 99,441 orders (including the 775 with no items).
// Used for order status, cancellation rate, delivery and reviews.
// ---------------------------------------------------------------------
let
    Source = fnDatabricksTable(SalesSchema, "fct_orders"),
    Kept = Table.SelectColumns(Source, {
        "order_id", "customer_key", "order_purchase_date_key", "order_status",
        "order_purchase_date", "order_delivered_customer_date", "order_estimated_delivery_date",
        "delivery_delay_days", "item_count", "order_items_value_in_brl", "order_freight_value_in_brl",
        "order_payment_value_in_brl", "review_score",
        "is_delivered", "is_on_time", "delivery_status", "delivery_delay_bucket", "delivery_delay_bucket_sort",
        "is_canceled_or_unavailable", "has_review"
    }),
    Typed = Table.TransformColumnTypes(Kept, {
        {"order_id", type text}, {"customer_key", type text}, {"order_purchase_date_key", Int64.Type},
        {"order_status", type text},
        {"order_purchase_date", type date}, {"order_delivered_customer_date", type date},
        {"order_estimated_delivery_date", type date},
        {"delivery_delay_days", Int64.Type}, {"item_count", Int64.Type},
        {"order_items_value_in_brl", Currency.Type}, {"order_freight_value_in_brl", Currency.Type},
        {"order_payment_value_in_brl", Currency.Type},
        {"review_score", type number},
        {"is_delivered", type logical}, {"is_on_time", type logical},
        {"delivery_status", type text}, {"delivery_delay_bucket", type text},
        {"delivery_delay_bucket_sort", Int64.Type},
        {"is_canceled_or_unavailable", type logical}, {"has_review", type logical}
    })
in
    Typed


// ---------------------------------------------------------------------
// QUERY 13 / 14   NAME: fct_marketing_funnel   (same columns; nulls are now 'unknown' / 'not closed' in dbt)
// ---------------------------------------------------------------------
let
    Source = fnDatabricksTable(MktSchema, "fct_marketing_funnel"),
    Kept = Table.SelectColumns(Source, {
        "seller_key", "deal_close_date_key", "lead_original_date_key",
        "mql_id", "lead_source", "lead_original_date",
        "business_segment", "lead_type", "lead_behaviour_profile", "business_type",
        "declared_monthly_revenue", "is_deal_closed", "days_to_close_deal"
    }),
    Typed = Table.TransformColumnTypes(Kept, {
        {"seller_key", type text},
        {"deal_close_date_key", Int64.Type}, {"lead_original_date_key", Int64.Type},
        {"mql_id", type text}, {"lead_source", type text},
        {"lead_original_date", type date},
        {"business_segment", type text}, {"lead_type", type text},
        {"lead_behaviour_profile", type text}, {"business_type", type text},
        {"declared_monthly_revenue", type number},
        {"is_deal_closed", type logical},
        {"days_to_close_deal", Int64.Type}
    })
in
    Typed


// ---------------------------------------------------------------------
// QUERY 14 / 14   NAME: _Measures   (unchanged)
// ---------------------------------------------------------------------
let
    Source = #table(type table [Measures = text], {})
in
    Source
