// Tabular Editor 2 script: Olist star schema model setup, v3.
// Safe to run more than once. It updates measures in place (same names), so visuals that already
// use a measure keep working.
//
// How to run: Power BI Desktop > External Tools ribbon > Tabular Editor.
// In Tabular Editor 2: Advanced Scripting tab > paste this whole file > Run (F5 or the green play button).
// Then File > Save (Ctrl+S) to push the changes back into Power BI Desktop.
//
// What it does:
//   1. Checks all 8 tables and every key column exist (stops with a clear message if not).
//   2. Deletes ALL relationships and creates the 8 correct ones (all active, many-to-one, single direction).
//      The old inactive deal_close_date relationship is gone on purpose: Deals by Close Date uses TREATAS.
//   3. Marks dim_date as the date table, sets sort orders, hides keys and helper columns,
//      and turns off default summing on numeric columns.
//   4. Creates or updates all 25 measures with format strings, folders and descriptions.

string[] needed = { "dim_date", "dim_customers", "dim_products", "dim_sellers", "fct_order_items", "fct_orders", "fct_marketing_funnel", "_Measures" };
foreach (var t in needed) {
    if (!Model.Tables.Contains(t)) { Error("Missing table: " + t + ". Load every query in power_query.m first (names must match exactly)."); return; }
}

// ---- 1. Column check ----
string[][] cols = {
    new[] { "fct_order_items", "product_key" }, new[] { "fct_order_items", "seller_key" },
    new[] { "fct_order_items", "customer_key" }, new[] { "fct_order_items", "order_purchase_date_key" },
    new[] { "fct_orders", "customer_key" }, new[] { "fct_orders", "order_purchase_date_key" },
    new[] { "fct_marketing_funnel", "seller_key" }, new[] { "fct_marketing_funnel", "lead_original_date_key" },
    new[] { "dim_products", "product_key" }, new[] { "dim_sellers", "seller_key" },
    new[] { "dim_customers", "customer_key" }, new[] { "dim_date", "date_key" },
    new[] { "dim_date", "date_day" }, new[] { "dim_date", "is_full_data_month" },
    new[] { "fct_orders", "delivery_delay_bucket" }, new[] { "fct_orders", "delivery_delay_bucket_sort" }
};
foreach (var c in cols) {
    if (!Model.Tables[c[0]].Columns.Contains(c[1])) { Error("Missing column " + c[0] + "[" + c[1] + "]. Re-paste that query from power_query.m and Close & Apply."); return; }
}

// ---- 2. Relationships ----
foreach (var r in Model.Relationships.ToList()) r.Delete();

Action<string, string, string, string> Rel = (fromTable, fromCol, toTable, toCol) => {
    var r = Model.AddRelationship();
    r.FromColumn = Model.Tables[fromTable].Columns[fromCol];
    r.ToColumn = Model.Tables[toTable].Columns[toCol];
    r.FromCardinality = RelationshipEndCardinality.Many;
    r.ToCardinality = RelationshipEndCardinality.One;
    r.CrossFilteringBehavior = CrossFilteringBehavior.OneDirection;
    r.IsActive = true;
};
Rel("fct_order_items", "product_key", "dim_products", "product_key");
Rel("fct_order_items", "seller_key", "dim_sellers", "seller_key");
Rel("fct_order_items", "customer_key", "dim_customers", "customer_key");
Rel("fct_order_items", "order_purchase_date_key", "dim_date", "date_key");
Rel("fct_orders", "customer_key", "dim_customers", "customer_key");
Rel("fct_orders", "order_purchase_date_key", "dim_date", "date_key");
Rel("fct_marketing_funnel", "seller_key", "dim_sellers", "seller_key");
Rel("fct_marketing_funnel", "lead_original_date_key", "dim_date", "date_key");

// ---- 3. Date table, sorting, hiding, summarization ----
var dd = Model.Tables["dim_date"];
dd.DataCategory = "Time";
dd.Columns["date_day"].IsKey = true;
dd.Columns["date_day"].FormatString = "dd mmm yyyy";
dd.Columns["month_start"].FormatString = "mmm yyyy";
dd.Columns["month_name"].SortByColumn = dd.Columns["month_num"];
dd.Columns["month_short"].SortByColumn = dd.Columns["month_num"];
dd.Columns["day_name"].SortByColumn = dd.Columns["day_of_week"];
dd.Columns["day_short"].SortByColumn = dd.Columns["day_of_week"];

var fo = Model.Tables["fct_orders"];
fo.Columns["delivery_delay_bucket"].SortByColumn = fo.Columns["delivery_delay_bucket_sort"];
fo.Columns["delivery_delay_bucket_sort"].IsHidden = true;

foreach (var t in Model.Tables.Where(x => x.Name != "_Measures")) {
    foreach (var c in t.Columns) {
        if (c.Name.EndsWith("_key")) c.IsHidden = true;
        // Nothing in this model should be summed by dragging a column: use the measures.
        c.SummarizeBy = AggregateFunction.None;
    }
}
Model.Tables["_Measures"].Columns[0].IsHidden = true;

// ---- 4. Measures ----
var mt = Model.Tables["_Measures"];
foreach (var old in new[] { @"Revenue PM", @"Revenue MoM %", @"Revenue YTD" }) {
    if (mt.Measures.Contains(old)) mt.Measures[old].Delete();
}
Action<string, string, string, string, string> Upsert = (name, expr, fmt, folder, desc) => {
    var m = mt.Measures.Contains(name) ? mt.Measures[name] : mt.AddMeasure(name);
    m.Expression = expr;
    m.FormatString = fmt;
    m.DisplayFolder = folder;
    m.Description = desc;
};

Upsert(@"Revenue", @"SUM ( fct_order_items[item_price_in_brl] )", @"""R$""#,0", @"Sales", @"Item price, all order statuses, excludes freight. Expect R$13,591,644 with no filters.");
Upsert(@"Freight", @"SUM ( fct_order_items[item_freight_value_in_brl] )", @"""R$""#,0", @"Sales", @"Freight charged on items. Expect R$2,251,910.");
Upsert(@"Items Sold", @"COUNTROWS ( fct_order_items )", @"#,0", @"Sales", @"One row per order item. Expect 112,650.");
Upsert(@"Orders", @"DISTINCTCOUNT ( fct_order_items[order_id] )", @"#,0", @"Sales", @"Orders with at least one item (98,666). Use Total Orders for every order including ones with no items.");
Upsert(@"Customers", @"DISTINCTCOUNT ( fct_order_items[customer_key] )", @"#,0", @"Sales", @"Distinct customers (customer_unique_id) who bought at least one item. Expect 95,420.");
Upsert(@"Avg Order Value", @"DIVIDE ( [Revenue], [Orders] )", @"""R$""#,0.00", @"Sales", @"Revenue per order. Expect R$137.75.");
Upsert(@"Items per Order", @"DIVIDE ( [Items Sold], [Orders] )", @"0.00", @"Sales", @"Average basket size. Expect 1.14.");
Upsert(@"Freight % of Revenue", @"DIVIDE ( [Freight], [Revenue] )", @"0.0%", @"Sales", @"Freight as a share of item revenue. Expect 16.6%.");
Upsert(@"Revenue Full Months", @"CALCULATE ( [Revenue], KEEPFILTERS ( dim_date[is_full_data_month] = TRUE () ) )", @"""R$""#,0", @"Growth", @"Revenue in months with complete data only (Jan 2017 to Aug 2018). Expect R$13,541,713 with no filters.");
Upsert(@"Revenue PY", @"VAR ShiftedDates =
    CALCULATETABLE (
        DATEADD ( dim_date[date_day], -1, YEAR ),
        dim_date[is_full_data_month] = TRUE ()
    )
RETURN
    IF (
        HASONEVALUE ( dim_date[year] ),
        CALCULATE ( [Revenue], ShiftedDates, dim_date[is_full_data_month] = TRUE () )
    )", @"""R$""#,0", @"Growth", @"Revenue for the same dates one year earlier, only where both years have complete data. Blank when no single year is in context, and blank for 2017 (2016 is incomplete). For 2018 it returns Jan-Aug 2017: R$3,113,000.");
Upsert(@"Revenue YoY %", @"VAR PY = [Revenue PY]
RETURN
    IF ( NOT ISBLANK ( PY ), DIVIDE ( [Revenue Full Months] - PY, PY ) )", @"0.0%", @"Growth", @"Like-for-like growth: complete months this year against the same months last year. For 2018: Jan-Aug 2018 vs Jan-Aug 2017 = +137.3%.");
Upsert(@"Total Orders", @"COUNTROWS ( fct_orders )", @"#,0", @"Delivery", @"Every order, any status, with or without items. Expect 99,441.");
Upsert(@"Delivered Orders", @"CALCULATE ( COUNTROWS ( fct_orders ), fct_orders[is_delivered] = TRUE () )", @"#,0", @"Delivery", @"Orders with a customer delivery date. Expect 96,476.");
Upsert(@"On Time Orders", @"CALCULATE ( COUNTROWS ( fct_orders ), fct_orders[is_on_time] = TRUE () )", @"#,0", @"Delivery", @"Delivered on or before the promised date. is_on_time is blank for undelivered orders, so they can never count as on time. Expect 89,941.");
Upsert(@"On Time %", @"DIVIDE ( [On Time Orders], [Delivered Orders] )", @"0.0%", @"Delivery", @"On-time share of delivered orders. Expect 93.2%.");
Upsert(@"Late Orders %", @"DIVIDE ( [Delivered Orders] - [On Time Orders], [Delivered Orders] )", @"0.0%", @"Delivery", @"Late share of delivered orders. Expect 6.8%.");
Upsert(@"Avg Delivery Delay Days", @"AVERAGE ( fct_orders[delivery_delay_days] )", @"0.0", @"Delivery", @"Delivered date minus promised date, averaged per order. Negative = early. Expect -11.9.");
Upsert(@"Avg Review Score (per order)", @"AVERAGE ( fct_orders[review_score] )", @"0.00", @"Delivery", @"Average review score, one value per order. Orders with no review are left out. Expect 4.09.");
Upsert(@"Reviewed Orders", @"CALCULATE ( COUNTROWS ( fct_orders ), fct_orders[has_review] = TRUE () )", @"#,0", @"Delivery", @"Orders with at least one review. Expect 98,673.");
Upsert(@"Cancelled or Unavailable Orders %", @"DIVIDE (
    CALCULATE ( COUNTROWS ( fct_orders ), fct_orders[is_canceled_or_unavailable] = TRUE () ),
    [Total Orders]
)", @"0.00%", @"Delivery", @"Share of all orders with status canceled or unavailable. Expect 1.24% (1,234 of 99,441).");
Upsert(@"Leads", @"COUNTROWS ( fct_marketing_funnel )", @"#,0", @"Marketing", @"Marketing qualified leads, dated by first contact. Expect 8,000.");
Upsert(@"Closed Deals", @"CALCULATE ( COUNTROWS ( fct_marketing_funnel ), fct_marketing_funnel[is_deal_closed] = TRUE () )", @"#,0", @"Marketing", @"Leads that became a deal, dated by the lead's first contact. Expect 842.");
Upsert(@"Lead to Deal %", @"DIVIDE ( [Closed Deals], [Leads] )", @"0.0%", @"Marketing", @"Conversion rate. Expect 10.5%.");
Upsert(@"Avg Days to Close", @"AVERAGE ( fct_marketing_funnel[days_to_close_deal] )", @"0.0", @"Marketing", @"First contact to deal close, closed deals only. Expect 48.4.");
Upsert(@"Deals by Close Date", @"CALCULATE (
    COUNTROWS ( fct_marketing_funnel ),
    TREATAS ( VALUES ( dim_date[date_key] ), fct_marketing_funnel[deal_close_date_key] ),
    REMOVEFILTERS ( dim_date )
)", @"#,0", @"Marketing", @"Closed deals counted in the month they closed. Uses TREATAS instead of an inactive relationship, so the 7,158 leads with no close date never add a (Blank) item to the date slicers. Expect 842 with no filters.");

Info("Done: 8 relationships, date table, sort orders, hidden keys, " + mt.Measures.Count + " measures. Now press Ctrl+S to save back to Power BI.");
