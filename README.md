# dbt-Olist

End-to-end dbt pipeline built on Databricks using the Brazilian [Olist e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Covers raw ingestion through mart layer with incremental models, SCD Type 2 snapshots, singular tests, and a GitHub Actions CI pipeline running against Databricks.

---

## Architecture

![DAG](docs/dag_screenshot.png)

```
Seeds (CSV)
    └── Staging (stg_*)          ← type casting, column cleanup, deduplication
         └── Intermediate         ← int_orders_enriched (order + customer + payment joined)
              └── Marts
                   ├── fct_order_items      (incremental, Delta)
                   └── fct_marketing_funnel (incremental, Delta)
Snapshots
    └── snapshot_sellers          (SCD Type 2 — check strategy)
```

---

## What This Demonstrates

- **Incremental models** — `fct_order_items` uses a Jinja macro (`incremental_filter`) for the lookback window; `fct_marketing_funnel` uses an inline CTE watermark on `lead_original_date`
- **SCD Type 2 snapshot** — `snapshot_sellers` tracks changes to `seller_city`, `seller_state`, `seller_zip_code_prefix` using dbt's check strategy
- **Test coverage** — `not_null`, `unique`, `accepted_values`, relationship tests, and 6 singular tests
- **GitHub Actions CI** — `dbt build` runs on schedule against Databricks prod (`olist_prod_*` schemas); seeds only reload when CSVs change via git diff check
- **Schema separation** — dev and prod schemas are fully isolated via `generate_schema_name` macro
- **dbt_utils** — `dbt_utils.star()` used in mart models

---

## Tech Stack

| Tool | Purpose |
|---|---|
| dbt-databricks | Transformation layer |
| Databricks | Compute + Delta Lake storage |
| Delta Lake | Incremental materializations |
| GitHub Actions | CI — automated dbt build on schedule |
| Python / venv | Local development |

---

## Schema Structure

| Schema | Contents |
|---|---|
| `olist_dev_raw` | Sales seeds (orders, customers, products, etc.) |
| `olist_dev_mkt_raw` | Marketing seeds (MQL, closed deals) |
| `olist_dev_stg` | Sales staging models |
| `olist_dev_mkt_stg` | Marketing staging models |
| `olist_dev_marts` | Sales marts + intermediate |
| `olist_dev_mkt_marts` | Marketing marts |

Prod schemas follow the same pattern with `olist_prod_*` prefix.

---

## How to Run

**Prerequisites:** Python venv, dbt-databricks installed, Databricks workspace access, `profiles.yml` in project root.

```bash
# Activate venv
C:\Projects\OLIST\dbt-env\Scripts\Activate.ps1

# Navigate to project
cd C:\Projects\OLIST\olist

# Run full build (dev)
dbt build --profiles-dir .

# Run full refresh on incremental models
dbt build --profiles-dir . --full-refresh

# Run specific model
dbt run --profiles-dir . --select fct_order_items

# Generate docs
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

---

## Key Decisions

- **Geolocation CSV trimmed** — lat/long columns dropped, only zip code, city, and state retained. The full dataset is 2M+ rows with coordinates unused in any mart model — keeping them added size with no analytical value.
- **Orphan products set to `severity: warn`** — 923 `product_id` values in `order_items` have no match in the products table. These are genuine data quality issues in the source, not a pipeline bug. A hard failure would block CI on data we cannot fix.
- **Three orders with no payment records** — present in the source data. Excluded from `fct_order_items` via inner join rather than surfacing nulls in the mart.
- **Duplicate `review_id` values** — deduplicated in staging using `ROW_NUMBER()` partitioned by `review_id`, keeping the most recent record.
- **`incremental_filter` macro** — used in `fct_order_items` to keep the lookback window logic reusable. `fct_marketing_funnel` uses an inline CTE instead because the watermark column (`lead_original_date`) is specific to that model.

---

## CI Pipeline

GitHub Actions runs `dbt build` on a schedule against `olist_prod_*` schemas on Databricks.

- Seeds only reload when the corresponding CSV file has changed (`git diff` check on the seeds directory)
- Current status: `PASS=121 WARN=1 ERROR=0`
- The single warning is the orphan `product_id` relationship test (intentional — see Key Decisions above)

---

## Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k orders from 2016–2018 across Brazilian marketplaces. Includes orders, customers, products, sellers, payments, reviews, and a separate marketing funnel dataset (MQL + closed deals).
