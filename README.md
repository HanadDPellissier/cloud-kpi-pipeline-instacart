# Cloud-Backed KPI Pipeline (Instacart) — S3 + Postgres (RDS) + SQL-first + Observability

## Problem
Manual KPI reporting is fragile: ad-hoc queries, inconsistent definitions, no lineage, and no operational visibility.
This project builds a production-style batch analytics pipeline that lands raw data in cloud storage, loads curated
tables into Postgres, enforces data quality gates, and produces analytics-ready KPI tables with full run-level
observability for fast debugging and defensible metrics.

## Stack
- **Cloud Storage:** AWS S3
- **Warehouse:** AWS RDS Postgres
- **Transforms:** SQL-first (raw → staging → marts → KPIs)
- **Language:** Python (boto3, psycopg)
- **Orchestration:** Prefect (planned)

## Dataset
Instacart Market Basket Analysis (Kaggle)

**Scale**
- ~3.4M orders
- ~32.4M order-product rows
- ~50k products

Chosen to intentionally surface real ingestion, performance, and data-quality challenges.

## Architecture (Current)
- S3 raw landing layer (partitioned by `load_date`)
- Postgres raw schema (ingested, replayable)
- Automated data quality gates (fail loudly)
- SQL-based staging layer (typed / cleaned)
- Analytics marts (facts + dimensions)
- KPI tables for stakeholder consumption
- Operational observability tables

## S3 Layout
```text
s3://cloud-kpi-instacart-hp/

raw/
├── orders/
│   └── load_date=YYYY-MM-DD/
│       └── orders.csv
├── order_products_prior/
│   └── load_date=YYYY-MM-DD/
│       └── order_products__prior.csv
├── products/
│   └── load_date=YYYY-MM-DD/
│       └── products.csv
├── aisles/
│   └── load_date=YYYY-MM-DD/
│       └── aisles.csv
└── departments/
    └── load_date=YYYY-MM-DD/
        └── departments.csv
```
## Postgres Schemas
- `raw.*` — ingested source data
- `stg.*` — typed, cleaned staging tables
- `marts.*` — analytics-ready fact and dimension tables
- `ops.*` — pipeline observability and data quality

## Raw Tables
- `raw.orders`
- `raw.order_products_prior`
- `raw.products`
- `raw.aisles`
- `raw.departments`

## Staging Tables
- `stg.stg_orders`
- `stg.stg_order_products_prior`
- `stg.stg_products`

## Marts (Analytics Layer)

### marts.fact_orders
**Grain:** 1 row per order (`order_id`)

Includes basket-level measures:
- `items_in_order`
- `reordered_items_in_order`
- `reorder_item_rate`
- `has_any_reorder`

Used for demand trends, basket size analysis, and reorder behavior.

### marts.fact_order_items
**Grain:** 1 row per (`order_id`, `product_id`)

Includes:
- `add_to_cart_order`
- `reordered`
- denormalized order context (`user_id`, `order_dow`, `order_hour_of_day`)

Used for product-level KPIs and reorder analysis.

### marts.dim_products
**Grain:** 1 row per product (`product_id`)

Includes:
- product name
- aisle and department enrichment

## KPI Tables (Consumer Outputs)

### marts.kpi_orders_by_dow
**Grain:** 1 row per day-of-week (`order_dow`)

Metrics:
- `total_orders`
- `avg_items_per_order`
- `pct_orders_with_any_reorder`
- `avg_reorder_item_rate`

### marts.kpi_products_reorder
**Grain:** 1 row per product

Metrics:
- `orders_with_product`
- `times_reordered`
- `product_reorder_rate`

Designed for stakeholder-friendly analysis and top-N product insights.

## Data Quality Gates
Automated checks executed after ingestion:
- Minimum row count thresholds
- Primary key NOT NULL validation
- Duplicate key detection
- Foreign key integrity checks
- Domain validation (categorical constraints)

Failures are logged and cause the pipeline to fail loudly.

## Observability

### ops.pipeline_runs
Tracks execution-level metrics:
- `run_id`
- `pipeline_name`
- `status`
- `rows_extracted`
- `rows_loaded_raw`
- `execution_seconds`
- timestamps

### ops.data_quality_results
Stores per-check results:
- `run_id`
- `check_name`
- `table_name`
- `status`
- `metric_value`
- `threshold`
- timestamp

## Performance (Measured)
Selected pipeline run times from `ops.pipeline_runs`:
- Order-products prior load (~32.4M rows): **432.561s**
- Staging build: **179.261s**
- Marts build (facts + dimension): **332.010s**
- KPI build: **88.904s**
- Data quality checks: **71.639s**

Note: `rows_loaded_marts` is currently logged as a placeholder and will be populated in a future enhancement.

## Current Capabilities
- Cloud-backed ingestion of large-scale datasets (30M+ rows)
- Replayable raw data lake
- SQL-first analytics engineering workflow
- Automated data quality enforcement
- End-to-end observability with run metrics
- Stakeholder-ready KPI tables

## Planned Enhancements
- Incremental loading with rolling lookback windows
- Prefect orchestration and scheduling
- KPI safety filters (minimum-volume thresholds)
- Query pack for common business questions
- Dashboard or BI integration