# Cloud-Backed KPI Pipeline (Instacart) — S3 + Postgres (RDS) + SQL-first + Observability

## Problem
Manual KPI reporting is fragile: ad-hoc queries, inconsistent definitions, no lineage, and no operational visibility.
This project implements a production-style batch analytics pipeline that ingests raw data into cloud storage,
loads curated tables into Postgres, enforces data quality gates, and produces analytics-ready KPI tables with
full run-level observability for fast debugging and defensible metrics.

The focus is on building reliable data foundations first, mirroring real analytics engineering and data
engineering systems rather than dashboard-first analytics.

---

## Stack
- **Cloud Storage:** AWS S3 (raw data lake)
- **Warehouse:** AWS RDS Postgres
- **Transforms:** SQL-first (raw → staging → marts → KPIs)
- **Language:** Python (boto3, psycopg)
- **Orchestration:** Custom job runner (Prefect-ready design)

---

## Dataset
Instacart Market Basket Analysis (Kaggle)

**Scale**
- ~3.4M orders  
- ~32.4M order-product rows  
- ~50k products  

Chosen intentionally to surface ingestion scale, performance constraints, and data-quality challenges that do
not appear in toy datasets.

---

## Architecture
- Immutable S3 raw landing layer (partitioned by `load_date`)
- Replayable Postgres raw schema
- Automated data quality gates (fail loudly)
- SQL-based staging layer (typed, cleaned)
- Analytics marts (facts + dimensions)
- KPI tables for stakeholder consumption
- Full operational observability (row counts, runtime, status)
- Single-command end-to-end execution

---

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

Raw data is immutable and fully replayable for backfills and recovery.

---

## Postgres Schemas
- `raw.*` — ingested source data
- `stg.*` — typed, cleaned staging tables
- `marts.*` — analytics-ready fact and dimension tables
- `ops.*` — pipeline observability and data quality logging

---

## Raw Tables
- `raw.orders`
- `raw.order_products_prior`
- `raw.products`
- `raw.aisles`
- `raw.departments`

---

## Staging Tables
- `stg.stg_orders`
- `stg.stg_order_products_prior`
- `stg.stg_products`

---

## Marts (Analytics Layer)

### marts.fact_orders
**Grain:** 1 row per order (`order_id`)

Basket-level metrics:
- `items_in_order`
- `reordered_items_in_order`
- `reorder_item_rate`
- `has_any_reorder`

Used for demand trends, basket-size analysis, and reorder behavior.

---

### marts.fact_order_items
**Grain:** 1 row per (`order_id`, `product_id`)

Includes:
- `add_to_cart_order`
- `reordered`
- denormalized order context (`user_id`, `order_dow`, `order_hour_of_day`)

Used for product-level KPIs and reorder analysis.

---

### marts.dim_products
**Grain:** 1 row per product (`product_id`)

Includes:
- product name
- aisle enrichment
- department enrichment

---

## KPI Tables (Consumer Outputs)

### marts.kpi_orders_by_dow
**Grain:** 1 row per day-of-week (`order_dow`)

Metrics:
- `total_orders`
- `avg_items_per_order`
- `pct_orders_with_any_reorder`
- `avg_reorder_item_rate`

---

### marts.kpi_products_reorder
**Grain:** 1 row per product

Metrics:
- `orders_with_product`
- `times_reordered`
- `product_reorder_rate`

Designed for stakeholder-friendly analysis and top-N product insights.

---

## Data Quality Gates
Automated checks executed after ingestion and before downstream publishing:
- Minimum row-count thresholds
- Primary key NOT NULL validation
- Duplicate key detection
- Foreign key integrity checks
- Domain validation (categorical constraints)

Failures are logged and cause the pipeline to fail loudly, preventing bad data
from reaching marts and KPIs.

---

## Observability

### ops.pipeline_runs
Execution-level logging per job and per pipeline run:
- `run_id`
- `pipeline_name`
- `status`
- `rows_extracted`
- `rows_loaded_raw`
- `rows_loaded_staging`
- `rows_loaded_marts`
- `execution_seconds`
- timestamps
- error messages

---

### ops.data_quality_results
Per-check audit logging:
- `run_id`
- `check_name`
- `table_name`
- `status`
- `metric_value`
- `threshold`
- timestamp

---

## End-to-End Execution
```bash
export RUN_DATE=YYYY-MM-DD
python -m jobs.run_all
```

The `run_all` job orchestrates:
1. Dimension loads
2. Orders load
3. Order-products prior load
4. Data quality checks (hard gate)
5. Staging builds
6. Marts builds
7. KPI builds

---

## Performance (Measured)
Representative production-scale runs:
- Order-products prior load (~32.4M rows): ~430s
- Staging build: ~210s
- Marts build (facts + dimensions): ~400s
- KPI build: ~130s
- Data quality checks: ~70s

End-to-end pipeline runtime: ~25 minutes.

---

## Current Capabilities
- Cloud-backed ingestion of large-scale datasets (35M+ rows)
- Replayable raw data lake
- SQL-first analytics engineering workflow
- Automated data quality enforcement
- End-to-end observability with run metrics
- Stakeholder-ready KPI tables

---

## Planned Enhancements
- Incremental loading with rolling lookback windows
- Prefect orchestration and scheduling
- KPI safety filters (minimum-volume thresholds)
- Query pack for common business questions
- BI or dashboard integration

---

## Summary
This project demonstrates production-grade analytics engineering:
cloud-native ingestion, large-scale processing, SQL-first modeling,
data quality enforcement, and operational observability — designed to
mirror real-world data platforms rather than toy examples.