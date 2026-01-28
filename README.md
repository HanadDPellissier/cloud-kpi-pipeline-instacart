# Cloud-Backed KPI Pipeline (Instacart) — S3 + Postgres (RDS) + SQL-first + Observability

## Problem
Manual KPI reporting is fragile: ad-hoc queries, inconsistent definitions, no lineage, and no operational visibility.
This project builds a production-style batch analytics pipeline that lands raw data in cloud storage, loads curated
tables into Postgres, enforces data quality gates, and logs run-level observability for fast debugging and defensible metrics.

## Stack
- **Cloud Storage:** AWS S3
- **Warehouse:** AWS RDS Postgres
- **Transforms:** SQL-first (raw → staging → marts)
- **Language:** Python (boto3, psycopg)
- **Orchestration:** Prefect (planned)

## Dataset
Instacart Market Basket Analysis (Kaggle)

Scale:
- ~3.4M orders
- ~32.4M order-product rows

## Architecture (Current)
- S3 raw layer (partitioned by load_date)
- Postgres raw schema (ingested)
- Data quality gates (fail loudly)
- Postgres staging schema (typed / cleaned)
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
- raw.*
- stg.*
- ops.pipeline_runs
- ops.data_quality_results

## Raw Tables
- raw.orders
- raw.order_products_prior
- raw.products
- raw.aisles
- raw.departments

## Staging Tables
- stg.stg_orders
- stg.stg_order_products_prior
- stg.stg_products

## Observability
### ops.pipeline_runs
Tracks pipeline execution metrics per run:
- run_id
- pipeline_name
- status
- rows_extracted
- rows_loaded_raw
- execution_seconds
- started_at

### ops.data_quality_results
Stores results of automated data quality checks:
- run_id
- check_name
- table_name
- status
- metric_value
- threshold
- created_at

## Data Quality Gates
- Minimum row count thresholds
- Primary key NOT NULL validation
- Duplicate key detection
- Foreign key integrity checks
- Domain validation (categorical constraints)

## Current Capabilities
- Large-scale ingestion (30M+ rows)
- Cloud-backed raw storage
- SQL-based staging layer
- Automated data quality enforcement
- Run-level logging and observability

## Planned Enhancements
- Analytics marts
- Incremental loading with lookback windows
- Prefect orchestration
- KPI aggregation and dashboards