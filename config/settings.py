from datetime import date

# ===== Pipeline identity =====
PIPELINE_NAME = "cloud_kpi_instacart_ingestion"

# ===== Run date =====
RUN_DATE = date.today().isoformat()

# ===== AWS / S3 =====
AWS_REGION = "us-west-2"
S3_BUCKET = "cloud-kpi-instacart-hp"

def s3_raw_prefix(table: str) -> str:
    return f"raw/{table}/load_date={RUN_DATE}/"

# ===== Tables we ingest =====
SMALL_TABLES = [
    "aisles",
    "departments",
    "products",
    "orders",
]

LARGE_TABLES = [
    "order_products",
]

# ===== Postgres =====
PG_HOST = "cloud-kpi-instacart.c5cewmeskkbj.us-west-2.rds.amazonaws.com"
PG_PORT = 5432
PG_DB = "analytics"
PG_USER = "postgres"

RAW_SCHEMA = "raw"
OPS_SCHEMA = "ops"