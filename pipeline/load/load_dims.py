# pipeline/load/load_dims.py

from pipeline.common.ddl import (
    ensure_raw_aisles_table,
    ensure_raw_departments_table,
    ensure_raw_products_table,
)
from pipeline.load.load_from_s3 import copy_csv_from_s3_to_table


def load_dims(run_date: str) -> int:
    rows_total = 0

    ensure_raw_aisles_table()
    rows_total += copy_csv_from_s3_to_table(
        s3_key=f"raw/aisles/load_date={run_date}/aisles.csv",
        truncate_sql="TRUNCATE TABLE raw.aisles;",
        copy_sql="COPY raw.aisles (aisle_id, aisle) FROM STDIN WITH (FORMAT csv, HEADER true)",
        count_sql="SELECT COUNT(*) FROM raw.aisles;",
    )

    ensure_raw_departments_table()
    rows_total += copy_csv_from_s3_to_table(
        s3_key=f"raw/departments/load_date={run_date}/departments.csv",
        truncate_sql="TRUNCATE TABLE raw.departments;",
        copy_sql="COPY raw.departments (department_id, department) FROM STDIN WITH (FORMAT csv, HEADER true)",
        count_sql="SELECT COUNT(*) FROM raw.departments;",
    )

    ensure_raw_products_table()
    rows_total += copy_csv_from_s3_to_table(
        s3_key=f"raw/products/load_date={run_date}/products.csv",
        truncate_sql="TRUNCATE TABLE raw.products;",
        copy_sql="COPY raw.products (product_id, product_name, aisle_id, department_id) FROM STDIN WITH (FORMAT csv, HEADER true)",
        count_sql="SELECT COUNT(*) FROM raw.products;",
    )

    return rows_total