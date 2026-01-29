import time

from pipeline.run_log import start_run, finish_run
from pipeline.ddl import (
    ensure_raw_aisles_table,
    ensure_raw_departments_table,
    ensure_raw_products_table,
)
from pipeline.load.load_from_s3 import copy_csv_from_s3_to_table

RUN_DATE = "2026-01-27"  # keep explicit for now; we’ll wire RUN_DATE from settings next

def main():
    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    rows_total = 0
    try:
        ensure_raw_aisles_table()
        n = copy_csv_from_s3_to_table(
            s3_key=f"raw/aisles/load_date={RUN_DATE}/aisles.csv",
            truncate_sql="TRUNCATE TABLE raw.aisles;",
            copy_sql="COPY raw.aisles (aisle_id, aisle) FROM STDIN WITH (FORMAT csv, HEADER true)",
            count_sql="SELECT COUNT(*) FROM raw.aisles;",
        )
        rows_total += n

        ensure_raw_departments_table()
        n = copy_csv_from_s3_to_table(
            s3_key=f"raw/departments/load_date={RUN_DATE}/departments.csv",
            truncate_sql="TRUNCATE TABLE raw.departments;",
            copy_sql="COPY raw.departments (department_id, department) FROM STDIN WITH (FORMAT csv, HEADER true)",
            count_sql="SELECT COUNT(*) FROM raw.departments;",
        )
        rows_total += n

        ensure_raw_products_table()
        n = copy_csv_from_s3_to_table(
            s3_key=f"raw/products/load_date={RUN_DATE}/products.csv",
            truncate_sql="TRUNCATE TABLE raw.products;",
            copy_sql="COPY raw.products (product_id, product_name, aisle_id, department_id) FROM STDIN WITH (FORMAT csv, HEADER true)",
            count_sql="SELECT COUNT(*) FROM raw.products;",
        )
        rows_total += n

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
            rows_extracted=rows_total,
            rows_loaded_raw=rows_total,
        )
        print("dims loaded. total_rows:", rows_total, "run_id:", run_id)

    except Exception as e:
        finish_run(
            run_id=run_id,
            status="FAILED",
            started_monotonic=t0,
            error_message=str(e),
        )
        raise

if __name__ == "__main__":
    main()