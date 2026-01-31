import time

from pipeline.common.run_log import start_run, finish_run
from pipeline.common.metrics import count_rows
from pipeline.sql.sql_runner import run_sql_file

MART_FILES = [
    "sql/marts/fact_orders.sql",
    "sql/marts/fact_order_items.sql",
    "sql/marts/dim_products.sql",
]

def main():
    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    try:
        for f in MART_FILES:
            run_sql_file(f)

        marts_total = (
            count_rows("marts.fact_orders")
            + count_rows("marts.fact_order_items")
            + count_rows("marts.dim_products")
        )

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
            rows_loaded_marts=marts_total,
        )

        print("marts built successfully. run_id:", run_id, "rows_loaded_marts:", marts_total)

    except Exception as e:
        finish_run(
            run_id=run_id,
            status="FAILED",
            started_monotonic=t0,
            error_message=str(e)[:4000],
        )
        raise

if __name__ == "__main__":
    main()