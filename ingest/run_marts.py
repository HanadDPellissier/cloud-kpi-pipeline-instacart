import time

from ingest.run_log import start_run, finish_run
from ingest.sql_runner import run_sql_file

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

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
            rows_loaded_marts=0,  # we’ll populate real counts in KPI phase
        )

        print("marts built successfully. run_id:", run_id)

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