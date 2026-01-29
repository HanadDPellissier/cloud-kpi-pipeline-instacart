import time

from pipeline.run_log import start_run, finish_run
from pipeline.sql.sql_runner import run_sql_file

STAGING_FILES = [
    "sql/staging/stg_orders.sql",
    "sql/staging/stg_products.sql",
    "sql/staging/stg_order_products_prior.sql",
]

def main():
    t0 = time.monotonic()
    run_id = start_run(lookback_days=0)

    try:
        for f in STAGING_FILES:
            run_sql_file(f)

        finish_run(
            run_id=run_id,
            status="SUCCESS",
            started_monotonic=t0,
            rows_loaded_staging=0,  # we’ll fill real counts next step
        )
        print("staging built. run_id:", run_id)

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